//! CDC slot consumption: WAL decoding, mapping/template rendering, and
//! QOS >= 1 persistence, shared by both process topologies (see
//! `crate::license::Feature::MultiProcess`):
//!
//! - **Community (single process):** `server::run_standalone` calls
//!   [`cdc_tick_core`] inline, in the same tick as socket I/O, with
//!   [`CdcQueueMode::DeliverAll`] — every rendered message goes to the sink
//!   for direct in-process delivery, exactly the original combined-worker
//!   behavior. No shared memory, no outbox rows.
//! - **Enterprise (two processes):** the dedicated `pgmqtt_cdc` worker
//!   (`run_cdc`) calls [`cdc_tick_core`] with [`CdcQueueMode::OutboxQos1`].
//!   Persisted messages (QOS >= 1, plus any QOS 0 message too large for the
//!   shared-memory ring) are queued to `pgmqtt_cdc_outbox` **inside the
//!   same transaction that advances the replication slot**, making the
//!   cross-process handoff exactly as durable as the messages themselves:
//!   no fixed-capacity buffer to overflow, nothing lost on a crash, and
//!   insertion order (= WAL order) preserved by the serial ids. Only small
//!   QOS 0 messages — fire-and-forget, never persisted — reach the sink,
//!   which pushes them through `crate::shmem_bridge`'s inline ring.
//!
//! The WAL-drain/persist/atomicity logic in `cdc_tick_core` is identical
//! either way; only where a finished message goes differs.
//!
//! In the enterprise topology, this worker has no visibility into
//! `crate::subscriptions` (that state lives in the other process), so it can
//! no longer skip persisting a message just because nobody is subscribed
//! yet. `deliver_messages` in `server::mod` reclaims any QOS >= 1 row that
//! turns out to have zero subscribers at delivery time, which preserves the
//! no-orphan-rows invariant regardless of topology.

use super::{db_action, with_subtransaction, MqttMessage};
use crate::ring_buffer;
use crate::topic_map;
use pgrx::bgworkers::BackgroundWorker;
use pgrx::datum::DatumWithOid;
use pgrx::log;
use pgrx::spi::{self, Spi};

/// Maximum number of WAL events to consume per `cdc_tick` batch transaction.
///
/// Each batch is one atomic PostgreSQL transaction: the replication slot LSN
/// only advances when **all** QOS ≥ 1 messages from that batch have been
/// durably inserted into `pgmqtt_messages`. Smaller values reduce the retry
/// cost if a batch fails; larger values reduce per-batch transaction overhead.
pub(crate) const CDC_BATCH_SIZE: usize = 4096;

/// Where `cdc_tick_core` routes a finished message. See the module docs.
#[derive(Copy, Clone)]
pub(crate) enum CdcQueueMode {
    /// Community single-process: every rendered message goes to the sink for
    /// direct in-process delivery.
    DeliverAll,
    /// Enterprise two-process: persisted messages are queued by id to
    /// `pgmqtt_cdc_outbox` in-transaction; only small QOS 0 messages reach
    /// the sink (for the shared-memory inline ring).
    OutboxQos1,
}

/// Run the CDC tick loop (enterprise, `pgmqtt_cdc` worker only): wake on the
/// shared BGW latch, drain the WAL slot every `pgmqtt.cdc_every_n_ticks`
/// ticks, queue persisted messages through `pgmqtt_cdc_outbox`, and hand
/// small QOS 0 messages to `pgmqtt_mqtt` via the shared-memory ring.
pub fn run_cdc(slot_name: &str) {
    let mut tick: u64 = 0;

    while BackgroundWorker::wait_latch(Some(super::latch_interval())) {
        tick = tick.wrapping_add(1);

        if BackgroundWorker::sighup_received() {
            log!("pgmqtt cdc: SIGHUP received");
            unsafe {
                pgrx::pg_sys::ProcessConfigFile(pgrx::pg_sys::GucContext::PGC_SIGHUP);
            }
        }

        if tick % crate::get_cdc_every_n_ticks_guc() == 0 {
            cdc_tick_core(slot_name, CdcQueueMode::OutboxQos1, |messages| {
                for msg in messages {
                    crate::shmem_bridge::push_inline(&msg.topic, &msg.payload);
                }
            });
        }
    }

    log!("pgmqtt cdc: SIGTERM received, shutting down");
}

/// One CDC tick: load mappings from DB, then drain the WAL slot in atomic
/// batches, persisting QOS ≥ 1 messages within the same transaction.
///
/// `sink` is called with each batch's sink-routed messages (all of them in
/// `DeliverAll` mode; only small QOS 0 ones in `OutboxQos1` mode) as soon as
/// that batch's transaction commits — not accumulated and called once with
/// the whole WAL backlog. This matters regardless of topology: even the
/// community single-process caller wants a batch delivered as soon as it's
/// durable rather than held until the entire backlog drains. In the
/// enterprise topology it additionally bounds the burst size any one
/// `crate::shmem_bridge` push has to absorb.
///
/// # Atomicity guarantee
///
/// For each batch the sequence is:
///   1. `pg_logical_slot_get_changes(..., upto_nchanges = CDC_BATCH_SIZE)`
///      fires the output plugin for each event, which pushes raw `ChangeEvent`s
///      into the in-memory `ring_buffer`.
///   2. The ring buffer is drained; every QOS ≥ 1 rendered message is
///      `INSERT`ed into `pgmqtt_messages` **within the same transaction**.
///      In `OutboxQos1` mode, all persisted ids are also queued to
///      `pgmqtt_cdc_outbox` in this same transaction.
///   3. On commit, the slot's `confirmed_flush_lsn` advances to cover exactly
///      those events — and only those events.
///
/// A crash between the SPI calls is impossible: they share one transaction.
/// If any insert fails the whole batch rolls back; the slot does not advance;
/// the same events will be re-read next tick (at-least-once delivery). In
/// `OutboxQos1` mode this extends across the process boundary: a message is
/// either fully invisible (batch rolled back) or durably persisted *and*
/// durably queued for delivery — there is no window where one exists without
/// the other, so a crash of either worker (or the whole postmaster) never
/// strands a persisted message.
///
/// Small QOS 0 messages are not persisted; they are collected during the
/// transaction and passed to the sink after commit (fire-and-forget).
pub(crate) fn cdc_tick_core(
    slot_name: &str,
    mode: CdcQueueMode,
    mut sink: impl FnMut(Vec<MqttMessage>),
) {
    // ── Startup: load mapping cache from pgmqtt_slot_mappings ────────────────
    //
    // pgmqtt_slot_mappings is the WAL-synchronized checkpoint: it is updated
    // atomically inside the same BackgroundWorker::transaction that advances
    // the slot LSN.  Loading from it on restart gives us the mapping state
    // exactly at confirmed_flush_lsn — never a "future" version.
    //
    // On a fresh slot (confirmed_flush_lsn IS NULL — nothing consumed yet) we
    // bootstrap by copying pgmqtt_topic_mappings → pgmqtt_slot_mappings once,
    // since pre-slot mapping rows will never appear as WAL events.
    if topic_map::get().is_none() {
        BackgroundWorker::transaction(|| {
            // Detect whether the slot has ever consumed data.
            let is_fresh = Spi::connect(|client| {
                let lsn: Option<String> = client
                    .select(
                        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots \
                         WHERE slot_name = $1",
                        None,
                        &[slot_name.into()],
                    )?
                    .first()
                    .get_one::<String>()?;
                Ok::<bool, spi::Error>(lsn.is_none())
            })
            .unwrap_or(false);

            if is_fresh {
                // Bootstrap: copy current user-facing table into the slot checkpoint.
                let _ = Spi::run(
                    "INSERT INTO pgmqtt_slot_mappings \
                         SELECT schema_name, table_name, mapping_name, \
                                topic_template, payload_template, qos, template_type \
                         FROM pgmqtt_topic_mappings \
                         ON CONFLICT DO NOTHING",
                );
                log!("pgmqtt: fresh slot — bootstrapped slot mappings from pgmqtt_topic_mappings");
            }

            // Load from the checkpoint into the in-process cache.
            if let Ok(mappings) = Spi::connect(|client| {
                let mut rows = Vec::new();
                if let Ok(table) = client.select(
                    "SELECT schema_name, table_name, mapping_name, \
                            topic_template, payload_template, qos, template_type \
                     FROM pgmqtt_slot_mappings",
                    None,
                    &[],
                ) {
                    for row in table {
                        let s: String = row
                            .get_by_name("schema_name")
                            .ok()
                            .flatten()
                            .unwrap_or_default();
                        let t: String = row
                            .get_by_name("table_name")
                            .ok()
                            .flatten()
                            .unwrap_or_default();
                        let mn: String = row
                            .get_by_name("mapping_name")
                            .ok()
                            .flatten()
                            .unwrap_or_else(|| "default".to_string());
                        let tt: String = row
                            .get_by_name("topic_template")
                            .ok()
                            .flatten()
                            .unwrap_or_default();
                        let pt: String = row
                            .get_by_name("payload_template")
                            .ok()
                            .flatten()
                            .unwrap_or_default();
                        let q: i32 = row.get_by_name("qos").ok().flatten().unwrap_or_default();
                        rows.push(topic_map::TopicMapping {
                            name: mn,
                            schema: s,
                            table: t,
                            topic_template: tt,
                            payload_template: pt,
                            qos: q as u8,
                        });
                    }
                }
                Ok::<_, spi::Error>(rows)
            }) {
                let count = mappings.len();
                let mapped_set = mappings
                    .iter()
                    .map(|m| (m.schema.clone(), m.table.clone()))
                    .collect::<std::collections::HashSet<_>>();
                topic_map::set_mappings(mappings);
                crate::ring_buffer::mapped_tables_init(mapped_set);
                log!(
                    "pgmqtt: loaded {} topic mappings from slot checkpoint",
                    count
                );
            }
        });
    }

    // ── Batched, atomic CDC drain loop ───────────────────────────────────────
    //
    // BackgroundWorker::transaction requires UnwindSafe + RefUnwindSafe, which
    // &mut T does NOT satisfy.  The fix: all mutable state lives *inside* the
    // closure (local variables, not captured).  The closure returns a
    // (messages, queued, batch_count, ok) tuple that we destructure after the
    // commit.
    //
    // Each batch's sink messages are pushed right after its own transaction
    // commits — NOT accumulated across the whole WAL backlog and pushed once
    // at the end. A single bulk INSERT can span many CDC_BATCH_SIZE-sized
    // batches; pushing incrementally caps the burst any one push has to
    // absorb at one batch's worth (instead of the entire backlog) and gives
    // the independent pgmqtt_mqtt process's per-tick drain a chance to
    // interleave, rather than receiving the whole backlog in one tight loop
    // after this function would otherwise return.
    loop {
        let (to_publish, outbox_queued, batch_count, batch_ok) = BackgroundWorker::transaction(
            move || -> (Vec<MqttMessage>, usize, usize, bool) {
                let mut to_publish: Vec<MqttMessage> = Vec::new();
                let mut outbox_ids: Vec<i64> = Vec::new();
                let mut batch_count: usize = 0;

                // ── Step 1: advance the slot by at most CDC_BATCH_SIZE events ──
                //
                // The output plugin (pg_decode_change) fires synchronously for each
                // row, pushing a ChangeEvent into ring_buffer.  Because this runs
                // inside the same transaction as the inserts below, the slot's
                // confirmed_flush_lsn only moves forward on COMMIT.
                let advance_query = format!(
                    "SELECT * FROM pg_logical_slot_get_changes('{}', NULL, {})",
                    slot_name, CDC_BATCH_SIZE
                );
                match Spi::connect(|client| {
                    // Suppress PostgreSQL's "starting logical decoding" LOG messages
                    // that fire on every slot read (every 80ms).  These are informational
                    // and extremely noisy in production.  In PostgreSQL's log_min_messages
                    // hierarchy, LOG sits above ERROR, so we need 'fatal' to suppress it.
                    // The 'true' flag makes this local to the current transaction only.
                    let _ = client.select(
                        "SELECT set_config('log_min_messages', 'fatal', true)",
                        None,
                        &[],
                    );
                    let mut n = 0usize;
                    let table = client.select(&advance_query, None, &[])?;
                    for _ in table {
                        n += 1;
                    }
                    Ok::<usize, spi::Error>(n)
                }) {
                    Ok(n) => {
                        batch_count = n;
                        if n > 0 {
                            log!("pgmqtt: slot batch fetched {} raw logical messages", n);
                            crate::metrics::add(
                                &crate::metrics::shared_cdc().events_processed,
                                n as u64,
                            );
                        }
                    }
                    Err(e) => {
                        log!("pgmqtt: error advancing slot: {:?} — skipping batch", e);
                        crate::metrics::inc(&crate::metrics::shared_cdc().slot_errors);
                        return (to_publish, 0, batch_count, false);
                    }
                }

                // ── Step 2: drain ring_buffer; process events in WAL order ──────
                //
                // MappingUpdate events apply mapping deltas both to the in-process
                // cache and to pgmqtt_slot_mappings within this transaction, so the
                // checkpoint stays atomically consistent with confirmed_flush_lsn.
                let events = ring_buffer::drain();

                for event in &events {
                    match event {
                        ring_buffer::RingEvent::MappingUpdate { op, columns } => {
                            // Helper to pull a column value by name.
                            let col = |name: &str| -> String {
                                columns
                                    .iter()
                                    .find(|(k, _)| k == name)
                                    .map(|(_, v)| v.clone())
                                    .unwrap_or_default()
                            };
                            let schema = col("schema_name");
                            let table = col("table_name");
                            let name = col("mapping_name");

                            if *op == "DELETE" {
                                topic_map::wal_remove(&schema, &table, &name);
                                // Only remove from the fast-path set if no other
                                // mappings remain for this (schema, table) pair.
                                if !topic_map::has_any_mapping(&schema, &table) {
                                    crate::ring_buffer::mapped_table_remove(&schema, &table);
                                }
                                let _ = pgrx::spi::Spi::connect_mut(|client| {
                                    client.update(
                                        "DELETE FROM pgmqtt_slot_mappings \
                                         WHERE schema_name = $1 AND table_name = $2 AND mapping_name = $3",
                                        None,
                                        &[schema.as_str().into(), table.as_str().into(), name.as_str().into()],
                                    ).map(|_| ())
                                });
                                log!("pgmqtt: WAL mapping DELETE {}.{} ({})", schema, table, name);
                            } else {
                                // INSERT or UPDATE
                                let topic_template = col("topic_template");
                                let payload_template = col("payload_template");
                                let qos: u8 = col("qos").parse().unwrap_or(0);
                                let mapping = topic_map::TopicMapping {
                                    name: name.clone(),
                                    schema: schema.clone(),
                                    table: table.clone(),
                                    topic_template: topic_template.clone(),
                                    payload_template: payload_template.clone(),
                                    qos,
                                };
                                topic_map::wal_upsert(mapping);
                                crate::ring_buffer::mapped_table_add(&schema, &table);
                                let tmpl_type = col("template_type");
                                let _ = pgrx::spi::Spi::connect_mut(|client| {
                                    client.update(
                                        "INSERT INTO pgmqtt_slot_mappings \
                                             (schema_name, table_name, mapping_name, \
                                              topic_template, payload_template, qos, template_type) \
                                         VALUES ($1, $2, $3, $4, $5, $6, $7) \
                                         ON CONFLICT (schema_name, table_name, mapping_name) DO UPDATE \
                                         SET topic_template = EXCLUDED.topic_template, \
                                             payload_template = EXCLUDED.payload_template, \
                                             qos = EXCLUDED.qos, \
                                             template_type = EXCLUDED.template_type",
                                        None,
                                        &[
                                            schema.as_str().into(),
                                            table.as_str().into(),
                                            name.as_str().into(),
                                            topic_template.as_str().into(),
                                            payload_template.as_str().into(),
                                            (qos as i32).into(),
                                            tmpl_type.as_str().into(),
                                        ],
                                    ).map(|_| ())
                                });
                                log!("pgmqtt: WAL mapping {} {}.{} ({})", op, schema, table, name);
                            }
                        }

                        ring_buffer::RingEvent::Data(change) => {
                            let rendered_messages = topic_map::render(
                                &change.schema,
                                &change.table,
                                change.op,
                                &change.columns,
                            );

                            if rendered_messages.is_empty() {
                                log!(
                                    "pgmqtt: no mapping match for {}.{}",
                                    change.schema,
                                    change.table
                                );
                                continue;
                            }

                            for rendered in rendered_messages {
                                let topic_str = rendered.topic.clone();

                                // Unlike the pre-split loop, this worker has no
                                // subscriber visibility in OutboxQos1 mode (that
                                // state lives in the pgmqtt_mqtt process) — every
                                // rendered message is persisted/queued
                                // unconditionally, and deliver_messages() reclaims
                                // the row on the other end if it turns out nobody
                                // is subscribed.

                                // A QOS 0 message too large for the shared-memory
                                // ring takes the persisted outbox path instead of
                                // being dropped; the delivery worker reclaims the
                                // row after the one delivery attempt.
                                let spill_qos0 = matches!(mode, CdcQueueMode::OutboxQos1)
                                    && rendered.qos == 0
                                    && !crate::shmem_bridge::fits_inline(
                                        &rendered.topic,
                                        &rendered.payload,
                                    );

                                if rendered.qos > 0 || spill_qos0 {
                                    // Persist within this transaction — committed atomically
                                    // with the slot advance above.  The subtransaction ensures a
                                    // PostgreSQL error inside persist_message is caught and counted
                                    // without crashing the background worker; the outer batch still
                                    // rolls back so events are retried on the next tick.
                                    let result = with_subtransaction(|| {
                                        pgrx::spi::Spi::connect_mut(|client| {
                                            let msg_id = db_action::persist_message(
                                                client,
                                                &topic_str,
                                                &rendered.payload,
                                                rendered.qos,
                                                false,
                                            )?;
                                            Ok::<_, spi::Error>(Some(msg_id))
                                        })
                                    });

                                    match result {
                                        Ok(Some(msg_id)) => {
                                            log!(
                                                "pgmqtt cdc: persisted QOS {} to '{}' (msg_id={})",
                                                rendered.qos,
                                                rendered.topic,
                                                msg_id
                                            );
                                            match mode {
                                                CdcQueueMode::OutboxQos1 => {
                                                    outbox_ids.push(msg_id);
                                                }
                                                CdcQueueMode::DeliverAll => {
                                                    to_publish.push(MqttMessage {
                                                        id: Some(msg_id),
                                                        topic: rendered.topic,
                                                        payload: rendered.payload,
                                                        qos: rendered.qos,
                                                    });
                                                }
                                            }
                                            crate::metrics::inc(
                                                &crate::metrics::shared_cdc().msgs_published,
                                            );
                                        }
                                        other => {
                                            log!(
                                                "pgmqtt cdc: error persisting QOS {} to '{}': {:?} \
                                                 — batch rolls back, events retried next tick",
                                                rendered.qos,
                                                topic_str,
                                                other
                                            );
                                            crate::metrics::inc(
                                                &crate::metrics::shared_cdc().persist_errors,
                                            );
                                            return (Vec::new(), 0, batch_count, false);
                                        }
                                    }
                                } else {
                                    // QOS 0 — fire-and-forget, pushed after commit.
                                    to_publish.push(MqttMessage {
                                        id: None,
                                        topic: rendered.topic,
                                        payload: rendered.payload,
                                        qos: 0,
                                    });
                                    crate::metrics::inc(&crate::metrics::shared_cdc().msgs_published);
                                }

                                log!(
                                    "pgmqtt: processed {} on {}.{} → topic='{}'",
                                    change.op,
                                    change.schema,
                                    change.table,
                                    topic_str
                                );
                            }
                        }
                    }
                }

                // ── Step 3 (OutboxQos1 only): queue persisted ids for delivery ──
                //
                // One batched insert, inside this same transaction: the slot
                // advance, the message rows, and the pending-delivery queue
                // rows all commit (or roll back) together. This is the whole
                // cross-process handoff for persisted messages — the delivery
                // worker reads pgmqtt_cdc_outbox in id (= WAL) order.
                if !outbox_ids.is_empty() {
                    let queued = outbox_ids.len();
                    let insert_result = with_subtransaction(|| {
                        pgrx::spi::Spi::connect_mut(|client| {
                            let args: Vec<DatumWithOid> = vec![outbox_ids.clone().into()];
                            client
                                .update(
                                    "INSERT INTO pgmqtt_cdc_outbox (id) SELECT unnest($1::bigint[])",
                                    None,
                                    &args,
                                )
                                .map(|_| ())
                        })
                    });
                    if let Err(e) = insert_result {
                        log!(
                            "pgmqtt cdc: error queueing {} message ids to pgmqtt_cdc_outbox: {:?} \
                             — batch rolls back, events retried next tick",
                            queued,
                            e
                        );
                        crate::metrics::inc(&crate::metrics::shared_cdc().persist_errors);
                        return (Vec::new(), 0, batch_count, false);
                    }
                    return (to_publish, queued, batch_count, true);
                }

                (to_publish, 0, batch_count, true)
            },
        );
        // ↑ COMMIT: slot LSN advances IFF all QOS ≥ 1 inserts committed.
        //   batch_ok=false means the transaction rolled back; slot unchanged.

        if !batch_ok {
            log!("pgmqtt cdc: batch transaction failed or rolled back — events will be retried");
        }

        if batch_ok {
            if outbox_queued > 0 {
                // After commit, so the woken reader always sees the rows.
                crate::shmem_bridge::ring_outbox_doorbell();
            }
            if !to_publish.is_empty() {
                sink(to_publish);
            }
        }

        // Stop when the batch was smaller than the limit — WAL fully drained.
        if batch_count < CDC_BATCH_SIZE {
            break;
        }
    }
}
