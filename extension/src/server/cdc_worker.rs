//! CDC slot consumption: WAL decoding, mapping/template rendering, and
//! QOS >= 1 persistence, shared by both process topologies.
//!
//! Community runs [`cdc_tick_core`] inline in the socket loop
//! ([`CdcQueueMode::DeliverAll`]: every message goes to the sink for direct
//! delivery). Enterprise runs it in the dedicated `pgmqtt_cdc` worker
//! ([`CdcQueueMode::OutboxQos1`]: persisted messages are queued to
//! `pgmqtt_cdc_outbox` inside the same transaction that advances the slot —
//! the handoff is exactly as durable as the messages, with id order = WAL
//! order; only small fire-and-forget QOS 0 messages reach the sink, bound
//! for `crate::shmem_bridge`'s inline ring).
//!
//! In `OutboxQos1` mode this worker has no subscriber visibility (that
//! state lives in the other process), so it persists unconditionally;
//! delivery-side reclamation preserves the no-orphan-rows invariant.

use super::{db_action, with_subtransaction, MqttMessage};
use crate::ring_buffer;
use crate::topic_map;
use pgrx::bgworkers::BackgroundWorker;
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

/// The `pgmqtt_cdc` worker's tick loop (enterprise only). Besides slot
/// consumption it absorbs the other DB-only pipelines, keeping their fsyncs
/// and failure modes off the socket loop: the QoS 1 inbound pump (whose
/// known target-table DDL crash then restarts this worker without touching
/// client connections) and the WAL flush beacon behind `pgmqtt_mqtt`'s
/// asynchronous commits.
pub fn run_cdc(slot_name: &str) {
    // Record the boot topology in this process too: the QoS 0 routing
    // below consults it (multi-worker sends everything through the
    // outbox).
    super::topology::set(-1, crate::socket_worker_count());
    super::topology::setup_replication_origin("pgmqtt_cdc");

    let mut tick: u64 = 0;
    let mut last_inbound_reload = std::time::Instant::now();

    // The inbound mapping cache is per-process; load it before first use and
    // refresh on the same ~500 ms cadence as pgmqtt_mqtt (which keeps its own
    // copy for matching topics at packet time).
    super::load_inbound_mappings();

    while BackgroundWorker::wait_latch(Some(super::latch_interval())) {
        tick = tick.wrapping_add(1);

        if BackgroundWorker::sighup_received() {
            log!("pgmqtt cdc: SIGHUP received");
            unsafe {
                pgrx::pg_sys::ProcessConfigFile(pgrx::pg_sys::GucContext::PGC_SIGHUP);
            }
        }

        // Flush beacon first: pgmqtt_mqtt has deferred PUBACKs waiting on
        // this. The synchronous commit is the whole point — it forces a
        // flush of all earlier WAL, including the socket worker's async
        // commits.
        if crate::shmem_bridge::take_wal_flush_request() {
            super::wal::force_flush();
        }

        if last_inbound_reload.elapsed() >= std::time::Duration::from_millis(500) {
            super::load_inbound_mappings();
            last_inbound_reload = std::time::Instant::now();
        }

        // Every tick, not gated by cdc_every_n_ticks: PUBACKs for inbound
        // QoS 1 publishes reflect durable intent as soon as the pending row
        // commits, but callers still expect the target-table row promptly.
        super::process_inbound_pending();

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

/// Drain the WAL slot in atomic batches, persisting QOS ≥ 1 messages within
/// the same transaction.
///
/// # Atomicity guarantee
///
/// Each batch is one transaction: the slot advance
/// (`pg_logical_slot_get_changes`), the `pgmqtt_messages` inserts, and (in
/// `OutboxQos1` mode) the outbox enqueue commit or roll back together. A
/// failed batch leaves the slot unmoved and the events are re-read next
/// tick — at-least-once, with no window where a persisted message exists
/// without its outbox row (or vice versa), so a crash of either worker or
/// the postmaster never strands a message.
///
/// `sink` fires per batch right after that batch's commit, not once for the
/// whole backlog: a single bulk INSERT can span many batches, and
/// incremental delivery bounds the burst any one push has to absorb.
/// Small QOS 0 messages are never persisted; they reach the sink after
/// commit, fire-and-forget.
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

    // BackgroundWorker::transaction requires UnwindSafe, which &mut T does
    // not satisfy — so all mutable state lives inside the closure and comes
    // back out through the returned tuple.
    loop {
        let (to_publish, outbox_queued, batch_count, batch_ok) = BackgroundWorker::transaction(
            move || -> (Vec<MqttMessage>, usize, usize, bool) {
                let mut to_publish: Vec<MqttMessage> = Vec::new();
                let mut outbox_ids: Vec<i64> = Vec::new();
                let mut batch_count: usize = 0;

                // Step 1: advance the slot by at most CDC_BATCH_SIZE events.
                // The output plugin pushes each event into ring_buffer; the
                // slot's confirmed_flush_lsn only moves forward when this
                // transaction (inserts included) commits.
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

                // Step 2: drain ring_buffer in WAL order. MappingUpdate
                // events apply to both the in-process cache and
                // pgmqtt_slot_mappings within this transaction, keeping the
                // checkpoint atomically consistent with confirmed_flush_lsn.
                let events = ring_buffer::drain();

                for event in &events {
                    match event {
                        ring_buffer::RingEvent::MappingUpdate { op, columns } => {
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

                                // A QOS 0 message too large for the shared-memory
                                // ring takes the persisted outbox path instead of
                                // being dropped. With several socket workers, ALL
                                // QoS 0 spills — the inline ring has a single
                                // consumer, and the outbox is the one medium every
                                // worker reads.
                                let spill_qos0 = matches!(mode, CdcQueueMode::OutboxQos1)
                                    && rendered.qos == 0
                                    && (crate::server::multi_worker()
                                        || !crate::shmem_bridge::fits_inline(
                                            &rendered.topic,
                                            &rendered.payload,
                                        ));

                                if rendered.qos > 0 || spill_qos0 {
                                    // The subtransaction catches a PostgreSQL
                                    // error inside persist_message without
                                    // crashing the worker; the outer batch
                                    // still rolls back so events are retried.
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

                // Step 3 (OutboxQos1 only): queue persisted ids for
                // delivery, still inside this transaction — the whole
                // cross-process handoff commits or rolls back with the slot
                // advance and the message rows.
                if !outbox_ids.is_empty() {
                    let queued = outbox_ids.len();
                    let insert_result = with_subtransaction(|| {
                        pgrx::spi::Spi::connect_mut(|client| {
                            super::outbox::enqueue(client, &outbox_ids)
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
