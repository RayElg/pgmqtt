//! CDC slot consumption: WAL decoding, mapping/template rendering, and
//! QOS >= 1 persistence, shared by both process topologies.
//!
//! Community runs [`cdc_tick_core`] inline in the socket loop
//! ([`CdcQueueMode::DeliverAll`]: every message goes to the sink for direct
//! delivery). Enterprise runs it in the dedicated `pgmqtt_cdc` worker
//! ([`CdcQueueMode::OutboxQos1`]: persisted messages are queued to
//! `pgmqtt_cdc_outbox` inside the same transaction that persists them, and
//! the slot is advanced only after that transaction commits — the handoff
//! is exactly as durable as the messages, with id order = WAL order; only
//! small fire-and-forget QOS 0 messages reach the sink, bound for
//! `crate::shmem_bridge`'s inline ring).
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

/// Maximum number of WAL events to process per `cdc_tick` batch transaction.
///
/// The replication slot only advances past a batch after **all** its QOS ≥ 1
/// messages are committed to `pgmqtt_messages` (see the at-least-once notes
/// on [`cdc_tick_core`]). Smaller values reduce the retry cost if a batch
/// fails; larger values reduce per-batch transaction overhead.
pub(crate) const CDC_BATCH_SIZE: usize = 4096;

/// Throttle for the idle slot advance: WAL that decodes to zero emitted
/// rows (chiefly pgmqtt's own origin-filtered bookkeeping) never yields an
/// LSN to advance past, so without this the slot would pin restart_lsn —
/// and WAL retention — indefinitely on a broker whose only writer is
/// itself. Each advance persists slot state to disk, so it runs at most
/// once per interval, not per tick.
const IDLE_ADVANCE_INTERVAL_SECS: i64 = 10;
static LAST_IDLE_ADVANCE_SECS: std::sync::atomic::AtomicI64 =
    std::sync::atomic::AtomicI64::new(0);

/// Format a parsed LSN back into PostgreSQL's textual form.
fn lsn_text(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

/// Confirm the slot up to `flush_lsn` (captured *before* the empty peek, so
/// everything at or below it has been decoded and produced nothing).
/// Conditional on actually moving forward — advancing backward is an error.
fn advance_slot_idle(slot_name: &str, flush_lsn: u64) {
    let now = crate::license::now_secs();
    let last = LAST_IDLE_ADVANCE_SECS.load(std::sync::atomic::Ordering::Relaxed);
    if now - last < IDLE_ADVANCE_INTERVAL_SECS {
        return;
    }
    LAST_IDLE_ADVANCE_SECS.store(now, std::sync::atomic::Ordering::Relaxed);
    let lsn = lsn_text(flush_lsn);
    BackgroundWorker::transaction(|| {
        let _ = Spi::connect_mut(|client| {
            suppress_decoding_logs(client);
            client
                .update(
                    &format!(
                        "SELECT pg_replication_slot_advance('{slot}', '{lsn}') \
                         WHERE '{lsn}'::pg_lsn > (SELECT confirmed_flush_lsn \
                                                  FROM pg_replication_slots \
                                                  WHERE slot_name = '{slot}')",
                        slot = slot_name,
                        lsn = lsn,
                    ),
                    None,
                    &[],
                )
                .map(|_| ())
        });
    });
}

/// `pg_replication_slot_advance` and the peek both start a decoding context,
/// which logs "starting logical decoding for slot ..." at LOG level on every
/// call — several lines per batch in production. Transaction-local, so the
/// suppression never leaks past the statement that needed it.
fn suppress_decoding_logs(client: &mut pgrx::spi::SpiClient<'_>) {
    let _ = client.select(
        "SELECT set_config('log_min_messages', 'fatal', true)",
        None,
        &[],
    );
}

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
    super::topology::set(-1, crate::boot_socket_workers());
    super::topology::setup_replication_origin("pgmqtt_cdc");
    if super::multi_worker() {
        // A floor orphaned by a crash of this worker would freeze every
        // delivery cursor; the transaction it covered is long over.
        crate::shmem_bridge::clear_enqueue_floor(crate::shmem_bridge::CDC_FLOOR_SLOT);
    }

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
            let _ = super::wal::force_flush();
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

/// Drain the WAL slot in bounded batches, persisting QOS ≥ 1 messages and
/// confirming the slot only after they are committed.
///
/// # At-least-once guarantee
///
/// Each batch *peeks* the slot (`pg_logical_slot_peek_changes` — decoding
/// runs, nothing is consumed), persists the rendered messages and (in
/// `OutboxQos1` mode) their outbox rows in one transaction, and only after
/// that transaction commits advances the slot past the batch
/// (`pg_replication_slot_advance`). Slot state is not transactional — the
/// consuming `pg_logical_slot_get_changes` confirms its position inside the
/// function call, so rolling back (or crashing out of) the surrounding
/// transaction does NOT un-consume events; verified empirically. Peek +
/// advance-after-commit is therefore the only ordering that can never lose
/// a message:
///
/// - a failed batch aborts and never advances — the same events replay next
///   tick;
/// - a crash after the commit but before the advance replays the whole
///   batch, re-persisting it (duplicate delivery, which QoS 1 permits);
/// - the message rows and their outbox rows still commit or roll back
///   together, so no window exists where one is visible without the other.
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

    // The closure requires UnwindSafe, which &mut T does not satisfy — so
    // all mutable state lives inside the closure and comes back out through
    // the returned tuple. `transaction_or_abort` (not
    // `BackgroundWorker::transaction`, which always commits) because a
    // failed batch must roll back the slot advance too, or the failed
    // events are consumed without their messages ever being persisted.
    loop {
        // Captured before the peek: everything flushed at this point is
        // covered by the peek's decode pass, so if that pass emits nothing
        // the slot can safely be confirmed up to here (throttled — see
        // advance_slot_idle).
        let pre_peek_flush = super::wal::read_lsn("pg_current_wal_flush_lsn()");

        let ((to_publish, outbox_queued, batch_count, batch_end_lsn), batch_ok) =
            super::transaction_or_abort(
            move || -> ((Vec<MqttMessage>, usize, usize, Option<String>), bool) {
                let mut to_publish: Vec<MqttMessage> = Vec::new();
                let mut outbox_ids: Vec<i64> = Vec::new();
                let mut batch_count: usize = 0;
                let mut batch_end_lsn: Option<String> = None;

                // Step 1: peek at most CDC_BATCH_SIZE events. The output
                // plugin pushes each event into ring_buffer; nothing is
                // consumed — the caller advances the slot past the batch's
                // last LSN only after this transaction commits (see the
                // at-least-once notes on cdc_tick_core).
                let peek_query = format!(
                    "SELECT lsn::text FROM pg_logical_slot_peek_changes('{}', NULL, {})",
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
                    let mut last: Option<String> = None;
                    let table = client.select(&peek_query, None, &[])?;
                    for row in table {
                        n += 1;
                        if let Ok(Some(lsn)) = row.get_by_name::<String, _>("lsn") {
                            last = Some(lsn);
                        }
                    }
                    Ok::<(usize, Option<String>), spi::Error>((n, last))
                }) {
                    Ok((n, last)) => {
                        batch_count = n;
                        batch_end_lsn = last;
                        if n > 0 {
                            log!("pgmqtt: slot batch fetched {} raw logical messages", n);
                            crate::metrics::add(
                                &crate::metrics::shared_cdc().events_processed,
                                n as u64,
                            );
                        }
                    }
                    Err(e) => {
                        log!("pgmqtt: error peeking slot: {:?} — skipping batch", e);
                        crate::metrics::inc(&crate::metrics::shared_cdc().slot_errors);
                        // Nothing was consumed, so anything the failed peek
                        // already pushed into the ring will be decoded again
                        // — drop it or the retry sees duplicates.
                        let _ = ring_buffer::drain();
                        return ((to_publish, 0, batch_count, None), false);
                    }
                }

                // Multi-worker: everything queued below lands in the shared
                // outbox, so this batch must hold an enqueue floor before
                // its first message insert — the delivery cursors otherwise
                // could advance past ids this transaction commits later
                // (see shmem_bridge). Cleared by the caller after the
                // transaction ends.
                if matches!(mode, CdcQueueMode::OutboxQos1) && crate::server::multi_worker() {
                    if let Err(e) = Spi::connect_mut(|client| {
                        super::outbox::arm_enqueue_floor(
                            client,
                            crate::shmem_bridge::CDC_FLOOR_SLOT,
                        )
                    }) {
                        log!(
                            "pgmqtt cdc: failed to arm enqueue floor: {:?} — batch aborted and retried",
                            e
                        );
                        // Same as the peek-error path: the aborted peek's
                        // events will be re-decoded on retry.
                        let _ = ring_buffer::drain();
                        return ((Vec::new(), 0, batch_count, None), false);
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
                                            return ((Vec::new(), 0, batch_count, None), false);
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
                // cross-process handoff commits or rolls back with the
                // message rows (the slot advances only after the commit).
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
                        return ((Vec::new(), 0, batch_count, None), false);
                    }
                    return ((to_publish, queued, batch_count, batch_end_lsn), true);
                }

                ((to_publish, 0, batch_count, batch_end_lsn), true)
            },
        );

        // Whether the batch committed or aborted, its enqueue transaction
        // is over — release the floor (before the doorbell, so a woken
        // reader's barrier already covers this batch's rows).
        if matches!(mode, CdcQueueMode::OutboxQos1) && crate::server::multi_worker() {
            crate::shmem_bridge::clear_enqueue_floor(crate::shmem_bridge::CDC_FLOOR_SLOT);
        }

        if !batch_ok {
            // The peek consumed nothing and no advance follows, so the same
            // events come back on the next tick — do not retry in a tight
            // loop here.
            log!("pgmqtt cdc: batch transaction rolled back — events will be retried next tick");
            break;
        }

        // An empty peek means all WAL up to the pre-peek flush point decodes
        // to nothing for us — confirm it (throttled) so the slot's WAL
        // retention stays bounded even when the broker's own (origin-
        // filtered) bookkeeping is the only write traffic.
        if batch_count == 0 {
            if let Some(flush) = pre_peek_flush {
                advance_slot_idle(slot_name, flush);
            }
            break;
        }

        // The batch is committed: advance the slot past it. This is the one
        // place a processed batch is consumed, and it happens strictly after
        // the messages and outbox rows are visible — a crash in between
        // merely replays the batch (duplicates, not loss). An advance
        // failure has the same effect; log it because each replay
        // re-persists the batch.
        if let Some(lsn) = batch_end_lsn {
            let advanced = BackgroundWorker::transaction(|| {
                Spi::connect_mut(|client| {
                    suppress_decoding_logs(client);
                    client
                        .update(
                            &format!(
                                "SELECT pg_replication_slot_advance('{}', '{}')",
                                slot_name, lsn
                            ),
                            None,
                            &[],
                        )
                        .map(|_| ())
                })
                .is_ok()
            });
            if !advanced {
                log!(
                    "pgmqtt cdc: failed to advance slot '{}' to {} — the committed batch \
                     will replay next tick (duplicate delivery possible)",
                    slot_name,
                    lsn
                );
                crate::metrics::inc(&crate::metrics::shared_cdc().slot_errors);
                break;
            }
        }

        if outbox_queued > 0 {
            // After commit, so the woken reader always sees the rows.
            crate::shmem_bridge::ring_outbox_doorbell();
        }
        if !to_publish.is_empty() {
            sink(to_publish);
        }

        // Stop when the batch was smaller than the limit — WAL fully drained.
        if batch_count < CDC_BATCH_SIZE {
            break;
        }
    }
}
