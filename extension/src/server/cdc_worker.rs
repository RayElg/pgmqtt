//! CDC slot consumption. Shared by both topologies via [`CdcQueueMode`]:
//! community runs [`cdc_tick_core`] inline in the socket loop, enterprise
//! in the dedicated `pgmqtt_cdc` worker.

use super::{db_action, with_subtransaction, MqttMessage};
use crate::ring_buffer;
use crate::topic_map;
use pgrx::bgworkers::BackgroundWorker;
use pgrx::log;
use pgrx::spi::{self, Spi};

/// The slot only advances past a batch once its QOS >= 1 messages commit.
pub(crate) const CDC_BATCH_SIZE: usize = 4096;

/// Max drain batches per `cdc_tick_core` call: an unbounded drain of a deep
/// backlog would starve the socket loop (Standalone) or the worker's other
/// tick duties (Bridged). The caller re-invokes while a backlog remains.
const MAX_DRAIN_BATCHES_PER_TICK: usize = 4;

/// Idle-advance throttle: fully-filtered WAL (e.g. the broker's own
/// origin-tagged bookkeeping) yields no LSN to advance past, which would
/// pin restart_lsn/WAL retention forever. Each advance persists slot state,
/// so it runs at most once per interval.
const IDLE_ADVANCE_INTERVAL_SECS: i64 = 10;
static LAST_IDLE_ADVANCE_SECS: std::sync::atomic::AtomicI64 =
    std::sync::atomic::AtomicI64::new(0);

fn idle_advance_due() -> bool {
    let last = LAST_IDLE_ADVANCE_SECS.load(std::sync::atomic::Ordering::Relaxed);
    crate::license::now_secs() - last >= IDLE_ADVANCE_INTERVAL_SECS
}

fn lsn_text(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

/// `flush_lsn` is captured before the empty peek, so everything at or
/// below it decoded to nothing. Guarded against moving backward.
fn advance_slot_idle(slot_name: &str, flush_lsn: u64) {
    LAST_IDLE_ADVANCE_SECS.store(
        crate::license::now_secs(),
        std::sync::atomic::Ordering::Relaxed,
    );
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

/// Every slot read/advance logs "starting logical decoding ..." at LOG
/// level — noise at production rates.
fn suppress_decoding_logs(client: &mut pgrx::spi::SpiClient<'_>) {
    let _ = client.select(
        "SELECT set_config('log_min_messages', 'fatal', true)",
        None,
        &[],
    );
}

#[derive(Copy, Clone)]
pub(crate) enum CdcQueueMode {
    /// Every rendered message goes to the sink for in-process delivery.
    DeliverAll,
    /// Persisted messages are queued by id to `pgmqtt_cdc_outbox`
    /// in-transaction; only small QOS 0 reaches the sink.
    OutboxQos1,
}

/// Also hosts the QoS 1 inbound pump and the WAL flush beacon, keeping
/// their fsyncs and failure modes off the socket loop.
pub fn run_cdc(slot_name: &str) {
    super::topology::setup_replication_origin("pgmqtt_cdc");
    let mut tick: u64 = 0;
    let mut last_inbound_reload = std::time::Instant::now();

    let mut drain_pending = false;
    // u64::MAX forces a first-tick drain: rows may predate this worker.
    let mut inbound_seen = u64::MAX;
    let mut last_inbound_sweep = std::time::Instant::now();

    super::load_inbound_mappings();

    while BackgroundWorker::wait_latch(Some(super::latch_interval())) {
        tick = tick.wrapping_add(1);

        if BackgroundWorker::sighup_received() {
            log!("pgmqtt cdc: SIGHUP received");
            unsafe {
                pgrx::pg_sys::ProcessConfigFile(pgrx::pg_sys::GucContext::PGC_SIGHUP);
            }
        }

        // First: pgmqtt_mqtt has deferred PUBACKs waiting on this. The
        // sync commit forces a flush of all earlier WAL, including the
        // socket worker's async commits.
        if crate::shmem_bridge::take_wal_flush_request() {
            let _ = super::wal::force_flush();
        }

        if last_inbound_reload.elapsed() >= std::time::Duration::from_millis(500) {
            super::load_inbound_mappings();
            last_inbound_reload = std::time::Instant::now();
        }

        // The doorbell keeps this prompt when work exists; the sweep
        // covers rows it cannot announce (retries, crash leftovers).
        let inbound_bell = crate::shmem_bridge::inbound_doorbell_seq();
        if inbound_bell != inbound_seen
            || last_inbound_sweep.elapsed() >= std::time::Duration::from_millis(100)
        {
            inbound_seen = inbound_bell;
            last_inbound_sweep = std::time::Instant::now();
            super::process_inbound_pending();
        }

        if tick % crate::get_cdc_every_n_ticks_guc() == 0 || drain_pending {
            drain_pending = !cdc_tick_core(slot_name, CdcQueueMode::OutboxQos1, |messages| {
                for msg in messages {
                    crate::shmem_bridge::push_inline(&msg.topic, &msg.payload);
                }
            });
        }
    }

    log!("pgmqtt cdc: SIGTERM received, shutting down");
}

/// Drain the WAL slot in bounded batches: peek, persist (+ outbox rows,
/// same transaction), then advance the slot past the batch after commit.
///
/// At-least-once: slot state is NOT transactional — a consuming
/// `get_changes` confirms in-function even if the transaction rolls back
/// (verified empirically) — so peek + advance-after-commit is the only
/// ordering that can't lose a message. A failed batch never advances
/// (events replay); a crash between commit and advance replays the batch
/// (duplicates, QoS 1-legal). `sink` fires per committed batch, bounding
/// each delivery burst; small QOS 0 is never persisted.
///
/// Returns `true` when the WAL was fully drained; `false` means the per-tick
/// batch budget ([`MAX_DRAIN_BATCHES_PER_TICK`]) was hit with WAL still
/// pending and the caller should re-invoke on the next tick.
pub(crate) fn cdc_tick_core(
    slot_name: &str,
    mode: CdcQueueMode,
    mut sink: impl FnMut(Vec<MqttMessage>),
) -> bool {
    // Startup: load the mapping cache from pgmqtt_slot_mappings — the
    // WAL-synchronized checkpoint (updated atomically with slot advances),
    // so a restart sees the mapping state exactly at confirmed_flush_lsn.
    // A fresh slot bootstraps it from pgmqtt_topic_mappings (pre-slot rows
    // never appear as WAL events).
    if topic_map::get().is_none() {
        BackgroundWorker::transaction(|| {
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

    // Mutable state lives inside the closure (UnwindSafe forbids &mut
    // captures). `transaction_or_abort` because a failed batch must roll
    // back everything it persisted, or the retry replays on top of it.
    let mut batches = 0usize;
    loop {
        // Idle-advance candidate, captured before the peek: if the peek
        // emits nothing, everything flushed up to here decoded to nothing
        // and the slot can be confirmed. Only read when the throttle is
        // due — the value is discarded otherwise.
        let pre_peek_flush = if idle_advance_due() {
            super::wal::read_lsn("pg_current_wal_flush_lsn()")
        } else {
            None
        };

        let ((to_publish, outbox_queued, batch_count, batch_end_lsn), batch_ok) =
            super::transaction_or_abort(
            move || -> ((Vec<MqttMessage>, usize, usize, Option<String>), bool) {
                let mut to_publish: Vec<MqttMessage> = Vec::new();
                let mut outbox_ids: Vec<i64> = Vec::new();
                // Inline-ring budget for this batch: free space never
                // shrinks under us, so inlining at most this many cannot
                // overflow. One bulk transaction can render far more QoS 0
                // than the ring holds; the excess spills to the outbox
                // rather than being dropped.
                let mut inline_budget = if matches!(mode, CdcQueueMode::OutboxQos1) {
                    crate::shmem_bridge::inline_free_slots()
                } else {
                    usize::MAX
                };
                let mut batch_count: usize = 0;
                let batch_end_lsn: Option<String>;

                // Step 1: peek at most CDC_BATCH_SIZE events into
                // ring_buffer; nothing is consumed — the caller advances
                // the slot only after this transaction commits.
                let peek_query = format!(
                    "SELECT lsn::text FROM pg_logical_slot_peek_changes('{}', NULL, {})",
                    slot_name, CDC_BATCH_SIZE
                );
                match Spi::connect_mut(|client| {
                    suppress_decoding_logs(client);
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
                        }
                    }
                    Err(e) => {
                        log!("pgmqtt: error peeking slot: {:?} — skipping batch", e);
                        crate::metrics::inc(&crate::metrics::shared_cdc().slot_errors);
                        // Drop what the failed peek pushed — the retry
                        // re-decodes it.
                        let _ = ring_buffer::drain();
                        return ((to_publish, 0, batch_count, None), false);
                    }
                }

                // Step 2: drain ring_buffer in WAL order. MappingUpdate
                // events update the cache and pgmqtt_slot_mappings in this
                // transaction, keeping the checkpoint consistent with
                // confirmed_flush_lsn.
                let events = ring_buffer::drain();

                // Counted from the ring, not from the peek row count: the peek
                // also returns one COMMIT marker per decoded transaction
                // (emitted solely to give the slot an exact advance boundary),
                // which would make this track cluster-wide commit traffic
                // rather than CDC load.
                if !events.is_empty() {
                    crate::metrics::add(
                        &crate::metrics::shared_cdc().events_processed,
                        events.len() as u64,
                    );
                }

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
                                // DeliverAll owns the subscription state, so
                                // unconsumable messages skip the persist (CDC
                                // is never retained). OutboxQos1 can't see
                                // subscribers cross-process; delivery-side
                                // reclamation covers it.
                                if matches!(mode, CdcQueueMode::DeliverAll)
                                    && !crate::subscriptions::has_subscribers(&rendered.topic)
                                {
                                    log!(
                                        "pgmqtt cdc: no subscribers for rendered topic '{}', skipping",
                                        rendered.topic
                                    );
                                    continue;
                                }

                                let topic_str = rendered.topic.clone();

                                // QOS 0 spills to the outbox rather than
                                // being dropped: oversize for the fixed slots,
                                // or no ring budget left this batch.
                                let spill_qos0 = matches!(mode, CdcQueueMode::OutboxQos1)
                                    && rendered.qos == 0
                                    && (inline_budget == 0
                                        || !crate::shmem_bridge::fits_inline(
                                            &rendered.topic,
                                            &rendered.payload,
                                        ));
                                if !spill_qos0 && rendered.qos == 0 {
                                    inline_budget = inline_budget.saturating_sub(1);
                                }

                                if rendered.qos > 0 || spill_qos0 {
                                    // Subtransaction: a persist error must not
                                    // crash the worker; the batch still aborts.
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

                // Step 3 (OutboxQos1): queue persisted ids in this same
                // transaction — the handoff commits or rolls back with the
                // message rows.
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

        if !batch_ok {
            // Nothing was consumed; the events replay next tick — don't
            // retry in a tight loop here.
            log!("pgmqtt cdc: batch transaction rolled back — events will be retried next tick");
            break true;
        }

        // Empty peek: everything up to the pre-peek flush point decodes to
        // nothing — confirm it (throttled) so WAL retention stays bounded.
        if batch_count == 0 {
            if let Some(flush) = pre_peek_flush {
                advance_slot_idle(slot_name, flush);
            }
            break true;
        }

        // Consume the committed batch — strictly after its rows are
        // visible, so a crash (or advance failure) in between merely
        // replays it.
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
                break true;
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
            break true;
        }

        // Yield after the budget so a deep backlog can't monopolize the
        // loop; the caller re-invokes next tick.
        batches += 1;
        if batches >= MAX_DRAIN_BATCHES_PER_TICK {
            break false;
        }
    }
}
