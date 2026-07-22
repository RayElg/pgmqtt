//! Durable cross-process handoff through `pgmqtt_cdc_outbox`.
//!
//! The CDC worker (and, with several socket workers, the publish path)
//! queues persisted message ids here in the same transaction that persists
//! the rows, so the handoff is exactly as durable as the messages
//! themselves. On the delivery side, [`Drain`] fetches batches each tick:
//! a single worker deletes ids on delivery, while N workers each keep a
//! cursor (`pgmqtt_outbox_cursors`) and slot 0 reclaims rows every cursor
//! has passed.

use super::{cdc_worker, db_action, MqttClient, MqttMessage, SessionDbAction};
use crate::subscriptions;
use pgrx::bgworkers::BackgroundWorker;
use pgrx::log;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Queue persisted message ids for delivery, inside the caller's
/// transaction — the ids become visible to every worker's fetch exactly
/// when the message rows do.
pub(crate) fn enqueue(
    client: &mut pgrx::spi::SpiClient<'_>,
    ids: &[i64],
) -> Result<(), pgrx::spi::Error> {
    let args: Vec<pgrx::datum::DatumWithOid> = vec![ids.to_vec().into()];
    client
        .update(
            "INSERT INTO pgmqtt_cdc_outbox (id) SELECT unnest($1::bigint[]) \
             ON CONFLICT DO NOTHING",
            None,
            &args,
        )
        .map(|_| ())
}

/// Publish this process's enqueue floor from the pgmqtt_messages sequence.
/// Must run inside the enqueuing transaction before its first insert:
/// every id it allocates is then above the floor, so delivery cursors
/// (which stop at the minimum active floor) can't pass an id it might
/// still commit. The caller clears the floor after the transaction ends.
pub(super) fn arm_enqueue_floor(
    client: &mut pgrx::spi::SpiClient<'_>,
    floor_slot: usize,
) -> Result<(), pgrx::spi::Error> {
    let floor: i64 = client
        .select(
            // Sequence state is non-transactional, so last_value covers
            // every allocation so far; a virgin sequence (is_called =
            // false) hands out last_value itself first.
            "SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END \
             FROM pgmqtt_messages_id_seq",
            None,
            &[],
        )?
        .first()
        .get_one::<i64>()?
        .unwrap_or(0);
    crate::shmem_bridge::publish_enqueue_floor(floor_slot, floor);
    Ok(())
}

/// Ensure this worker has a cursor row and return it. A new slot starts at
/// the minimum existing cursor (never behind the GC watermark); first boot
/// starts everyone at 0. Advisory-locked like all worker-startup writes.
fn seed_cursor(slot: i32) -> i64 {
    BackgroundWorker::transaction(|| {
        pgrx::spi::Spi::connect_mut(|client| {
            let _ = client.select(
                &format!(
                    "SELECT pg_advisory_xact_lock({})",
                    crate::SCHEMA_MIGRATION_LOCK_KEY
                ),
                None,
                &[],
            );
            let args: Vec<pgrx::datum::DatumWithOid> = vec![slot.into()];
            client.update(
                "INSERT INTO pgmqtt_outbox_cursors (worker_slot, last_id) \
                 VALUES ($1, COALESCE((SELECT MIN(last_id) FROM pgmqtt_outbox_cursors), 0)) \
                 ON CONFLICT (worker_slot) DO UPDATE SET last_seen = now()",
                None,
                &args,
            )?;
            let args: Vec<pgrx::datum::DatumWithOid> = vec![slot.into()];
            client
                .select(
                    "SELECT last_id FROM pgmqtt_outbox_cursors WHERE worker_slot = $1",
                    None,
                    &args,
                )?
                .first()
                .get_one::<i64>()
        })
    })
    .ok()
    .flatten()
    .unwrap_or(0)
}

/// Claim `(message_id, shared-group)` pairs: the claims table's primary
/// key picks one winner cluster-wide (every worker matches its own group
/// members against every outbox row). A claim this worker already holds
/// counts as won — a crash between claim and recorded delivery re-fetches
/// the message, and losing the race against itself would strand it.
/// Returns the won group keys by message id; `None` on error, and the
/// caller delivers unclaimed (duplicates beat loss).
pub(super) fn claim_share_groups(
    pairs: &[(i64, &str)],
) -> Option<HashMap<i64, std::collections::HashSet<String>>> {
    let ids: Vec<i64> = pairs.iter().map(|(id, _)| *id).collect();
    let groups: Vec<&str> = pairs.iter().map(|(_, g)| *g).collect();
    let slot = super::worker_slot();
    BackgroundWorker::transaction(|| {
        pgrx::spi::Spi::connect_mut(|client| {
            let _ = client.select(
                "SELECT set_config('synchronous_commit', 'off', true)",
                None,
                &[],
            );
            let args: Vec<pgrx::datum::DatumWithOid> =
                vec![ids.into(), groups.into(), slot.into()];
            let table = client.update(
                "WITH ins AS (\
                     INSERT INTO pgmqtt_share_claims (message_id, group_key, worker_slot) \
                     SELECT u.message_id, u.group_key, $3 \
                     FROM unnest($1::bigint[], $2::text[]) AS u(message_id, group_key) \
                     ON CONFLICT DO NOTHING \
                     RETURNING message_id, group_key) \
                 SELECT message_id, group_key FROM ins \
                 UNION \
                 SELECT c.message_id, c.group_key \
                 FROM pgmqtt_share_claims c \
                 JOIN unnest($1::bigint[], $2::text[]) AS u(message_id, group_key) \
                   ON c.message_id = u.message_id AND c.group_key = u.group_key \
                 WHERE c.worker_slot = $3",
                None,
                &args,
            )?;
            let mut won: HashMap<i64, std::collections::HashSet<String>> = HashMap::new();
            for row in table {
                let id: Option<i64> = row.get_by_name("message_id")?;
                let group: Option<String> = row.get_by_name("group_key")?;
                if let (Some(id), Some(group)) = (id, group) {
                    won.entry(id).or_default().insert(group);
                }
            }
            Ok::<_, pgrx::spi::Error>(won)
        })
    })
    .ok()
}

/// Slot-0 GC (multi-worker): delete outbox rows every cursor has passed
/// and reclaim orphaned messages — per-delivery cleanup would race the
/// other workers. Bounded per pass; the swept count lets the caller tell a
/// drained pass from leftover backlog. Also returns how many cursors were
/// ignored as stale (no heartbeat within `pgmqtt.outbox_cursor_stale_secs`)
/// — a wedged or crash-looping worker must not pin the watermark and grow
/// the outbox, messages table, and WAL without bound. The ignored worker,
/// if it revives, resumes above the watermark and misses whatever GC
/// reclaimed for its local subscribers: bounded growth wins over a worker
/// dead for minutes.
fn gc_below_min_cursor() -> (usize, u64) {
    let stale_secs = crate::get_outbox_cursor_stale_secs_guc();
    // Watermark = MIN(last_id) over live cursors; stale_secs = 0 disables
    // staleness and every cursor counts.
    let watermark = format!(
        "SELECT COALESCE(MIN(last_id), 0) FROM pgmqtt_outbox_cursors \
         WHERE {} = 0 OR last_seen >= now() - make_interval(secs => {})",
        stale_secs, stale_secs
    );
    BackgroundWorker::transaction(|| {
        pgrx::spi::Spi::connect_mut(|client| {
            let stale: i64 = if stale_secs == 0 {
                0
            } else {
                client
                    .select(
                        &format!(
                            "SELECT count(*) FROM pgmqtt_outbox_cursors \
                             WHERE last_seen < now() - make_interval(secs => {})",
                            stale_secs
                        ),
                        None,
                        &[],
                    )?
                    .first()
                    .get_one::<i64>()?
                    .unwrap_or(0)
            };
            let swept = client.update(
                &format!(
                    "DELETE FROM pgmqtt_cdc_outbox \
                     WHERE id IN (\
                         SELECT id FROM pgmqtt_cdc_outbox \
                         WHERE id <= ({}) \
                         ORDER BY id LIMIT {}) \
                     RETURNING id",
                    watermark,
                    cdc_worker::CDC_BATCH_SIZE
                ),
                None,
                &[],
            )?;
            let ids: Vec<i64> = swept
                .into_iter()
                .filter_map(|row| row.get_by_name::<i64, _>("id").ok().flatten())
                .collect();
            for id in &ids {
                let _ = db_action::cleanup_orphaned_message(client, *id);
            }
            client.update(
                &format!(
                    "DELETE FROM pgmqtt_share_claims WHERE message_id <= ({})",
                    watermark
                ),
                None,
                &[],
            )?;
            Ok::<_, pgrx::spi::Error>((ids.len(), stale as u64))
        })
    })
    .unwrap_or_else(|e| {
        log!("pgmqtt: outbox GC failed: {}", e);
        (0, 0)
    })
}

/// Decode one `id, topic, payload, qos` row. The message is `None` for a
/// dangling id (LEFT JOIN NULLs) — the caller still gets the id so it can
/// dequeue or advance past it instead of re-scanning it forever.
fn message_from_row(
    row: &pgrx::spi::SpiHeapTupleData,
) -> Result<(Option<i64>, Option<MqttMessage>), pgrx::spi::Error> {
    let Some(id) = row.get_by_name::<i64, _>("id")? else {
        return Ok((None, None));
    };
    let Some(topic) = row.get_by_name::<String, _>("topic")? else {
        return Ok((Some(id), None));
    };
    let payload: Vec<u8> = row.get_by_name("payload")?.unwrap_or_default();
    let qos: i32 = row.get_by_name("qos")?.unwrap_or(0);
    Ok((
        Some(id),
        Some(MqttMessage {
            id: Some(id),
            topic: Arc::from(topic.as_str()),
            payload: Arc::from(payload),
            qos: qos as u8,
        }),
    ))
}

/// Fetch one bounded batch. `after`: multi-worker cursor mode (rows stay
/// for the other workers; slot-0 GC reclaims). `None`: single-worker mode
/// (the caller deletes delivered ids). `view_capped` = a committed outbox
/// row sits above the enqueue cap, so keep re-querying — the rows above it
/// may never get another doorbell.
fn fetch_batch(after: Option<i64>) -> (Vec<i64>, Vec<MqttMessage>, bool) {
    BackgroundWorker::transaction(|| {
        pgrx::spi::Spi::connect(|client| {
            let mut ids = Vec::new();
            let mut out = Vec::new();
            let mut view_capped = false;
            if let Some(cursor) = after {
                // Two-phase: ids + topics first, payloads only for local
                // subscription matches — payload reads are the N-worker
                // amplification. Safe (unlike a worker-local fast path)
                // because the cursor still advances over skipped ids; only
                // local delivery is elided. has_subscribers is side-effect
                // free — never pre-filter with match_topic_split, it
                // advances $share round-robin.
                //
                // The barrier caps the fetch: ids above it may still gain a
                // smaller committed sibling from an in-flight enqueue, and
                // passing that hole skips the message forever (see
                // shmem_bridge). Under READ COMMITTED the statement snapshot
                // is taken at execution — after the barrier read — so a
                // writer arming its floor in that gap is invisible to the
                // barrier while a later sibling's commit is visible to the
                // snapshot. Reading the id sequence FIRST closes the gap:
                // every id <= seq_cap was allocated before the barrier read,
                // so its writer either armed its floor first (the barrier
                // covers it) or already ended (the snapshot sees it). Ids
                // above seq_cap wait for a later fetch.
                let seq_cap: i64 = client
                    .select(
                        "SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END \
                         FROM pgmqtt_messages_id_seq",
                        None,
                        &[],
                    )?
                    .first()
                    .get_one::<i64>()?
                    .unwrap_or(0);
                let barrier = crate::shmem_bridge::enqueue_barrier().min(seq_cap);
                let query = format!(
                    "SELECT o.id, m.topic \
                     FROM pgmqtt_cdc_outbox o \
                     LEFT JOIN pgmqtt_messages m ON m.id = o.id \
                     WHERE o.id > {} AND o.id <= {} \
                     ORDER BY o.id \
                     LIMIT {}",
                    cursor,
                    barrier,
                    cdc_worker::CDC_BATCH_SIZE
                );
                let mut wanted: Vec<i64> = Vec::new();
                let table = client.select(&query, None, &[])?;
                for row in table {
                    let id: i64 = match row.get_by_name("id")? {
                        Some(v) => v,
                        None => continue,
                    };
                    ids.push(id);
                    let topic: Option<String> = row.get_by_name("topic")?;
                    let Some(topic) = topic else {
                        continue;
                    };
                    if subscriptions::has_subscribers(&topic) {
                        wanted.push(id);
                    }
                }
                // The cap is now always finite, so "capped" must mean "a
                // committed row sits above it" — its doorbell ring may have
                // been consumed by this very tick, so keep polling instead
                // of waiting out the 30s safety check. Skipped when the
                // fetch came back full (the caller re-polls anyway).
                if ids.len() < cdc_worker::CDC_BATCH_SIZE {
                    let args: Vec<pgrx::datum::DatumWithOid> = vec![barrier.into()];
                    view_capped = !client
                        .select(
                            "SELECT 1 FROM pgmqtt_cdc_outbox WHERE id > $1 LIMIT 1",
                            None,
                            &args,
                        )?
                        .is_empty();
                }
                if !wanted.is_empty() {
                    let args: Vec<pgrx::datum::DatumWithOid> = vec![wanted.into()];
                    let table = client.select(
                        // ORDER BY id keeps delivery order = outbox order.
                        "SELECT id, topic, payload, qos FROM pgmqtt_messages \
                         WHERE id = ANY($1::bigint[]) ORDER BY id",
                        None,
                        &args,
                    )?;
                    for row in table {
                        if let (_, Some(msg)) = message_from_row(&row)? {
                            out.push(msg);
                        }
                    }
                }
            } else {
                // Single worker needs every message regardless of local
                // matches: no-subscriber reclaim and QoS-0-spill cleanup
                // key off this batch.
                let query = format!(
                    "SELECT o.id, m.topic, m.payload, m.qos \
                     FROM pgmqtt_cdc_outbox o \
                     LEFT JOIN pgmqtt_messages m ON m.id = o.id \
                     ORDER BY o.id \
                     LIMIT {}",
                    cdc_worker::CDC_BATCH_SIZE
                );
                let table = client.select(&query, None, &[])?;
                for row in table {
                    let (id, msg) = message_from_row(&row)?;
                    if let Some(id) = id {
                        ids.push(id);
                    }
                    if let Some(msg) = msg {
                        out.push(msg);
                    }
                }
            }
            Ok::<_, pgrx::spi::Error>((ids, out, view_capped))
        })
    })
    .unwrap_or_else(|e| {
        log!("pgmqtt: failed to fetch pending CDC outbox batch: {}", e);
        (Vec::new(), Vec::new(), false)
    })
}

/// Per-tick outbox consumption for a Bridged (enterprise) socket worker,
/// including the small-QoS-0 shared-memory ring.
pub(super) struct Drain {
    /// Starts true so the first tick always queries — the doorbell does
    /// not survive a postmaster crash.
    pending: bool,
    doorbell_seen: u64,
    /// Multi-worker delivery cursor, seeded from `pgmqtt_outbox_cursors`.
    cursor: i64,
    last_safety_check: Instant,
    last_gc: Instant,
    /// Last GC pass swept a full batch — keep sweeping every tick or the
    /// table grows without bound.
    gc_backlog: bool,
    /// Stale-cursor count from the last GC pass, to log only transitions
    /// (GC runs every second).
    stale_seen: u64,
}

impl Drain {
    pub(super) fn new() -> Self {
        Drain {
            pending: true,
            doorbell_seen: 0,
            cursor: if super::multi_worker() {
                seed_cursor(super::worker_slot())
            } else {
                0
            },
            last_safety_check: Instant::now(),
            last_gc: Instant::now(),
            gc_backlog: false,
            stale_seen: 0,
        }
    }

    pub(super) fn tick(
        &mut self,
        clients: &mut HashMap<String, MqttClient>,
        publishes: &mut Vec<super::PendingPublish>,
        session_db_actions: &mut Vec<SessionDbAction>,
    ) {
        let multi = super::multi_worker();
        let slot = super::worker_slot();

        // The doorbell is only a wakeup hint; the first-tick query and the
        // 30s safety re-check keep correctness independent of it.
        let doorbell = crate::shmem_bridge::outbox_doorbell_seq();
        if self.last_safety_check.elapsed() >= Duration::from_secs(30) {
            self.last_safety_check = Instant::now();
            self.pending = true;
            // Cursor heartbeat: last_seen otherwise only moves when rows
            // are consumed, and an idle worker must not look dead to
            // slot-0 GC's staleness check.
            if multi {
                session_db_actions.push(SessionDbAction::TouchOutboxCursor { worker_slot: slot });
            }
        }
        if self.pending || doorbell != self.doorbell_seen {
            self.doorbell_seen = doorbell;
            let (outbox_ids, outbox_messages, view_capped) =
                fetch_batch(if multi { Some(self.cursor) } else { None });
            // One bounded batch per tick; a full fetch (or barrier-capped
            // view) keeps fetching on later ticks without a new doorbell.
            self.pending = outbox_ids.len() >= cdc_worker::CDC_BATCH_SIZE || view_capped;
            // Dequeue/advance is queued BEFORE the delivery-time cleanup
            // actions: the orphan-reclaim predicate refuses to delete a
            // message whose outbox row still exists, so within the one
            // end-of-tick transaction the dequeue must execute first. A
            // crash before that commit re-fetches and re-delivers
            // (at-least-once).
            if let Some(&max_id) = outbox_ids.last() {
                if multi {
                    self.cursor = self.cursor.max(max_id);
                    session_db_actions.push(SessionDbAction::AdvanceOutboxCursor {
                        worker_slot: slot,
                        last_id: max_id,
                    });
                } else {
                    session_db_actions.push(SessionDbAction::DrainCdcOutbox { ids: outbox_ids });
                }
            }
            if !outbox_messages.is_empty() {
                super::deliver_messages(&outbox_messages, clients, publishes, session_db_actions);
                // QoS-0 spill rows get no session_messages tracking, so
                // nothing else reclaims them: clean up after the one
                // delivery attempt (multi-worker: slot-0 GC owns it).
                if !multi {
                    for msg in &outbox_messages {
                        if msg.qos == 0 {
                            if let Some(message_id) = msg.id {
                                session_db_actions
                                    .push(SessionDbAction::CleanupOrphanedMessage { message_id });
                            }
                        }
                    }
                }
            }
        }

        // GC once per second, but a full sweep means enqueue is outpacing
        // it — sweep every tick (still bounded) until a short pass.
        if multi
            && slot == 0
            && (self.gc_backlog || self.last_gc.elapsed() >= Duration::from_secs(1))
        {
            self.last_gc = Instant::now();
            let (swept, stale) = gc_below_min_cursor();
            self.gc_backlog = swept >= cdc_worker::CDC_BATCH_SIZE;
            crate::metrics::get()
                .outbox_stale_cursors
                .store(stale, std::sync::atomic::Ordering::Relaxed);
            if stale != self.stale_seen {
                log!(
                    "pgmqtt: outbox GC ignoring {} stale delivery cursor(s), was {} — \
                     a worker has not heartbeated within pgmqtt.outbox_cursor_stale_secs",
                    stale,
                    self.stale_seen
                );
                self.stale_seen = stale;
            }
        }

        // Small QoS 0 rides the shared-memory ring; draining an empty ring
        // is just a lock.
        let bridged_inline = crate::shmem_bridge::drain_inline();
        if !bridged_inline.is_empty() {
            let inline_messages: Vec<MqttMessage> = bridged_inline
                .into_iter()
                .map(|(topic, payload)| MqttMessage {
                    id: None,
                    topic: Arc::from(topic.as_str()),
                    payload: Arc::from(payload),
                    qos: 0,
                })
                .collect();
            super::deliver_messages(&inline_messages, clients, publishes, session_db_actions);
        }
    }
}
