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

/// Ensure this worker has a cursor row and return its position. A brand-new
/// slot starts at the minimum of the existing cursors (never behind the GC
/// watermark, so it can't be handed already-reclaimed rows); the very first
/// boot starts everyone at 0. Serialized under the startup advisory lock
/// like every other worker-startup write.
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
                 ON CONFLICT (worker_slot) DO NOTHING",
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

/// Slot-0 GC (multi-worker): delete outbox rows every worker's cursor has
/// passed, reclaiming orphaned message rows along the way — this replaces
/// the per-delivery cleanup that a single worker can do safely but N
/// workers cannot (another worker may not have delivered the row yet).
/// Bounded per pass.
fn gc_below_min_cursor() {
    BackgroundWorker::transaction(|| {
        pgrx::spi::Spi::connect_mut(|client| {
            let swept = client.update(
                &format!(
                    "DELETE FROM pgmqtt_cdc_outbox \
                     WHERE id IN (\
                         SELECT id FROM pgmqtt_cdc_outbox \
                         WHERE id <= (SELECT COALESCE(MIN(last_id), 0) FROM pgmqtt_outbox_cursors) \
                         ORDER BY id LIMIT {}) \
                     RETURNING id",
                    cdc_worker::CDC_BATCH_SIZE
                ),
                None,
                &[],
            )?;
            let ids: Vec<i64> = swept
                .into_iter()
                .filter_map(|row| row.get_by_name::<i64, _>("id").ok().flatten())
                .collect();
            for id in ids {
                let _ = db_action::cleanup_orphaned_message(client, id);
            }
            Ok::<_, pgrx::spi::Error>(())
        })
    })
    .unwrap_or_else(|e| {
        log!("pgmqtt: outbox GC failed: {}", e);
    });
}

/// Fetch one bounded batch. `after`: multi-worker cursor mode — rows stay
/// for the other workers and are reclaimed by the slot-0 GC. `None`:
/// single-worker mode — the caller deletes delivered ids.
///
/// Returns `(fetched_outbox_ids, messages)`. The two can differ: a dangling
/// outbox id whose message row no longer exists is still returned in the id
/// list so the caller dequeues it (or advances past it) instead of
/// re-scanning it forever.
fn fetch_batch(after: Option<i64>) -> (Vec<i64>, Vec<MqttMessage>) {
    BackgroundWorker::transaction(|| {
        pgrx::spi::Spi::connect(|client| {
            let mut ids = Vec::new();
            let mut out = Vec::new();
            if let Some(cursor) = after {
                // Multi-worker: every worker scans every outbox row, so
                // payloads are the read amplification. Fetch ids + topics
                // first and pull full rows only for topics this worker's
                // own subscription tree matches. Safe where a
                // worker-local-state fast path is not: the cursor still
                // advances over non-matching ids, so only *local* delivery
                // is skipped — other workers judge against their own trees,
                // and reclamation stays with the slot-0 GC either way.
                // has_subscribers() is side-effect-free (no $share
                // round-robin advance), so deliver_messages' later
                // match_topic is unaffected. False positives only cost a
                // payload fetch; false negatives can't happen (same trie +
                // shared-group walk).
                let query = format!(
                    "SELECT o.id, m.topic \
                     FROM pgmqtt_cdc_outbox o \
                     LEFT JOIN pgmqtt_messages m ON m.id = o.id \
                     WHERE o.id > {} \
                     ORDER BY o.id \
                     LIMIT {}",
                    cursor,
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
                        let id: i64 = match row.get_by_name("id")? {
                            Some(v) => v,
                            None => continue,
                        };
                        let topic: Option<String> = row.get_by_name("topic")?;
                        let Some(topic) = topic else {
                            continue;
                        };
                        let payload: Vec<u8> = row.get_by_name("payload")?.unwrap_or_default();
                        let qos: i32 = row.get_by_name("qos")?.unwrap_or(0);
                        out.push(MqttMessage {
                            id: Some(id),
                            topic: Arc::from(topic.as_str()),
                            payload: Arc::from(payload),
                            qos: qos as u8,
                        });
                    }
                }
            } else {
                // Single worker deletes on delivery, so each row is fetched
                // exactly once — and it needs every message regardless of
                // local matches: the no-subscriber orphan reclaim and the
                // oversize-QoS-0 spill cleanup both key off this batch.
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
                    let id: i64 = match row.get_by_name("id")? {
                        Some(v) => v,
                        None => continue,
                    };
                    ids.push(id);
                    let topic: Option<String> = row.get_by_name("topic")?;
                    let Some(topic) = topic else {
                        continue;
                    };
                    let payload: Vec<u8> = row.get_by_name("payload")?.unwrap_or_default();
                    let qos: i32 = row.get_by_name("qos")?.unwrap_or(0);
                    out.push(MqttMessage {
                        id: Some(id),
                        topic: Arc::from(topic.as_str()),
                        payload: Arc::from(payload),
                        qos: qos as u8,
                    });
                }
            }
            Ok::<_, pgrx::spi::Error>((ids, out))
        })
    })
    .unwrap_or_else(|e| {
        log!("pgmqtt: failed to fetch pending CDC outbox batch: {}", e);
        (Vec::new(), Vec::new())
    })
}

/// Per-tick outbox consumption for a Bridged (enterprise) socket worker,
/// including the small-QoS-0 shared-memory ring.
pub(super) struct Drain {
    /// Starts true so the first tick always queries: rows queued before a
    /// restart are recovered without depending on the shared-memory
    /// doorbell, which does not survive a postmaster crash.
    pending: bool,
    doorbell_seen: u64,
    /// Multi-worker delivery cursor, seeded from `pgmqtt_outbox_cursors` so
    /// a restarted worker resumes where its predecessor durably left off.
    cursor: i64,
    last_safety_check: Instant,
    last_gc: Instant,
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

        // The doorbell is only a wakeup hint that lets idle ticks skip the
        // SPI query; correctness never depends on it (first-tick query,
        // plus a slow safety re-check for a failed end-of-tick action
        // transaction that left rows queued with no new doorbell coming).
        let doorbell = crate::shmem_bridge::outbox_doorbell_seq();
        if self.last_safety_check.elapsed() >= Duration::from_secs(30) {
            self.last_safety_check = Instant::now();
            self.pending = true;
        }
        if self.pending || doorbell != self.doorbell_seen {
            self.doorbell_seen = doorbell;
            let (outbox_ids, outbox_messages) =
                fetch_batch(if multi { Some(self.cursor) } else { None });
            // One batch per tick keeps this loop's CDC work bounded even
            // against a huge backlog; a full fetch means more may be
            // waiting, so keep fetching on subsequent ticks without needing
            // another doorbell.
            self.pending = outbox_ids.len() >= cdc_worker::CDC_BATCH_SIZE;
            if !outbox_messages.is_empty() {
                super::deliver_messages(&outbox_messages, clients, publishes, session_db_actions);
                // Oversize-QoS-0 spill rows get no session_messages
                // tracking (QoS 0 has no PUBACK), so nothing else would
                // ever reclaim them: clean up right after the one delivery
                // attempt. (Multi-worker: the slot-0 GC owns all
                // reclamation instead.)
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
            if let Some(&max_id) = outbox_ids.last() {
                if multi {
                    // Rows are shared with the other workers and reclaimed
                    // by the slot-0 GC once every cursor has passed them.
                    // The advance commits with this tick's delivery state; a
                    // crash re-fetches from the old cursor (at-least-once),
                    // so the in-memory cursor can move immediately.
                    self.cursor = self.cursor.max(max_id);
                    session_db_actions.push(SessionDbAction::AdvanceOutboxCursor {
                        worker_slot: slot,
                        last_id: max_id,
                    });
                } else {
                    // Committed atomically with this tick's delivery state:
                    // a crash before that commit leaves the rows queued and
                    // they are re-fetched and re-delivered — at-least-once.
                    session_db_actions.push(SessionDbAction::DrainCdcOutbox { ids: outbox_ids });
                }
            }
        }

        if multi && slot == 0 && self.last_gc.elapsed() >= Duration::from_secs(1) {
            self.last_gc = Instant::now();
            gc_below_min_cursor();
        }

        // Small QoS 0 messages travel through shared memory only; draining
        // is a lock + memcpy when the ring is empty, which is the common
        // case.
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
