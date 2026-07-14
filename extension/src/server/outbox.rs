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

/// Publish this process's enqueue floor from the current pgmqtt_messages
/// sequence position. Must run inside the enqueuing write transaction,
/// *before* its first message insert: every id the transaction goes on to
/// allocate is then strictly greater than the floor, so delivery cursors
/// (which stop at the minimum active floor) cannot pass an id this
/// transaction might still commit. The caller clears the floor after the
/// transaction ends.
pub(super) fn arm_enqueue_floor(
    client: &mut pgrx::spi::SpiClient<'_>,
    floor_slot: usize,
) -> Result<(), pgrx::spi::Error> {
    let floor: i64 = client
        .select(
            // Sequence state is non-transactional: last_value covers every
            // allocation so far, ours all come later. is_called = false only
            // on a virgin sequence, whose first nextval returns last_value
            // itself.
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

/// Claim `(message_id, shared-group)` pairs for this worker. Every worker
/// matches its local group members against every outbox row, so without
/// arbitration a group with members on N workers receives each message N
/// times; the claims table's primary key picks one winner cluster-wide.
///
/// A claim this worker already holds counts as won: claims commit before
/// delivery, so a worker that crashed between claiming and durably
/// recording delivery re-fetches the message (its cursor didn't advance)
/// and must be able to re-win its own claim — losing that race against
/// itself would strand the message for the group. Redelivery after such a
/// crash is at-least-once, as everywhere else.
///
/// Returns the pairs this worker won, or `None` on error — the caller then
/// delivers unclaimed (duplicates beat losing the message for the group).
pub(super) fn claim_share_groups(
    pairs: &[(i64, &str)],
) -> Option<std::collections::HashSet<(i64, String)>> {
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
            let mut won = std::collections::HashSet::new();
            for row in table {
                let id: Option<i64> = row.get_by_name("message_id")?;
                let group: Option<String> = row.get_by_name("group_key")?;
                if let (Some(id), Some(group)) = (id, group) {
                    won.insert((id, group));
                }
            }
            Ok::<_, pgrx::spi::Error>(won)
        })
    })
    .ok()
}

/// Slot-0 GC (multi-worker): delete outbox rows every worker's cursor has
/// passed, reclaiming orphaned message rows along the way — this replaces
/// the per-delivery cleanup that a single worker can do safely but N
/// workers cannot (another worker may not have delivered the row yet).
/// Bounded per pass; returns the number of rows swept so the caller can
/// tell a drained pass (short batch) from one that left backlog behind.
fn gc_below_min_cursor() -> usize {
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
            for id in &ids {
                let _ = db_action::cleanup_orphaned_message(client, *id);
            }
            client.update(
                "DELETE FROM pgmqtt_share_claims \
                 WHERE message_id <= (SELECT COALESCE(MIN(last_id), 0) FROM pgmqtt_outbox_cursors)",
                None,
                &[],
            )?;
            Ok::<_, pgrx::spi::Error>(ids.len())
        })
    })
    .unwrap_or_else(|e| {
        log!("pgmqtt: outbox GC failed: {}", e);
        0
    })
}

/// Fetch one bounded batch. `after`: multi-worker cursor mode — rows stay
/// for the other workers and are reclaimed by the slot-0 GC. `None`:
/// single-worker mode — the caller deletes delivered ids.
///
/// Returns `(fetched_outbox_ids, messages, view_capped)`. Ids and messages
/// can differ: a dangling outbox id whose message row no longer exists is
/// still returned in the id list so the caller dequeues it (or advances
/// past it) instead of re-scanning it forever. `view_capped` reports that
/// an enqueue barrier truncated this fetch — rows may exist above it whose
/// doorbell already rang (or never will, if the in-flight enqueuer aborts),
/// so the caller must keep re-querying rather than wait for a new doorbell.
fn fetch_batch(after: Option<i64>) -> (Vec<i64>, Vec<MqttMessage>, bool) {
    BackgroundWorker::transaction(|| {
        pgrx::spi::Spi::connect(|client| {
            let mut ids = Vec::new();
            let mut out = Vec::new();
            let mut view_capped = false;
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
                //
                // The enqueue barrier caps the fetch: ids above it may still
                // gain a smaller committed sibling from an in-flight enqueue
                // transaction, and advancing the cursor past that hole would
                // skip the message forever (see shmem_bridge's floor docs).
                // Read *after* this transaction's snapshot was taken, so any
                // enqueuer invisible to the snapshot still holds its floor.
                let barrier = crate::shmem_bridge::enqueue_barrier();
                view_capped = barrier != i64::MAX;
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
    /// Set when the last GC pass swept a full batch: enqueue may outpace a
    /// once-per-second bounded sweep, so a full pass keeps GC running every
    /// tick until it catches up (otherwise the table grows without bound).
    gc_backlog: bool,
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
            let (outbox_ids, outbox_messages, view_capped) =
                fetch_batch(if multi { Some(self.cursor) } else { None });
            // One batch per tick keeps this loop's CDC work bounded even
            // against a huge backlog; a full fetch means more may be
            // waiting, so keep fetching on subsequent ticks without needing
            // another doorbell. A barrier-capped view also keeps fetching:
            // the rows above the barrier may never get another doorbell.
            self.pending = outbox_ids.len() >= cdc_worker::CDC_BATCH_SIZE || view_capped;
            // Queued before the delivery-time cleanup actions: everything
            // commits in one end-of-tick transaction, and the orphan-reclaim
            // predicate refuses to delete a message whose outbox row still
            // exists — the dequeue must execute first within that
            // transaction for single-worker reclaim to see it gone.
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
        }

        // GC normally runs once per second, but a full sweep means enqueue
        // is outpacing one bounded batch per second — keep sweeping every
        // tick (still one bounded batch per tick, so the loop stays
        // responsive) until a short pass shows it caught up.
        if multi
            && slot == 0
            && (self.gc_backlog || self.last_gc.elapsed() >= Duration::from_secs(1))
        {
            self.last_gc = Instant::now();
            self.gc_backlog = gc_below_min_cursor() >= cdc_worker::CDC_BATCH_SIZE;
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
