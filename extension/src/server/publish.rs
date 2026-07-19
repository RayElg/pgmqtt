//! The publish pipeline: split transient from persistent, persist (with a
//! synchronous or asynchronous commit), deliver, and — for asynchronous
//! commits — the [`DeferredQueue`] that gates client-visible effects on the
//! WAL flush pointer.

use super::{
    db_action, deliver_messages, latch_interval, multi_worker, outbox, wal, MqttClient,
    MqttMessage, SessionDbAction,
};
use crate::mqtt;
use pgrx::bgworkers::BackgroundWorker;
use pgrx::log;
use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

pub(super) struct PendingPublish {
    pub(super) topic: Arc<str>,
    pub(super) payload: Arc<[u8]>,
    pub(super) qos: u8,
    pub(super) retain: bool,
    pub(super) log_sender: String,
    pub(super) packet_id: Option<u16>,
    /// Mapping names that matched this publish for inbound table writes.
    /// Only populated for QoS >= 1; the persist path inserts tracking rows
    /// into `pgmqtt_inbound_pending` atomically with message persistence.
    pub(super) inbound_mappings: Vec<Arc<str>>,
}

fn split_publishes(
    pending: Vec<PendingPublish>,
) -> (Vec<PendingPublish>, Vec<PendingPublish>) {
    let mut persistent = Vec::new();
    let mut transient = Vec::new();
    for p in pending {
        if p.qos > 0 || p.retain {
            persistent.push(p);
        } else {
            transient.push(p);
        }
    }
    (persistent, transient)
}

fn send_puback(clients: &mut HashMap<String, MqttClient>, client_id: &str, packet_id: u16) {
    if let Some(client) = clients.get_mut(client_id) {
        let puback = mqtt::build_puback(packet_id);
        let _ = client.transport.write_all(&puback);
        crate::metrics::inc(&crate::metrics::get().pubacks_sent);
    }
}

/// Persist a run of plain (non-retain) publishes with one set-based INSERT.
///
/// `unnest ... WITH ORDINALITY ... ORDER BY ord` inserts rows in array order,
/// so the sequence assigns ascending ids in that order and the sorted
/// RETURNING ids map one-to-one onto the run.
fn persist_plain_run(
    client: &mut pgrx::spi::SpiClient<'_>,
    run: &[PendingPublish],
    to_publish: &mut Vec<MqttMessage>,
    inbound_rows: &mut Vec<(i64, Arc<str>)>,
) -> Result<(), pgrx::spi::Error> {
    if run.is_empty() {
        return Ok(());
    }
    let topics: Vec<&str> = run.iter().map(|p| &*p.topic).collect();
    let payloads: Vec<&[u8]> = run.iter().map(|p| &*p.payload).collect();
    let qos: Vec<i32> = run.iter().map(|p| p.qos as i32).collect();
    let args: Vec<pgrx::datum::DatumWithOid> = vec![topics.into(), payloads.into(), qos.into()];
    // NULLIF keeps the empty-payload representation identical to
    // db_action::persist_message (empty -> NULL).
    let table = client.update(
        "INSERT INTO pgmqtt_messages (topic, payload, qos, retain) \
         SELECT u.topic, NULLIF(u.payload, ''::bytea), u.qos, false \
         FROM unnest($1::text[], $2::bytea[], $3::int[]) \
              WITH ORDINALITY AS u(topic, payload, qos, ord) \
         ORDER BY u.ord \
         RETURNING id",
        None,
        &args,
    )?;
    let mut ids: Vec<i64> = Vec::with_capacity(run.len());
    for row in table {
        if let Ok(Some(id)) = row.get_by_name::<i64, _>("id") {
            ids.push(id);
        }
    }
    if ids.len() != run.len() {
        return Err(pgrx::spi::Error::SpiError(
            pgrx::spi::SpiErrorCodes::NoAttribute,
        ));
    }
    ids.sort_unstable();
    for (p, &id) in run.iter().zip(&ids) {
        for mapping_name in &p.inbound_mappings {
            inbound_rows.push((id, mapping_name.clone()));
        }
        if crate::get_debug_log_guc() {
            log!(
                "pgmqtt: pushing message from '{}' to topic '{}' with qos={}",
                p.log_sender,
                p.topic,
                p.qos
            );
        }
        to_publish.push(MqttMessage {
            id: Some(id),
            topic: p.topic.clone(),
            payload: p.payload.clone(),
            qos: p.qos,
        });
    }
    Ok(())
}

/// Persist one batch of QoS >= 1 / retained publishes in one transaction.
/// With `synchronous = false` the commit skips its WAL flush: rows are
/// visible and ordered, but the caller MUST NOT emit client-visible
/// effects until the flush LSN passes the commit — see [`DeferredQueue`].
fn persist_publish_batch(
    persistent: &[PendingPublish],
    synchronous: bool,
) -> (Vec<MqttMessage>, bool) {
    let multi = multi_worker();
    let floor_slot = super::worker_slot().max(0) as usize;
    let result = BackgroundWorker::transaction(|| {
        let mut to_publish = Vec::new();
        pgrx::spi::Spi::connect_mut(|client| {
            if !synchronous {
                let _ = client.select(
                    "SELECT set_config('synchronous_commit', 'off', true)",
                    None,
                    &[],
                );
            }
            if multi {
                // Floor before the first insert, or delivery cursors
                // could pass ids this transaction commits later.
                outbox::arm_enqueue_floor(client, floor_slot)?;
            }
            let mut inbound_rows: Vec<(i64, Arc<str>)> = Vec::new();
            let mut i = 0;
            while i < persistent.len() {
                if !persistent[i].retain {
                    // Runs of plain publishes become one INSERT; chunking
                    // by runs (not partitioning) keeps id order = batch
                    // order across a retain/plain mix.
                    let start = i;
                    while i < persistent.len() && !persistent[i].retain {
                        i += 1;
                    }
                    persist_plain_run(
                        client,
                        &persistent[start..i],
                        &mut to_publish,
                        &mut inbound_rows,
                    )?;
                    continue;
                }
                let p = &persistent[i];
                i += 1;
                let mut msg_id_opt: Option<i64> = None;

                // MQTT-3.3.1-6/7/10: clear pgmqtt_retained, persisting the
                // forwarded clear at QoS 1 for reconnect redelivery.
                // Multi-worker persists the QoS 0 clear too — an
                // unpersisted clear can't travel the outbox and would reach
                // no subscriber on any worker.
                if p.retain && p.payload.is_empty() {
                    let topic_ref: &str = &p.topic;
                    let args: Vec<pgrx::datum::DatumWithOid> = vec![topic_ref.into()];
                    let table = client.update(
                        "DELETE FROM pgmqtt_retained WHERE topic = $1 RETURNING message_id",
                        None,
                        &args,
                    )?;
                    for row in table {
                        if let Ok(Some(old_id)) = row.get_by_name::<i64, _>("message_id") {
                            db_action::cleanup_orphaned_message(client, old_id)?;
                        }
                    }
                    if p.qos > 0 || multi {
                        let msg_id = db_action::persist_message(
                            client,
                            &p.topic,
                            &p.payload,
                            p.qos,
                            false,
                        )?;
                        msg_id_opt = Some(msg_id);
                    }
                } else {
                    let msg_id = db_action::persist_message(
                        client,
                        &p.topic,
                        &p.payload,
                        p.qos,
                        p.retain,
                    )?;
                    msg_id_opt = Some(msg_id);

                    if p.retain {
                        let topic_ref: &str = &p.topic;
                        let args: Vec<pgrx::datum::DatumWithOid> =
                            vec![topic_ref.into(), msg_id.into()];
                        let table = client.update(
                            "WITH old AS ( \
                                 SELECT message_id AS old_id FROM pgmqtt_retained WHERE topic = $1 \
                             ), upsert AS ( \
                                 INSERT INTO pgmqtt_retained (topic, message_id) VALUES ($1, $2) \
                                 ON CONFLICT (topic) DO UPDATE SET message_id = EXCLUDED.message_id \
                                 RETURNING 1 \
                             ) \
                             SELECT old_id FROM old, upsert",
                            None,
                            &args,
                        )?;
                        let mut old_msg_id: Option<i64> = None;
                        for row in table {
                            if let Ok(Some(id)) = row.get_by_name::<i64, _>("old_id") {
                                old_msg_id = Some(id);
                            }
                        }
                        if let Some(old_id) = old_msg_id {
                            if old_id != msg_id {
                                db_action::cleanup_orphaned_message(client, old_id)?;
                            }
                        }
                    }
                }

                if let Some(msg_id) = msg_id_opt {
                    for mapping_name in &p.inbound_mappings {
                        inbound_rows.push((msg_id, mapping_name.clone()));
                    }
                }

                if crate::get_debug_log_guc() {
                    log!(
                        "pgmqtt: pushing message from '{}' to topic '{}' with qos={}",
                        p.log_sender,
                        p.topic,
                        p.qos
                    );
                }
                to_publish.push(MqttMessage {
                    id: msg_id_opt,
                    topic: p.topic.clone(),
                    payload: p.payload.clone(),
                    qos: p.qos,
                });
            }

            // Inbound-pending tracking rows (virtual subscriber), committed
            // atomically with the messages so the PUBACK reflects durable
            // intent to process.
            if !inbound_rows.is_empty() {
                let msg_ids: Vec<i64> = inbound_rows.iter().map(|(id, _)| *id).collect();
                let mappings: Vec<&str> = inbound_rows.iter().map(|(_, m)| &**m).collect();
                let args: Vec<pgrx::datum::DatumWithOid> = vec![msg_ids.into(), mappings.into()];
                client.update(
                    "INSERT INTO pgmqtt_inbound_pending (message_id, mapping_name) \
                     SELECT u.message_id, u.mapping_name \
                     FROM unnest($1::bigint[], $2::text[]) AS u(message_id, mapping_name) \
                     ON CONFLICT DO NOTHING",
                    None,
                    &args,
                )?;
            }

            // Multi-worker: every persisted publish rides the outbox (no
            // direct local delivery), enqueued in this same transaction.
            if multi {
                let outbox_ids: Vec<i64> = to_publish.iter().filter_map(|m| m.id).collect();
                if !outbox_ids.is_empty() {
                    outbox::enqueue(client, &outbox_ids)?;
                }
            }
            Ok::<_, pgrx::spi::Error>(())
        })?;
        Ok::<(Vec<MqttMessage>, bool), pgrx::spi::Error>((to_publish, true))
    })
    .unwrap_or((Vec::new(), false));
    if multi {
        crate::shmem_bridge::clear_enqueue_floor(floor_slot);
    }
    result
}

/// Deliver transient (QoS 0, non-retained) publishes immediately — no
/// durability contract gates them. Returns cascading will-publishes for
/// the caller's own publish path.
fn deliver_transient(
    transient: Vec<PendingPublish>,
    clients: &mut HashMap<String, MqttClient>,
    session_db_actions: &mut Vec<SessionDbAction>,
) -> Vec<PendingPublish> {
    let transient_msgs: Vec<MqttMessage> = transient
        .into_iter()
        .map(|p| {
            log!(
                "pgmqtt: pushing transient message from '{}' to topic '{}' with qos=0",
                p.log_sender,
                p.topic
            );
            MqttMessage {
                id: None,
                topic: p.topic,
                payload: p.payload,
                qos: 0,
            }
        })
        .collect();
    let mut cascade = Vec::new();
    deliver_messages(&transient_msgs, clients, &mut cascade, session_db_actions);
    cascade
}

pub(super) fn publish_messages_batch(
    pending: Vec<PendingPublish>,
    clients: &mut HashMap<String, MqttClient>,
    session_db_actions: &mut Vec<SessionDbAction>,
) {
    if pending.is_empty() {
        return;
    }
    // Multi-worker routes everything (QoS 0 included) through the outbox
    // — the explicit trade: QoS 0 loses its no-DB fast path.
    let multi = multi_worker();
    let (persistent, transient) = if multi {
        (pending, Vec::new())
    } else {
        split_publishes(pending)
    };

    if !persistent.is_empty() {
        let (to_publish, ok) = persist_publish_batch(&persistent, true);

        if ok {
            let mut cascade = Vec::new();
            if multi {
                crate::shmem_bridge::ring_outbox_doorbell();
            } else {
                deliver_messages(&to_publish, clients, &mut cascade, session_db_actions);
            }

            // PUBACKs only after successful commit — MQTT at-least-once.
            for p in &persistent {
                if p.qos == 1 {
                    if let Some(pid) = p.packet_id {
                        send_puback(clients, &p.log_sender, pid);
                    }
                }
            }

            // Will messages from cascading disconnects (rare: socket write
            // failure during delivery). Bounded by number of clients.
            if !cascade.is_empty() {
                publish_messages_batch(cascade, clients, session_db_actions);
            }
        }
    }

    if !transient.is_empty() {
        let cascade = deliver_transient(transient, clients, session_db_actions);
        if !cascade.is_empty() {
            publish_messages_batch(cascade, clients, session_db_actions);
        }
    }
}

/// An async-committed batch whose client-visible effects are parked until
/// the flush LSN reaches `watermark` (commit record on disk). Watermarks
/// are captured in commit order, so FIFO release preserves publish order.
struct DeferredRelease {
    watermark: u64,
    queued_at: std::time::Instant,
    messages: Vec<MqttMessage>,
    /// `(client_id, packet_id)` PUBACKs owed once durable.
    pubacks: Vec<(String, u16)>,
    /// Multi-worker: ring the doorbell at release, not commit — siblings
    /// must not deliver before the durability point the PUBACKs promise.
    ring_doorbell: bool,
}

/// How long a [`DeferredRelease`] may wait before the socket loop stops
/// trusting the off-loop flush path and pays one synchronous flush itself
/// — one bounded fsync beats a wal_writer_delay-sized (200 ms) PUBACK
/// tail. Worst case degrades to pre-split behavior, never below it.
fn flush_fallback_after() -> Duration {
    std::cmp::max(latch_interval() * 4, Duration::from_millis(20))
}

/// FIFO of [`DeferredRelease`] batches owned by a Bridged socket worker.
#[derive(Default)]
pub(super) struct DeferredQueue {
    queue: VecDeque<DeferredRelease>,
}

impl DeferredQueue {
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Release every durably-flushed batch: deliver, send owed PUBACKs
    /// (FIFO — watermarks are monotonic). If the flush pointer lags, ask
    /// the CDC worker to force it; the fsync happens off this loop.
    pub(super) fn release_due(
        &mut self,
        clients: &mut HashMap<String, MqttClient>,
        publishes: &mut Vec<PendingPublish>,
        session_db_actions: &mut Vec<SessionDbAction>,
    ) {
        if self.queue.is_empty() {
            return;
        }
        let mut flush = wal::read_lsn("pg_current_wal_flush_lsn()");
        // Oldest batch outlived the beacon round trip: pay one bounded
        // synchronous flush here rather than a 200 ms PUBACK tail.
        if self.queue.front().is_some_and(|d| {
            flush.map_or(true, |f| d.watermark > f)
                && d.queued_at.elapsed() >= flush_fallback_after()
        }) {
            if wal::force_flush() {
                // A successful flush covers every earlier commit, including
                // batches whose watermark capture failed outright.
                for d in self.queue.iter_mut() {
                    if d.watermark == wal::WATERMARK_UNCONFIRMED {
                        d.watermark = 0;
                    }
                }
            }
            flush = wal::read_lsn("pg_current_wal_flush_lsn()");
        }
        let mut ring = false;
        while self
            .queue
            .front()
            .is_some_and(|d| flush.is_some_and(|f| d.watermark <= f))
        {
            let released = self.queue.pop_front().expect("front checked");
            ring |= released.ring_doorbell;
            deliver_messages(&released.messages, clients, publishes, session_db_actions);
            for (client_id, pid) in released.pubacks {
                send_puback(clients, &client_id, pid);
            }
        }
        if ring {
            // Durability confirmed — now siblings may fetch and deliver.
            crate::shmem_bridge::ring_outbox_doorbell();
        }
        if !self.queue.is_empty() {
            crate::shmem_bridge::request_wal_flush();
        }
    }

    /// Shutdown: one synchronous flush (blocking is fine here), then
    /// deliver and ack everything parked. If the flush fails, PUBACKs are
    /// withheld — publishers retransmit rather than being told an
    /// unflushed message is safe.
    pub(super) fn release_all(
        &mut self,
        clients: &mut HashMap<String, MqttClient>,
        session_db_actions: &mut Vec<SessionDbAction>,
    ) {
        if self.queue.is_empty() {
            return;
        }
        let flushed = wal::force_flush();
        if !flushed {
            log!(
                "pgmqtt: shutdown WAL flush failed — withholding deferred PUBACKs; \
                 publishers will retransmit"
            );
        }
        let mut cascade = Vec::new();
        let mut ring = false;
        for released in self.queue.drain(..) {
            ring |= released.ring_doorbell && flushed;
            deliver_messages(&released.messages, clients, &mut cascade, session_db_actions);
            if flushed {
                for (client_id, pid) in released.pubacks {
                    send_puback(clients, &client_id, pid);
                }
            }
        }
        if ring {
            crate::shmem_bridge::ring_outbox_doorbell();
        }
        if !cascade.is_empty() {
            publish_messages_batch(cascade, clients, session_db_actions);
        }
    }
}

/// Bridged variant of [`publish_messages_batch`]: async commit, delivery +
/// PUBACKs parked in the [`DeferredQueue`]. Only sound with the process
/// split — inline CDC's sync commits would force catch-up flushes on this
/// loop anyway. Transient messages still deliver immediately (QoS 0 may
/// overtake QoS 1: MQTT ordering is per-QoS-flow).
pub(super) fn publish_messages_batch_deferred(
    pending: Vec<PendingPublish>,
    clients: &mut HashMap<String, MqttClient>,
    session_db_actions: &mut Vec<SessionDbAction>,
    deferred: &mut DeferredQueue,
) {
    if pending.is_empty() {
        return;
    }
    let multi = multi_worker();
    let (persistent, transient) = if multi {
        (pending, Vec::new())
    } else {
        split_publishes(pending)
    };

    if !persistent.is_empty() {
        let (messages, ok) = persist_publish_batch(&persistent, false);
        if ok {
            let pubacks = persistent
                .iter()
                .filter(|p| p.qos == 1)
                .filter_map(|p| p.packet_id.map(|pid| (p.log_sender.clone(), pid)))
                .collect();
            deferred.queue.push_back(DeferredRelease {
                watermark: wal::capture_insert_watermark(),
                queued_at: std::time::Instant::now(),
                messages: if multi { Vec::new() } else { messages },
                pubacks,
                // The doorbell rings at release, not here: waking the other
                // workers now would let them deliver QoS 1+ effects before
                // the durability point the deferred PUBACKs promise.
                ring_doorbell: multi,
            });
        }
    }

    if !transient.is_empty() {
        let cascade = deliver_transient(transient, clients, session_db_actions);
        if !cascade.is_empty() {
            publish_messages_batch_deferred(cascade, clients, session_db_actions, deferred);
        }
    }
}
