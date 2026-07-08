//! Database actions for session/message/subscription operations.
//!
//! `SessionDbAction` collects mutations during an event loop iteration,
//! which are then executed atomically in a single transaction via
//! `execute_session_db_actions`.

use pgrx::bgworkers::BackgroundWorker;
use pgrx::datum::DatumWithOid;
use pgrx::spi;

/// Persist a message to `pgmqtt_messages`, returning the generated ID.
///
/// Used by both the CDC path and the client PUBLISH path.
pub fn persist_message(
    client: &mut spi::SpiClient<'_>,
    topic: &str,
    payload: &[u8],
    qos: u8,
    retain: bool,
) -> Result<i64, spi::Error> {
    let payload_arg: Option<&[u8]> = if payload.is_empty() { None } else { Some(payload) };
    let spi_args: Vec<DatumWithOid> = vec![
        topic.into(),
        payload_arg.into(),
        (qos as i32).into(),
        retain.into(),
    ];
    let table = client.update(
        "INSERT INTO pgmqtt_messages (topic, payload, qos, retain) \
         VALUES ($1, $2, $3, $4) RETURNING id",
        None,
        &spi_args,
    )?;
    for row in table {
        if let Ok(Some(id)) = row.get_by_name::<i64, _>("id") {
            return Ok(id);
        }
    }
    Err(spi::Error::SpiError(spi::SpiErrorCodes::NoAttribute))
}

/// Delete a message from `pgmqtt_messages` if it has no remaining references
/// (no session_messages, no inbound_pending) and is not retained.
///
/// Safe to call unconditionally — does nothing if references still exist.
pub fn cleanup_orphaned_message(
    client: &mut spi::SpiClient<'_>,
    message_id: i64,
) -> Result<(), spi::Error> {
    let args: Vec<DatumWithOid> = vec![message_id.into()];
    let result = match crate::statements::with_plans(|p| {
        client.update(&p.del_orphan_msg, None, &args)
    }) {
        Some(r) => r,
        None => client.update(
            "DELETE FROM pgmqtt_messages \
             WHERE id = $1 \
               AND NOT EXISTS \
                 (SELECT 1 FROM pgmqtt_session_messages WHERE message_id = $1) \
               AND NOT EXISTS \
                 (SELECT 1 FROM pgmqtt_inbound_pending WHERE message_id = $1) \
               AND NOT EXISTS \
                 (SELECT 1 FROM pgmqtt_retained WHERE message_id = $1)",
            None,
            &args,
        ),
    };
    result?;
    Ok(())
}

/// Batched database operation: insert/update/delete session, message, or subscription.
#[derive(Debug)]
pub enum SessionDbAction {
    /// Upsert a session row (next_packet_id, expiry_interval).
    UpsertSession {
        client_id: String,
        next_packet_id: u16,
        expiry_interval: u32,
    },
    /// Mark a session as disconnected (set disconnected_at = now).
    MarkDisconnected {
        client_id: String,
    },
    /// Delete a session and cascade-delete its messages.
    DeleteSession {
        client_id: String,
    },
    /// Batch insert multiple session_messages rows for the same message_id (one per client).
    InsertMessageBatch {
        message_id: i64,
        entries: Vec<(String, Option<u16>)>, // (client_id, packet_id)
    },
    /// Update a message's packet_id (promote from queue to inflight).
    UpdateMessageInflight {
        client_id: String,
        message_id: i64,
        packet_id: u16,
    },
    /// Delete a message row (client ACKed QoS 1).
    DeleteMessage {
        client_id: String,
        message_id: i64,
    },
    /// Insert a subscription row.
    InsertSubscription {
        client_id: String,
        topic_filter: String,
        qos: u8,
    },
    /// Delete a subscription row.
    DeleteSubscription {
        client_id: String,
        topic_filter: String,
    },
    /// Reclaim a persisted message that turned out to have no subscribers at
    /// delivery time (no-op if it's still retained or otherwise referenced).
    CleanupOrphanedMessage {
        message_id: i64,
    },
    /// Remove delivered ids from `pgmqtt_cdc_outbox` (enterprise
    /// multiprocess, single socket worker). Queued in the same tick that
    /// delivered the messages, so the dequeue commits atomically with the
    /// delivery state — if this transaction never commits, the ids stay
    /// queued and the messages are re-fetched and re-delivered
    /// (at-least-once).
    DrainCdcOutbox {
        ids: Vec<i64>,
    },
    /// Multi-worker counterpart of `DrainCdcOutbox`: rows are shared with
    /// the other socket workers, so instead of deleting, advance this
    /// worker's delivery cursor (slot-0 GC reclaims rows below the minimum
    /// cursor). GREATEST() keeps a delayed/replayed action from moving the
    /// cursor backwards.
    AdvanceOutboxCursor {
        worker_slot: i32,
        last_id: i64,
    },
}

/// Execute all queued DB actions in a single atomic transaction.
///
/// Each action is applied in order. If any operation fails, the entire
/// transaction rolls back (and the same actions will be retried on the
/// next poll loop). This guarantees at-least-once semantics.
///
/// Hot-path queries use session-level prepared statements created by
/// `crate::statements::prepare_hot_path_statements()` at BGW startup.
pub fn execute_session_db_actions(actions: Vec<SessionDbAction>) {
    execute_session_db_actions_inner(actions, true)
}

/// [`execute_session_db_actions`] with an asynchronous commit
/// (`SET LOCAL synchronous_commit = off`) — enterprise multiprocess only,
/// where the socket loop must not block on a WAL flush. Safe because every
/// action here is reconstructible or at-least-once: a lost `DrainCdcOutbox`
/// delete re-delivers, session/subscription state is re-upserted on the
/// next event, and losing the final few milliseconds of bookkeeping on a
/// postmaster crash sits inside the same recovery window as the crash
/// itself (which also destroys the in-memory state those rows mirror).
pub fn execute_session_db_actions_async(actions: Vec<SessionDbAction>) {
    execute_session_db_actions_inner(actions, false)
}

fn execute_session_db_actions_inner(actions: Vec<SessionDbAction>, synchronous: bool) {
    if actions.is_empty() {
        return;
    }

    BackgroundWorker::transaction(move || {
        let _ = pgrx::spi::Spi::connect_mut(|client| {
            let m = crate::metrics::get();

            if !synchronous {
                let _ = client.select(
                    "SELECT set_config('synchronous_commit', 'off', true)",
                    None,
                    &[],
                );
            }

            for action in actions {
                match action {
                    SessionDbAction::UpsertSession {
                        client_id,
                        next_packet_id,
                        expiry_interval,
                    } => {
                        let args: Vec<DatumWithOid> = vec![
                            client_id.as_str().into(),
                            (next_packet_id as i32).into(),
                            (expiry_interval as i32).into(),
                        ];
                        if let Err(e) = client.update(
                            "INSERT INTO pgmqtt_sessions (client_id, next_packet_id, expiry_interval, disconnected_at) \
                             VALUES ($1, $2, $3, NULL) \
                             ON CONFLICT (client_id) DO UPDATE \
                             SET next_packet_id = EXCLUDED.next_packet_id, \
                                 expiry_interval = EXCLUDED.expiry_interval, \
                                 disconnected_at = NULL",
                            None,
                            &args,
                        ) {
                            crate::metrics::inc(&m.db_session_errors);
                            pgrx::log!("pgmqtt: failed to upsert session '{}': {}", client_id, e);
                        }
                    }
                    SessionDbAction::MarkDisconnected { client_id } => {
                        let args: Vec<DatumWithOid> = vec![client_id.as_str().into()];
                        if let Err(e) = client.update(
                            "UPDATE pgmqtt_sessions SET disconnected_at = now() WHERE client_id = $1",
                            None,
                            &args,
                        ) {
                            crate::metrics::inc(&m.db_session_errors);
                            pgrx::log!("pgmqtt: failed to mark session '{}' disconnected: {}", client_id, e);
                        }
                    }
                    SessionDbAction::DeleteSession { client_id } => {
                        let args: Vec<DatumWithOid> = vec![client_id.as_str().into()];
                        if let Err(e) = client.update(
                            "DELETE FROM pgmqtt_sessions WHERE client_id = $1",
                            None,
                            &args,
                        ) {
                            crate::metrics::inc(&m.db_session_errors);
                            pgrx::log!("pgmqtt: failed to delete session '{}': {}", client_id, e);
                        }
                        // CASCADE on pgmqtt_sessions deletes this client's pgmqtt_session_messages
                        // rows. Messages that now have no remaining session_messages are cleaned up
                        // by the DeleteMessage action when each subscriber ACKs. A global sweep
                        // here would race with InsertMessageBatch in the same transaction.
                    }
                    SessionDbAction::InsertMessageBatch {
                        message_id,
                        entries,
                    } => {
                        for (cid, packet_id) in &entries {
                            let pid_arg = packet_id.map(|p| p as i32);
                            let args: Vec<DatumWithOid> = vec![
                                message_id.into(),
                                cid.as_str().into(),
                                pid_arg.into(),
                            ];
                            let result = match crate::statements::with_plans(|p| {
                                client.update(&p.ins_sess_msg, None, &args)
                            }) {
                                Some(r) => r,
                                None => client.update(
                                    "INSERT INTO pgmqtt_session_messages \
                                     (message_id, client_id, packet_id, sent_at) \
                                     VALUES ($1, $2, $3, \
                                       CASE WHEN $3 IS NULL THEN NULL ELSE now() END) \
                                     ON CONFLICT (client_id, message_id) DO NOTHING",
                                    None,
                                    &args,
                                ),
                            };
                            if let Err(e) = result {
                                crate::metrics::inc(&m.db_message_errors);
                                pgrx::log!("pgmqtt: failed to insert session_message for message {}, client '{}': {}", message_id, cid, e);
                            }
                        }
                    }
                    SessionDbAction::UpdateMessageInflight {
                        client_id,
                        message_id,
                        packet_id,
                    } => {
                        let args: Vec<DatumWithOid> = vec![
                            (packet_id as i32).into(),
                            client_id.as_str().into(),
                            message_id.into(),
                        ];
                        let result = match crate::statements::with_plans(|p| {
                            client.update(&p.upd_inflight, None, &args)
                        }) {
                            Some(r) => r,
                            None => client.update(
                                "UPDATE pgmqtt_session_messages \
                                 SET packet_id = $1, sent_at = now() \
                                 WHERE client_id = $2 AND message_id = $3",
                                None,
                                &args,
                            ),
                        };
                        if let Err(e) = result {
                            crate::metrics::inc(&m.db_message_errors);
                            pgrx::log!("pgmqtt: failed to update message {} as inflight for session '{}': {}", message_id, client_id, e);
                        }
                    }
                    SessionDbAction::DeleteMessage {
                        client_id,
                        message_id,
                    } => {
                        let del_args: Vec<DatumWithOid> =
                            vec![client_id.as_str().into(), message_id.into()];
                        let result = match crate::statements::with_plans(|p| {
                            client.update(&p.del_sess_msg, None, &del_args)
                        }) {
                            Some(r) => r,
                            None => client.update(
                                "DELETE FROM pgmqtt_session_messages \
                                 WHERE client_id = $1 AND message_id = $2",
                                None,
                                &del_args,
                            ),
                        };
                        if let Err(e) = result {
                            crate::metrics::inc(&m.db_message_errors);
                            pgrx::log!("pgmqtt: failed to delete message {} from session '{}': {}", message_id, client_id, e);
                        }
                        if let Err(e) = cleanup_orphaned_message(client, message_id) {
                            crate::metrics::inc(&m.db_message_errors);
                            pgrx::log!("pgmqtt: failed to delete orphaned message {}: {}", message_id, e);
                        }
                    }
                    SessionDbAction::InsertSubscription {
                        client_id,
                        topic_filter,
                        qos,
                    } => {
                        let args: Vec<DatumWithOid> = vec![
                            client_id.as_str().into(),
                            topic_filter.as_str().into(),
                            (qos as i32).into(),
                        ];
                        if let Err(e) = client.update(
                            "INSERT INTO pgmqtt_subscriptions (client_id, topic_filter, qos) \
                             VALUES ($1, $2, $3) \
                             ON CONFLICT (client_id, topic_filter) DO UPDATE \
                             SET qos = EXCLUDED.qos",
                            None,
                            &args,
                        ) {
                            crate::metrics::inc(&m.db_subscription_errors);
                            pgrx::log!("pgmqtt: failed to insert subscription for '{}' to '{}': {}", client_id, topic_filter, e);
                        }
                    }
                    SessionDbAction::DeleteSubscription {
                        client_id,
                        topic_filter,
                    } => {
                        let args: Vec<DatumWithOid> =
                            vec![client_id.as_str().into(), topic_filter.as_str().into()];
                        if let Err(e) = client.update(
                            "DELETE FROM pgmqtt_subscriptions WHERE client_id = $1 AND topic_filter = $2",
                            None,
                            &args,
                        ) {
                            crate::metrics::inc(&m.db_subscription_errors);
                            pgrx::log!("pgmqtt: failed to delete subscription for '{}' from '{}': {}", client_id, topic_filter, e);
                        }
                    }
                    SessionDbAction::CleanupOrphanedMessage { message_id } => {
                        if let Err(e) = cleanup_orphaned_message(client, message_id) {
                            crate::metrics::inc(&m.db_message_errors);
                            pgrx::log!(
                                "pgmqtt: failed to clean up orphaned message {} (no subscribers at delivery time): {}",
                                message_id, e
                            );
                        }
                    }
                    SessionDbAction::DrainCdcOutbox { ids } => {
                        let count = ids.len();
                        let args: Vec<DatumWithOid> = vec![ids.into()];
                        if let Err(e) = client.update(
                            "DELETE FROM pgmqtt_cdc_outbox WHERE id = ANY($1::bigint[])",
                            None,
                            &args,
                        ) {
                            crate::metrics::inc(&m.db_message_errors);
                            pgrx::log!(
                                "pgmqtt: failed to dequeue {} delivered CDC outbox ids: {}",
                                count, e
                            );
                        }
                    }
                    SessionDbAction::AdvanceOutboxCursor {
                        worker_slot,
                        last_id,
                    } => {
                        let args: Vec<DatumWithOid> =
                            vec![worker_slot.into(), last_id.into()];
                        if let Err(e) = client.update(
                            "UPDATE pgmqtt_outbox_cursors \
                             SET last_id = GREATEST(last_id, $2) \
                             WHERE worker_slot = $1",
                            None,
                            &args,
                        ) {
                            crate::metrics::inc(&m.db_message_errors);
                            pgrx::log!(
                                "pgmqtt: failed to advance outbox cursor for slot {}: {}",
                                worker_slot, e
                            );
                        }
                    }
                }
            }
            crate::metrics::inc(&m.db_batches_committed);
            Ok::<_, pgrx::spi::Error>(())
        });
    });
}
