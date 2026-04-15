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
    client.update(
        "DELETE FROM pgmqtt_messages \
         WHERE id = $1 AND retain = false \
           AND NOT EXISTS (SELECT 1 FROM pgmqtt_session_messages WHERE message_id = $1) \
           AND NOT EXISTS (SELECT 1 FROM pgmqtt_inbound_pending WHERE message_id = $1)",
        None,
        &args,
    )?;
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
}

/// Execute all queued DB actions in a single atomic transaction.
///
/// Each action is applied in order. If any operation fails, the entire
/// transaction rolls back (and the same actions will be retried on the
/// next poll loop). This guarantees at-least-once semantics.
pub fn execute_session_db_actions(actions: Vec<SessionDbAction>) {
    if actions.is_empty() {
        return;
    }

    BackgroundWorker::transaction(move || {
        let _ = pgrx::spi::Spi::connect_mut(|client| {
            let m = crate::metrics::get();
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
                        // Build multi-row VALUES clause: ($1, $2, ...), ($3, $4, ...), etc.
                        let mut values_clauses = Vec::new();
                        let mut args: Vec<DatumWithOid> = vec![message_id.into()];
                        let mut param_idx = 2;

                        for (client_id, packet_id) in &entries {
                            let pid_arg = packet_id.map(|p| p as i32);
                            values_clauses.push(format!(
                                "($1, ${}, ${}, CASE WHEN ${} IS NULL THEN NULL ELSE now() END)",
                                param_idx,
                                param_idx + 1,
                                param_idx + 1
                            ));
                            args.push(client_id.as_str().into());
                            args.push(pid_arg.into());
                            param_idx += 2;
                        }

                        let values_str = values_clauses.join(",");
                        let query = format!(
                            "INSERT INTO pgmqtt_session_messages (message_id, client_id, packet_id, sent_at) \
                             VALUES {} \
                             ON CONFLICT (client_id, message_id) DO NOTHING",
                            values_str
                        );

                        if let Err(e) = client.update(&query, None, &args) {
                            crate::metrics::inc(&m.db_message_errors);
                            pgrx::log!("pgmqtt: failed to batch insert messages for message {}: {}", message_id, e);
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
                        if let Err(e) = client.update(
                            "UPDATE pgmqtt_session_messages SET packet_id = $1, sent_at = now() \
                             WHERE client_id = $2 AND message_id = $3",
                            None,
                            &args,
                        ) {
                            crate::metrics::inc(&m.db_message_errors);
                            pgrx::log!("pgmqtt: failed to update message {} as inflight for session '{}': {}", message_id, client_id, e);
                        }
                    }
                    SessionDbAction::DeleteMessage {
                        client_id,
                        message_id,
                    } => {
                        let args: Vec<DatumWithOid> =
                            vec![client_id.as_str().into(), message_id.into()];
                        if let Err(e) = client.update(
                            "DELETE FROM pgmqtt_session_messages WHERE client_id = $1 AND message_id = $2",
                            None,
                            &args,
                        ) {
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
                }
            }
            crate::metrics::inc(&m.db_batches_committed);
            Ok::<_, pgrx::spi::Error>(())
        });
    });
}
