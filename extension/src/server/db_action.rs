//! Database actions for session/message/subscription operations.
//!
//! `SessionDbAction` collects mutations during an event loop iteration,
//! which are then executed atomically in a single transaction via
//! `execute_session_db_actions`.

use pgrx::bgworkers::BackgroundWorker;
use pgrx::datum::DatumWithOid;

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
    /// Insert a message row (QoS ≥ 1 only).
    InsertMessage {
        client_id: String,
        message_id: i64,
        packet_id: Option<u16>,
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
                        let _ = client.update(
                            "INSERT INTO pgmqtt_sessions (client_id, next_packet_id, expiry_interval, disconnected_at) \
                             VALUES ($1, $2, $3, NULL) \
                             ON CONFLICT (client_id) DO UPDATE \
                             SET next_packet_id = EXCLUDED.next_packet_id, \
                                 expiry_interval = EXCLUDED.expiry_interval, \
                                 disconnected_at = NULL",
                            None,
                            &args,
                        );
                    }
                    SessionDbAction::MarkDisconnected { client_id } => {
                        let args: Vec<DatumWithOid> = vec![client_id.as_str().into()];
                        let _ = client.update(
                            "UPDATE pgmqtt_sessions SET disconnected_at = now() WHERE client_id = $1",
                            None,
                            &args,
                        );
                    }
                    SessionDbAction::DeleteSession { client_id } => {
                        let args: Vec<DatumWithOid> = vec![client_id.as_str().into()];
                        let _ = client.update(
                            "DELETE FROM pgmqtt_sessions WHERE client_id = $1",
                            None,
                            &args,
                        );
                        // Cleanup orphaned messages (CASCADE deletes session_messages, then ref_count = 0)
                        let _ = client.update(
                            "DELETE FROM pgmqtt_messages WHERE ref_count <= 0 AND retain = false",
                            None,
                            &[],
                        );
                    }
                    SessionDbAction::InsertMessage {
                        client_id,
                        message_id,
                        packet_id,
                    } => {
                        let pid_arg = packet_id.map(|p| p as i32);
                        let args: Vec<DatumWithOid> =
                            vec![client_id.as_str().into(), message_id.into(), pid_arg.into()];
                        let _ = client.update(
                            "INSERT INTO pgmqtt_session_messages (client_id, message_id, packet_id, sent_at) \
                             VALUES ($1, $2, $3, CASE WHEN $3 IS NULL THEN NULL ELSE now() END) \
                             ON CONFLICT (client_id, message_id) DO NOTHING",
                            None,
                            &args,
                        );
                        // Increment ref_count (one per session_message inserted)
                        let args2: Vec<DatumWithOid> = vec![message_id.into()];
                        let _ = client.update(
                            "UPDATE pgmqtt_messages SET ref_count = ref_count + 1 WHERE id = $1",
                            None,
                            &args2,
                        );
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

                        let _ = client.update(&query, None, &args);

                        // Increment ref_count by the number of rows we attempted to insert
                        let args2: Vec<DatumWithOid> = vec![message_id.into(), (entries.len() as i32).into()];
                        let _ = client.update(
                            "UPDATE pgmqtt_messages SET ref_count = ref_count + $2 WHERE id = $1",
                            None,
                            &args2,
                        );
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
                        let _ = client.update(
                            "UPDATE pgmqtt_session_messages SET packet_id = $1, sent_at = now() \
                             WHERE client_id = $2 AND message_id = $3",
                            None,
                            &args,
                        );
                    }
                    SessionDbAction::DeleteMessage {
                        client_id,
                        message_id,
                    } => {
                        let args: Vec<DatumWithOid> =
                            vec![client_id.as_str().into(), message_id.into()];
                        let _ = client.update(
                            "DELETE FROM pgmqtt_session_messages WHERE client_id = $1 AND message_id = $2",
                            None,
                            &args,
                        );
                        // Decrement ref_count and delete message if orphaned (not retained and no refs)
                        let args2: Vec<DatumWithOid> = vec![message_id.into()];
                        let _ = client.update(
                            "DELETE FROM pgmqtt_messages \
                             WHERE id = $1 AND ref_count <= 1 AND retain = false",
                            None,
                            &args2,
                        );
                        // Decrement ref_count for retained messages or those with other refs
                        let _ = client.update(
                            "UPDATE pgmqtt_messages SET ref_count = GREATEST(0, ref_count - 1) WHERE id = $1",
                            None,
                            &args2,
                        );
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
                        let _ = client.update(
                            "INSERT INTO pgmqtt_subscriptions (client_id, topic_filter, qos) \
                             VALUES ($1, $2, $3) \
                             ON CONFLICT (client_id, topic_filter) DO UPDATE \
                             SET qos = EXCLUDED.qos",
                            None,
                            &args,
                        );
                    }
                    SessionDbAction::DeleteSubscription {
                        client_id,
                        topic_filter,
                    } => {
                        let args: Vec<DatumWithOid> =
                            vec![client_id.as_str().into(), topic_filter.as_str().into()];
                        let _ = client.update(
                            "DELETE FROM pgmqtt_subscriptions WHERE client_id = $1 AND topic_filter = $2",
                            None,
                            &args,
                        );
                    }
                }
            }
            Ok::<_, pgrx::spi::Error>(())
        });
    });
}
