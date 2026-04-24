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

/// Inner SPI work for a batch of session DB actions, called with an already-open SpiClient.
///
/// Shared by `execute_session_db_actions` (standalone transaction) and by the merged
/// tick transaction in `publish_and_execute_db_work` (mod.rs), which combines message
/// persistence and session mutations into one WAL commit.
pub fn execute_session_db_actions_inner(
    client: &mut spi::SpiClient<'_>,
    actions: Vec<SessionDbAction>,
) -> Result<(), spi::Error> {
    let m = crate::metrics::get();

    let mut insert_batch: Vec<(i64, String, Option<u16>)> = Vec::new(); // (msg_id, client_id, packet_id)
    let mut delete_batch: Vec<(String, i64)> = Vec::new();
    let mut update_batch: Vec<(String, i64, u16)> = Vec::new();
    // Deferred until after insert_batch: if a subscriber hits the queue-full limit in the
    // same tick that messages are queued for it, DeleteSession and InsertMessageBatch both
    // land in the same transaction.  InsertMessageBatch is already deferred; DeleteSession
    // must be too — otherwise the FK on pgmqtt_sessions is violated when insert_batch runs.
    let mut delete_sessions: Vec<String> = Vec::new();
    // Track client_ids that were upserted in this transaction. clean_start=true emits
    // DeleteSession before UpsertSession for the same client_id; since DeleteSession is
    // deferred, it would otherwise delete the session that UpsertSession just created and
    // cascade-delete its subscriptions. Skip the delete if the session was re-established.
    let mut upserted_sessions: std::collections::HashSet<String> = std::collections::HashSet::new();

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
                } else {
                    upserted_sessions.insert(client_id);
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
                delete_sessions.push(client_id);
            }
            // Collected below; all messages in the tick are inserted together.
            SessionDbAction::InsertMessageBatch { message_id, entries } => {
                for (client_id, packet_id) in entries {
                    insert_batch.push((message_id, client_id, packet_id));
                }
            }
            // Collected below for bulk execution
            SessionDbAction::DeleteMessage { client_id, message_id } => {
                delete_batch.push((client_id, message_id));
            }
            SessionDbAction::UpdateMessageInflight { client_id, message_id, packet_id } => {
                update_batch.push((client_id, message_id, packet_id));
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

    // Bulk-insert all session_messages for every message delivered this tick.
    // One INSERT replaces N per-message inserts with freshly-parsed queries.
    if !insert_batch.is_empty() {
        let mut clauses: Vec<String> = Vec::with_capacity(insert_batch.len());
        let mut args: Vec<DatumWithOid> = Vec::with_capacity(insert_batch.len() * 3);
        let mut p = 1usize;
        for (mid, cid, pid) in &insert_batch {
            let pid_arg = pid.map(|v| v as i32);
            clauses.push(format!(
                "(${}::bigint,${}::text,${}::int,CASE WHEN ${} IS NULL THEN NULL ELSE now() END)",
                p, p + 1, p + 2, p + 2
            ));
            args.push((*mid).into());
            args.push(cid.as_str().into());
            args.push(pid_arg.into());
            p += 3;
        }
        let q = format!(
            "INSERT INTO pgmqtt_session_messages (message_id,client_id,packet_id,sent_at) \
             VALUES {} ON CONFLICT (client_id,message_id) DO NOTHING",
            clauses.join(",")
        );
        if let Err(e) = client.update(&q, None, &args) {
            crate::metrics::inc(&m.db_message_errors);
            pgrx::log!("pgmqtt: batch insert session_messages failed: {}", e);
        }
    }

    // Delete sessions after insert_batch so the FK on pgmqtt_sessions is satisfied.
    // CASCADE handles their pgmqtt_session_messages rows.
    // Skip any client_id that was re-established via UpsertSession in this same tick
    // (clean_start=true path emits DeleteSession before UpsertSession for the same client).
    for client_id in delete_sessions {
        if upserted_sessions.contains(&client_id) {
            continue;
        }
        let args: Vec<DatumWithOid> = vec![client_id.as_str().into()];
        if let Err(e) = client.update(
            "DELETE FROM pgmqtt_sessions WHERE client_id = $1",
            None,
            &args,
        ) {
            crate::metrics::inc(&m.db_session_errors);
            pgrx::log!("pgmqtt: failed to delete session '{}': {}", client_id, e);
        }
    }

    // Bulk-delete session_messages for all PUBACKed messages in this tick.
    // One DELETE statement replaces N individual deletes + N orphan checks.
    if !delete_batch.is_empty() {
        let mut clauses: Vec<String> = Vec::with_capacity(delete_batch.len());
        let mut args: Vec<DatumWithOid> = Vec::with_capacity(delete_batch.len() * 2);
        let mut p = 1usize;
        for (cid, mid) in &delete_batch {
            clauses.push(format!("(${}::text,${}::bigint)", p, p + 1));
            args.push(cid.as_str().into());
            args.push((*mid).into());
            p += 2;
        }
        let q = format!(
            "DELETE FROM pgmqtt_session_messages \
             WHERE (client_id,message_id) IN (VALUES {})",
            clauses.join(",")
        );
        if let Err(e) = client.update(&q, None, &args) {
            crate::metrics::inc(&m.db_message_errors);
            pgrx::log!("pgmqtt: batch delete session_messages failed: {}", e);
        }

        // Single orphan-cleanup pass for all unique message_ids just deleted.
        let mut seen = std::collections::HashSet::new();
        let mut oc: Vec<String> = Vec::new();
        let mut oa: Vec<DatumWithOid> = Vec::new();
        let mut op = 1usize;
        for (_, mid) in &delete_batch {
            if seen.insert(*mid) {
                oc.push(format!("(${}::bigint)", op));
                oa.push((*mid).into());
                op += 1;
            }
        }
        let oq = format!(
            "DELETE FROM pgmqtt_messages \
             WHERE id IN (SELECT v FROM (VALUES {}) t(v) \
             WHERE NOT EXISTS (SELECT 1 FROM pgmqtt_session_messages WHERE message_id = v) \
               AND NOT EXISTS (SELECT 1 FROM pgmqtt_inbound_pending  WHERE message_id = v)) \
               AND retain = false",
            oc.join(",")
        );
        if let Err(e) = client.update(&oq, None, &oa) {
            crate::metrics::inc(&m.db_message_errors);
            pgrx::log!("pgmqtt: batch orphan cleanup failed: {}", e);
        }
    }

    // Bulk-update inflight packet_ids for all queue-promoted messages this tick.
    if !update_batch.is_empty() {
        let mut clauses: Vec<String> = Vec::with_capacity(update_batch.len());
        let mut args: Vec<DatumWithOid> = Vec::with_capacity(update_batch.len() * 3);
        let mut p = 1usize;
        for (cid, mid, pid) in &update_batch {
            clauses.push(format!("(${}::text,${}::bigint,${}::int)", p, p + 1, p + 2));
            args.push(cid.as_str().into());
            args.push((*mid).into());
            args.push((*pid as i32).into());
            p += 3;
        }
        let q = format!(
            "UPDATE pgmqtt_session_messages sm \
             SET packet_id = u.packet_id, sent_at = now() \
             FROM (VALUES {}) AS u(client_id,message_id,packet_id) \
             WHERE sm.client_id = u.client_id AND sm.message_id = u.message_id",
            clauses.join(",")
        );
        if let Err(e) = client.update(&q, None, &args) {
            crate::metrics::inc(&m.db_message_errors);
            pgrx::log!("pgmqtt: batch update inflight failed: {}", e);
        }
    }

    crate::metrics::inc(&m.db_batches_committed);
    Ok(())
}

/// Execute all queued DB actions in a single atomic transaction.
///
/// All actions run inside one transaction. `DeleteMessage` and
/// `UpdateMessageInflight` entries are coalesced into single bulk
/// statements (VALUES-list DELETE / UPDATE FROM) to avoid one SPI
/// round-trip per ACK. Everything else executes in order.
pub fn execute_session_db_actions(actions: Vec<SessionDbAction>) {
    if actions.is_empty() {
        return;
    }

    BackgroundWorker::transaction(move || {
        let _ = pgrx::spi::Spi::connect_mut(|client| {
            execute_session_db_actions_inner(client, actions)
        });
    });
}
