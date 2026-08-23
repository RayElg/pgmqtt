//! Database actions for session/message/subscription operations.
//!
//! `SessionDbAction` collects mutations during an event loop iteration,
//! which are then executed atomically in a single transaction via
//! `execute_session_db_actions`.

use pgrx::bgworkers::BackgroundWorker;
use pgrx::datum::DatumWithOid;
use pgrx::spi;
use std::sync::Mutex;

/// Message ids whose reclaim found every reference gone except an
/// unconsumed `pgmqtt_inbound_pending` row. Drained back onto the next
/// executed batch: the pump always terminates a pending row (successful
/// write, retry, or dead-letter all delete it), so entries clear within
/// its bounded backoff window. Capped defensively — overflow means
/// pending rows are sticking, which the inbound metrics already surface.
static DEFERRED_RECLAIMS: Mutex<Vec<i64>> = Mutex::new(Vec::new());
const DEFERRED_RECLAIMS_CAP: usize = 10_000;

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
                 (SELECT 1 FROM pgmqtt_retained WHERE message_id = $1) \
               AND NOT EXISTS \
                 (SELECT 1 FROM pgmqtt_cdc_outbox WHERE id = $1)",
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
    /// Upsert a session row (next_packet_id, expiry_interval, identity).
    UpsertSession {
        client_id: String,
        next_packet_id: u16,
        expiry_interval: u32,
        /// Principal the session is bound to (see MqttSession::auth_principal).
        auth_principal: String,
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
    /// Reclaim a persisted message with no subscribers at delivery time
    /// (no-op while still referenced; retried later when the only reference
    /// is a not-yet-consumed inbound_pending row — see DEFERRED_RECLAIMS).
    CleanupOrphanedMessage {
        message_id: i64,
    },
    /// Dequeue delivered outbox ids — commits with the delivery state, so
    /// an uncommitted tick re-delivers.
    DrainCdcOutbox {
        ids: Vec<i64>,
    },
}

/// Execute all queued DB actions in one transaction. The batch runs in one
/// subtransaction; on failure each action retries individually so one
/// poisoned action can't sink the rest, and stragglers are dropped
/// (logged + counted) — recovery relies on the surrounding at-least-once
/// design. Hot-path queries use the session-prepared statements.
pub fn execute_session_db_actions(actions: Vec<SessionDbAction>) {
    execute_session_db_actions_inner(actions, true)
}

/// [`execute_session_db_actions`] with an asynchronous commit (enterprise
/// multiprocess — the socket loop must not block on a WAL flush). Safe:
/// every action is reconstructible or at-least-once, and a postmaster
/// crash loses only bookkeeping inside its own recovery window.
pub fn execute_session_db_actions_async(actions: Vec<SessionDbAction>) {
    execute_session_db_actions_inner(actions, false)
}

fn execute_session_db_actions_inner(actions: Vec<SessionDbAction>, synchronous: bool) {
    let mut actions = actions;
    {
        let mut queue = DEFERRED_RECLAIMS.lock().unwrap_or_else(|p| p.into_inner());
        for message_id in queue.drain(..) {
            actions.push(SessionDbAction::CleanupOrphanedMessage { message_id });
        }
    }
    if actions.is_empty() {
        return;
    }

    BackgroundWorker::transaction(move || {
        let m = crate::metrics::get();

        if !synchronous {
            // Outside the subtransactions: a batch rollback must not undo
            // the transaction-local set_config.
            let _ = pgrx::spi::Spi::connect_mut(|client| {
                client
                    .select(
                        "SELECT set_config('synchronous_commit', 'off', true)",
                        None,
                        &[],
                    )
                    .map(|_| ())
            });
        }

        // Whole batch in one subtransaction: atomic, and an ERROR is
        // caught instead of aborting the outer transaction.
        let batch = super::with_subtransaction(|| {
            pgrx::spi::Spi::connect_mut(|client| {
                for action in &actions {
                    apply_action(client, action)?;
                }
                Ok(())
            })
        });
        if batch.is_ok() {
            crate::metrics::inc(&m.db_batches_committed);
            return;
        }

        // One action poisoned the batch: retry each individually, drop
        // (log + count) the failures.
        let mut dropped = 0usize;
        for action in &actions {
            let one = super::with_subtransaction(|| {
                pgrx::spi::Spi::connect_mut(|client| apply_action(client, action))
            });
            if one.is_err() {
                dropped += 1;
                crate::metrics::inc(action_error_metric(&m, action));
                pgrx::log!("pgmqtt: dropped DB action after batch failure: {:?}", action);
            }
        }
        pgrx::log!(
            "pgmqtt: DB action batch failed; {} of {} actions retried individually and dropped",
            dropped,
            actions.len()
        );
        // Only a batch whose every action landed counts as committed; the
        // dropped ones are already reported through db_*_errors.
        if dropped == 0 {
            crate::metrics::inc(&m.db_batches_committed);
        }
    });
}

/// Which error counter a failed action reports to.
fn action_error_metric<'a>(
    m: &'a crate::metrics::BrokerMetrics,
    action: &SessionDbAction,
) -> &'a std::sync::atomic::AtomicU64 {
    match action {
        SessionDbAction::UpsertSession { .. }
        | SessionDbAction::MarkDisconnected { .. }
        | SessionDbAction::DeleteSession { .. } => &m.db_session_errors,
        SessionDbAction::InsertSubscription { .. }
        | SessionDbAction::DeleteSubscription { .. } => &m.db_subscription_errors,
        _ => &m.db_message_errors,
    }
}

/// Apply one action; any error propagates so the caller's subtransaction
/// rolls back.
fn apply_action(
    client: &mut pgrx::spi::SpiClient<'_>,
    action: &SessionDbAction,
) -> Result<(), spi::Error> {
    match action {
        SessionDbAction::UpsertSession {
            client_id,
            next_packet_id,
            expiry_interval,
            auth_principal,
        } => {
            let args: Vec<DatumWithOid> = vec![
                client_id.as_str().into(),
                (*next_packet_id as i32).into(),
                (*expiry_interval as i32).into(),
                auth_principal.as_str().into(),
            ];
            client.update(
                "INSERT INTO pgmqtt_sessions \
                 (client_id, next_packet_id, expiry_interval, disconnected_at, \
                  auth_principal) \
                 VALUES ($1, $2, $3, NULL, $4) \
                 ON CONFLICT (client_id) DO UPDATE \
                 SET next_packet_id = EXCLUDED.next_packet_id, \
                     expiry_interval = EXCLUDED.expiry_interval, \
                     disconnected_at = NULL, \
                     auth_principal = EXCLUDED.auth_principal",
                None,
                &args,
            )?;
        }
        SessionDbAction::MarkDisconnected { client_id } => {
            let args: Vec<DatumWithOid> = vec![client_id.as_str().into()];
            client.update(
                "UPDATE pgmqtt_sessions SET disconnected_at = now() WHERE client_id = $1",
                None,
                &args,
            )?;
        }
        SessionDbAction::DeleteSession { client_id } => {
            let args: Vec<DatumWithOid> = vec![client_id.as_str().into()];
            client.update("DELETE FROM pgmqtt_sessions WHERE client_id = $1", None, &args)?;
            // CASCADE on pgmqtt_sessions deletes this client's pgmqtt_session_messages
            // rows. Messages that now have no remaining session_messages are cleaned up
            // by the DeleteMessage action when each subscriber ACKs. A global sweep
            // here would race with InsertMessageBatch in the same transaction.
        }
        SessionDbAction::InsertMessageBatch {
            message_id,
            entries,
        } => {
            for (cid, packet_id) in entries {
                let pid_arg = packet_id.map(|p| p as i32);
                let args: Vec<DatumWithOid> =
                    vec![(*message_id).into(), cid.as_str().into(), pid_arg.into()];
                match crate::statements::with_plans(|p| {
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
                }?;
            }
        }
        SessionDbAction::UpdateMessageInflight {
            client_id,
            message_id,
            packet_id,
        } => {
            let args: Vec<DatumWithOid> = vec![
                (*packet_id as i32).into(),
                client_id.as_str().into(),
                (*message_id).into(),
            ];
            match crate::statements::with_plans(|p| client.update(&p.upd_inflight, None, &args)) {
                Some(r) => r,
                None => client.update(
                    "UPDATE pgmqtt_session_messages \
                     SET packet_id = $1, sent_at = now() \
                     WHERE client_id = $2 AND message_id = $3",
                    None,
                    &args,
                ),
            }?;
        }
        SessionDbAction::DeleteMessage {
            client_id,
            message_id,
        } => {
            let del_args: Vec<DatumWithOid> =
                vec![client_id.as_str().into(), (*message_id).into()];
            match crate::statements::with_plans(|p| client.update(&p.del_sess_msg, None, &del_args))
            {
                Some(r) => r,
                None => client.update(
                    "DELETE FROM pgmqtt_session_messages \
                     WHERE client_id = $1 AND message_id = $2",
                    None,
                    &del_args,
                ),
            }?;
            cleanup_orphaned_message(client, *message_id)?;
        }
        SessionDbAction::InsertSubscription {
            client_id,
            topic_filter,
            qos,
        } => {
            let args: Vec<DatumWithOid> = vec![
                client_id.as_str().into(),
                topic_filter.as_str().into(),
                (*qos as i32).into(),
            ];
            client.update(
                "INSERT INTO pgmqtt_subscriptions (client_id, topic_filter, qos) \
                 VALUES ($1, $2, $3) \
                 ON CONFLICT (client_id, topic_filter) DO UPDATE \
                 SET qos = EXCLUDED.qos",
                None,
                &args,
            )?;
        }
        SessionDbAction::DeleteSubscription {
            client_id,
            topic_filter,
        } => {
            let args: Vec<DatumWithOid> =
                vec![client_id.as_str().into(), topic_filter.as_str().into()];
            client.update(
                "DELETE FROM pgmqtt_subscriptions WHERE client_id = $1 AND topic_filter = $2",
                None,
                &args,
            )?;
        }
        SessionDbAction::CleanupOrphanedMessage { message_id } => {
            cleanup_orphaned_message(client, *message_id)?;
            // A surviving row is usually legitimate: fan-out keeps it
            // referenced via session_messages until ACKs land, and those
            // ACKs carry their own reclaim. The one case nothing follows up
            // on is a lone inbound_pending reference — this action raced the
            // CDC worker's pump (multiprocess) or simply ran first in the
            // same tick. Defer rather than orphan the row.
            let blocked_by_pending_only: bool = client
                .select(
                    "SELECT EXISTS (SELECT 1 FROM pgmqtt_messages WHERE id = $1) \
                     AND NOT EXISTS (SELECT 1 FROM pgmqtt_session_messages \
                                    WHERE message_id = $1) \
                     AND NOT EXISTS (SELECT 1 FROM pgmqtt_retained \
                                    WHERE message_id = $1) \
                     AND NOT EXISTS (SELECT 1 FROM pgmqtt_cdc_outbox WHERE id = $1) \
                     AND EXISTS (SELECT 1 FROM pgmqtt_inbound_pending \
                                 WHERE message_id = $1)",
                    None,
                    &[(*message_id).into()],
                )?
                .first()
                .get_one::<bool>()?
                .unwrap_or(false);
            if blocked_by_pending_only {
                let mut queue =
                    DEFERRED_RECLAIMS.lock().unwrap_or_else(|p| p.into_inner());
                if queue.len() < DEFERRED_RECLAIMS_CAP && !queue.contains(message_id) {
                    queue.push(*message_id);
                }
            }
        }
        SessionDbAction::DrainCdcOutbox { ids } => {
            let args: Vec<DatumWithOid> = vec![ids.clone().into()];
            client.update(
                "DELETE FROM pgmqtt_cdc_outbox WHERE id = ANY($1::bigint[])",
                None,
                &args,
            )?;
        }
    }
    Ok(())
}
