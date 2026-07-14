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

/// Execute all queued DB actions in one transaction.
///
/// The batch is applied inside a subtransaction: on success everything
/// commits atomically. If any statement fails, the subtransaction rolls the
/// whole batch back and each action is retried individually in its own
/// subtransaction, so one poisoned action cannot poison the rest. Actions
/// that still fail are dropped (logged + counted in `db_*_errors`); there
/// is no cross-tick retry queue — recovery relies on the surrounding
/// at-least-once design (outbox re-delivery, session/subscription state
/// re-upserted on the next event).
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
        let m = crate::metrics::get();

        if !synchronous {
            // Outside the subtransactions below: set_config(..., true) is
            // transaction-local, and a batch rollback must not undo it.
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

        // Fast path: the whole batch in one subtransaction — atomic, and a
        // PostgreSQL-level error is caught instead of aborting the outer
        // transaction (which would silently discard every action while
        // per-statement logs claimed partial progress).
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

        // Isolation path: one action poisoned the batch — retry each in its
        // own subtransaction so the rest still commit, and drop (log +
        // count) the failures. No cross-tick retry: recovery relies on the
        // surrounding at-least-once design.
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
        crate::metrics::inc(&m.db_batches_committed);
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
                crate::server::worker_slot().max(0).into(),
            ];
            client.update(
                "INSERT INTO pgmqtt_sessions \
                 (client_id, next_packet_id, expiry_interval, disconnected_at, \
                  auth_principal, owner_slot) \
                 VALUES ($1, $2, $3, NULL, $4, $5) \
                 ON CONFLICT (client_id) DO UPDATE \
                 SET next_packet_id = EXCLUDED.next_packet_id, \
                     expiry_interval = EXCLUDED.expiry_interval, \
                     disconnected_at = NULL, \
                     auth_principal = EXCLUDED.auth_principal, \
                     owner_slot = EXCLUDED.owner_slot",
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
        }
        SessionDbAction::DrainCdcOutbox { ids } => {
            let args: Vec<DatumWithOid> = vec![ids.clone().into()];
            client.update(
                "DELETE FROM pgmqtt_cdc_outbox WHERE id = ANY($1::bigint[])",
                None,
                &args,
            )?;
        }
        SessionDbAction::AdvanceOutboxCursor {
            worker_slot,
            last_id,
        } => {
            let args: Vec<DatumWithOid> = vec![(*worker_slot).into(), (*last_id).into()];
            client.update(
                "UPDATE pgmqtt_outbox_cursors \
                 SET last_id = GREATEST(last_id, $2) \
                 WHERE worker_slot = $1",
                None,
                &args,
            )?;
        }
    }
    Ok(())
}
