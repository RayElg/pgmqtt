//! Session-lifetime prepared statements for the BGW hot path.
//!
//! Each plan is prepared via `SpiClient::prepare_mut` and immediately promoted
//! to an `OwnedPreparedStatement` via `.keep()`, which calls `SPI_keepplan` so
//! the plan survives `SPI_finish()` and lasts for the lifetime of the BGW process.
//!
//! Call `prepare_hot_path_statements()` once at BGW startup (inside a
//! `BackgroundWorker::transaction`). Hot-path code then calls
//! `with_plans(|p| ...)` to execute against the cached plan; callers fall back
//! to inline SQL when `None` is returned (plans not yet initialised).

use std::cell::RefCell;

use pgrx::pg_sys::PgOid;
use pgrx::spi::{self, OwnedPreparedStatement};

pub struct HotPathPlans {
    pub ins_sess_msg: OwnedPreparedStatement,
    pub del_sess_msg: OwnedPreparedStatement,
    pub del_orphan_msg: OwnedPreparedStatement,
    pub upd_inflight: OwnedPreparedStatement,
}

thread_local! {
    static PLANS: RefCell<Option<HotPathPlans>> = const { RefCell::new(None) };
}

/// Prepare all hot-path statements for the current BGW session.
///
/// Must be called from within an enclosing `BackgroundWorker::transaction`.
/// Safe to call multiple times — a new set of plans replaces the prior set on
/// each call (e.g. after a BGW restart following a schema change).
pub fn prepare_hot_path_statements() {
    let result = spi::Spi::connect_mut(|client| {
        let ins_sess_msg = client
            .prepare_mut(
                "INSERT INTO pgmqtt_session_messages \
                 (message_id, client_id, packet_id, sent_at) \
                 VALUES ($1, $2, $3, \
                   CASE WHEN $3 IS NULL THEN NULL ELSE now() END) \
                 ON CONFLICT (client_id, message_id) DO NOTHING",
                &[
                    PgOid::from_untagged(pgrx::pg_sys::INT8OID),
                    PgOid::from_untagged(pgrx::pg_sys::TEXTOID),
                    PgOid::from_untagged(pgrx::pg_sys::INT4OID),
                ],
            )?
            .keep();

        let del_sess_msg = client
            .prepare_mut(
                "DELETE FROM pgmqtt_session_messages \
                 WHERE client_id = $1 AND message_id = $2",
                &[
                    PgOid::from_untagged(pgrx::pg_sys::TEXTOID),
                    PgOid::from_untagged(pgrx::pg_sys::INT8OID),
                ],
            )?
            .keep();

        let del_orphan_msg = client
            .prepare_mut(
                "DELETE FROM pgmqtt_messages \
                 WHERE id = $1 \
                   AND NOT EXISTS \
                     (SELECT 1 FROM pgmqtt_session_messages WHERE message_id = $1) \
                   AND NOT EXISTS \
                     (SELECT 1 FROM pgmqtt_inbound_pending WHERE message_id = $1) \
                   AND NOT EXISTS \
                     (SELECT 1 FROM pgmqtt_retained WHERE message_id = $1)",
                &[PgOid::from_untagged(pgrx::pg_sys::INT8OID)],
            )?
            .keep();

        let upd_inflight = client
            .prepare_mut(
                "UPDATE pgmqtt_session_messages \
                 SET packet_id = $1, sent_at = now() \
                 WHERE client_id = $2 AND message_id = $3",
                &[
                    PgOid::from_untagged(pgrx::pg_sys::INT4OID),
                    PgOid::from_untagged(pgrx::pg_sys::TEXTOID),
                    PgOid::from_untagged(pgrx::pg_sys::INT8OID),
                ],
            )?
            .keep();

        Ok::<_, spi::Error>(HotPathPlans {
            ins_sess_msg,
            del_sess_msg,
            del_orphan_msg,
            upd_inflight,
        })
    });

    match result {
        Ok(plans) => PLANS.with(|cell| *cell.borrow_mut() = Some(plans)),
        Err(e) => pgrx::log!("pgmqtt: failed to prepare hot-path statements: {}", e),
    }
}

/// Run `f` with the session-level prepared plans.
///
/// Returns `None` if `prepare_hot_path_statements` has not been called yet;
/// callers should fall back to inline SQL in that case.
pub fn with_plans<F, R>(f: F) -> Option<R>
where
    F: FnOnce(&HotPathPlans) -> R,
{
    PLANS.with(|cell| cell.borrow().as_ref().map(f))
}
