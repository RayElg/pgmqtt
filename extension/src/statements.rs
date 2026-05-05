//! Server-side prepared statements for the BGW hot path.
//!
//! `PREPARE` is session-level in PostgreSQL — it survives SPI_finish() and
//! persists for the lifetime of the BGW process.  Calling
//! `prepare_hot_path_statements()` once at startup means every subsequent
//! `EXECUTE pgmqtt_*(...)` call skips parse + plan phases entirely.

const STATEMENTS: &[(&str, &str)] = &[
    (
        "pgmqtt_ins_msg",
        "PREPARE pgmqtt_ins_msg(text, bytea, int4, bool) AS \
         INSERT INTO pgmqtt_messages (topic, payload, qos, retain) \
         VALUES ($1, $2, $3, $4) RETURNING id",
    ),
    (
        "pgmqtt_ins_sess_msg",
        "PREPARE pgmqtt_ins_sess_msg(int8, text, int4) AS \
         INSERT INTO pgmqtt_session_messages (message_id, client_id, packet_id, sent_at) \
         VALUES ($1, $2, $3, CASE WHEN $3 IS NULL THEN NULL ELSE now() END) \
         ON CONFLICT (client_id, message_id) DO NOTHING",
    ),
    (
        "pgmqtt_del_sess_msg",
        "PREPARE pgmqtt_del_sess_msg(text, int8) AS \
         DELETE FROM pgmqtt_session_messages WHERE client_id = $1 AND message_id = $2",
    ),
    (
        "pgmqtt_del_orphan_msg",
        "PREPARE pgmqtt_del_orphan_msg(int8) AS \
         DELETE FROM pgmqtt_messages WHERE id = $1 AND retain = false \
           AND NOT EXISTS (SELECT 1 FROM pgmqtt_session_messages WHERE message_id = $1) \
           AND NOT EXISTS (SELECT 1 FROM pgmqtt_inbound_pending WHERE message_id = $1)",
    ),
    (
        "pgmqtt_upd_inflight",
        "PREPARE pgmqtt_upd_inflight(int4, text, int8) AS \
         UPDATE pgmqtt_session_messages SET packet_id = $1, sent_at = now() \
         WHERE client_id = $2 AND message_id = $3",
    ),
];

/// Prepare all hot-path statements for the current BGW session.
///
/// Must be called from within an enclosing `BackgroundWorker::transaction`.
/// Safe to call multiple times — existing statements are deallocated first
/// so a BGW restart after a schema change always gets a fresh plan.
pub fn prepare_hot_path_statements() {
    let _ = pgrx::spi::Spi::connect_mut(|client| {
        // Clear any previously prepared statements from a prior BGW start.
        let _ = client.update("DEALLOCATE ALL", None, &[]);
        for &(name, stmt) in STATEMENTS {
            if let Err(e) = client.update(stmt, None, &[]) {
                pgrx::log!("pgmqtt: failed to prepare statement '{}': {}", name, e);
            }
        }
        Ok::<_, pgrx::spi::Error>(())
    });
}
