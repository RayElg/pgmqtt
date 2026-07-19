//! Schema migration for 0.5.0's two-BGW CDC/MQTT split: the durable
//! outbox handoff (ids queued in the persisting transaction, dequeued with
//! delivery state — as atomic as the messages), per-worker cursors, share
//! claims, and session identity/ownership columns. All DDL is idempotent
//! and re-run on every BGW start.

use pgrx::spi::Spi;

fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("pgmqtt: failed to {}: {}", operation, e));
}

pub fn init_050() {
    // Pending-delivery queue of pgmqtt_messages ids. No FK on purpose:
    // the LEFT JOIN tolerates (and reaps) dangling ids, cheaper than FK
    // checks on the CDC hot path.
    run_ddl(
        "CREATE TABLE IF NOT EXISTS pgmqtt_cdc_outbox (id bigint PRIMARY KEY)",
        "create pgmqtt_cdc_outbox",
    );
    // socket_workers > 1: per-worker delivery cursors instead of
    // delete-on-delivery; slot 0 GCs below min(last_id). Advancement is
    // gated on the enqueue floors (`shmem_bridge::enqueue_barrier`) — ids
    // commit out of allocation order.
    run_ddl(
        "CREATE TABLE IF NOT EXISTS pgmqtt_outbox_cursors (\
             worker_slot int PRIMARY KEY, \
             last_id bigint NOT NULL DEFAULT 0)",
        "create pgmqtt_outbox_cursors",
    );
    // Cluster-wide $share arbitration: first worker to claim
    // (message_id, group) delivers. worker_slot lets a crashed worker
    // re-win its own claims on re-fetch (losing to itself would strand the
    // message). GC'd with the outbox; -1 marks pre-upgrade rows.
    run_ddl(
        "CREATE TABLE IF NOT EXISTS pgmqtt_share_claims (\
             message_id bigint NOT NULL, \
             group_key text NOT NULL, \
             PRIMARY KEY (message_id, group_key))",
        "create pgmqtt_share_claims",
    );
    run_ddl(
        "ALTER TABLE pgmqtt_share_claims ADD COLUMN IF NOT EXISTS worker_slot int NOT NULL DEFAULT -1",
        "add worker_slot to pgmqtt_share_claims",
    );
    // Sessions bind to the creating principal ('role:{name}', 'jwt:...',
    // 'anon'): a different identity must not receive the previous one's
    // payloads. '' = pre-upgrade row, resumes unbound once.
    run_ddl(
        "ALTER TABLE pgmqtt_sessions ADD COLUMN IF NOT EXISTS auth_principal text NOT NULL DEFAULT ''",
        "add auth_principal to pgmqtt_sessions",
    );
    // Which socket worker last owned the connection, so a restarting worker
    // marks only *its own* orphaned sessions as disconnected instead of
    // clobbering sessions live on sibling workers.
    run_ddl(
        "ALTER TABLE pgmqtt_sessions ADD COLUMN IF NOT EXISTS owner_slot int NOT NULL DEFAULT 0",
        "add owner_slot to pgmqtt_sessions",
    );
    // Ownership column so each socket worker maintains only its own rows in
    // the (UNLOGGED, rebuildable) connections cache.
    run_ddl(
        "ALTER TABLE pgmqtt_connections_cache ADD COLUMN IF NOT EXISTS worker_slot int NOT NULL DEFAULT 0",
        "add worker_slot to connections_cache",
    );
    run_ddl(
        "ALTER TABLE pgmqtt_metrics_current   ADD COLUMN IF NOT EXISTS cdc_bridge_dropped bigint NOT NULL DEFAULT 0",
        "add cdc_bridge_dropped to metrics_current",
    );
    run_ddl(
        "ALTER TABLE pgmqtt_metrics_snapshots ADD COLUMN IF NOT EXISTS cdc_bridge_dropped bigint NOT NULL DEFAULT 0",
        "add cdc_bridge_dropped to metrics_snapshots",
    );
}
