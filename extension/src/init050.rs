//! Schema migration from pgmqtt v0.4.x → the two-BGW CDC/MQTT split.
//!
//! CDC slot consumption now runs in its own `pgmqtt_cdc` background worker
//! (enterprise `multiprocess` feature). QoS >= 1 messages are handed to
//! `pgmqtt_mqtt` through `pgmqtt_cdc_outbox`: the CDC worker queues each
//! persisted message's id there *inside the same transaction that persists
//! it* (advancing the replication slot only after that commit — see
//! `cdc_tick_core`), and the delivery worker deletes the row in the same
//! transaction that records delivery state. The handoff is therefore
//! exactly as durable and atomic as the messages themselves — a crash on
//! either side leaves the pending set intact for redelivery (at-least-once).
//!
//! QoS 0 messages travel through a shared-memory ring instead (see
//! `crate::shmem_bridge`), which can drop on sustained overflow;
//! `cdc_bridge_dropped` makes that loss visible alongside the existing
//! `cdc_ring_buffer_dropped` counter.
//!
//! All DDL here is idempotent and safe to re-run on every BGW start.

use pgrx::spi::Spi;

fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("pgmqtt: failed to {}: {}", operation, e));
}

pub fn init_050() {
    // Pending-delivery queue for CDC-persisted messages (enterprise
    // multiprocess topology). Rows reference pgmqtt_messages ids, but with no
    // FK on purpose: the two tables are written by different workers and the
    // delivery worker's LEFT JOIN tolerates (and then reaps) a dangling id,
    // which is cheaper than FK checks on the CDC hot path.
    run_ddl(
        "CREATE TABLE IF NOT EXISTS pgmqtt_cdc_outbox (id bigint PRIMARY KEY)",
        "create pgmqtt_cdc_outbox",
    );
    // With socket_workers > 1, delivery switches from delete-on-delivery to
    // per-worker cursors over the outbox: each worker delivers rows past its
    // own cursor to its own subscribers, and slot 0 garbage-collects rows
    // below min(last_id) (reclaiming orphaned messages at the same time).
    // Cursor advancement is additionally gated on the shared-memory enqueue
    // floors (see `shmem_bridge::enqueue_barrier`): ids are allocated in
    // sequence order but commit in any order, so without the barrier a
    // cursor could pass a lower id whose transaction commits later — that
    // message would never be delivered despite the publisher's PUBACK.
    run_ddl(
        "CREATE TABLE IF NOT EXISTS pgmqtt_outbox_cursors (\
             worker_slot int PRIMARY KEY, \
             last_id bigint NOT NULL DEFAULT 0)",
        "create pgmqtt_outbox_cursors",
    );
    // Cluster-wide arbitration for shared-subscription delivery: every
    // socket worker matches its local group members against every outbox
    // row, so the first worker to claim (message_id, group) delivers and
    // the rest skip. worker_slot records the winner so a crashed worker can
    // re-win its own claims after restarting (its cursor only advances with
    // delivered state, so it re-fetches the message and must not lose the
    // claim race against itself — that would strand the message for the
    // group). Reclaimed by the slot-0 outbox GC; -1 marks pre-upgrade rows.
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
    // Durable sessions are bound to the authenticated principal that created
    // them ('role:{name}', 'jwt:...', or 'anon'): a CONNECT under the same
    // client_id but a different accepted identity must not receive the
    // previous identity's queued payloads. '' marks pre-upgrade rows, which
    // resume unbound once and are then stamped.
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
