//! Schema migration from pgmqtt v0.4.x → the two-BGW CDC/MQTT split.
//!
//! CDC slot consumption now runs in its own `pgmqtt_cdc` background worker
//! (enterprise `multiprocess` feature). QoS >= 1 messages are handed to
//! `pgmqtt_mqtt` through `pgmqtt_cdc_outbox`: the CDC worker queues each
//! persisted message's id there *inside the same transaction that advances
//! the replication slot*, and the delivery worker deletes the row in the
//! same transaction that records delivery state. The handoff is therefore
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

pub fn init_040() {
    // Pending-delivery queue for CDC-persisted messages (enterprise
    // multiprocess topology). Rows reference pgmqtt_messages ids, but with no
    // FK on purpose: the two tables are written by different workers and the
    // delivery worker's LEFT JOIN tolerates (and then reaps) a dangling id,
    // which is cheaper than FK checks on the CDC hot path.
    run_ddl(
        "CREATE TABLE IF NOT EXISTS pgmqtt_cdc_outbox (id bigint PRIMARY KEY)",
        "create pgmqtt_cdc_outbox",
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
