//! Schema migration from pgmqtt v0.1.0 → v0.2.0.
//!
//! Changes introduced in this migration:
//!
//! - `pgmqtt_connections_cache` and `pgmqtt_metrics_current` are converted to
//!   UNLOGGED tables.  Both tables are pure in-process projections that the BGW
//!   reconstructs from its in-memory counters on the next flush cycle; losing
//!   them on a PostgreSQL crash is correct behaviour, not data loss.  Making
//!   them UNLOGGED removes their WAL write overhead and reduces CDC slot decode
//!   work (the slot previously decoded and immediately discarded every UPDATE
//!   to these tables).
//!
//! `ALTER TABLE … SET UNLOGGED` is idempotent: running it on a table that is
//! already UNLOGGED is a no-op, so `init_020` can be called on every BGW start
//! (same pattern as `init_010`).

use pgrx::spi::Spi;

fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("pgmqtt: failed to {}: {}", operation, e));
}

/// Apply v0.2.0 schema changes.
pub fn init_020() {
    run_ddl(
        "ALTER TABLE pgmqtt_connections_cache SET UNLOGGED",
        "set pgmqtt_connections_cache unlogged",
    );
    run_ddl(
        "ALTER TABLE pgmqtt_metrics_current SET UNLOGGED",
        "set pgmqtt_metrics_current unlogged",
    );
}
