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
//! - `pgmqtt_messages`, `pgmqtt_sessions`, `pgmqtt_subscriptions`, and
//!   `pgmqtt_session_messages` are set to `REPLICA IDENTITY NOTHING`.  None
//!   of these tables are logically replicated to downstream subscribers, and
//!   pgmqtt's own CDC slot only consumes INSERT/UPDATE new-tuples from
//!   `pgmqtt_messages` — old-tuple images are never read.  Eliminating them
//!   from UPDATE and DELETE WAL records reduces WAL volume and CDC slot decode
//!   work, particularly on write-heavy workloads.  Physical streaming
//!   replication is unaffected.
//!
//! - `client_id` (on `pgmqtt_sessions`, `pgmqtt_session_messages`,
//!   `pgmqtt_subscriptions`, `pgmqtt_connections_cache`) and `topic_filter`
//!   (on `pgmqtt_subscriptions`) are changed to `COLLATE "C"`.  These columns
//!   are arbitrary identifiers: all comparisons are equality lookups performed
//!   by the BGW, never locale-aware sorts.  `COLLATE "C"` replaces the
//!   locale-aware `bttextcmp` / `varstr_cmp` path in B-tree operations with a
//!   plain `memcmp`, eliminating a significant profiling hotspot on
//!   write-heavy workloads.  The change is guarded by a collation check so it
//!   is a no-op on databases already running v0.2.0+.
//!
//! All DDL here is idempotent and safe to re-run on every BGW start.

use pgrx::spi::Spi;

fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("pgmqtt: failed to {}: {}", operation, e));
}

/// Change a text column to COLLATE "C" if it isn't already, rebuilding its indexes.
///
/// Guards with a pg_attribute check so re-running on an already-migrated database
/// is a no-op (avoids an unnecessary table rewrite on every BGW start).
fn set_collate_c(table: &str, column: &str) {
    let sql = format!(
        "DO $$ BEGIN \
           IF NOT EXISTS ( \
             SELECT 1 FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid \
             JOIN pg_collation col ON col.oid = a.attcollation \
             WHERE c.relname = '{table}' \
               AND c.relnamespace = 'public'::regnamespace \
               AND a.attname = '{column}' \
               AND col.collname = 'C' \
           ) THEN \
             ALTER TABLE {table} ALTER COLUMN {column} TYPE text COLLATE \"C\"; \
           END IF; \
         END $$"
    );
    Spi::run(&sql).unwrap_or_else(|e| {
        pgrx::error!(
            "pgmqtt: failed to set {}.{} collate C: {}",
            table, column, e
        )
    });
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
    run_ddl(
        "ALTER TABLE pgmqtt_messages REPLICA IDENTITY NOTHING",
        "set pgmqtt_messages replica identity nothing",
    );
    run_ddl(
        "ALTER TABLE pgmqtt_sessions REPLICA IDENTITY NOTHING",
        "set pgmqtt_sessions replica identity nothing",
    );
    run_ddl(
        "ALTER TABLE pgmqtt_subscriptions REPLICA IDENTITY NOTHING",
        "set pgmqtt_subscriptions replica identity nothing",
    );
    run_ddl(
        "ALTER TABLE pgmqtt_session_messages REPLICA IDENTITY NOTHING",
        "set pgmqtt_session_messages replica identity nothing",
    );
    set_collate_c("pgmqtt_sessions", "client_id");
    set_collate_c("pgmqtt_session_messages", "client_id");
    set_collate_c("pgmqtt_subscriptions", "client_id");
    set_collate_c("pgmqtt_subscriptions", "topic_filter");
    set_collate_c("pgmqtt_connections_cache", "client_id");
}
