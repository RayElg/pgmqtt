//! Schema migration from pgmqtt v0.2.0 → v0.3.0.
//!
//! Adds:
//!
//! - `pgmqtt_acls` — per-role per-topic ACL allowlist consulted on CONNECT
//!   when the client authenticates via Postgres-role password auth. Enforced
//!   only when the active enterprise license includes the `acl` feature.
//!
//! - `pgmqtt_admin_commands` — broker-side command queue drained by the BGW
//!   every tick. Backs the SQL functions `pgmqtt_disconnect_client`,
//!   `pgmqtt_disconnect_role`, and `pgmqtt_reload_acls`. Unlogged because
//!   the BGW loses in-memory state across a crash anyway; a dropped command
//!   simply needs to be reissued.
//!
//! All DDL here is idempotent and safe to re-run on every BGW start.

use pgrx::spi::Spi;

fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("pgmqtt: failed to {}: {}", operation, e));
}

pub fn init_030() {
    // Per-topic ACLs (enterprise: 'acl' feature). Loaded on successful
    // password-auth CONNECT into the connection's allowlist; rules mirror the
    // JWT sub_claims / pub_claims semantics (MQTT wildcards supported).
    run_ddl(
        "CREATE TABLE IF NOT EXISTS pgmqtt_acls (
            role_name      name NOT NULL,
            topic_filter   text NOT NULL,
            can_publish    boolean NOT NULL DEFAULT false,
            can_subscribe  boolean NOT NULL DEFAULT false,
            PRIMARY KEY (role_name, topic_filter)
        )",
        "create pgmqtt_acls table",
    );
    run_ddl(
        "CREATE INDEX IF NOT EXISTS idx_pgmqtt_acls_role ON pgmqtt_acls(role_name)",
        "create idx_pgmqtt_acls_role",
    );

    // Admin command queue: BGW drains every tick. SQL functions
    // pgmqtt_disconnect_client / _role / pgmqtt_reload_acls insert rows here.
    run_ddl(
        "CREATE UNLOGGED TABLE IF NOT EXISTS pgmqtt_admin_commands (
            id          bigserial PRIMARY KEY,
            kind        text NOT NULL,
            target      text NOT NULL,
            reason_code smallint,
            issued_at   timestamptz NOT NULL DEFAULT now()
        )",
        "create pgmqtt_admin_commands table",
    );
}
