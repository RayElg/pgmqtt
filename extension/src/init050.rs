//! Schema migration for 0.5.0's two-BGW CDC/MQTT split. All DDL is
//! idempotent and re-run on every BGW start.

use pgrx::spi::Spi;

fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("pgmqtt: failed to {}: {}", operation, e));
}

pub fn init_050() {
    // No FK on purpose: the LEFT JOIN tolerates (and reaps) dangling ids,
    // cheaper than FK checks on the CDC hot path.
    run_ddl(
        "CREATE TABLE IF NOT EXISTS pgmqtt_cdc_outbox (id bigint PRIMARY KEY)",
        "create pgmqtt_cdc_outbox",
    );
    // 'role:{name}' / 'jwt:...' / 'anon'; a different identity must not
    // receive the previous one's payloads. '' = pre-upgrade, resumes once.
    run_ddl(
        "ALTER TABLE pgmqtt_sessions ADD COLUMN IF NOT EXISTS auth_principal text NOT NULL DEFAULT ''",
        "add auth_principal to pgmqtt_sessions",
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
