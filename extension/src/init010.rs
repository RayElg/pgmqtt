//! SQL initialization for pgmqtt v0.1.0 schema.
//!
//! This module contains all DDL statements for the initial schema version.
//! As a greenfield extension (no migrations from prior versions), all tables
//! and indexes are created with their final v0.1.0 structure.

use pgrx::spi::Spi;

/// Execute SQL, logging and ignoring errors (for DDL operations).
fn run_sql_or_error(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("pgmqtt: failed to {}: {}", operation, e));
}

/// Initialize all v0.1.0 tables and indexes.
pub fn init_010() {
    // Topic mappings: CDC → MQTT topic + payload templates.
    // Multiple mappings per (schema, table) are allowed via distinct mapping_name values,
    // enabling parallel publish to multiple topics during schema migrations.
    run_sql_or_error(
        "CREATE TABLE IF NOT EXISTS pgmqtt_topic_mappings (
            schema_name   text NOT NULL,
            table_name    text NOT NULL,
            mapping_name  text NOT NULL DEFAULT 'default',
            topic_template text NOT NULL,
            payload_template text NOT NULL,
            qos int DEFAULT 0,
            template_type text NOT NULL DEFAULT 'jinja2',
            PRIMARY KEY (schema_name, table_name, mapping_name)
        )",
        "create mappings table",
    );

    // Internal WAL-synchronized mapping checkpoint.
    // Updated atomically with each slot advance (same BackgroundWorker::transaction).
    // On restart the worker loads from here — never from pgmqtt_topic_mappings — so the
    // in-memory cache always reflects the mapping state at confirmed_flush_lsn, not
    // a "future" state that the WAL hasn't reached yet.
    run_sql_or_error(
        "CREATE TABLE IF NOT EXISTS pgmqtt_slot_mappings (
            schema_name   text NOT NULL,
            table_name    text NOT NULL,
            mapping_name  text NOT NULL DEFAULT 'default',
            topic_template text NOT NULL,
            payload_template text NOT NULL,
            qos int DEFAULT 0,
            template_type text NOT NULL DEFAULT 'jinja2',
            PRIMARY KEY (schema_name, table_name, mapping_name)
        )",
        "create slot mappings table",
    );

    // Message store: all MQTT messages routed through pgmqtt
    run_sql_or_error(
        "CREATE TABLE IF NOT EXISTS pgmqtt_messages (
            id bigserial PRIMARY KEY,
            topic text NOT NULL,
            payload bytea,
            qos int DEFAULT 0,
            retain boolean DEFAULT false,
            created_at timestamptz DEFAULT NOW()
        )",
        "create messages table",
    );

    // Retained messages: map from topic to current retained message
    run_sql_or_error(
        "CREATE TABLE IF NOT EXISTS pgmqtt_retained (
            topic text PRIMARY KEY,
            message_id bigint REFERENCES pgmqtt_messages(id) ON DELETE CASCADE
        )",
        "create retained table",
    );

    // Client sessions: persistent session state per client_id
    run_sql_or_error(
        "CREATE TABLE IF NOT EXISTS pgmqtt_sessions (
            client_id text PRIMARY KEY,
            next_packet_id integer NOT NULL DEFAULT 1,
            expiry_interval integer NOT NULL DEFAULT 0,
            disconnected_at timestamptz
        )",
        "create sessions table",
    );

    // Session messages: per-session queues for pending messages
    run_sql_or_error(
        "CREATE TABLE IF NOT EXISTS pgmqtt_session_messages (
            client_id text NOT NULL REFERENCES pgmqtt_sessions(client_id) ON DELETE CASCADE,
            message_id bigint NOT NULL REFERENCES pgmqtt_messages(id) ON DELETE CASCADE,

            packet_id integer,
            sent_at timestamptz,

            created_at timestamptz NOT NULL DEFAULT NOW(),

            PRIMARY KEY (client_id, message_id)
        )",
        "create session messages table",
    );

    // Client subscriptions: topic filters each client is subscribed to
    run_sql_or_error(
        "CREATE TABLE IF NOT EXISTS pgmqtt_subscriptions (
            client_id text NOT NULL REFERENCES pgmqtt_sessions(client_id) ON DELETE CASCADE,
            topic_filter text NOT NULL,
            qos integer NOT NULL DEFAULT 0,
            PRIMARY KEY (client_id, topic_filter)
        )",
        "create subscriptions table",
    );

    // Inbound MQTT → PostgreSQL table mappings.
    // Allows MQTT devices to publish messages that are automatically written
    // to PostgreSQL tables based on topic pattern matching and column maps.
    run_sql_or_error(
        "CREATE TABLE IF NOT EXISTS pgmqtt_inbound_mappings (
            mapping_name     text NOT NULL DEFAULT 'default',
            topic_pattern    text NOT NULL,
            target_schema    text NOT NULL DEFAULT 'public',
            target_table     text NOT NULL,
            column_map       jsonb NOT NULL,
            op               text NOT NULL DEFAULT 'insert',
            conflict_columns text[] DEFAULT NULL,
            template_type    text NOT NULL DEFAULT 'jsonpath',
            PRIMARY KEY (mapping_name)
        )",
        "create inbound mappings table",
    );

    // Indexes for efficient queries
    let _ = Spi::run(
        "CREATE INDEX IF NOT EXISTS idx_session_messages_message_id ON pgmqtt_session_messages(message_id)",
    );
    let _ = Spi::run(
        "CREATE INDEX IF NOT EXISTS idx_session_messages_client_id ON pgmqtt_session_messages(client_id)",
    );
    let _ = Spi::run("CREATE INDEX IF NOT EXISTS idx_subscriptions_client_id ON pgmqtt_subscriptions(client_id)");
    let _ = Spi::run(
        "CREATE INDEX IF NOT EXISTS idx_retained_message_id ON pgmqtt_retained(message_id)",
    );
}
