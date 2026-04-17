//! SQL initialization for pgmqtt v0.1.0 schema.
//!
//! This module contains all DDL statements for the initial schema version.
//! As a greenfield extension (no migrations from prior versions), all tables
//! and indexes are created with their final v0.1.0 structure.

use pgrx::spi::Spi;

/// Execute a DDL statement, aborting extension init if it fails.
fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("pgmqtt: failed to {}: {}", operation, e));
}

/// Initialize all v0.1.0 tables and indexes.
pub fn init_010() {
    // Topic mappings: CDC → MQTT topic + payload templates.
    // Multiple mappings per (schema, table) are allowed via distinct mapping_name values,
    // enabling parallel publish to multiple topics during schema migrations.
    run_ddl(
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
    run_ddl(
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
    run_ddl(
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
    run_ddl(
        "CREATE TABLE IF NOT EXISTS pgmqtt_retained (
            topic text PRIMARY KEY,
            message_id bigint REFERENCES pgmqtt_messages(id) ON DELETE CASCADE
        )",
        "create retained table",
    );

    // Client sessions: persistent session state per client_id
    run_ddl(
        "CREATE TABLE IF NOT EXISTS pgmqtt_sessions (
            client_id text PRIMARY KEY,
            next_packet_id integer NOT NULL DEFAULT 1,
            expiry_interval integer NOT NULL DEFAULT 0,
            disconnected_at timestamptz
        )",
        "create sessions table",
    );

    // Session messages: per-session queues for pending messages
    run_ddl(
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
    run_ddl(
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
    run_ddl(
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

    // Tracks QoS 1 messages awaiting inbound table writes (virtual subscriber).
    // One row per (message, mapping) pair. Deleted on successful write;
    // moved to dead_letters after MAX_RETRIES or non-retryable errors.
    //
    // mapping_name intentionally has no FK to pgmqtt_inbound_mappings:
    // if a mapping is removed while pending rows still reference it, the
    // virtual subscriber detects "mapping no longer exists" and dead-letters
    // the orphaned row rather than silently dropping it.
    run_ddl(
        "CREATE TABLE IF NOT EXISTS pgmqtt_inbound_pending (
            message_id   bigint NOT NULL REFERENCES pgmqtt_messages(id) ON DELETE CASCADE,
            mapping_name text NOT NULL,
            retry_count  int NOT NULL DEFAULT 0,
            last_error   text,
            next_retry_at timestamptz NOT NULL DEFAULT now(),
            created_at    timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (message_id, mapping_name)
        )",
        "create inbound pending table",
    );

    // Dead-letter table for inbound messages that failed permanently.
    run_ddl(
        "CREATE TABLE IF NOT EXISTS pgmqtt_dead_letters (
            id                  bigserial PRIMARY KEY,
            original_message_id bigint,
            topic               text NOT NULL,
            payload             bytea,
            mapping_name        text NOT NULL,
            error_message       text,
            retry_count         int NOT NULL DEFAULT 0,
            dead_lettered_at    timestamptz NOT NULL DEFAULT now()
        )",
        "create dead letters table",
    );

    // Indexes for efficient queries
    run_ddl(
        "CREATE INDEX IF NOT EXISTS idx_session_messages_message_id ON pgmqtt_session_messages(message_id)",
        "create idx_session_messages_message_id",
    );
    run_ddl(
        "CREATE INDEX IF NOT EXISTS idx_session_messages_client_id ON pgmqtt_session_messages(client_id)",
        "create idx_session_messages_client_id",
    );
    run_ddl(
        "CREATE INDEX IF NOT EXISTS idx_subscriptions_client_id ON pgmqtt_subscriptions(client_id)",
        "create idx_subscriptions_client_id",
    );
    run_ddl(
        "CREATE INDEX IF NOT EXISTS idx_retained_message_id ON pgmqtt_retained(message_id)",
        "create idx_retained_message_id",
    );
    run_ddl(
        "CREATE INDEX IF NOT EXISTS idx_inbound_pending_next_retry ON pgmqtt_inbound_pending(next_retry_at)",
        "create idx_inbound_pending_next_retry",
    );

    // ── Enterprise observability tables ──────────────────────────────────────

    // Live metrics snapshot: single row updated by the background worker on each
    // flush interval. The CHECK (id = 1) constraint enforces the single-row invariant.
    run_ddl(
        "CREATE TABLE IF NOT EXISTS pgmqtt_metrics_current (
            id                       int PRIMARY KEY DEFAULT 1 CHECK (id = 1),
            captured_at              bigint NOT NULL DEFAULT 0,
            started_at               bigint NOT NULL DEFAULT 0,
            last_reset_at            bigint NOT NULL DEFAULT 0,
            connections_accepted     bigint NOT NULL DEFAULT 0,
            connections_rejected     bigint NOT NULL DEFAULT 0,
            connections_current      bigint NOT NULL DEFAULT 0,
            disconnections_clean     bigint NOT NULL DEFAULT 0,
            disconnections_unclean   bigint NOT NULL DEFAULT 0,
            wills_fired              bigint NOT NULL DEFAULT 0,
            sessions_created         bigint NOT NULL DEFAULT 0,
            sessions_resumed         bigint NOT NULL DEFAULT 0,
            sessions_expired         bigint NOT NULL DEFAULT 0,
            msgs_received            bigint NOT NULL DEFAULT 0,
            msgs_received_qos0       bigint NOT NULL DEFAULT 0,
            msgs_received_qos1       bigint NOT NULL DEFAULT 0,
            bytes_received           bigint NOT NULL DEFAULT 0,
            msgs_sent                bigint NOT NULL DEFAULT 0,
            bytes_sent               bigint NOT NULL DEFAULT 0,
            msgs_dropped_queue_full  bigint NOT NULL DEFAULT 0,
            pubacks_sent             bigint NOT NULL DEFAULT 0,
            pubacks_received         bigint NOT NULL DEFAULT 0,
            subscribe_ops            bigint NOT NULL DEFAULT 0,
            unsubscribe_ops          bigint NOT NULL DEFAULT 0,
            cdc_events_processed     bigint NOT NULL DEFAULT 0,
            cdc_msgs_published       bigint NOT NULL DEFAULT 0,
            cdc_render_errors        bigint NOT NULL DEFAULT 0,
            cdc_slot_errors          bigint NOT NULL DEFAULT 0,
            cdc_persist_errors       bigint NOT NULL DEFAULT 0,
            inbound_writes_ok        bigint NOT NULL DEFAULT 0,
            inbound_writes_failed    bigint NOT NULL DEFAULT 0,
            inbound_retries          bigint NOT NULL DEFAULT 0,
            inbound_dead_letters     bigint NOT NULL DEFAULT 0,
            db_batches_committed     bigint NOT NULL DEFAULT 0,
            db_session_errors        bigint NOT NULL DEFAULT 0,
            db_message_errors        bigint NOT NULL DEFAULT 0,
            db_subscription_errors   bigint NOT NULL DEFAULT 0
        )",
        "create metrics current table",
    );

    // Historical time-series: one row per flush interval, queryable by time range.
    // All timestamps stored as Unix epoch seconds (bigint) for portability.
    run_ddl(
        "CREATE TABLE IF NOT EXISTS pgmqtt_metrics_snapshots (
            id                       bigserial,
            snapshot_at              bigint NOT NULL,
            started_at               bigint NOT NULL DEFAULT 0,
            last_reset_at            bigint NOT NULL DEFAULT 0,
            connections_accepted     bigint NOT NULL DEFAULT 0,
            connections_rejected     bigint NOT NULL DEFAULT 0,
            connections_current      bigint NOT NULL DEFAULT 0,
            disconnections_clean     bigint NOT NULL DEFAULT 0,
            disconnections_unclean   bigint NOT NULL DEFAULT 0,
            wills_fired              bigint NOT NULL DEFAULT 0,
            sessions_created         bigint NOT NULL DEFAULT 0,
            sessions_resumed         bigint NOT NULL DEFAULT 0,
            sessions_expired         bigint NOT NULL DEFAULT 0,
            msgs_received            bigint NOT NULL DEFAULT 0,
            msgs_received_qos0       bigint NOT NULL DEFAULT 0,
            msgs_received_qos1       bigint NOT NULL DEFAULT 0,
            bytes_received           bigint NOT NULL DEFAULT 0,
            msgs_sent                bigint NOT NULL DEFAULT 0,
            bytes_sent               bigint NOT NULL DEFAULT 0,
            msgs_dropped_queue_full  bigint NOT NULL DEFAULT 0,
            pubacks_sent             bigint NOT NULL DEFAULT 0,
            pubacks_received         bigint NOT NULL DEFAULT 0,
            subscribe_ops            bigint NOT NULL DEFAULT 0,
            unsubscribe_ops          bigint NOT NULL DEFAULT 0,
            cdc_events_processed     bigint NOT NULL DEFAULT 0,
            cdc_msgs_published       bigint NOT NULL DEFAULT 0,
            cdc_render_errors        bigint NOT NULL DEFAULT 0,
            cdc_slot_errors          bigint NOT NULL DEFAULT 0,
            cdc_persist_errors       bigint NOT NULL DEFAULT 0,
            inbound_writes_ok        bigint NOT NULL DEFAULT 0,
            inbound_writes_failed    bigint NOT NULL DEFAULT 0,
            inbound_retries          bigint NOT NULL DEFAULT 0,
            inbound_dead_letters     bigint NOT NULL DEFAULT 0,
            db_batches_committed     bigint NOT NULL DEFAULT 0,
            db_session_errors        bigint NOT NULL DEFAULT 0,
            db_message_errors        bigint NOT NULL DEFAULT 0,
            db_subscription_errors   bigint NOT NULL DEFAULT 0
        ) WITH (fillfactor=100)",
        "create metrics snapshots table",
    );
    // BRIN is far smaller than B-tree for an append-only time-series table
    // where snapshot_at increases monotonically with insertion order.
    run_ddl(
        "CREATE INDEX IF NOT EXISTS idx_metrics_snapshots_time ON pgmqtt_metrics_snapshots USING BRIN (snapshot_at)",
        "create idx_metrics_snapshots_time",
    );

    // Per-client connection detail: truncated and rewritten by the background
    // worker on each connections-cache flush interval.
    run_ddl(
        "CREATE TABLE IF NOT EXISTS pgmqtt_connections_cache (
            client_id            text PRIMARY KEY,
            transport            text NOT NULL DEFAULT 'mqtt',
            connected_at_unix    bigint NOT NULL DEFAULT 0,
            last_activity_at_unix bigint NOT NULL DEFAULT 0,
            keep_alive_secs      int NOT NULL DEFAULT 0,
            msgs_received        bigint NOT NULL DEFAULT 0,
            msgs_sent            bigint NOT NULL DEFAULT 0,
            bytes_received       bigint NOT NULL DEFAULT 0,
            bytes_sent           bigint NOT NULL DEFAULT 0,
            subscriptions        int NOT NULL DEFAULT 0,
            queue_depth          int NOT NULL DEFAULT 0,
            inflight_count       int NOT NULL DEFAULT 0,
            will_set             boolean NOT NULL DEFAULT false,
            cached_at_unix       bigint NOT NULL DEFAULT 0
        )",
        "create connections cache table",
    );
}
