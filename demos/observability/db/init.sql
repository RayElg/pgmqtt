-- pgmqtt Observability Demo
-- Sets up the hook function, alert log, and PostgREST access for all four
-- observability scenarios: SQL polling, NOTIFY stream, hook function, Grafana.

CREATE EXTENSION IF NOT EXISTS pgmqtt;

-- ─── Scenario 3: Hook function ────────────────────────────────────────────────
-- Called by the background worker on every metrics flush.
-- Records every snapshot; marks rows where any error counter is non-zero.

CREATE TABLE pgmqtt_hook_log (
    id        BIGSERIAL    PRIMARY KEY,
    logged_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    is_alert  BOOLEAN      NOT NULL DEFAULT false,
    payload   JSONB        NOT NULL
);

CREATE OR REPLACE FUNCTION pgmqtt_alert_hook(snap JSONB) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO pgmqtt_hook_log (is_alert, payload)
    VALUES (
        COALESCE((snap->>'connections_rejected')::bigint, 0) > 0 OR
        COALESCE((snap->>'db_session_errors')::bigint, 0)      > 0 OR
        COALESCE((snap->>'db_message_errors')::bigint, 0)      > 0 OR
        COALESCE((snap->>'db_subscription_errors')::bigint, 0) > 0 OR
        COALESCE((snap->>'cdc_render_errors')::bigint, 0)      > 0 OR
        COALESCE((snap->>'cdc_slot_errors')::bigint, 0)        > 0 OR
        COALESCE((snap->>'cdc_persist_errors')::bigint, 0)     > 0,
        snap
    );
END;
$$;

-- Wire the hook once the function exists
ALTER SYSTEM SET pgmqtt.metrics_hook_function = 'public.pgmqtt_alert_hook';
SELECT pg_reload_conf();
