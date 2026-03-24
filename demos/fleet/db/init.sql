-- pgmqtt Fleet Demo
-- Architecture: Simulator → MQTT → pgmqtt inbound → Postgres ← PostgREST ← Browser
--               Browser → PostgREST → vehicle_commands → pgmqtt outbound → MQTT → Simulator

CREATE EXTENSION IF NOT EXISTS pgmqtt;

-- =============================================================================
-- Tables
-- =============================================================================

-- Device twin: current state per vehicle (upserted via inbound mapping)
CREATE TABLE vehicle_status (
    vehicle_id TEXT PRIMARY KEY,
    lat NUMERIC,
    lng NUMERIC,
    speed NUMERIC,
    fuel NUMERIC,
    engine_temp NUMERIC,
    odometer NUMERIC,
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Audit log: every telemetry reading (append-only via inbound mapping)
CREATE TABLE vehicle_telemetry (
    id BIGSERIAL PRIMARY KEY,
    vehicle_id TEXT NOT NULL,
    lat NUMERIC,
    lng NUMERIC,
    speed NUMERIC,
    fuel NUMERIC,
    engine_temp NUMERIC,
    odometer NUMERIC,
    recorded_at TIMESTAMPTZ DEFAULT now()
);

-- Safety events (append-only via inbound mapping)
CREATE TABLE vehicle_events (
    id BIGSERIAL PRIMARY KEY,
    vehicle_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    details TEXT,
    recorded_at TIMESTAMPTZ DEFAULT now()
);

-- Operator commands (written via PostgREST, published via outbound CDC)
CREATE TABLE vehicle_commands (
    id BIGSERIAL PRIMARY KEY,
    vehicle_id TEXT NOT NULL,
    command TEXT NOT NULL,
    issued_at TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE vehicle_commands REPLICA IDENTITY FULL;

-- Auto-update timestamp on vehicle_status upsert
CREATE OR REPLACE FUNCTION update_timestamp() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER vehicle_status_updated
    BEFORE INSERT OR UPDATE ON vehicle_status
    FOR EACH ROW EXECUTE FUNCTION update_timestamp();

-- =============================================================================
-- Aggregation view (served directly by PostgREST)
-- =============================================================================

CREATE VIEW fleet_summary AS
SELECT
    (SELECT count(*)::int FROM vehicle_status) AS total_vehicles,
    (SELECT round(avg(speed), 1) FROM vehicle_status) AS avg_speed,
    (SELECT round(avg(fuel), 1) FROM vehicle_status) AS avg_fuel,
    (SELECT count(*)::int FROM vehicle_events
     WHERE recorded_at > now() - interval '5 minutes') AS recent_events,
    (SELECT count(*)::int FROM vehicle_telemetry) AS total_readings;

-- =============================================================================
-- PostgREST role & grants (zero-service-layer REST API)
-- =============================================================================

CREATE ROLE web_anon NOLOGIN;
GRANT web_anon TO postgres;
GRANT USAGE ON SCHEMA public TO web_anon;
GRANT SELECT ON vehicle_status, vehicle_telemetry, vehicle_events, fleet_summary TO web_anon;
GRANT INSERT ON vehicle_commands TO web_anon;
GRANT USAGE, SELECT ON SEQUENCE vehicle_commands_id_seq TO web_anon;

-- =============================================================================
-- Inbound mappings (MQTT → Postgres)
-- =============================================================================

-- 1. Telemetry audit log — every message becomes an append-only row
SELECT pgmqtt_add_inbound_mapping(
    'fleet/{vehicle_id}/telemetry',
    'vehicle_telemetry',
    '{"vehicle_id": "{vehicle_id}", "lat": "$.lat", "lng": "$.lng", "speed": "$.speed", "fuel": "$.fuel", "engine_temp": "$.engine_temp", "odometer": "$.odometer"}'::jsonb,
    'insert',
    NULL,
    'public',
    'telemetry_log'
);

-- 2. Device twin — upsert keeps latest state per vehicle
SELECT pgmqtt_add_inbound_mapping(
    'fleet/{vehicle_id}/telemetry',
    'vehicle_status',
    '{"vehicle_id": "{vehicle_id}", "lat": "$.lat", "lng": "$.lng", "speed": "$.speed", "fuel": "$.fuel", "engine_temp": "$.engine_temp", "odometer": "$.odometer"}'::jsonb,
    'upsert',
    ARRAY['vehicle_id'],
    'public',
    'vehicle_twin'
);

-- 3. Safety events
SELECT pgmqtt_add_inbound_mapping(
    'fleet/{vehicle_id}/event',
    'vehicle_events',
    '{"vehicle_id": "{vehicle_id}", "event_type": "$.event_type", "severity": "$.severity", "details": "$.details"}'::jsonb,
    'insert',
    NULL,
    'public',
    'vehicle_events_log'
);

-- =============================================================================
-- Outbound mapping (Postgres → MQTT via CDC)
-- =============================================================================

-- Commands inserted via PostgREST are published to the target vehicle
SELECT pgmqtt_add_outbound_mapping(
    'public',
    'vehicle_commands',
    'fleet/{{ columns.vehicle_id }}/command',
    '{{ columns | tojson }}'
);
