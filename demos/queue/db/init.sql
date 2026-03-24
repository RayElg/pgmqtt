-- pgmqtt Queue Demo
-- Architecture: Producer → PostgREST → jobs table → pgmqtt outbound CDC → MQTT → Workers
--               Workers → MQTT → pgmqtt inbound → jobs table (status update) → Dashboard

CREATE EXTENSION IF NOT EXISTS pgmqtt;

-- =============================================================================
-- Tables
-- =============================================================================

-- Job queue: rows ARE the queue entries
CREATE TABLE jobs (
    id TEXT PRIMARY KEY DEFAULT 'job-' || substr(md5(random()::text), 1, 8),
    job_type TEXT NOT NULL,
    worker_slot INT NOT NULL DEFAULT 0,
    payload JSONB DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'queued',
    result TEXT,
    worker_id TEXT,
    submitted_at TIMESTAMPTZ DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);
ALTER TABLE jobs REPLICA IDENTITY FULL;

-- Assign worker slot on new job creation + auto-set timestamps on status transitions
CREATE OR REPLACE FUNCTION job_timestamps() RETURNS TRIGGER AS $$
BEGIN
    -- Hash-based round-robin: deterministic 0/1 from the generated job ID
    IF TG_OP = 'INSERT' AND NEW.status = 'queued' THEN
        NEW.worker_slot = abs(hashtext(NEW.id)) % 2;
    END IF;
    IF NEW.status = 'running' AND (OLD IS NULL OR OLD.status IS DISTINCT FROM 'running') THEN
        NEW.started_at = now();
    END IF;
    IF NEW.status IN ('completed', 'failed') AND (OLD IS NULL OR OLD.status NOT IN ('completed', 'failed')) THEN
        NEW.completed_at = now();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER job_status_timestamps
    BEFORE INSERT OR UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION job_timestamps();

-- Worker registry (device twin, upserted via heartbeat)
CREATE TABLE workers (
    worker_id TEXT PRIMARY KEY,
    worker_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'idle',
    current_job TEXT,
    last_heartbeat TIMESTAMPTZ DEFAULT now(),
    jobs_completed INT DEFAULT 0,
    jobs_failed INT DEFAULT 0
);

-- =============================================================================
-- Aggregation view (served directly by PostgREST)
-- =============================================================================

CREATE VIEW queue_stats AS
SELECT
    (SELECT count(*)::int FROM jobs WHERE status = 'queued') AS queued,
    (SELECT count(*)::int FROM jobs WHERE status = 'running') AS running,
    (SELECT count(*)::int FROM jobs WHERE status = 'completed') AS completed,
    (SELECT count(*)::int FROM jobs WHERE status = 'failed') AS failed,
    (SELECT count(*)::int FROM jobs) AS total,
    (SELECT round(avg(EXTRACT(EPOCH FROM (completed_at - submitted_at)))::numeric, 1)
     FROM jobs WHERE completed_at IS NOT NULL
       AND completed_at > now() - interval '2 minutes') AS avg_latency_s,
    (SELECT count(*)::int FROM jobs
     WHERE completed_at > now() - interval '1 minute') AS throughput_per_min;

-- =============================================================================
-- PostgREST role & grants
-- =============================================================================

CREATE ROLE web_anon NOLOGIN;
GRANT web_anon TO postgres;
GRANT USAGE ON SCHEMA public TO web_anon;
GRANT SELECT ON jobs, workers, queue_stats TO web_anon;
GRANT INSERT ON jobs TO web_anon;

-- =============================================================================
-- Inbound mappings (MQTT → Postgres)
-- =============================================================================

-- Worker claims a job (status → running)
SELECT pgmqtt_add_inbound_mapping(
    'jobs/{job_id}/started',
    'jobs',
    '{"id": "{job_id}", "status": "$.status", "worker_id": "$.worker_id", "job_type": "$.job_type"}'::jsonb,
    'upsert',
    ARRAY['id'],
    'public',
    'job_started'
);

-- Worker reports result
SELECT pgmqtt_add_inbound_mapping(
    'jobs/{job_id}/result',
    'jobs',
    '{"id": "{job_id}", "status": "$.status", "result": "$.result", "job_type": "$.job_type"}'::jsonb,
    'upsert',
    ARRAY['id'],
    'public',
    'job_result'
);

-- Worker heartbeat → device twin
SELECT pgmqtt_add_inbound_mapping(
    'workers/{worker_id}/heartbeat',
    'workers',
    '{"worker_id": "{worker_id}", "worker_type": "$.worker_type", "status": "$.status", "current_job": "$.current_job", "jobs_completed": "$.jobs_completed", "jobs_failed": "$.jobs_failed"}'::jsonb,
    'upsert',
    ARRAY['worker_id'],
    'public',
    'worker_heartbeat'
);

-- =============================================================================
-- Outbound mapping (Postgres → MQTT via CDC)
-- =============================================================================

-- Every change to jobs table is published; workers only act on status=queued
-- worker_slot (0 or 1) routes each job to exactly one worker per type
SELECT pgmqtt_add_outbound_mapping(
    'public',
    'jobs',
    'jobs/{{ columns.job_type }}/{{ columns.worker_slot }}/pending',
    '{{ columns | tojson }}'
);
