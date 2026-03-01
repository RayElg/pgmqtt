-- Create the extension
CREATE EXTENSION IF NOT EXISTS pgmqtt;

-- Create a table for machine vitals
CREATE TABLE IF NOT EXISTS machine_vitals (
    machine_id TEXT PRIMARY KEY,
    cpu_usage NUMERIC,
    mem_usage NUMERIC,
    temperature NUMERIC,
    status TEXT,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Set replica identity full to ensure we get column data on updates/deletes
ALTER TABLE machine_vitals REPLICA IDENTITY FULL;

-- Add mapping: publish every change to vitals/<op>/<machine_id>
-- Topic: vitals/{{ op | lower }}/{{ columns.machine_id }}
-- Payload: JSON of columns
SELECT pgmqtt_add_mapping(
    'public',
    'machine_vitals',
    'vitals/{{ op | lower }}/{{ columns.machine_id }}',
    '{{ columns | tojson }}'
);

-- Seed with some initial data
INSERT INTO machine_vitals (machine_id, cpu_usage, mem_usage, temperature, status)
VALUES 
('M-001', 12.5, 45.2, 38.4, 'ONLINE'),
('M-002', 8.1, 30.7, 36.2, 'ONLINE'),
('M-003', 45.8, 78.4, 52.1, 'BUSY')
ON CONFLICT (machine_id) DO NOTHING;
