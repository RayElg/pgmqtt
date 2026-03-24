const mqtt = require('mqtt');

const BROKER_URL = process.env.MQTT_URL || 'mqtt://localhost:1883';
const API_URL = process.env.API_URL || 'http://postgrest:3000';
const sleep = ms => new Promise(r => setTimeout(r, ms));

// === Worker definitions ===

const workerDefs = [
    { id: 'W-001', type: 'image_resize', slot: 0, minMs: 3000, maxMs: 6000 },
    { id: 'W-002', type: 'image_resize', slot: 1, minMs: 3000, maxMs: 6000 },
    { id: 'W-003', type: 'email_send',   slot: 0, minMs: 1000, maxMs: 3000 },
    { id: 'W-004', type: 'email_send',   slot: 1, minMs: 1000, maxMs: 3000 },
    { id: 'W-005', type: 'data_export',  slot: 0, minMs: 4000, maxMs: 8000 },
    { id: 'W-006', type: 'data_export',  slot: 1, minMs: 4000, maxMs: 8000 },
    { id: 'W-007', type: 'pdf_generate', slot: 0, minMs: 3000, maxMs: 7000 },
    { id: 'W-008', type: 'pdf_generate', slot: 1, minMs: 3000, maxMs: 7000 },
];

const state = {};
workerDefs.forEach(w => {
    state[w.id] = { ...w, busy: false, currentJob: null, completed: 0, failed: 0 };
});

// === MQTT connection ===

const client = mqtt.connect(BROKER_URL, {
    protocolVersion: 5,
    clean: true,
    reconnectPeriod: 5000,
});

client.on('connect', () => {
    console.log(`Workers connected to ${BROKER_URL}`);

    // Each worker subscribes to its own slot — no two workers share a topic
    workerDefs.forEach(w => {
        client.subscribe(`jobs/${w.type}/${w.slot}/pending`, (err) => {
            if (!err) console.log(`[${w.id}] subscribed to jobs/${w.type}/${w.slot}/pending`);
        });
    });

    // Heartbeats every 2 s
    setInterval(sendHeartbeats, 2000);
    sendHeartbeats();

    // Start auto-generating jobs after PostgREST is up
    waitForPostgREST().then(startJobGenerator);
});

function sendHeartbeats() {
    Object.values(state).forEach(w => {
        client.publish(`workers/${w.id}/heartbeat`, JSON.stringify({
            worker_type: w.type,
            status: w.busy ? 'busy' : 'idle',
            current_job: w.currentJob || '',
            jobs_completed: w.completed,
            jobs_failed: w.failed,
        }));
    });
}

// === Job dispatch (MQTT → worker) ===

client.on('message', (topic, payload) => {
    try {
        const msg = JSON.parse(payload.toString());
        if (msg.status !== 'queued') return;

        // topic: jobs/{type}/{slot}/pending
        const parts = topic.split('/');
        const jobType = parts[1];
        const slot = parseInt(parts[2]);
        const worker = Object.values(state).find(
            w => w.type === jobType && w.slot === slot && !w.busy
        );
        if (!worker) return;

        processJob(worker, msg);
    } catch (e) {
        console.error('Message error:', e.message);
    }
});

// === Job processing ===

async function processJob(worker, job) {
    worker.busy = true;
    worker.currentJob = job.id;

    // Claim the job
    client.publish(`jobs/${job.id}/started`, JSON.stringify({
        status: 'running',
        worker_id: worker.id,
        job_type: job.job_type,
    }));
    console.log(`[${worker.id}] started ${job.id} (${job.job_type})`);

    // Simulate processing
    const ms = worker.minMs + Math.random() * (worker.maxMs - worker.minMs);
    await sleep(ms);

    // Report result (90 % success)
    const ok = Math.random() > 0.1;
    const status = ok ? 'completed' : 'failed';
    const result = ok
        ? `Done by ${worker.id} in ${(ms / 1000).toFixed(1)}s`
        : `Error in ${worker.id}: processing timeout`;

    client.publish(`jobs/${job.id}/result`, JSON.stringify({
        status,
        result,
        job_type: job.job_type,
    }));

    if (ok) worker.completed++; else worker.failed++;
    console.log(`[${worker.id}] ${ok ? 'completed' : 'FAILED'} ${job.id} (${(ms / 1000).toFixed(1)}s)`);

    worker.busy = false;
    worker.currentJob = null;

    // Drain: pick up any queued jobs left behind
    setTimeout(() => { if (!worker.busy) drainQueue(worker); }, 300);
}

async function drainQueue(worker) {
    try {
        const resp = await fetch(
            `${API_URL}/jobs?status=eq.queued&job_type=eq.${worker.type}&worker_slot=eq.${worker.slot}&order=submitted_at.asc&limit=1`
        );
        const jobs = await resp.json();
        if (jobs.length > 0 && !worker.busy) {
            processJob(worker, jobs[0]);
        }
    } catch {}
}

// === Job generator (auto-submits via PostgREST) ===

const JOB_TYPES = ['image_resize', 'email_send', 'data_export', 'pdf_generate'];

async function waitForPostgREST() {
    for (;;) {
        try {
            const r = await fetch(`${API_URL}/`);
            if (r.ok || r.status === 404) return;
        } catch {}
        console.log('Waiting for PostgREST...');
        await sleep(2000);
    }
}

function startJobGenerator() {
    console.log('Job generator started');
    let idx = 0;
    function next() {
        const type = JOB_TYPES[idx % JOB_TYPES.length];
        idx++;
        fetch(`${API_URL}/jobs`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', Prefer: 'return=minimal' },
            body: JSON.stringify({ job_type: type }),
        }).catch(() => {});
        setTimeout(next, 1200 + Math.random() * 800);
    }
    next();
}

client.on('error', (err) => console.error('MQTT error:', err.message));
client.on('close', () => console.log('Disconnected, reconnecting...'));
