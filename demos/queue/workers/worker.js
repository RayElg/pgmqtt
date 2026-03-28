const mqtt = require('mqtt');

const BROKER_URL  = process.env.MQTT_URL    || 'mqtt://localhost:1883';
const WORKER_ID   = process.env.WORKER_ID   || require('os').hostname();
const WORKER_TYPE = process.env.WORKER_TYPE;
const MIN_MS      = parseInt(process.env.MIN_MS || '1000');
const MAX_MS      = parseInt(process.env.MAX_MS || '3000');
const sleep = ms => new Promise(r => setTimeout(r, ms));

if (!WORKER_TYPE) {
    console.error('WORKER_TYPE env var is required');
    process.exit(1);
}

const SHARED_FILTER = `$share/${WORKER_TYPE}_workers/jobs/${WORKER_TYPE}/pending`;

let completed = 0;
let failed = 0;
let currentJob = null;

const client = mqtt.connect(BROKER_URL, {
    protocolVersion: 5,
    clientId: WORKER_ID,
    clean: true,
    reconnectPeriod: 5000,
});

client.on('connect', () => {
    console.log(`[${WORKER_ID}] connected to ${BROKER_URL}`);
    client.subscribe(SHARED_FILTER, (err) => {
        if (err) console.error(`[${WORKER_ID}] subscribe error:`, err.message);
        else console.log(`[${WORKER_ID}] subscribed: ${SHARED_FILTER}`);
    });
    setInterval(sendHeartbeat, 2000);
    sendHeartbeat();
});

function sendHeartbeat() {
    client.publish(`workers/${WORKER_ID}/heartbeat`, JSON.stringify({
        worker_type: WORKER_TYPE,
        status: currentJob ? 'busy' : 'idle',
        current_job: currentJob || '',
        jobs_completed: completed,
        jobs_failed: failed,
    }));
}

client.on('message', async (topic, payload) => {
    // Unsubscribe immediately so the broker routes the next job elsewhere.
    client.unsubscribe(SHARED_FILTER);
    try {
        const job = JSON.parse(payload.toString());
        if (job.status === 'queued') await processJob(job);
    } catch (e) {
        console.error(`[${WORKER_ID}] message error:`, e.message);
    }
    // Ready for the next job.
    client.subscribe(SHARED_FILTER);
});

async function processJob(job) {
    currentJob = job.id;

    client.publish(`jobs/${job.id}/started`, JSON.stringify({
        status: 'running',
        worker_id: WORKER_ID,
        job_type: WORKER_TYPE,
    }));
    console.log(`[${WORKER_ID}] started ${job.id}`);

    const ms = MIN_MS + Math.random() * (MAX_MS - MIN_MS);
    await sleep(ms);

    const ok = Math.random() > 0.1;
    const status = ok ? 'completed' : 'failed';
    const result = ok
        ? `Done by ${WORKER_ID} in ${(ms / 1000).toFixed(1)}s`
        : `Error in ${WORKER_ID}: processing timeout`;

    client.publish(`jobs/${job.id}/result`, JSON.stringify({
        status,
        result,
        job_type: WORKER_TYPE,
    }));

    if (ok) completed++; else failed++;
    console.log(`[${WORKER_ID}] ${ok ? 'completed' : 'FAILED'} ${job.id} (${(ms / 1000).toFixed(1)}s)`);

    currentJob = null;
}

client.on('error', (err) => console.error(`[${WORKER_ID}] error:`, err.message));
client.on('close', () => console.log(`[${WORKER_ID}] disconnected`));
