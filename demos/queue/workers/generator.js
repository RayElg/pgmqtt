const API_URL  = process.env.API_URL || 'http://postgrest:3000';
const sleep = ms => new Promise(r => setTimeout(r, ms));

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

async function run() {
    await waitForPostgREST();
    console.log('Job generator started');
    let idx = 0;
    for (;;) {
        const type = JOB_TYPES[idx % JOB_TYPES.length];
        idx++;
        fetch(`${API_URL}/jobs`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', Prefer: 'return=minimal' },
            body: JSON.stringify({ job_type: type }),
        }).catch(() => {});
        await sleep(1200 + Math.random() * 800);
    }
}

run();
