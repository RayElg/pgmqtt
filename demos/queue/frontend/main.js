import mqtt from 'mqtt';
import Chart from 'chart.js/auto';

// === Config ===
const WS_URL = `ws://${window.location.hostname}:9001`;
const API = '/api';

// === DOM ===
const dom = {
    mqttStatus: document.getElementById('mqtt-status'),
    restStatus: document.getElementById('rest-status'),
    statsRow: document.getElementById('stats-row'),
    workersGrid: document.getElementById('workers-grid'),
    jobsBody: document.getElementById('jobs-body'),
    mqttFeed: document.getElementById('mqtt-feed'),
    jobType: document.getElementById('job-type'),
    submitBtn: document.getElementById('submit-job'),
};

// =============================================================================
// Charts — rolling throughput & latency
// =============================================================================

const MAX_POINTS = 30;
const chartLabels = [];
const throughputData = [];
const latencyData = [];

const chartOpts = (min, max) => ({
    responsive: true,
    maintainAspectRatio: false,
    scales: {
        x: { display: false },
        y: {
            min, max,
            grid: { color: 'rgba(255,255,255,0.05)' },
            ticks: { color: '#8b949e', font: { size: 10 } },
        },
    },
    plugins: { legend: { display: false } },
    animation: { duration: 400 },
    elements: { point: { radius: 0 } },
});

const throughputChart = new Chart(document.getElementById('throughput-chart'), {
    type: 'line',
    data: {
        labels: chartLabels,
        datasets: [{
            data: throughputData,
            borderColor: '#58a6ff',
            backgroundColor: 'rgba(88, 166, 255, 0.1)',
            fill: true,
            tension: 0.4,
            borderWidth: 2,
        }],
    },
    options: chartOpts(0, 60),
});

const latencyChart = new Chart(document.getElementById('latency-chart'), {
    type: 'line',
    data: {
        labels: chartLabels,
        datasets: [{
            data: latencyData,
            borderColor: '#bc8cff',
            backgroundColor: 'rgba(188, 140, 255, 0.1)',
            fill: true,
            tension: 0.4,
            borderWidth: 2,
        }],
    },
    options: chartOpts(0, 15),
});

function updateCharts(s) {
    chartLabels.push(new Date().toLocaleTimeString());
    throughputData.push(s.throughput_per_min ?? 0);
    latencyData.push(s.avg_latency_s ?? 0);
    if (chartLabels.length > MAX_POINTS) {
        chartLabels.shift();
        throughputData.shift();
        latencyData.shift();
    }
    throughputChart.update();
    latencyChart.update();
}

// =============================================================================
// MQTT — real-time stream
// =============================================================================

const client = mqtt.connect(WS_URL, { protocolVersion: 5, clean: true, connectTimeout: 5000 });

client.on('connect', () => {
    setStatus(dom.mqttStatus, true, 'MQTT');
    addFeedEntry(null, 'Connected to pgmqtt broker', 'system');
    client.subscribe('jobs/#');
});

client.on('message', (topic, payload) => {
    try {
        const data = JSON.parse(payload.toString());
        // Dispatch: jobs/{type}/pending   (3 segments)
        // Started:  jobs/{job_id}/started (3 segments)
        // Result:   jobs/{job_id}/result  (3 segments)
        const parts = topic.split('/');
        const last = parts[parts.length - 1];

        if (last === 'pending' && data.status === 'queued') {
            addFeedEntry(topic, data, 'dispatch');
        } else if (last === 'started') {
            addFeedEntry(topic, data, 'started');
        } else if (last === 'result') {
            addFeedEntry(topic, data, data.status === 'completed' ? 'completed' : 'failed');
        }
    } catch {}
});

client.on('close', () => setStatus(dom.mqttStatus, false, 'MQTT'));
client.on('error', () => {});

// =============================================================================
// PostgREST polling
// =============================================================================

async function fetchJSON(path) {
    const resp = await fetch(`${API}${path}`, { cache: 'no-store' });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    return resp.json();
}

async function pollStats() {
    try {
        const rows = await fetchJSON('/queue_stats');
        const s = rows[0];
        if (s) {
            renderPipeline(s);
            renderStats(s);
            updateCharts(s);
        }
        setStatus(dom.restStatus, true, 'REST');
    } catch {
        setStatus(dom.restStatus, false, 'REST');
    }
}

async function pollWorkers() {
    try {
        const data = await fetchJSON('/workers?order=worker_id');
        renderWorkers(data);
    } catch {}
}

async function pollJobs() {
    try {
        const data = await fetchJSON('/jobs?order=submitted_at.desc&limit=20');
        renderJobTable(data);
    } catch {}
}

// =============================================================================
// Rendering
// =============================================================================

function setStatus(el, ok, label) {
    el.innerHTML = `<span class="dot ${ok ? 'green' : 'red'}"></span><span>${label}</span>`;
}

function renderPipeline(s) {
    document.getElementById('count-queued').textContent = s.queued;
    document.getElementById('count-running').textContent = s.running;
    document.getElementById('count-completed').textContent = s.completed;
    document.getElementById('count-failed').textContent = s.failed;

    document.querySelector('.stage-queued').classList.toggle('active', s.queued > 0);
    document.querySelector('.stage-running').classList.toggle('active', s.running > 0);
}

function renderStats(s) {
    const done = s.completed + s.failed;
    const rate = done > 0 ? Math.round((s.completed / done) * 100) : 100;

    const cards = [
        { value: s.throughput_per_min ?? 0, label: 'Throughput / min' },
        { value: s.avg_latency_s != null ? `${s.avg_latency_s}s` : '\u2014', label: 'Avg Latency' },
        { value: `${rate}%`, label: 'Success Rate' },
        { value: s.total ?? 0, label: 'Total Jobs' },
    ];
    dom.statsRow.innerHTML = cards.map(c => `
        <div class="stat-card">
            <div class="stat-value">${c.value}</div>
            <div class="stat-label">${c.label}</div>
        </div>
    `).join('');
}

function renderWorkers(workers) {
    if (!workers.length) {
        dom.workersGrid.innerHTML = '<div class="placeholder">Waiting for workers...</div>';
        return;
    }
    dom.workersGrid.innerHTML = workers.map(w => {
        const busy = w.status === 'busy';
        return `
        <div class="worker-card ${busy ? 'busy' : ''}">
            <div class="worker-header">
                <span class="worker-id">${w.worker_id}</span>
                <span class="type-badge ${w.worker_type}">${w.worker_type}</span>
            </div>
            <div class="worker-status">
                <span class="dot ${busy ? 'busy' : 'idle'}"></span>
                <span>${busy ? 'Processing' : 'Idle'}</span>
            </div>
            <div class="worker-job">${busy && w.current_job ? w.current_job : ''}</div>
            <div class="worker-progress">
                <div class="progress-bar">
                    ${busy ? '<div class="progress-fill indeterminate"></div>' : ''}
                </div>
            </div>
            <div class="worker-stats">${w.jobs_completed} completed \u00b7 ${w.jobs_failed} failed</div>
        </div>`;
    }).join('');
}

function renderJobTable(jobs) {
    if (!jobs.length) {
        dom.jobsBody.innerHTML = '<tr><td colspan="5" class="empty">Waiting for jobs...</td></tr>';
        return;
    }
    dom.jobsBody.innerHTML = jobs.map(j => {
        const color = {
            queued: 'var(--text-secondary)',
            running: 'var(--accent-color)',
            completed: 'var(--success)',
            failed: 'var(--danger)',
        }[j.status] || 'inherit';

        let dur = '\u2014';
        if (j.completed_at && j.submitted_at) {
            dur = `${((new Date(j.completed_at) - new Date(j.submitted_at)) / 1000).toFixed(1)}s`;
        } else if (j.status === 'running') {
            dur = '\u2026';
        }

        return `
        <tr>
            <td>${j.id}</td>
            <td><span class="type-badge ${j.job_type}">${j.job_type}</span></td>
            <td><span style="color:${color};font-weight:600">${j.status}</span></td>
            <td>${j.worker_id || '\u2014'}</td>
            <td>${dur}</td>
        </tr>`;
    }).join('');
}

function addFeedEntry(topic, data, type) {
    const entry = document.createElement('div');
    entry.className = `feed-entry ${type || ''}`;
    const time = new Date().toLocaleTimeString();

    if (type === 'system' || !topic) {
        entry.textContent = `[${time}] ${typeof data === 'string' ? data : JSON.stringify(data)}`;
    } else {
        const str = typeof data === 'object' ? JSON.stringify(data) : data;
        entry.innerHTML = `<span class="time">${time}</span><span class="topic">${topic}</span><br><span class="payload">${str}</span>`;
    }

    dom.mqttFeed.prepend(entry);
    while (dom.mqttFeed.children.length > 50) dom.mqttFeed.removeChild(dom.mqttFeed.lastChild);
}

// =============================================================================
// Submit Job — POST via PostgREST → row inserted → CDC dispatches to MQTT
// =============================================================================

dom.submitBtn.addEventListener('click', async () => {
    const type = dom.jobType.value;
    dom.submitBtn.disabled = true;
    try {
        await fetch(`${API}/jobs`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ job_type: type }),
        });
        addFeedEntry(null, `Submitted ${type} job`, 'system');
    } catch (e) {
        addFeedEntry(null, `Failed: ${e.message}`, 'system');
    }
    dom.submitBtn.disabled = false;
});

// =============================================================================
// Polling
// =============================================================================

setInterval(pollStats, 1500);
setInterval(pollWorkers, 1000);
setInterval(pollJobs, 2000);

pollStats();
pollWorkers();
pollJobs();
