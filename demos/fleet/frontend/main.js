import mqtt from 'mqtt';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';
import Chart from 'chart.js/auto';

// === Config ===
const WS_URL = `ws://${window.location.hostname}:9001`;
const API = '/api';

// === DOM ===
const dom = {
    mqttStatus: document.getElementById('mqtt-status'),
    restStatus: document.getElementById('rest-status'),
    statsRow: document.getElementById('stats-row'),
    vehicleGrid: document.getElementById('vehicle-grid'),
    telemetryBody: document.getElementById('telemetry-body'),
    mqttFeed: document.getElementById('mqtt-feed'),
    eventsList: document.getElementById('events-list'),
    cmdVehicle: document.getElementById('cmd-vehicle'),
    cmdType: document.getElementById('cmd-type'),
    cmdSend: document.getElementById('cmd-send'),
};

let knownVehicles = [];

// =============================================================================
// Map — Leaflet with dark tiles
// =============================================================================

const map = L.map('map').setView([43.6750, -79.3850], 13);
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>',
    maxZoom: 19,
}).addTo(map);
const mapMarkers = {};
let mapFitted = false;

function updateMapMarkers(vehicles) {
    const bounds = [];
    vehicles.forEach(v => {
        const lat = Number(v.lat);
        const lng = Number(v.lng);
        bounds.push([lat, lng]);

        if (mapMarkers[v.vehicle_id]) {
            mapMarkers[v.vehicle_id].setLatLng([lat, lng]);
            mapMarkers[v.vehicle_id].setTooltipContent(
                `<b>${v.vehicle_id}</b><br>${v.speed} km/h`
            );
        } else {
            mapMarkers[v.vehicle_id] = L.circleMarker([lat, lng], {
                radius: 8,
                color: '#58a6ff',
                fillColor: '#58a6ff',
                fillOpacity: 0.8,
                weight: 2,
            }).addTo(map).bindTooltip(
                `<b>${v.vehicle_id}</b><br>${v.speed} km/h`,
                { permanent: true, direction: 'top', offset: [0, -12], className: 'vehicle-tooltip' }
            );
        }
    });

    if (!mapFitted && bounds.length > 0) {
        map.fitBounds(bounds, { padding: [50, 50] });
        mapFitted = true;
    }
}

// =============================================================================
// Charts — rolling averages
// =============================================================================

const MAX_POINTS = 30;
const chartLabels = [];
const speedData = [];
const fuelData = [];

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

const speedChart = new Chart(document.getElementById('speed-chart'), {
    type: 'line',
    data: {
        labels: chartLabels,
        datasets: [{
            data: speedData,
            borderColor: '#58a6ff',
            backgroundColor: 'rgba(88, 166, 255, 0.1)',
            fill: true,
            tension: 0.4,
            borderWidth: 2,
        }],
    },
    options: chartOpts(0, 140),
});

const fuelChart = new Chart(document.getElementById('fuel-chart'), {
    type: 'line',
    data: {
        labels: chartLabels,
        datasets: [{
            data: fuelData,
            borderColor: '#3fb950',
            backgroundColor: 'rgba(63, 185, 80, 0.1)',
            fill: true,
            tension: 0.4,
            borderWidth: 2,
        }],
    },
    options: chartOpts(0, 100),
});

function updateCharts(s) {
    chartLabels.push(new Date().toLocaleTimeString());
    speedData.push(s.avg_speed);
    fuelData.push(s.avg_fuel);
    if (chartLabels.length > MAX_POINTS) {
        chartLabels.shift();
        speedData.shift();
        fuelData.shift();
    }
    speedChart.update();
    fuelChart.update();
}

// =============================================================================
// MQTT — real-time stream from pgmqtt broker
// =============================================================================

const client = mqtt.connect(WS_URL, { protocolVersion: 5, clean: true, connectTimeout: 5000 });

client.on('connect', () => {
    setStatus(dom.mqttStatus, true, 'MQTT');
    addFeedEntry(null, 'Connected to pgmqtt broker', true);
    client.subscribe('fleet/#');
});

client.on('message', (topic, payload) => {
    try {
        addFeedEntry(topic, JSON.parse(payload.toString()));
    } catch {
        addFeedEntry(topic, payload.toString());
    }
});

client.on('close', () => setStatus(dom.mqttStatus, false, 'MQTT'));
client.on('error', () => {});

// =============================================================================
// PostgREST — polls database state via REST (zero service layer)
// =============================================================================

async function fetchJSON(path) {
    const resp = await fetch(`${API}${path}`, { cache: 'no-store' });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    return resp.json();
}

async function pollSummary() {
    try {
        const rows = await fetchJSON('/fleet_summary');
        const s = rows[0];
        if (s) {
            renderSummary(s);
            updateCharts(s);
        }
        setStatus(dom.restStatus, true, 'REST');
    } catch {
        setStatus(dom.restStatus, false, 'REST');
    }
}

async function pollVehicles() {
    try {
        const data = await fetchJSON('/vehicle_status?order=vehicle_id');
        renderVehicleGrid(data);
        updateVehicleDropdown(data);
        updateMapMarkers(data);
    } catch { /* status handled by pollSummary */ }
}

async function pollTelemetry() {
    try {
        const data = await fetchJSON('/vehicle_telemetry?order=recorded_at.desc&limit=15');
        renderTelemetryTable(data);
    } catch {}
}

async function pollEvents() {
    try {
        const data = await fetchJSON('/vehicle_events?order=recorded_at.desc&limit=10');
        renderEventsList(data);
    } catch {}
}

// =============================================================================
// Rendering
// =============================================================================

function setStatus(el, ok, label) {
    el.innerHTML = `<span class="dot ${ok ? 'green' : 'red'}"></span><span>${label}</span>`;
}

function renderSummary(s) {
    const cards = [
        { value: s.total_vehicles ?? 0, label: 'Vehicles' },
        { value: s.avg_speed ?? '\u2014', label: 'Avg Speed (km/h)' },
        { value: s.avg_fuel ?? '\u2014', label: 'Avg Fuel (%)' },
        { value: s.recent_events ?? 0, label: 'Events (5 min)' },
        { value: s.total_readings ?? 0, label: 'Total Readings' },
    ];
    dom.statsRow.innerHTML = cards.map(c => `
        <div class="stat-card">
            <div class="stat-value">${c.value}</div>
            <div class="stat-label">${c.label}</div>
        </div>
    `).join('');
}

function renderVehicleGrid(vehicles) {
    if (!vehicles.length) {
        dom.vehicleGrid.innerHTML = '<div class="placeholder">Waiting for telemetry...</div>';
        return;
    }
    dom.vehicleGrid.innerHTML = vehicles.map(v => {
        const speedPct = Math.min(100, (v.speed / 140) * 100);
        const speedColor = v.speed > 120 ? 'var(--danger)' : v.speed > 105 ? 'var(--warning)' : 'var(--accent-color)';
        const fuelColor = v.fuel < 15 ? 'var(--danger)' : v.fuel < 30 ? 'var(--warning)' : 'var(--success)';
        const tempPct = Math.min(100, ((v.engine_temp - 70) / 60) * 100);
        const tempColor = v.engine_temp > 115 ? 'var(--danger)' : v.engine_temp > 100 ? 'var(--warning)' : 'var(--success)';

        return `
        <div class="vehicle-card">
            <div class="vehicle-header">
                <span class="vehicle-id">${v.vehicle_id}</span>
                <span class="coord">${Number(v.lat).toFixed(4)}, ${Number(v.lng).toFixed(4)}</span>
            </div>
            <div class="vitals">
                <div class="vital-item">
                    <div class="vital-label"><span>Speed</span><span class="vital-value">${v.speed} km/h</span></div>
                    <div class="progress-bar"><div class="progress-fill" style="width:${speedPct}%;background:${speedColor}"></div></div>
                </div>
                <div class="vital-item">
                    <div class="vital-label"><span>Fuel</span><span class="vital-value">${v.fuel}%</span></div>
                    <div class="progress-bar"><div class="progress-fill" style="width:${v.fuel}%;background:${fuelColor}"></div></div>
                </div>
                <div class="vital-item">
                    <div class="vital-label"><span>Engine</span><span class="vital-value">${v.engine_temp}&deg;C</span></div>
                    <div class="progress-bar"><div class="progress-fill" style="width:${tempPct}%;background:${tempColor}"></div></div>
                </div>
            </div>
        </div>`;
    }).join('');
}

function renderTelemetryTable(rows) {
    if (!rows.length) {
        dom.telemetryBody.innerHTML = '<tr><td colspan="5" class="empty">Waiting for data...</td></tr>';
        return;
    }
    dom.telemetryBody.innerHTML = rows.map(r => `
        <tr>
            <td>${new Date(r.recorded_at).toLocaleTimeString()}</td>
            <td>${r.vehicle_id}</td>
            <td>${r.speed}</td>
            <td>${r.fuel}%</td>
            <td>${r.engine_temp}&deg;C</td>
        </tr>
    `).join('');
}

function renderEventsList(events) {
    if (!events.length) {
        dom.eventsList.innerHTML = '<div class="empty">No events yet</div>';
        return;
    }
    dom.eventsList.innerHTML = events.map(e => {
        const cls = e.severity === 'critical' ? 'critical' : e.severity === 'warning' ? 'warning' : 'info';
        return `
        <div class="event-entry">
            <span class="event-time">${new Date(e.recorded_at).toLocaleTimeString()}</span>
            <span class="event-vehicle">${e.vehicle_id}</span>
            <span class="severity-badge ${cls}">${e.severity}</span>
            <span class="event-type">${e.event_type}</span>
        </div>`;
    }).join('');
}

function addFeedEntry(topic, data, isSystem = false) {
    const entry = document.createElement('div');
    const isCommand = !isSystem && topic && topic.includes('/command');
    const isDispatch = isSystem && typeof data === 'string' && data.includes('dispatched');
    entry.className = `feed-entry ${isSystem ? 'system' : ''} ${isCommand ? 'command' : ''} ${isDispatch ? 'dispatch' : ''}`;
    const time = new Date().toLocaleTimeString();

    if (isSystem) {
        entry.textContent = `[${time}] ${data}`;
    } else if (isCommand) {
        const str = typeof data === 'object' ? JSON.stringify(data) : data;
        entry.innerHTML = `<span class="time">${time}</span><span class="topic">${topic}</span><br><span class="payload">${str}</span>`;
    } else {
        const str = typeof data === 'object' ? JSON.stringify(data) : data;
        entry.innerHTML = `<span class="time">${time}</span><span class="topic">${topic}</span><span class="payload">${str}</span>`;
    }

    dom.mqttFeed.prepend(entry);
    while (dom.mqttFeed.children.length > 50) {
        dom.mqttFeed.removeChild(dom.mqttFeed.lastChild);
    }
}

function updateVehicleDropdown(vehicles) {
    const ids = vehicles.map(v => v.vehicle_id);
    if (JSON.stringify(ids) === JSON.stringify(knownVehicles)) return;
    knownVehicles = ids;
    const current = dom.cmdVehicle.value;
    dom.cmdVehicle.innerHTML = ids.map(id =>
        `<option value="${id}" ${id === current ? 'selected' : ''}>${id}</option>`
    ).join('');
}

// =============================================================================
// Commands — POST via PostgREST → row inserted → CDC publishes to MQTT
// =============================================================================

dom.cmdSend.addEventListener('click', async () => {
    const vehicleId = dom.cmdVehicle.value;
    const command = dom.cmdType.value;
    if (!vehicleId) return;

    dom.cmdSend.disabled = true;
    try {
        await fetch(`${API}/vehicle_commands`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ vehicle_id: vehicleId, command }),
        });
        addFeedEntry(null, `Command "${command}" dispatched to ${vehicleId}`, true);
    } catch (e) {
        addFeedEntry(null, `Failed: ${e.message}`, true);
    }
    dom.cmdSend.disabled = false;
});

// =============================================================================
// Polling
// =============================================================================

setInterval(pollSummary, 2000);
setInterval(pollVehicles, 2000);
setInterval(pollTelemetry, 2000);
setInterval(pollEvents, 5000);

pollSummary();
pollVehicles();
pollTelemetry();
pollEvents();
