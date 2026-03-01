import mqtt from 'mqtt';

const machineGrid = document.getElementById('machine-grid');
const eventFeed = document.getElementById('event-feed');
const connStatus = document.getElementById('conn-status');

const machines = {};

// Connect directly to pgmqtt on port 9001 (WebSocket)
const WS_URL = `ws://${window.location.hostname}:9001`;

function addLog(topic, message, isSystem = false) {
    const entry = document.createElement('div');
    entry.className = `feed-entry ${isSystem ? 'system' : ''}`;

    const time = new Date().toLocaleTimeString();

    if (isSystem) {
        entry.textContent = `[${time}] SYSTEM: ${message}`;
    } else {
        entry.innerHTML = `
            <span class="time">${time}</span>
            <span class="topic">${topic}</span>
            <span class="payload">${JSON.stringify(message)}</span>
        `;
    }

    eventFeed.prepend(entry);

    // Limit log size
    if (eventFeed.children.length > 50) {
        eventFeed.removeChild(eventFeed.lastChild);
    }
}

function updateMachineCard(id, data) {
    if (!machines[id]) {
        // Remove placeholder if it's the first machine
        if (Object.keys(machines).length === 0) {
            machineGrid.innerHTML = '';
        }

        const card = document.createElement('div');
        card.className = 'machine-card';
        card.id = `machine-${id}`;
        machineGrid.appendChild(card);
        machines[id] = card;
    }

    const card = machines[id];
    const statusClass = data.status === 'ONLINE' ? 'online' : 'error';

    // Smoothly update content
    card.innerHTML = `
        <div class="machine-header">
            <span class="machine-id">${id}</span>
            <span class="status-tag ${statusClass}">${data.status}</span>
        </div>
        <div class="vitals">
            <div class="vital-item">
                <div class="vital-label">
                    <span>CPU Usage</span>
                    <span class="vital-value">${data.cpu_usage}%</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" style="width: ${data.cpu_usage}%"></div>
                </div>
            </div>
            <div class="vital-item">
                <div class="vital-label">
                    <span>Memory Usage</span>
                    <span class="vital-value">${data.mem_usage}%</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" style="width: ${data.mem_usage}%; background-color: var(--success)"></div>
                </div>
            </div>
            <div class="vital-item">
                <div class="vital-label">
                    <span>Temperature</span>
                    <span class="vital-value">${data.temperature}°C</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" style="width: ${Math.min(100, (data.temperature / 100) * 100)}%; background-color: var(--warning)"></div>
                </div>
            </div>
        </div>
    `;
}

console.log(`Connecting to MQTT via ${WS_URL}...`);
addLog(null, `Connecting to ${WS_URL}`, true);

const client = mqtt.connect(WS_URL, {
    protocolVersion: 5,
    clean: true,
    connectTimeout: 5000,
});

client.on('connect', () => {
    console.log('Connected to broker');
    connStatus.innerHTML = '<span class="dot green"></span><span class="text">CONNECTED</span>';
    addLog(null, 'Connected to pgmqtt broker', true);

    client.subscribe('vitals/#', (err) => {
        if (!err) {
            addLog(null, 'Subscribed to vitals/#', true);
        }
    });
});

client.on('message', (topic, payload) => {
    try {
        const data = JSON.parse(payload.toString());
        addLog(topic, data);

        // Topic format: vitals/<op>/<machine_id>
        const parts = topic.split('/');
        if (parts.length >= 3) {
            const machineId = parts[2];
            updateMachineCard(machineId, data);
        }
    } catch (e) {
        console.error('Failed to parse message', e);
    }
});

client.on('error', (err) => {
    console.error('MQTT Error:', err);
    addLog(null, `Error: ${err.message}`, true);
});

client.on('close', () => {
    console.log('Connection closed');
    connStatus.innerHTML = '<span class="dot red"></span><span class="text">DISCONNECTED</span>';
    addLog(null, 'Connection lost', true);
});
