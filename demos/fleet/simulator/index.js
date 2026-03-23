const mqtt = require('mqtt');

const BROKER_URL = process.env.MQTT_URL || 'mqtt://localhost:1883';

const vehicles = ['V-001', 'V-002', 'V-003', 'V-004', 'V-005'];

// Initialize vehicle state (Toronto — spread across the city)
const startPositions = [
    { lat: 43.6710, lng: -79.3870 },  // Downtown / Financial District
    { lat: 43.6790, lng: -79.4130 },  // Kensington / Little Italy
    { lat: 43.6920, lng: -79.3980 },  // Annex / Bloor West
    { lat: 43.6580, lng: -79.3700 },  // Distillery / Corktown
    { lat: 43.6850, lng: -79.3470 },  // Leslieville / Riverdale
];
const state = {};
vehicles.forEach((id, i) => {
    const sp = startPositions[i];
    state[id] = {
        lat: sp.lat + (Math.random() - 0.5) * 0.008,
        lng: sp.lng + (Math.random() - 0.5) * 0.008,
        speed: 25 + Math.random() * 35,
        fuel: 50 + Math.random() * 50,
        engine_temp: 185 + Math.random() * 20,
        odometer: 10000 + Math.random() * 50000,
    };
});

const client = mqtt.connect(BROKER_URL, {
    protocolVersion: 5,
    clean: true,
    reconnectPeriod: 5000,
});

client.on('connect', () => {
    console.log(`Simulator connected to ${BROKER_URL}`);

    // Listen for commands from dispatch
    client.subscribe('fleet/+/command', (err) => {
        if (!err) console.log('Subscribed to fleet/+/command');
    });

    // Publish telemetry: one vehicle per second, rotating
    let idx = 0;
    setInterval(() => {
        const vehicleId = vehicles[idx % vehicles.length];
        idx++;
        const s = state[vehicleId];

        // Random walk
        s.lat += (Math.random() - 0.5) * 0.0005;
        s.lng += (Math.random() - 0.5) * 0.0005;
        s.speed = Math.max(0, Math.min(85, s.speed + (Math.random() - 0.5) * 8));
        s.fuel = Math.max(0, Math.min(100, s.fuel - Math.random() * 0.05));
        s.engine_temp = Math.max(160, Math.min(260, s.engine_temp + (Math.random() - 0.5) * 3));
        s.odometer += s.speed / 3600;

        const payload = {
            lat: +s.lat.toFixed(6),
            lng: +s.lng.toFixed(6),
            speed: +s.speed.toFixed(1),
            fuel: +s.fuel.toFixed(1),
            engine_temp: +s.engine_temp.toFixed(1),
            odometer: +s.odometer.toFixed(1),
        };

        client.publish(`fleet/${vehicleId}/telemetry`, JSON.stringify(payload));
        console.log(`[${vehicleId}] telemetry: speed=${payload.speed} fuel=${payload.fuel} engine=${payload.engine_temp}`);

        // Threshold-based events
        if (s.speed > 75) {
            client.publish(`fleet/${vehicleId}/event`, JSON.stringify({
                event_type: 'speeding',
                severity: 'warning',
                details: `Speed ${s.speed.toFixed(1)} mph exceeds 75 mph limit`,
            }));
        }
        if (s.engine_temp > 240) {
            client.publish(`fleet/${vehicleId}/event`, JSON.stringify({
                event_type: 'overheating',
                severity: 'critical',
                details: `Engine temp ${s.engine_temp.toFixed(1)}°F exceeds threshold`,
            }));
        }
        if (s.fuel < 15) {
            client.publish(`fleet/${vehicleId}/event`, JSON.stringify({
                event_type: 'low_fuel',
                severity: 'warning',
                details: `Fuel level at ${s.fuel.toFixed(1)}%`,
            }));
        }
        if (Math.random() > 0.97) {
            client.publish(`fleet/${vehicleId}/event`, JSON.stringify({
                event_type: 'hard_brake',
                severity: 'info',
                details: 'Sudden deceleration event detected',
            }));
        }
    }, 1000);
});

client.on('message', (topic, payload) => {
    const vehicleId = topic.split('/')[1];
    console.log(`[${vehicleId}] COMMAND RECEIVED: ${payload.toString()}`);
});

client.on('error', (err) => {
    console.error('MQTT error:', err.message);
});

client.on('close', () => {
    console.log('Disconnected, reconnecting...');
});
