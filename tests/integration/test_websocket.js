#!/usr/bin/env node
/**
 * test_websocket.js — MQTT-over-WebSocket integration test for pgmqtt
 *
 * What it tests:
 *   1. Connects to the broker via ws://localhost:9001 using mqttjs (MQTT 5)
 *   2. Subscribes to 'ws_test/#'
 *   3. Waits for a SUBACK → confirms WebSocket upgrade + MQTT handshake worked
 *   4. If PG_CONTAINER env var is set, inserts a row via docker exec to
 *      trigger a CDC PUBLISH and verifies it arrives over the WebSocket
 *
 * Usage (from pg-docker/):
 *   node test_websocket.js
 *
 * Env vars:
 *   WS_URL        — broker WebSocket URL (default: ws://localhost:9001)
 *   PG_CONTAINER  — Docker container name for psql exec (optional)
 *   TIMEOUT_MS    — per-step timeout in ms (default: 8000)
 */

'use strict';

const mqtt = require('mqtt');
const { execSync } = require('child_process');

const WS_URL = process.env.WS_URL || 'ws://127.0.0.1:9001';
const PG_CONTAINER = process.env.PG_CONTAINER || 'postgres';
const TIMEOUT_MS = parseInt(process.env.TIMEOUT_MS || '8000', 10);

const TABLE = 'ws_test_table';
const TOPIC_BASE = 'ws_test';
const TOPIC_FILTER = `${TOPIC_BASE}/#`;

let exitCode = 0;

// ── Helpers ───────────────────────────────────────────────────────────────────

function psql(sql) {
    if (!PG_CONTAINER) return;
    execSync(
        `docker compose exec -T ${PG_CONTAINER} psql -U postgres -d postgres -c "${sql.replace(/"/g, '\\"')}"`,
        { stdio: 'pipe' }
    );
}

function bail(msg) {
    console.error(`\n✗ FAILED: ${msg}`);
    process.exit(1);
}

function withTimeout(ms, label, fn) {
    return new Promise((resolve, reject) => {
        const t = setTimeout(() => reject(new Error(`Timed out waiting for: ${label}`)), ms);
        fn((...args) => { clearTimeout(t); resolve(...args); },
            (...args) => { clearTimeout(t); reject(...args); });
    });
}

// ── Main ──────────────────────────────────────────────────────────────────────

(async () => {
    console.log(`Connecting to ${WS_URL} …`);

    const client = mqtt.connect(WS_URL, {
        protocolVersion: 5,
        clientId: `pgmqtt-ws-test-${Date.now()}`,
        clean: true,
        connectTimeout: TIMEOUT_MS,
        reconnectPeriod: 0,          // no auto-reconnect during tests
    });

    // ── Step 1: wait for CONNACK ──────────────────────────────────────────────
    await withTimeout(TIMEOUT_MS, 'CONNACK', (resolve, reject) => {
        client.on('connect', () => resolve());
        client.on('error', (e) => reject(e));
    });
    console.log('✓ Connected via WebSocket (CONNACK received)');

    // ── Step 2: subscribe ─────────────────────────────────────────────────────
    await withTimeout(TIMEOUT_MS, 'SUBACK', (resolve, reject) => {
        client.subscribe(TOPIC_FILTER, { qos: 0 }, (err, granted) => {
            if (err) return reject(err);
            const g = granted[0];
            if (g.qos > 1) return reject(new Error(`Unexpected QoS grant: ${g.qos}`));
            resolve(g);
        });
    });
    console.log(`✓ Subscribed to '${TOPIC_FILTER}' (QoS 0 granted)`);

    // ── Step 3: optional CDC PUBLISH test ────────────────────────────────────
    if (PG_CONTAINER) {
        console.log(`\nSetting up CDC table '${TABLE}' in container '${PG_CONTAINER}' …`);
        psql(`DROP TABLE IF EXISTS ${TABLE}`);
        psql(`CREATE TABLE ${TABLE} (id serial PRIMARY KEY, data text)`);
        psql(`ALTER TABLE ${TABLE} REPLICA IDENTITY FULL`);
        psql(
            `SELECT pgmqtt_add_outbound_mapping('public', '${TABLE}', ` +
            `'${TOPIC_BASE}/{{ op | lower }}', '{{ columns | tojson }}')`
        );

        // Give the worker a moment to reload mappings
        await new Promise(r => setTimeout(r, 600));

        const received = withTimeout(TIMEOUT_MS, 'PUBLISH from CDC', (resolve, reject) => {
            client.on('message', (topic, payload) => {
                console.log(`✓ Received PUBLISH: topic='${topic}' payload=${payload.toString()}`);
                resolve({ topic, payload: payload.toString() });
            });
        });

        psql(`INSERT INTO ${TABLE} (data) VALUES ('ws_websocket_test')`);
        console.log(`  → Inserted row into '${TABLE}', waiting for PUBLISH …`);

        const msg = await received;
        if (!msg.topic.startsWith(TOPIC_BASE)) {
            bail(`Unexpected topic: ${msg.topic}`);
        }
        console.log('✓ CDC PUBLISH received over WebSocket');
    } else {
        console.log('\n(Skipping CDC PUBLISH test — PG_CONTAINER not set)');
    }

    // ── Done ──────────────────────────────────────────────────────────────────
    client.end();
    console.log('\n=== WebSocket test PASSED ===');
    process.exit(0);
})().catch((err) => {
    console.error(`\n✗ ERROR: ${err.message}`);
    process.exit(1);
});
