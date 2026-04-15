const POSTGREST = '/api';
const BRIDGE    = '/sse';

const POLL_MS   = 500;    // how often to refresh SQL tabs
const MAX_STREAM_ENTRIES = 60;

// ─── Tab switching ─────────────────────────────────────────────────────────
document.querySelectorAll('.tab').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
        btn.classList.add('active');
        document.getElementById(`tab-${btn.dataset.tab}`).classList.add('active');
    });
});

// ─── Helpers ───────────────────────────────────────────────────────────────
function fmtNum(v) {
    if (v === null || v === undefined) return '—';
    return Number(v).toLocaleString();
}
function fmtTs(unix) {
    if (!unix) return '—';
    return new Date(unix * 1000).toLocaleTimeString();
}
function fmtAge(unix) {
    if (!unix) return '—';
    const secs = Math.floor(Date.now() / 1000) - unix;
    if (secs < 5)   return 'just now';
    if (secs < 60)  return `${secs}s ago`;
    if (secs < 3600) return `${Math.floor(secs/60)}m ago`;
    return `${Math.floor(secs/3600)}h ago`;
}

// Flash an element once with the .changed class
function flashEl(el) {
    el.classList.remove('changed');
    void el.offsetWidth; // force reflow
    el.classList.add('changed');
}

// ─── Tab 1: SQL Polling — pgmqtt_metrics() ────────────────────────────────
const HIGHLIGHT_COUNTERS = [
    'connections_current',
    'msgs_received',
    'msgs_sent',
    'db_batches_committed',
];

// Track previous values for delta display
const _prevMetrics = {};

async function fetchMetrics() {
    try {
        const res = await fetch(`${POSTGREST}/rpc/pgmqtt_metrics`, {
            headers: { Accept: 'application/json' },
        });
        if (!res.ok) return;
        const rows = await res.json();
        if (!Array.isArray(rows) || !rows.length) return;

        const byName = Object.fromEntries(rows.map(r => [r.metric_name, r.value]));

        // Build stat cards once
        const statRow = document.getElementById('stat-row');
        if (!statRow.dataset.built) {
            statRow.dataset.built = '1';
            HIGHLIGHT_COUNTERS.forEach(k => {
                const card = document.createElement('div');
                card.className = 'stat-card';
                card.innerHTML = `
                    <div class="stat-value" id="sv-${k}">—</div>
                    <div class="stat-label">${k.replace(/_/g,' ')}</div>`;
                statRow.appendChild(card);
            });
        }

        // Update stat cards with flash + delta badge
        HIGHLIGHT_COUNTERS.forEach(k => {
            const el = document.getElementById(`sv-${k}`);
            if (!el) return;
            const cur = byName[k] ?? 0;
            const prev = _prevMetrics[k];
            const delta = (prev !== undefined && cur > prev) ? cur - prev : 0;

            el.textContent = fmtNum(cur);

            if (delta > 0) {
                flashEl(el);
                // remove any existing badge then add a fresh one
                const old = el.parentElement.querySelector('.delta-badge');
                if (old) old.remove();
                const badge = document.createElement('span');
                badge.className = 'delta-badge';
                badge.textContent = `+${fmtNum(delta)}`;
                el.after(badge);
                setTimeout(() => badge.remove(), 2500);
            }
            _prevMetrics[k] = cur;
        });

        // Full metrics table
        const tbody = document.getElementById('metrics-tbody');
        tbody.innerHTML = rows.map(r => `
            <tr>
                <td>${r.metric_name}</td>
                <td>${fmtNum(r.value)}</td>
                <td style="color:var(--muted)">${r.unit}</td>
                <td style="color:var(--muted);font-size:.75rem">${r.description}</td>
            </tr>`).join('');

    } catch (_) {}
}
fetchMetrics();
setInterval(fetchMetrics, POLL_MS);

// ─── Tab 2: Connections ────────────────────────────────────────────────────
const _prevConnIds = new Set();

async function fetchConns() {
    try {
        const res = await fetch(`${POSTGREST}/rpc/pgmqtt_connections`, {
            headers: { Accept: 'application/json' },
        });
        if (!res.ok) return;
        const rows = await res.json();
        if (!Array.isArray(rows)) return;

        const tbody = document.getElementById('conns-tbody');
        if (!rows.length) {
            tbody.innerHTML = '<tr><td colspan="8" style="color:var(--muted);text-align:center;padding:2rem">No connected clients yet</td></tr>';
            _prevConnIds.clear();
            return;
        }

        const curIds = new Set(rows.map(r => r.client_id));
        tbody.innerHTML = rows.map(r => {
            const isNew = !_prevConnIds.has(r.client_id);
            return `<tr class="${isNew ? 'row-new' : ''}">
                <td>${r.client_id}</td>
                <td style="color:var(--accent2)">${r.transport}</td>
                <td>${fmtTs(r.connected_at_unix)}</td>
                <td>${fmtAge(r.last_activity_at_unix)}</td>
                <td>${r.keep_alive_secs}s</td>
                <td>${fmtNum(r.msgs_received)}</td>
                <td>${fmtNum(r.msgs_sent)}</td>
                <td>${fmtNum(r.queue_depth)}</td>
            </tr>`;
        }).join('');

        _prevConnIds.clear();
        curIds.forEach(id => _prevConnIds.add(id));
    } catch (_) {}
}
fetchConns();
setInterval(fetchConns, POLL_MS);

// ─── Tab 3: Live Stream — NOTIFY → SSE ────────────────────────────────────
const STREAM_KEYS = [
    'connections_current',
    'msgs_received',
    'msgs_sent',
    'bytes_received',
    'db_batches_committed',
];

const dot   = document.getElementById('stream-dot');
const label = document.getElementById('stream-label');
const feed  = document.getElementById('stream-feed');

// Track last values to show deltas in the stream
let _streamPrev = {};

function connectStream() {
    const es = new EventSource(`${BRIDGE}/stream`);

    es.onopen = () => {
        dot.classList.add('live');
        label.textContent = 'Connected — waiting for first flush…';
    };

    es.onmessage = (ev) => {
        let snap;
        try { snap = JSON.parse(ev.data); } catch (_) { return; }

        label.textContent = `Live — ${new Date().toLocaleTimeString()}`;

        const ts = new Date(snap.captured_at_unix * 1000).toLocaleTimeString();
        const kvHtml = STREAM_KEYS
            .filter(k => snap[k] !== undefined)
            .map(k => {
                const cur   = snap[k];
                const prev  = _streamPrev[k];
                const delta = (prev !== undefined && cur > prev) ? ` <b>+${fmtNum(cur - prev)}</b>` : '';
                return `<span>${k.replace(/_/g,' ')} <b>${fmtNum(cur)}</b>${delta}</span>`;
            })
            .join('');

        _streamPrev = { ..._streamPrev, ...snap };

        const entry = document.createElement('div');
        entry.className = 'stream-entry row-new';
        entry.innerHTML = `<div class="ts">${ts}</div><div class="kv">${kvHtml}</div>`;
        feed.prepend(entry);
        while (feed.children.length > MAX_STREAM_ENTRIES) feed.removeChild(feed.lastChild);
    };

    es.onerror = () => {
        dot.classList.remove('live');
        label.textContent = 'Disconnected — reconnecting…';
        es.close();
        setTimeout(connectStream, 3000);
    };
}
connectStream();

// ─── Tab 4: Hook Log ───────────────────────────────────────────────────────
let _maxHookId = 0;

async function fetchHookLog() {
    try {
        const res = await fetch(
            `${POSTGREST}/pgmqtt_hook_log?order=id.desc&limit=30`,
            { headers: { Accept: 'application/json' } }
        );
        if (!res.ok) return;
        const rows = await res.json();
        if (!Array.isArray(rows)) return;

        const tbody = document.getElementById('hook-tbody');
        if (!rows.length) {
            tbody.innerHTML = '<tr><td colspan="7" style="color:var(--muted);text-align:center;padding:2rem">Waiting for first flush…</td></tr>';
            return;
        }

        const newMax = rows[0]?.id ?? 0;
        tbody.innerHTML = rows.map(r => {
            const p    = r.payload || {};
            const errs = (p.db_session_errors||0) + (p.db_message_errors||0) + (p.db_subscription_errors||0)
                       + (p.cdc_render_errors||0) + (p.cdc_slot_errors||0) + (p.cdc_persist_errors||0)
                       + (p.connections_rejected||0);
            const isNew = r.id > _maxHookId;
            return `
            <tr class="hook-row${r.is_alert ? ' alert' : ''}${isNew ? ' row-new' : ''}">
                <td>${new Date(r.logged_at).toLocaleTimeString()}</td>
                <td style="color:${r.is_alert ? 'var(--danger)' : 'var(--accent2)'}">
                    ${r.is_alert ? '⚠ YES' : 'no'}
                </td>
                <td>${fmtNum(p.connections_current)}</td>
                <td>${fmtNum(p.msgs_received)}</td>
                <td>${fmtNum(p.db_batches_committed)}</td>
                <td style="color:${errs > 0 ? 'var(--danger)' : 'var(--muted)'}">${errs}</td>
                <td class="payload-cell">${JSON.stringify(p)}</td>
            </tr>`;
        }).join('');

        _maxHookId = newMax;
    } catch (_) {}
}
fetchHookLog();
setInterval(fetchHookLog, POLL_MS);
