# pgmqtt Enterprise Features

This document covers the enterprise-only features of pgmqtt: **license management**, **JWT authentication**, **topic-level access control**, and **observability**.

---

## Table of Contents

- [Feature Matrix](#feature-matrix)
- [License Management](#license-management)
- [JWT Authentication](#jwt-authentication)
- [Topic-Level Access Control](#topic-level-access-control)
- [TLS (MQTTS / WSS)](#tls-mqtts--wss)
- [Multi-Process CDC](#multi-process-cdc)
- [Port Management](#port-management)
- [Observability & Metrics](#observability--metrics)
- [SQL Functions](#sql-functions)

---

## Feature Matrix

| Feature | Community | Enterprise |
|---------|-----------|------------|
| MQTT 5.0 broker | Yes | Yes |
| CDC-to-MQTT topic mappings | Yes | Yes |
| MQTT-over-WebSocket | Yes | Yes |
| QoS 0 and QoS 1 | Yes | Yes |
| Session persistence | Yes | Yes |
| Retained messages | Yes | Yes |
| Will messages | Yes | Yes |
| HTTP healthcheck endpoint | Yes | Yes |
| Max concurrent connections | 1,000 | License-defined |
| TLS (MQTTS / WSS) | No | Yes (`tls` feature) |
| Username/password authentication (pg_authid SCRAM-SHA-256) | Yes | Yes |
| Admin commands (`pgmqtt_disconnect_*`, `pgmqtt_reload_acls`) | Yes | Yes |
| JWT authentication | No | Yes (`jwt` feature) |
| Per-topic ACLs (via `pgmqtt_acls` table) | No | Yes (`acl` feature) |
| Per-topic ACLs (via JWT claims) | No | Yes (`jwt` feature) |
| WebSocket-only JWT enforcement | No | Yes (`jwt` feature) |
| JWT `client_id` binding | No | Yes (`jwt` feature) |
| Broker metrics & snapshots | No | Yes (`metrics` feature) |
| Per-connection cache | No | Yes (`metrics` feature) |
| Prometheus exposition | No | Yes (`metrics` feature) |
| Metrics hook functions | No | Yes (`metrics` feature) |
| NOTIFY streaming | No | Yes (`metrics` feature) |
| Dedicated CDC worker process | No | Yes (`multiprocess` feature) |
| License grace period | N/A | Yes |
| `pgmqtt_license_status()` | Returns `community` | Returns `active`/`grace`/`expired` |

---

## License Management

### Overview

pgmqtt uses a signed license token to gate enterprise features. Without a valid license, the extension runs in **Community** mode (max 1,000 concurrent MQTT connections, no enterprise features).

If a license key is set but invalid (bad format, bad signature, tampered), the status is reported as `invalid` with a reason — the extension does **not** silently fall back to Community.

Since this is in an early stage, contact me if you want a license key to try out: raynor (at) rynr (dot) dev

### License Token Format

```
base64url(JSON_payload).base64url(Ed25519_signature)
```

The signature is computed over the raw JSON payload bytes using Ed25519. Tokens are verified against a public key compiled into the extension binary.

### License Payload

```json
{
  "customer": "acme-corp",
  "expires_at": 1710354890,
  "grace_expires_at": 1711049890,
  "features": ["tls", "jwt"],
  "max_connections": 100
}
```

| Field | Type | Description |
|-------|------|-------------|
| `customer` | string | Customer identifier |
| `expires_at` | i64 | Unix timestamp — license expiration |
| `grace_expires_at` | i64 | Unix timestamp — hard cutoff after grace period |
| `features` | string[] | Enabled features: `"tls"`, `"jwt"`, `"acl"`, `"metrics"`, `"multiprocess"` |
| `max_connections` | usize | Maximum concurrent MQTT connections |

Unrecognized feature names in the `features` array will produce a warning log.

### License States

| Status | Condition | Behavior |
|--------|-----------|----------|
| **Community** | No license key set | 1,000 connections, no enterprise features |
| **Invalid** | License key set but malformed/bad signature | 1,000 connections, no enterprise features, warning logged |
| **Active** | `now < expires_at` | Full enterprise features per `features` array |
| **Grace** | `expires_at < now < grace_expires_at` | Enterprise features still available (warn in logs) |
| **Expired** | `now > grace_expires_at` | Falls back to Community limits |

### Configuration

```sql
-- Set license key (persists across restarts)
ALTER SYSTEM SET pgmqtt.license_key = '<token>';
SELECT pg_reload_conf();

-- Check current status
SELECT * FROM pgmqtt_license_status();
```

### Generating Test Licenses

```bash
python scripts/gen_test_license.py \
  --customer test-co \
  --days 30 \
  --features tls jwt \
  --max-connections 50
```


---

## JWT Authentication

### Overview

When configured, pgmqtt validates JSON Web Tokens on MQTT CONNECT. Tokens use **Ed25519** signatures (not HMAC, not RSA). The server verifies the token, checks expiration, and optionally enforces per-topic publish/subscribe permissions and client identity binding via claims.

### JWT Token Format

Standard 3-part JWT: `header.payload.signature`, each part base64url-encoded. The signature covers the literal bytes `"header.payload"`.

### Supported Claims

```json
{
  "sub": "device-42",
  "iat": 1710000000,
  "exp": 1710086400,
  "client_id": "device-42",
  "sub_claims": ["sensors/+", "alerts/#"],
  "pub_claims": ["telemetry/device-42"]
}
```

| Claim | Required | Description |
|-------|----------|-------------|
| `sub` | No | Subject (informational) |
| `iat` | No | Issued-at timestamp |
| `exp` | **Yes** | Expiration timestamp — token rejected if expired |
| `client_id` | No | If set, the MQTT CONNECT `client_id` **must** match this value exactly. Mismatch returns NOT_AUTHORIZED. |
| `sub_claims` | No | Topic filters the client may SUBSCRIBE to |
| `pub_claims` | No | Topic filters the client may PUBLISH to |

### Client ID Binding

When a JWT contains a `client_id` claim, the server enforces that the MQTT CONNECT packet's client identifier matches exactly. This prevents a device from impersonating another device's identity even with a valid token.

- JWT has `client_id: "device-42"`, CONNECT with `client_id="device-42"` → allowed
- JWT has `client_id: "device-42"`, CONNECT with `client_id="device-99"` → **rejected** (0x87)
- JWT has no `client_id` claim → any CONNECT client_id is allowed

### Token Delivery

Tokens can be provided through three channels, checked in this priority order:

1. **MQTT CONNECT password field** — works for both TCP and WebSocket
2. **WebSocket query parameter** — `ws://host:9001/?jwt=<token>`
3. **HTTP Authorization header** — `Authorization: Bearer <token>` during WebSocket upgrade

If a token is present in the password field, query param and header are ignored.

### Configuration

```sql
-- Set the Ed25519 public key (base64url-encoded, 32 bytes)
ALTER SYSTEM SET pgmqtt.jwt_public_key = '<base64url_key>';

-- Or PEM format
ALTER SYSTEM SET pgmqtt.jwt_public_key = '-----BEGIN PUBLIC KEY-----...';

-- Require JWT for all connections (default: false)
ALTER SYSTEM SET pgmqtt.jwt_required = 'on';

-- Require JWT for WebSocket connections only (default: false)
-- When on, TCP connections remain anonymous-capable even if jwt_required is off.
ALTER SYSTEM SET pgmqtt.jwt_required_ws = 'on';

SELECT pg_reload_conf();
```

### Behavior Matrix

| `jwt_public_key` | `jwt_required` | `jwt_required_ws` | Transport | Token | Result |
|---|---|---|---|---|---|
| unset | any | any | any | any | Allowed (no validation) |
| set | `off` | `off` | any | absent | Allowed (anonymous) |
| set | `off` | `off` | any | valid | Allowed, claims enforced |
| set | `off` | `off` | any | invalid | **Rejected** (0x87) |
| set | `on` | any | any | absent | **Rejected** (0x87) |
| set | `off` | `on` | TCP | absent | Allowed (anonymous) |
| set | `off` | `on` | WS/WSS | absent | **Rejected** (0x87) |
| set | `off` | `on` | WS/WSS | valid | Allowed, claims enforced |

**Note:** `jwt_required_ws` only affects WebSocket transports (WS and WSS). When `jwt_required` is `on`, it applies to all transports regardless of `jwt_required_ws`.

### Important Caveats

- **Claims are connection-scoped and immutable for JWT.** Once a client connects with a JWT, its permissions are fixed for the session lifetime. There is no token refresh mechanism — if a token expires mid-session, the existing connection continues operating. Disconnect and reconnect to pick up new claims. (Password-auth ACLs can be refreshed without disconnect via [`pgmqtt_reload_acls`](#admin-commands).)
- **Persistent subscriptions are re-validated on reconnect.** When a client reconnects (without `clean_start`), any persisted subscription whose topic filter is no longer covered by the current `sub_claims` / `pgmqtt_acls` is dropped from both the in-memory tree and `pgmqtt_subscriptions`. This applies regardless of whether the change came from a new JWT, an edited ACL row, or `pgmqtt_reload_acls`.
- **Will messages are authorized at CONNECT and re-checked at fire time.** A Will whose topic is not covered by the client's `pub_claims` causes the CONNECT to be rejected with `0x87` (NOT_AUTHORIZED). If `pgmqtt_reload_acls` later narrows the pub allowlist, a stored Will whose topic is no longer covered is silently dropped instead of being published on disconnect.
- **No audience/issuer validation.** Any valid Ed25519-signed JWT with a non-expired `exp` is accepted. If you share signing keys across services, consider adding application-level claim validation.

---

## Topic-Level Access Control

### Overview

Per-topic authorization is enforced on every SUBSCRIBE and PUBLISH packet for the lifetime of the connection. The allowlist comes from one of two sources, depending on how the client authenticated:

- **JWT** (`jwt` license feature): the `sub_claims` / `pub_claims` arrays inside the validated token.
- **Password auth** (`acl` license feature): rows in the `pgmqtt_acls` table keyed on the authenticated role.

Without the corresponding license feature the table is never consulted, so the client is **unrestricted** — Community-tier password-authenticated clients get full topic access (and `acl_default_deny` has no effect).

### The `pgmqtt_acls` table

```sql
CREATE TABLE pgmqtt_acls (
    role_name     name    NOT NULL,
    topic_filter  text    NOT NULL,
    can_publish   boolean NOT NULL DEFAULT false,
    can_subscribe boolean NOT NULL DEFAULT false,
    PRIMARY KEY (role_name, topic_filter)
);
```

Example: a role that can publish telemetry it owns and subscribe to its own command channel.

```sql
INSERT INTO pgmqtt_acls (role_name, topic_filter, can_publish, can_subscribe) VALUES
  ('mqtt_devices', 'telemetry/{client_id}', true,  false),
  ('mqtt_devices', 'cmd/+',                 false, true);
```

ACLs are loaded once on CONNECT and cached on the connection. To refresh a live connection without disconnecting it, use [`pgmqtt_reload_acls`](#admin-commands).

### Rules

- **Default-deny for password auth (`acl` feature).** A password-authenticated role with **no covering `pgmqtt_acls` row** is denied that operation. Evaluated independently per side: a role granted only `can_subscribe` rows is still denied all publishing. Set `pgmqtt.acl_default_deny = off` (below) to restore the legacy "no rows = unrestricted" behavior.
- **Non-empty allowlist.** The client can only operate on topics that match at least one entry. `can_publish` and `can_subscribe` are evaluated independently.
- **JWT empty claims = unrestricted.** A JWT with an empty/absent `sub_claims` (or `pub_claims`) can operate on any topic. JWT keeps these semantics regardless of `acl_default_deny`, which governs only the `pgmqtt_acls` path.
- **MQTT wildcards supported.** Entries can use `+` (single-level) and `#` (multi-level) wildcards with standard MQTT semantics.

#### Opting back into fail-open: `pgmqtt.acl_default_deny`

| GUC | Type | Default | Description |
|-----|------|---------|-------------|
| `pgmqtt.acl_default_deny` | bool | `on` | When `on` (default), a password-authenticated role with no covering `pgmqtt_acls` row is denied. Set `off` to grant such a role unrestricted access instead (the pre-0.3.0 behavior). |

```sql
-- Only if you specifically want the legacy fail-open behavior:
ALTER SYSTEM SET pgmqtt.acl_default_deny = 'off';
SELECT pg_reload_conf();
```

Notes:

- **Only effective with the `acl` license feature.** Without it, `pgmqtt_acls` is never consulted, so the GUC is a no-op and Community-tier password-authenticated clients remain unrestricted regardless of its value.
- **JWT is unaffected.** JWT `sub_claims` / `pub_claims` keep their own "empty claims = unrestricted" semantics; `acl_default_deny` only governs the `pgmqtt_acls` path.

### Enforcement

| Operation | Denied Response |
|-----------|----------------|
| SUBSCRIBE to unauthorized topic | SUBACK with reason code `0x87` (NOT_AUTHORIZED) |
| PUBLISH QoS 1 to unauthorized topic | PUBACK with reason code `0x87` (NOT_AUTHORIZED) |
| PUBLISH QoS 0 to unauthorized topic | Silently dropped (per MQTT spec, QoS 0 has no acknowledgment) |
| CONNECT with Will on unauthorized topic | CONNACK with reason code `0x87` (NOT_AUTHORIZED); connection closed |
| CONNECT with Will using a wildcard / NUL in the topic | CONNACK with reason code `0x90` (TOPIC_NAME_INVALID); connection closed |
| Reconnect with persisted subscription no longer covered | Subscription pruned silently from in-memory tree and `pgmqtt_subscriptions` |
| `pgmqtt_reload_acls` narrows the allowlist | Now-unauthorized subscriptions pruned; a stored Will on a now-unauthorized topic is dropped |

### Wildcard Matching Examples

| Claim | Topic | Match? |
|-------|-------|--------|
| `sensors/+` | `sensors/temp` | Yes |
| `sensors/+` | `sensors/temp/deep` | No |
| `sensors/#` | `sensors/temp/deep` | Yes |
| `devices/42` | `devices/42` | Yes |
| `devices/42` | `devices/99` | No |


---

## TLS (MQTTS / WSS)

Enterprise builds include native TLS listeners — no reverse proxy required.

- **MQTTS** (default port 8883): standard MQTT over TLS. Compatible with any MQTT client that supports TLS.
- **WSS** (default port 9002): MQTT-over-WebSocket over TLS. Compatible with browser clients and MQTT.js.

Both listeners share a single certificate/key pair and the same TLS configuration.

### Setup

```sql
-- Provide paths to a PEM certificate and private key readable by the postgres process
ALTER SYSTEM SET pgmqtt.tls_cert_file = '/etc/pgmqtt/server.crt';
ALTER SYSTEM SET pgmqtt.tls_key_file  = '/etc/pgmqtt/server.key';

-- Enable whichever listeners you need
ALTER SYSTEM SET pgmqtt.mqtts_enabled = 'on';
ALTER SYSTEM SET pgmqtt.wss_enabled   = 'on';

-- A restart is required for listener binding changes to take effect
```

> **Note:** Listener and TLS GUCs are read once at BGW startup. `pg_reload_conf()` stores the new value but the broker does not rebind until the BGW is restarted. See [configuration.md](configuration.md) for details.

### Generating a self-signed certificate (for testing)

```bash
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout server.key -out server.crt -days 365 \
  -subj "/CN=localhost"
```

---

## Multi-Process CDC

Requires an enterprise license with the `multiprocess` feature, present **at PostgreSQL startup**.

### Overview

Without this feature, a single `pgmqtt_mqtt` background worker does everything: socket I/O, message delivery, and CDC replication-slot consumption, all interleaved in one tick loop. Under sustained heavy write load on CDC-mapped tables, draining the WAL backlog competes with servicing connected clients — a long drain delays accepting connections and delivering messages.

With `multiprocess`, the broker splits into a socket-side worker and a database-side worker, with the goal that **no WAL fsync ever runs on the socket loop**:

- **`pgmqtt_cdc`** owns the logical replication slot: it decodes WAL, renders topic mappings, and persists QoS ≥ 1 messages — exactly the same atomic batch pipeline as before, just in its own process. It also runs the QoS 1 inbound pump (`pgmqtt_inbound_pending` → target tables, up to 50 single-row synchronous commits per tick under load) and issues the WAL flush beacon behind `pgmqtt_mqtt`'s asynchronous commits (below). A slow WAL drain, a long inbound backlog, or the inbound pump's failure modes (e.g. target-table DDL races) can no longer stall or kill socket I/O — a crash here restarts this worker while clients stay connected.
- **`pgmqtt_mqtt`** keeps the sockets and delivers. It drains the CDC handoff on every tick, regardless of `pgmqtt.cdc_every_n_ticks` (that GUC paces the CDC worker's slot polling instead), and commits its own writes asynchronously.

Both workers appear in `pg_stat_activity` with `backend_type` values `pgmqtt_mqtt` and `pgmqtt_cdc`.

### The cross-process handoff

How a message crosses the process boundary depends on its durability class:

| Path | Carries | Mechanism | Loss behavior |
|------|---------|-----------|---------------|
| Outbox (`pgmqtt_cdc_outbox`) | QoS ≥ 1 messages, plus any QoS 0 message too large for the inline ring | Message ids are queued **in the same transaction that persists the rows and advances the replication slot**; `pgmqtt_mqtt` fetches them in id (= WAL) order and dequeues in the same transaction that records delivery state | **Lossless.** There is no fixed capacity to overflow and the queue survives crashes of either worker or the whole server; a crash mid-delivery re-delivers (at-least-once, per MQTT QoS 1 semantics) |
| Inline ring (shared memory) | QoS 0 messages with topic ≤ 256 bytes and payload ≤ 1,024 bytes | Fixed-capacity ring (8,192 messages); a shared-memory doorbell also wakes the delivery worker for outbox work without idle polling | Drops the oldest message on sustained overflow — QoS 0 is fire-and-forget, and this keeps never-persisted messages off the WAL entirely |

Inline-ring drops increment the `cdc_bridge_dropped` counter (see [Observability & Metrics](#observability--metrics)). A nonzero value means the QoS 0 write rate exceeded what the ring absorbs while `pgmqtt_mqtt` was busy or stuck; QoS ≥ 1 traffic is never affected.

### Asynchronous group commit

In the multiprocess topology, `pgmqtt_mqtt` commits its write transactions (client QoS 1 publish persistence, retained-message updates, session bookkeeping, QoS 0 inbound writes) with `synchronous_commit = off`, so the socket loop never waits on an fsync. **Durability guarantees are unchanged**: client-visible effects — the PUBACK to the publisher and QoS ≥ 1 delivery to subscribers — are deferred until `pg_current_wal_flush_lsn()` passes the transaction's commit record, i.e. until the write is physically on disk, exactly the same point at which the single-process broker sends them.

The WAL flush itself is driven from off the socket loop: under CDC or inbound load, the `pgmqtt_cdc` worker's own synchronous commits advance the flush pointer for free (group commit); when nothing else is flushing, `pgmqtt_mqtt` signals `pgmqtt_cdc` through shared memory and it issues one small synchronous commit that flushes everything at once. If a deferred batch outlives that round trip (~4 ticks — e.g. the CDC worker is restarting), `pgmqtt_mqtt` pays one synchronous flush itself, so the worst case is bounded at roughly the pre-split behavior. In practice a QoS 1 PUBACK arrives 2–5 ticks after the publish, and one fsync covers every write from every pipeline in that window rather than each transaction paying its own.

> **Tuning:** setting `wal_writer_delay = '10ms'` (PostgreSQL setting, default 200 ms) lets the WAL writer pick up the asynchronous commits almost immediately, which both lowers the QoS 1 PUBACK median to parity with single-process mode and makes the beacon/fallback paths nearly irrelevant. The WAL writer hibernates when idle, so the shorter delay costs nothing on a quiet server. Measured on a 4-core host: p50 ≈ 13 ms, p99 ≈ 26 ms with `10ms`, versus p50 ≈ 18 ms with the default.

This is only sound because of the process split: in the single-worker topology the inline CDC batch commits are synchronous and would force catch-up flushes on the same loop anyway, so community mode keeps plain synchronous commits.

### Scaling out: multiple socket workers

`pgmqtt.socket_workers` (1–8, default 1; requires restart) runs several `pgmqtt_mqtt` processes. Listeners share the same ports via `SO_REUSEPORT`, so the kernel load-balances incoming connections across the workers; each worker owns its clients' sockets, sessions, and subscription matching. The readiness-based polling means each worker's per-tick cost scales with its *active* clients, so N workers raise both the connection ceiling and the aggregate socket throughput.

How the workers stay coherent:

- **All publishes route through the shared outbox** — client publishes (QoS 0 included) and CDC messages alike — and each worker delivers rows past its own cursor (`pgmqtt_outbox_cursors`) to its own subscribers. Slot 0 garbage-collects rows once every cursor has passed them, reclaiming orphaned messages at the same time. This is the explicit trade: QoS 0 gives up its no-database fast path when `socket_workers > 1`.
- **Session takeover broadcasts a kick** through shared memory: a new CONNECT with an existing client_id disconnects the old connection whichever worker holds it (reason 0x8E), and the session resumes from its persisted state.
- **Admin commands fan out**: slot 0 drains `pgmqtt_admin_commands` and broadcasts each command to every worker, so disconnects and ACL reloads reach clients wherever they live.
- **Slot 0 owns the singleton duties**: metrics flush (counters are in shared memory, so the totals cover all workers), session-expiry sweeps, outbox GC. The connection cap from the license is enforced against the cluster-wide connection gauge. The HTTP healthcheck is answered by whichever worker the kernel picks — a 200 means "a worker's loop is ticking".

v1 caveats, deliberate and documented:

- **Shared subscriptions (`$share`)** deliver each message to exactly one member cluster-wide (workers claim each `(message, group)` pair through `pgmqtt_share_claims`), but the winning member is picked by each worker's local rotation — balancing is not globally round-robin.
- **A crashed socket worker's sessions** stay "connected" in `pgmqtt_sessions` until their clients reconnect (only a full PostgreSQL restart resets all sessions). On reconnect, a session that last lived on another worker is resumed from its persisted state, including queued and unacknowledged messages.
- **QoS 1 PUBACK and delivery latency** gain the outbox round trip (~1–2 ticks) relative to a single socket worker; QoS 0 end-to-end roughly doubles (measured ~12 ms vs ~5.5 ms at defaults).
- A **retained-message replacement** can reclaim the previous message row before a lagging worker delivered it (window of one cursor lag, typically milliseconds).

### Restart required

Process topology is decided **once, at PostgreSQL startup**: background workers can only be registered while the server is starting, so `_PG_init` reads `pgmqtt.license_key` at that moment to decide whether to register the second worker. `ALTER SYSTEM SET pgmqtt.license_key` + `pg_reload_conf()` updates the license for every runtime feature check, but adding or removing `multiprocess` only takes effect after a **full PostgreSQL restart**. Until then the broker keeps its current topology.

### Behavioral notes

- **Delivery guarantees are unchanged.** QoS ≥ 1 CDC messages are persisted *and queued for delivery* atomically with the slot advance (at-least-once, end to end); QoS 0 remains fire-and-forget.
- **No-subscriber cleanup moved.** The CDC worker cannot see subscriptions (that state lives in the `pgmqtt_mqtt` process), so it persists every rendered QoS ≥ 1 message; `pgmqtt_mqtt` reclaims any row that turns out to have zero subscribers at delivery time. No orphaned rows either way.
- **`pgmqtt.cdc_every_n_ticks` changes meaning.** It paces only the CDC worker's slot polling; bridge draining and delivery in `pgmqtt_mqtt` run every tick. Raising it still reduces WAL-decode overhead but no longer trades away socket responsiveness.

---

## Port Management

Each listener can be independently enabled or disabled.

```sql
-- Disable plain TCP (WebSocket-only mode)
ALTER SYSTEM SET pgmqtt.mqtt_enabled = 'off';
SELECT pg_reload_conf();
```

| Listener | GUC Enable | GUC Port | Default Port | Default Enabled |
|----------|-----------|----------|-------------|----------------|
| MQTT TCP | `pgmqtt.mqtt_enabled` | `pgmqtt.mqtt_port` | 1883 | **on** |
| MQTT WebSocket | `pgmqtt.ws_enabled` | `pgmqtt.ws_port` | 9001 | **on** |
| MQTTS (TCP + TLS) | `pgmqtt.mqtts_enabled` | `pgmqtt.mqtts_port` | 8883 | **off** |
| WSS (WebSocket + TLS) | `pgmqtt.wss_enabled` | `pgmqtt.wss_port` | 9002 | **off** |

Disabled listeners are not bound at all. TLS listeners require a valid `tls_cert_file` and `tls_key_file` to be set before enabling.

---

## Observability & Metrics

Requires an enterprise license with the `metrics` feature.

### Overview

The background worker maintains atomic counters that are periodically flushed to two PostgreSQL tables:

- **`pgmqtt_metrics_current`** — single-row table with the latest snapshot (upserted each flush).
- **`pgmqtt_metrics_snapshots`** — append-only time-series, one row per flush interval.

The flush interval, retention, and all other behavior are controlled via GUCs (see [configuration.md](configuration.md)).

### Available Counters

| Category | Counter | Type | Description |
|----------|---------|------|-------------|
| Connections | `connections_accepted` | counter | CONNACK success sent |
| | `connections_rejected` | counter | Auth failure or license limit |
| | `connections_current` | gauge | Currently connected clients |
| | `disconnections_clean` | counter | Normal DISCONNECT packets |
| | `disconnections_unclean` | counter | Keepalive timeout, error, or shutdown |
| | `wills_fired` | counter | Will messages published |
| Sessions | `sessions_created` | counter | Brand-new sessions |
| | `sessions_resumed` | counter | Resumed from persistent state |
| | `sessions_expired` | counter | Reaped by expiry sweeper |
| Messages in | `msgs_received` | counter | PUBLISH packets from clients |
| | `msgs_received_qos0` | counter | QoS 0 PUBLISHes received |
| | `msgs_received_qos1` | counter | QoS 1 PUBLISHes received |
| | `bytes_received` | counter | Payload bytes received |
| Messages out | `msgs_sent` | counter | PUBLISH packets to subscribers |
| | `bytes_sent` | counter | Payload bytes sent |
| | `msgs_dropped_queue_full` | counter | Dropped because client queue was full |
| QoS | `pubacks_sent` | counter | PUBACK packets sent |
| | `pubacks_received` | counter | PUBACK packets received |
| Subscriptions | `subscribe_ops` | counter | SUBSCRIBE operations |
| | `unsubscribe_ops` | counter | UNSUBSCRIBE operations |
| CDC | `cdc_events_processed` | counter | WAL events decoded from the CDC slot |
| | `cdc_msgs_published` | counter | Messages emitted from CDC pipeline |
| | `cdc_render_errors` | counter | Template rendering failures (bad mapping config) |
| | `cdc_slot_errors` | counter | Replication slot I/O errors |
| | `cdc_persist_errors` | counter | Message persist failures (DB write in CDC path) |
| | `cdc_ring_buffer_dropped` | counter | CDC events dropped by the decode ring buffer on overflow (data loss signal) |
| | `cdc_bridge_dropped` | counter | QoS 0 messages dropped by the `pgmqtt_cdc` → `pgmqtt_mqtt` shared-memory inline ring on overflow (`multiprocess` feature) |
| Inbound | `inbound_writes_ok` | counter | Successful MQTT-to-DB writes |
| | `inbound_writes_failed` | counter | Failed MQTT-to-DB writes |
| | `inbound_retries` | counter | Write retries |
| | `inbound_dead_letters` | counter | Messages moved to dead-letter table |
| DB batches | `db_batches_committed` | counter | Session action batches committed |
| | `db_session_errors` | counter | Errors in session upsert/disconnect/delete |
| | `db_message_errors` | counter | Errors in message insert/update/delete |
| | `db_subscription_errors` | counter | Errors in subscription insert/delete |
| Lifecycle | `started_at_unix` | gauge | Unix timestamp when the broker started |
| | `last_reset_at_unix` | gauge | Unix timestamp of last counter reset |

### Per-Connection Cache

`pgmqtt_connections_cache` is refreshed every `pgmqtt.metrics_connections_cache_interval` seconds and provides per-client detail:

```sql
SELECT * FROM pgmqtt_connections();
```

| Column | Type | Description |
|--------|------|-------------|
| `client_id` | text | MQTT client identifier |
| `transport` | text | `tcp`, `ws`, `tls`, or `wss` |
| `connected_at_unix` | bigint | Connection time (Unix seconds) |
| `last_activity_at_unix` | bigint | Last packet time (Unix seconds) |
| `keep_alive_secs` | int | Negotiated keep-alive |
| `msgs_received` | bigint | Messages received from this client |
| `msgs_sent` | bigint | Messages sent to this client |
| `bytes_received` | bigint | Payload bytes received |
| `bytes_sent` | bigint | Payload bytes sent |
| `subscriptions` | int | Active subscription count |
| `queue_depth` | int | Queued messages waiting for delivery |
| `inflight_count` | int | In-flight QoS 1 messages |
| `will_set` | bool | Whether a Will message is configured |

### Prometheus Integration

Two paths to Prometheus:

1. **`pgmqtt_prometheus_metrics()`** — returns all counters in Prometheus text exposition format. Use with `postgres_exporter` custom queries or any SQL-capable scraper.

2. **`pgmqtt_metrics()`** — returns rows of `(metric_name, value, unit, description)`, compatible with the bundled `docker/postgres_exporter/queries.yaml`.

### Hook Functions

A SQL function can be called after every metrics flush. The function receives the full snapshot as a JSONB argument.

```sql
-- Create a hook that logs alerts
CREATE FUNCTION my_alert_hook(snap JSONB) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO alert_log (is_alert, payload)
    VALUES (
        COALESCE((snap->>'connections_rejected')::bigint, 0) > 0 OR
        COALESCE((snap->>'cdc_slot_errors')::bigint, 0) > 0,
        snap
    );
END;
$$;

-- Wire it up
ALTER SYSTEM SET pgmqtt.metrics_hook_function = 'public.my_alert_hook';
SELECT pg_reload_conf();
```

The hook function name must be a valid SQL identifier (alphanumeric + underscores, optionally schema-qualified). Invalid names are silently skipped.

### NOTIFY Streaming

Configure a channel to receive JSON snapshots via PostgreSQL NOTIFY:

```sql
ALTER SYSTEM SET pgmqtt.metrics_notify_channel = 'pgmqtt_metrics';
SELECT pg_reload_conf();

-- Then in another session:
LISTEN pgmqtt_metrics;
```

Each notification payload is a JSON object with all counter values.

---

## GUC Reference

See **[configuration.md](configuration.md)** for the full GUC reference, including the performance-tuning settings (`tick_interval_ms`, `cdc_every_n_ticks`, `max_client_buffer_bytes`, `debug_log`) added in recent releases.

---

## SQL Functions

### `pgmqtt_license_status()`

Returns the current license state as a composite row.

```sql
SELECT * FROM pgmqtt_license_status();
```

| Column | Type | Example |
|--------|------|---------|
| `customer` | text | `"acme-corp"` |
| `status` | text | `"active"`, `"grace"`, `"community"`, `"expired"`, `"invalid: <reason>"` |
| `expires_at` | bigint | `1710354890` |
| `grace_expires_at` | bigint | `1711049890` |
| `features` | text[] | `{tls,jwt}` |
| `max_connections` | int | `100` |

For Community mode, `customer` is empty and `max_connections` is `1000`.
For Invalid status, `status` includes the reason (e.g., `"invalid: signature verification failed"`).

### `pgmqtt_metrics()`

Returns all broker metrics as rows. Requires `metrics` license feature.

```sql
SELECT * FROM pgmqtt_metrics();
```

| Column | Type | Description |
|--------|------|-------------|
| `metric_name` | text | Counter name (e.g., `connections_accepted`) |
| `value` | bigint | Current value |
| `unit` | text | `total`, `gauge`, `bytes`, `unix_seconds`, or `milliseconds` |
| `description` | text | Human-readable description |

### `pgmqtt_connections()`

Returns per-client connection detail from the connection cache. Requires `metrics` license feature.

```sql
SELECT * FROM pgmqtt_connections();
```

See [Per-Connection Cache](#per-connection-cache) for column details.

### `pgmqtt_prometheus_metrics()`

Returns all metrics in Prometheus text exposition format. Requires `metrics` license feature.

```sql
SELECT pgmqtt_prometheus_metrics();
```

### `pgmqtt_disconnect_client(text, int)` / `pgmqtt_disconnect_role(text, int)` / `pgmqtt_reload_acls(text)`

Admin commands — see [interfaces.md → Admin Commands](interfaces.md#admin-commands).

