# pgmqtt Configuration Reference

All settings are PostgreSQL GUCs in the `pgmqtt` namespace. Every GUC is superuser-only.

**Applying changes:**
```sql
ALTER SYSTEM SET pgmqtt.<setting> = <value>;
SELECT pg_reload_conf();
```

Most settings take effect immediately after `pg_reload_conf()`. Listener settings (ports, enabled flags, TLS paths) are read once when the background worker starts — changing them requires a BGW restart to take effect:

```sql
-- Restart the BGW (terminates and reconnects all MQTT clients)
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE application_name = 'pgmqtt_mqtt';
```

---

## Listeners

Controls which TCP/WebSocket ports the broker binds at startup.

| GUC | Type | Default | Description |
|-----|------|---------|-------------|
| `pgmqtt.mqtt_enabled` | bool | `on` | Bind the plain MQTT TCP listener |
| `pgmqtt.mqtt_port` | int | `1883` | MQTT TCP listener port |
| `pgmqtt.ws_enabled` | bool | `on` | Bind the plain MQTT-over-WebSocket listener |
| `pgmqtt.ws_port` | int | `9001` | WebSocket listener port |
| `pgmqtt.mqtts_enabled` | bool | `off` | Bind the MQTTS (TCP + TLS) listener — requires `tls_cert_file` and `tls_key_file` |
| `pgmqtt.mqtts_port` | int | `8883` | MQTTS listener port |
| `pgmqtt.wss_enabled` | bool | `off` | Bind the WSS (WebSocket + TLS) listener — requires `tls_cert_file` and `tls_key_file` |
| `pgmqtt.wss_port` | int | `9002` | WSS listener port |
| `pgmqtt.http_enabled` | bool | `on` | Bind the HTTP healthcheck listener |
| `pgmqtt.http_port` | int | `8080` | HTTP healthcheck listener port |

The healthcheck is served from the broker's own event loop: a `200` on `GET /health` means the broker is actually ticking, not merely that the process exists. The port stays closed while the node is a streaming replica and opens on promotion, so load balancers can use it to route MQTT traffic to the primary.

> **Restart required.** Listener settings are read once at BGW startup. `pg_reload_conf()` stores the new value in PostgreSQL's GUC system but the broker does not rebind until the BGW is restarted.

---

## Worker Database

| GUC | Type | Default | Description |
|-----|------|---------|-------------|
| `pgmqtt.database` | string | `postgres` | Database the MQTT+CDC background worker connects to. All pgmqtt tables, mappings, and the replication slot live in this database. |

> **Restart required.** Read once when the `pgmqtt_mqtt` worker starts.

---

## TLS

Shared certificate used by the MQTTS and WSS listeners.

| GUC | Type | Default | Description |
|-----|------|---------|-------------|
| `pgmqtt.tls_cert_file` | string | `""` | Filesystem path to a PEM certificate file, readable by the `postgres` process |
| `pgmqtt.tls_key_file` | string | `""` | Filesystem path to a PEM private key file, readable by the `postgres` process |

> **Restart required.** Certificate paths are loaded at BGW startup.

Generating a self-signed certificate for testing:
```bash
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout server.key -out server.crt -days 365 \
  -subj "/CN=localhost"
```

---

## License

| GUC | Type | Default | Description |
|-----|------|---------|-------------|
| `pgmqtt.license_key` | string | `""` | Signed enterprise license token. Empty = Community mode (1,000-connection cap, no enterprise features). |

Hot-reloadable — updated token takes effect after `pg_reload_conf()` without restarting.

> **Exception: the `multiprocess` feature.** Process topology (whether the dedicated `pgmqtt_cdc` worker exists) is decided once at PostgreSQL startup, because background workers can only be registered while the server is starting. Adding or removing `multiprocess` from the license therefore requires a **full PostgreSQL restart** — `pg_reload_conf()` updates the license for every other feature check but cannot start or stop the second worker. See [enterprise.md → Multi-Process CDC](enterprise.md#multi-process-cdc).

---

## JWT Authentication

Enterprise feature (`jwt`). Ignored when no license is active.

| GUC | Type | Default | Description |
|-----|------|---------|-------------|
| `pgmqtt.jwt_public_key` | string | `""` | Ed25519 public key for JWT verification. Accepts base64url (32 bytes) or PEM (`-----BEGIN PUBLIC KEY-----`) format. Empty = JWT validation disabled. |
| `pgmqtt.jwt_required` | bool | `off` | Reject any MQTT connection that does not present a valid JWT. Applies to all transports. |
| `pgmqtt.jwt_required_ws` | bool | `off` | Reject WebSocket connections without a valid JWT. TCP connections are unaffected. Overridden by `jwt_required`. |

Hot-reloadable — key rotation and policy changes take effect on the next connection after `pg_reload_conf()`. Existing connections retain their original claims for the session lifetime.

---

## Password Authentication

Validates the MQTT CONNECT `username` and `password` against the SCRAM-SHA-256 verifier stored in `pg_authid` for that Postgres role. Validation runs in-process during the CONNECT handshake — no extra database connection is opened.

- Only `SCRAM-SHA-256` verifiers are accepted. The deprecated `md5` scheme is explicitly rejected.
- `rolcanlogin` and `rolvaliduntil` are honored. Roles with `LOGIN` disabled or with an expired `VALID UNTIL` cannot authenticate via MQTT.
- `pg_hba.conf` is **not** consulted — it governs libpq connections only. Use `pgmqtt.password_auth_role_filter` (a SQL `LIKE` pattern) if you want to restrict MQTT auth to a specific subset of roles.
- The broker only handles ASCII passwords reliably. Postgres applies SASLprep (RFC 4013) when computing the verifier; for non-ASCII passwords the input you provide on CONNECT must already be SASLprep-normalized.

### GUCs

| GUC | Type | Default | Description |
|-----|------|---------|-------------|
| `pgmqtt.password_auth_enabled` | bool | `off` | Master switch. When off, username/password on CONNECT is ignored. |
| `pgmqtt.password_auth_required` | bool | `off` | Reject any CONNECT that does not present a valid username/password. Only effective when `password_auth_enabled = on`. |
| `pgmqtt.password_auth_role_filter` | string | `""` | Optional SQL `LIKE` pattern, e.g. `'mqtt\_%'`, restricting which roles may authenticate via MQTT. Empty = any login role. |

### Setup

```sql
-- Create a dedicated login role for MQTT clients.
CREATE ROLE mqtt_devices LOGIN PASSWORD 'hunter2';

-- Enable password auth on the broker.
ALTER SYSTEM SET pgmqtt.password_auth_enabled = 'on';

-- Optional: restrict MQTT auth to roles named like 'mqtt_*'.
ALTER SYSTEM SET pgmqtt.password_auth_role_filter = 'mqtt\_%';

-- Optional: require password auth on every CONNECT.
ALTER SYSTEM SET pgmqtt.password_auth_required = 'on';

SELECT pg_reload_conf();
```

> **Confirm the verifier scheme.** Run `SHOW password_encryption;` — it must be `scram-sha-256` (PostgreSQL 14+ default). If your cluster still uses `md5`, MQTT password auth will fail with `BadVerifier` for every role until you set `password_encryption = scram-sha-256` and have users reset their passwords.

### Behavior Matrix

| `password_auth_enabled` | `password_auth_required` | Username | Password | Result |
|---|---|---|---|---|
| `off` | any | any | any | Allowed (anonymous) — username/password ignored |
| `on` | `off` | absent | absent | Allowed (anonymous) |
| `on` | `off` | present | valid | Allowed, authenticated as the role |
| `on` | `off` | present | invalid | **Rejected** (0x86 BAD_USERNAME_PASSWORD) |
| `on` | `on` | absent | any | **Rejected** (0x86) |
| `on` | `on` | present | valid | Allowed |
| `on` | `on` | present | invalid | **Rejected** (0x86) |

When `password_auth_required = on` but `password_auth_enabled = off`, the broker rejects with `NOT_AUTHORIZED` (0x87) so the misconfiguration is loud.

### Coexistence with JWT

Both auth modes can be enabled simultaneously. On every CONNECT the broker sniffs the password field:

- If it parses as `header.payload.signature` (three non-empty base64url segments separated by `.`) → routed to JWT.
- Otherwise → routed to the password path (if `password_auth_enabled = on`).

A plaintext password that happens to contain two dots and be base64url-decodable in all three segments will be misrouted to JWT. This is rare enough in practice that the simple sniff is preferred over a configuration knob. If you hit it, change the password.

### Per-Topic ACLs

When a client authenticates via password auth, per-topic access control can be enforced through the `pgmqtt_acls` table. This requires the enterprise `acl` license feature — see [enterprise.md → Topic-Level Access Control](enterprise.md#topic-level-access-control). Without the `acl` feature, password-authenticated clients get unrestricted topic access.

With the `acl` feature, a role with **no covering `pgmqtt_acls` row is denied** by default. Set `pgmqtt.acl_default_deny = off` to restore the legacy fail-open behavior where such a role gets unrestricted access — see [enterprise.md → `acl_default_deny`](enterprise.md#opting-back-into-fail-open-pgmqttacl_default_deny).

### Admin Commands

Three SQL functions let operators manage live connections. See [interfaces.md → Admin Commands](interfaces.md#admin-commands) for full details:

- `pgmqtt_disconnect_client(client_id, reason_code)` — kick a single client.
- `pgmqtt_disconnect_role(role_name, reason_code)` — kick all clients authenticated as a role.
- `pgmqtt_reload_acls(target)` — refresh a live connection's ACL allowlist without disconnecting.

---

## Performance Tuning

Controls the BGW event loop cadence and CDC read frequency. These are the primary levers for trading latency against throughput.

| GUC | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `pgmqtt.tick_interval_ms` | int | `5` | 1 – 1000 | BGW poll interval in milliseconds. Lower values reduce publish/subscribe latency at the cost of more frequent wakeups. The original default was 80 ms; 5 ms is recommended for most workloads. |
| `pgmqtt.max_client_buffer_bytes` | int | `1048576` | 65536 – 16777216 | Per-client socket buffer cap applied in both directions. **Inbound:** the broker stops reading from a publisher once this many bytes are buffered between ticks — excess stays in the kernel TCP buffer and is consumed on the next tick; a publisher that sustains this rate across ticks is disconnected. **Outbound:** when a slow subscriber's unsent write buffer reaches this size, QoS 1 delivery disconnects the subscriber and QoS 0 delivery drops the message. |
| `pgmqtt.socket_workers` | int | `1` | 1 – 8 | Number of `pgmqtt_mqtt` socket worker processes (enterprise `multiprocess` feature only; ignored otherwise). Read once at PostgreSQL startup — **requires a full restart**. With more than one worker, listeners share ports via `SO_REUSEPORT` and the kernel load-balances incoming connections; all publishes (including QoS 0) route through the shared outbox so every worker's subscribers see them. See [enterprise.md → Scaling out](enterprise.md#scaling-out-multiple-socket-workers). |
| `pgmqtt.cdc_every_n_ticks` | int | `1` | 1 – 1000 | Run the CDC slot read every N ticks. At the 5 ms default tick, `n=16` means CDC runs every ~80 ms. Raising this value reduces WAL decode CPU and `execute_session_db_actions` fsync overhead at the cost of increased CDC delivery latency (up to `n × tick_interval_ms`). Profiling shows `n=16` delivers **+67 % subscriber throughput** on write-heavy QoS 1 workloads. With the enterprise `multiprocess` feature this paces the `pgmqtt_cdc` worker's slot polling only; the `pgmqtt_mqtt` worker drains the shared-memory bridge on every tick regardless, so raising it no longer delays socket work — see [enterprise.md → Multi-Process CDC](enterprise.md#multi-process-cdc). |
| `pgmqtt.debug_log` | bool | `off` | — | Emit verbose per-message and per-CDC-event log lines. Off by default; enabling at high message rates adds `elog(LOG)` overhead on every PUBLISH/PUBACK/CDC event. |

### Recommended settings for throughput-sensitive workloads

```sql
ALTER SYSTEM SET pgmqtt.cdc_every_n_ticks = 16;   -- +67% QoS 1 subscriber throughput
SELECT pg_reload_conf();
```

### Recommended settings for latency-sensitive workloads

```sql
ALTER SYSTEM SET pgmqtt.tick_interval_ms   = 5;    -- default
ALTER SYSTEM SET pgmqtt.cdc_every_n_ticks  = 1;    -- default; CDC on every tick
SELECT pg_reload_conf();
```

---

## Observability

Enterprise feature (`metrics`). Counters are maintained regardless of license; the flush functions require the feature.

| GUC | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `pgmqtt.metrics_snapshot_interval` | int | `60` | 0 – 86400 | Seconds between metric flushes to `pgmqtt_metrics_snapshots`. `0` disables periodic flushing. |
| `pgmqtt.metrics_retention_days` | int | `3` | 0 – 3650 | Days to retain rows in `pgmqtt_metrics_snapshots`. `0` keeps rows forever. |
| `pgmqtt.metrics_connections_cache_interval` | int | `10` | 0 – 3600 | Seconds between refreshes of `pgmqtt_connections_cache`. `0` disables the cache. |
| `pgmqtt.metrics_hook_function` | string | `""` | — | SQL function called after each metric flush: `schema.func(jsonb) -> void`. Empty = no hook. |
| `pgmqtt.metrics_notify_channel` | string | `""` | — | PostgreSQL NOTIFY channel for streaming metric snapshots as JSON. Empty = disabled. |

Example hook:
```sql
CREATE FUNCTION my_alert_hook(snap JSONB) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO alert_log (is_alert, payload)
    VALUES (COALESCE((snap->>'cdc_slot_errors')::bigint, 0) > 0, snap);
END;
$$;

ALTER SYSTEM SET pgmqtt.metrics_hook_function = 'public.my_alert_hook';
SELECT pg_reload_conf();
```

Example NOTIFY stream:
```sql
ALTER SYSTEM SET pgmqtt.metrics_notify_channel = 'pgmqtt_metrics';
SELECT pg_reload_conf();

-- In another session:
LISTEN pgmqtt_metrics;
```
