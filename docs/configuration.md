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

> **Restart required.** Listener settings are read once at BGW startup. `pg_reload_conf()` stores the new value in PostgreSQL's GUC system but the broker does not rebind until the BGW is restarted.

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

## Performance Tuning

Controls the BGW event loop cadence and CDC read frequency. These are the primary levers for trading latency against throughput.

| GUC | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `pgmqtt.tick_interval_ms` | int | `5` | 1 – 1000 | BGW poll interval in milliseconds. Lower values reduce publish/subscribe latency at the cost of more frequent wakeups. The original default was 80 ms; 5 ms is recommended for most workloads. |
| `pgmqtt.max_client_buffer_bytes` | int | `262144` | 65536 – 16777216 | Hard cap on the inbound byte buffer accumulated per client between ticks (bytes). Publishers that exceed this limit in a single tick are disconnected. |
| `pgmqtt.cdc_every_n_ticks` | int | `1` | 1 – 1000 | Run the CDC slot read every N ticks. At the 5 ms default tick, `n=16` means CDC runs every ~80 ms. Raising this value reduces WAL decode CPU and `execute_session_db_actions` fsync overhead at the cost of increased CDC delivery latency (up to `n × tick_interval_ms`). Profiling shows `n=16` delivers **+67 % subscriber throughput** on write-heavy QoS 1 workloads. |
| `pgmqtt.debug_log` | bool | `off` | — | Emit verbose per-message and per-CDC-event log lines. Off by default; enabling at high message rates adds `elog(LOG)` overhead on every PUBLISH/PUBACK/CDC event. |
| `pgmqtt.async_session_writes` | bool | `off` | — | Set `synchronous_commit = off` for session and delivery-tracking writes (`pgmqtt_session_messages`). Eliminates the per-commit `fdatasync` on those writes. **Trade-off:** on a PostgreSQL crash, surviving subscribers may receive duplicate QoS 1 deliveries on reconnect (the PUBACK durability guarantee is unaffected — `pgmqtt_messages` is always written synchronously). Not recommended at `cdc_every_n_ticks = 1`; at n ≥ 16 the WAL writer naturally flushes before the next CDC cycle and the benefit is near zero. |

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
