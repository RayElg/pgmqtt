# pgmqtt Configuration Reference

All pgmqtt behavior is governed by PostgreSQL GUCs in the `pgmqtt.*` namespace.
Every GUC listed here is **superuser-only**. Most are reloadable via
`SELECT pg_reload_conf()` — a handful require a worker restart and are called
out explicitly.

## Setting GUCs

```sql
ALTER SYSTEM SET pgmqtt.<name> = <value>;
SELECT pg_reload_conf();
```

For GUCs that require a restart, issue `pg_ctl restart` or restart the
Postgres container after `ALTER SYSTEM SET`.

---

## Table of Contents

- [Listeners and Ports](#listeners-and-ports)
- [Performance Tuning](#performance-tuning)
- [TLS (enterprise)](#tls-enterprise)
- [Authentication (enterprise)](#authentication-enterprise)
- [Observability / Metrics (enterprise)](#observability--metrics-enterprise)
- [Licensing (enterprise)](#licensing-enterprise)

---

## Listeners and Ports

Requires worker restart (read once at startup).

| GUC | Type | Default | Description |
|-----|------|---------|-------------|
| `pgmqtt.mqtt_enabled` | bool | `true` | Enable plain MQTT TCP listener |
| `pgmqtt.mqtt_port` | int | `1883` | MQTT TCP listener port |
| `pgmqtt.ws_enabled` | bool | `true` | Enable plain MQTT WebSocket listener |
| `pgmqtt.ws_port` | int | `9001` | MQTT-over-WebSocket listener port |
| `pgmqtt.mqtts_enabled` | bool | `false` | Enable MQTTS listener (requires TLS files) |
| `pgmqtt.mqtts_port` | int | `8883` | MQTTS (TCP + TLS) listener port |
| `pgmqtt.wss_enabled` | bool | `false` | Enable WSS listener (requires TLS files) |
| `pgmqtt.wss_port` | int | `9002` | WSS (WebSocket + TLS) listener port |

---

## Performance Tuning

Reloadable. All affect broker throughput and memory use per tick. Defaults are
sized for workloads up to tens of thousands of messages/second; raise for
sustained high-rate deployments, lower for memory-constrained environments.

| GUC | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `pgmqtt.max_client_buffer_kb` | int | `1024` | 16 – 16384 | Per-client inbound accumulation limit in KiB. Connections whose parsed-but-not-yet-consumed buffer exceeds this are disconnected. |
| `pgmqtt.client_read_buf_kb` | int | `256` | 4 – 16384 | Bytes drained from each client socket per broker tick, in KiB. Clamped at runtime to `max_client_buffer_kb`. Larger values increase per-tick batch size for pipelined publishers and reduce syscall overhead. |
| `pgmqtt.cdc_batch_size` | int | `2048` | 32 – 65536 | Max WAL events consumed per CDC batch transaction. Larger values amortize the per-batch `fdatasync` cost; smaller values reduce retry scope if a batch fails. |
| `pgmqtt.inbound_batch_size` | int | `200` | 1 – 10000 | Max `pgmqtt_inbound_pending` rows processed per tick. Each row commits in its own subtransaction — do not raise this above what a single tick can reasonably sync. |

### Sizing guidance

- **Pipelined QoS 1 publishers**: raise `client_read_buf_kb` before anything
  else. The broker drains one chunk per client per tick, so the chunk size
  caps per-tick batch size (and therefore msg/fdatasync).
- **CDC-heavy workloads**: raise `cdc_batch_size`. The slot LSN advances once
  per batch, so bigger batches mean proportionally fewer commits.
- **Inbound-heavy workloads**: raising `inbound_batch_size` alone has
  diminishing returns because each pending row commits independently. If
  inbound throughput is the bottleneck, consider batching writes at the
  application level or opening an issue.

---

## TLS (enterprise)

Requires enterprise license with the `tls` feature. Reloadable for
`tls_cert_file` / `tls_key_file`, but listener enable/port changes require a
worker restart.

| GUC | Type | Default | Description |
|-----|------|---------|-------------|
| `pgmqtt.tls_cert_file` | string | `""` | Path to PEM certificate file for TLS listeners |
| `pgmqtt.tls_key_file` | string | `""` | Path to PEM private key file for TLS listeners |

See [enterprise.md — TLS](enterprise.md#tls-mqtts--wss) for full setup.

---

## Authentication (enterprise)

Requires enterprise license with the `jwt` feature.

| GUC | Type | Default | Description |
|-----|------|---------|-------------|
| `pgmqtt.jwt_public_key` | string | `""` | Ed25519 public key (base64url or PEM) |
| `pgmqtt.jwt_required` | bool | `false` | Require JWT for all MQTT connections |
| `pgmqtt.jwt_required_ws` | bool | `false` | Require JWT for WebSocket connections only |

See [enterprise.md — JWT Authentication](enterprise.md#jwt-authentication) for
claim structure and ACL semantics.

---

## Observability / Metrics (enterprise)

Requires enterprise license with the `metrics` feature.

| GUC | Type | Default | Description |
|-----|------|---------|-------------|
| `pgmqtt.metrics_snapshot_interval` | int | `60` | Seconds between metric flushes (0 = disabled) |
| `pgmqtt.metrics_retention_days` | int | `3` | Days to retain snapshot rows (0 = keep forever) |
| `pgmqtt.metrics_connections_cache_interval` | int | `10` | Seconds between connection cache refreshes (0 = disabled) |
| `pgmqtt.metrics_hook_function` | string | `""` | SQL function called after each flush: `schema.func(jsonb)` |
| `pgmqtt.metrics_notify_channel` | string | `""` | NOTIFY channel for JSON snapshots (empty = disabled) |

See [enterprise.md — Observability & Metrics](enterprise.md#observability--metrics)
for snapshot schema and hook contract.

---

## Licensing (enterprise)

| GUC | Type | Default | Description |
|-----|------|---------|-------------|
| `pgmqtt.license_key` | string | `""` | Signed enterprise license token |

See [enterprise.md — License Management](enterprise.md#license-management) for
format and grace-period behavior.
