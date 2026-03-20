# pgmqtt Enterprise Features

This document covers the enterprise-only features of pgmqtt: **license management**, **JWT authentication**, and **topic-level access control**.

---

## Table of Contents

- [Feature Matrix](#feature-matrix)
- [License Management](#license-management)
- [JWT Authentication](#jwt-authentication)
- [Topic-Level Access Control](#topic-level-access-control)
- [TLS (MQTTS / WSS)](#tls-mqtts--wss)
- [Port Management](#port-management)
- [GUC Reference](#guc-reference)
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
| JWT authentication | No | Yes (`jwt` feature) |
| Per-topic ACLs (via JWT claims) | No | Yes (`jwt` feature) |
| WebSocket-only JWT enforcement | No | Yes (`jwt` feature) |
| JWT `client_id` binding | No | Yes (`jwt` feature) |
| Multi-node replication | No | Yes (`multi_node` feature) |
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
  "features": ["tls", "jwt", "multi_node"],
  "max_connections": 100
}
```

| Field | Type | Description |
|-------|------|-------------|
| `customer` | string | Customer identifier |
| `expires_at` | i64 | Unix timestamp — license expiration |
| `grace_expires_at` | i64 | Unix timestamp — hard cutoff after grace period |
| `features` | string[] | Enabled features: `"tls"`, `"jwt"`, `"multi_node"` |
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

- **Claims are connection-scoped and immutable.** Once a client connects with a JWT, its permissions are fixed for the session lifetime. There is no token refresh mechanism — if a token expires mid-session, the existing connection continues operating. Disconnect and reconnect to pick up new claims.
- **No audience/issuer validation.** Any valid Ed25519-signed JWT with a non-expired `exp` is accepted. If you share signing keys across services, consider adding application-level claim validation.

---

## Topic-Level Access Control

### Overview

When a JWT contains `sub_claims` or `pub_claims`, the server enforces per-topic authorization on every SUBSCRIBE and PUBLISH packet for the lifetime of the connection.

### Rules

- **Empty claims array = unrestricted.** If `sub_claims` is `[]` or absent, the client can subscribe to any topic. Same for `pub_claims` and publishing.
- **Non-empty claims = allowlist.** The client can only operate on topics that match at least one entry in the claims array.
- **MQTT wildcards supported.** Claim entries can use `+` (single-level) and `#` (multi-level) wildcards with standard MQTT semantics.

### Enforcement

| Operation | Denied Response |
|-----------|----------------|
| SUBSCRIBE to unauthorized topic | SUBACK with reason code `0x87` (NOT_AUTHORIZED) |
| PUBLISH QoS 1 to unauthorized topic | PUBACK with reason code `0x87` (NOT_AUTHORIZED) |
| PUBLISH QoS 0 to unauthorized topic | Silently dropped (per MQTT spec, QoS 0 has no acknowledgment) |

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

> **Note:** Changes to `tls_cert_file`, `tls_key_file`, and the `*_enabled` / `*_port` GUCs require a PostgreSQL restart (or background worker restart). They are read once at startup and cannot be hot-reloaded with `pg_reload_conf()`.

### Generating a self-signed certificate (for testing)

```bash
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout server.key -out server.crt -days 365 \
  -subj "/CN=localhost"
```

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

## GUC Reference

All GUCs are superuser-only and reloadable via `SELECT pg_reload_conf()` (no restart required).

| GUC | Type | Default | Description |
|-----|------|---------|-------------|
| `pgmqtt.license_key` | string | `""` | Signed enterprise license token |
| `pgmqtt.jwt_public_key` | string | `""` | Ed25519 public key (base64url or PEM) |
| `pgmqtt.jwt_required` | bool | `false` | Require JWT for all MQTT connections |
| `pgmqtt.jwt_required_ws` | bool | `false` | Require JWT for WebSocket connections only |
| `pgmqtt.mqtt_port` | int | `1883` | MQTT TCP listener port |
| `pgmqtt.ws_port` | int | `9001` | MQTT-over-WebSocket listener port |
| `pgmqtt.mqtts_port` | int | `8883` | MQTTS (TCP + TLS) listener port |
| `pgmqtt.wss_port` | int | `9002` | WSS (WebSocket + TLS) listener port |
| `pgmqtt.mqtt_enabled` | bool | `true` | Enable plain MQTT TCP listener |
| `pgmqtt.ws_enabled` | bool | `true` | Enable plain MQTT WebSocket listener |
| `pgmqtt.mqtts_enabled` | bool | `false` | Enable MQTTS listener (requires `tls_cert_file` / `tls_key_file`) |
| `pgmqtt.wss_enabled` | bool | `false` | Enable WSS listener (requires `tls_cert_file` / `tls_key_file`) |
| `pgmqtt.tls_cert_file` | string | `""` | Path to PEM certificate file for TLS listeners |
| `pgmqtt.tls_key_file` | string | `""` | Path to PEM private key file for TLS listeners |

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
