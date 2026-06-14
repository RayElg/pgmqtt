# pgmqtt Interfaces

This document describes the SQL-callable interfaces and configuration tables used to govern the `pgmqtt` extension.

## SQL Functions

`pgmqtt` introduces several functions to manage how changes in PostgreSQL tables map to MQTT topics.

### 1. `pgmqtt_add_outbound_mapping`

Registers a new logical decoding mapping for a given table. Multiple mappings per (schema, table) are supported via distinct `mapping_name` values, enabling parallel publish to multiple topics — see [topic-mapping-migration.md](topic-mapping-migration.md).

**Signature:**
```sql
pgmqtt_add_outbound_mapping(
    schema_name text,
    table_name text,
    topic_template text,
    payload_template text,
    qos integer DEFAULT 0,
    mapping_name text DEFAULT NULL,
    template_type text DEFAULT 'jinja2'
) RETURNS text
```

**Parameters:**
- `schema_name`: The database schema of the table (e.g., `'public'`).
- `table_name`: The table name you want to capture changes from.
- `topic_template`: A Jinja2-compatible template string used to determine the MQTT topic for each change.
- `payload_template`: A Jinja2-compatible template string defining what the MQTT message body will contain.
- `qos`: Optional. The Quality of Service level for messages generated from this mapping (0 or 1). Defaults to 0.
- `mapping_name`: Optional. Identifier distinguishing multiple mappings for the same table. `NULL` is treated as `'default'`. Re-adding with the same name updates the mapping in place.
- `template_type`: Optional. The template engine to use for rendering topic and payload templates. Defaults to `'jinja2'`.

**Example:**
```sql
SELECT pgmqtt_add_outbound_mapping(
    'public',
    'events',
    'events/{{ op | lower }}',
    '{{ columns | tojson }}',
    1
);

-- Add a second mapping for the same table (fires in parallel):
SELECT pgmqtt_add_outbound_mapping(
    'public',
    'events',
    'events/v2/{{ op | lower }}',
    '{"id": "{{ columns.id }}"}',
    0,
    'v2'
);
```

---

### 2. `pgmqtt_remove_outbound_mapping`

Removes an existing topic mapping by name. Note that any changes already in the internal ring buffer for this mapping may still be dispatched.

**Signature:**
```sql
pgmqtt_remove_outbound_mapping(
    schema_name text,
    table_name text,
    mapping_name text DEFAULT NULL
) RETURNS boolean
```
Returns `true` if the mapping was found and successfully deleted, or `false` otherwise. `mapping_name` defaults to `'default'` when `NULL`; to remove all mappings for a table, call once per mapping name.

**Example:**
```sql
SELECT pgmqtt_remove_outbound_mapping('public', 'events');          -- removes 'default'
SELECT pgmqtt_remove_outbound_mapping('public', 'events', 'v2');    -- removes 'v2'
```

---

### 3. `pgmqtt_list_outbound_mappings`

Lists all currently active topic mappings.

**Signature:**
```sql
pgmqtt_list_outbound_mappings()
```
Returns a set of rows detailing the mappings.

**Example:**
```sql
SELECT * FROM pgmqtt_list_outbound_mappings();
```
Output:
```text
  schema_name | table_name | mapping_name |       topic_template      |    payload_template    | qos | template_type
 -------------+------------+--------------+---------------------------+------------------------+-----+--------------
  public      | events     | default      | events/{{ op | lower }}   | {{ columns | tojson }} |   1 | jinja2
```

---

### 4. `pgmqtt_status`

Returns a single-row operational snapshot of the broker.

**Signature:**
```sql
pgmqtt_status() RETURNS TABLE (...)
```

**Example:**
```sql
SELECT * FROM pgmqtt_status();
```

Output columns:

| Column | Type | Description |
|--------|------|-------------|
| `active_connections` | int | Clients currently connected (session has no `disconnected_at`) |
| `total_subscriptions` | int | Total active topic filter subscriptions across all clients |
| `total_retained_messages` | int | Number of topics with a retained message stored |
| `pending_session_messages` | int | Messages queued for offline or slow clients |
| `cdc_mappings` | int | Number of registered CDC topic mappings |
| `cdc_slot_active` | bigint | `1` if the `pgmqtt_slot` logical replication slot is active, `0` otherwise |
| `inbound_mappings` | int | Number of registered inbound MQTT → table mappings |
| `inbound_pending` | int | QoS 1 inbound messages awaiting table writes (virtual subscriber queue) |
| `dead_letters` | int | Inbound messages that failed permanently and were dead-lettered |

All counts are taken from a single SQL statement and reflect a consistent snapshot.

---

### 5. `pgmqtt_add_inbound_mapping`

Registers a mapping that automatically writes incoming MQTT messages to a PostgreSQL table. Topic patterns with `{variable}` segments capture values from the topic path; column maps reference those variables and JSON payload fields.

**Signature:**
```sql
pgmqtt_add_inbound_mapping(
    topic_pattern text,
    target_table text,
    column_map jsonb,
    op text DEFAULT 'insert',
    conflict_columns text[] DEFAULT NULL,
    target_schema text DEFAULT 'public',
    mapping_name text DEFAULT 'default',
    template_type text DEFAULT 'jsonpath'
) RETURNS text
```

**Parameters:**
- `topic_pattern`: Pattern with `{variable}` segments to match incoming topics (e.g., `'sensor/{site_id}/temperature/{sensor_id}'`). MQTT wildcards (`+`, `#`) are not allowed.
- `target_table`: Name of the PostgreSQL table to write to.
- `column_map`: JSON object mapping column names to source expressions (see below).
- `op`: Operation type — `'insert'`, `'upsert'`, or `'delete'`. Defaults to `'insert'`.
- `conflict_columns`: Required for `upsert` (ON CONFLICT columns) and `delete` (WHERE columns). Must reference columns in the column map.
- `target_schema`: Schema of the target table. Defaults to `'public'`.
- `mapping_name`: Unique identifier for this mapping. Defaults to `'default'`.
- `template_type`: Optional. The template engine to use for column map expressions. Defaults to `'jsonpath'`.

**Column Map Expressions:**

| Expression | Meaning | Example |
|---|---|---|
| `{var_name}` | Value captured from a topic pattern variable | `{site_id}` |
| `$.path.to.field` | Value extracted from the JSON payload (dot-path) | `$.temperature` |
| `$payload` | Entire raw payload as text | |
| `$topic` | Raw topic string | |

**Validation** (performed at creation time):
- Target table and all column map keys must exist
- All `{var}` references in column map must appear in the topic pattern
- For `upsert`: conflict columns must be provided
- For `delete`: conflict columns define the WHERE clause

**Examples:**

Insert sensor readings:
```sql
SELECT pgmqtt_add_inbound_mapping(
    'sensor/{site_id}/temperature/{sensor_id}',
    'sensor_readings',
    '{"site_id": "{site_id}", "sensor_id": "{sensor_id}", "value": "$.temperature"}'::jsonb,
    'insert',
    NULL,
    'public',
    'temp_readings'
);
```

Upsert device status:
```sql
SELECT pgmqtt_add_inbound_mapping(
    'device/{device_id}/status',
    'device_status',
    '{"device_id": "{device_id}", "online": "$.online", "last_seen": "$.ts"}'::jsonb,
    'upsert',
    ARRAY['device_id'],
    'public',
    'device_status'
);
```

Delete by key:
```sql
SELECT pgmqtt_add_inbound_mapping(
    'device/{device_id}/deregister',
    'devices',
    '{"device_id": "{device_id}"}'::jsonb,
    'delete',
    ARRAY['device_id'],
    'public',
    'device_deregister'
);
```

---

### 6. `pgmqtt_remove_inbound_mapping`

Removes an inbound mapping by name.

**Signature:**
```sql
pgmqtt_remove_inbound_mapping(
    mapping_name text DEFAULT 'default'
) RETURNS boolean
```

Returns `true` if the mapping was found and removed, `false` otherwise.

**Example:**
```sql
SELECT pgmqtt_remove_inbound_mapping('temp_readings');
```

---

### 7. `pgmqtt_list_inbound_mappings`

Lists all registered inbound mappings.

**Signature:**
```sql
pgmqtt_list_inbound_mappings() RETURNS TABLE (
    mapping_name text,
    topic_pattern text,
    target_schema text,
    target_table text,
    column_map jsonb,
    op text,
    conflict_columns text[],
    template_type text
)
```

**Example:**
```sql
SELECT * FROM pgmqtt_list_inbound_mappings();
```

---

### 8. `pgmqtt_license_status` *(enterprise)*

Returns the current license state. See [enterprise.md](enterprise.md#license-management) for full details.

**Example:**
```sql
SELECT * FROM pgmqtt_license_status();
```

| Column | Type | Example |
|--------|------|---------|
| `customer` | text | `"acme-corp"` |
| `status` | text | `"active"`, `"grace"`, `"expired"`, `"community"`, `"invalid: <reason>"` |
| `expires_at` | bigint | Unix timestamp |
| `grace_expires_at` | bigint | Unix timestamp |
| `features` | text[] | `{tls,jwt}` |
| `max_connections` | int | `100` |

---

## Internal Tables

The extension maintains several internal tables to track state and persist messages.

### `pgmqtt_topic_mappings`

Holds the persisted configuration for CDC mappings. Managed via the SQL functions above.

Columns:
- `schema_name`: Source schema.
- `table_name`: Source table.
- `topic_template`: Jinja2 template for topic.
- `payload_template`: Jinja2 template for payload.
- `qos`: Target QoS level for messages.
- `template_type`: Template engine type (e.g., `'jinja2'`).

### `pgmqtt_messages`

Stores persisted MQTT messages for QoS 1 delivery and session support.

Columns:
- `id`: Incrementing message ID (bigint).
- `topic`: MQTT topic.
- `payload`: Message payload (bytea).
- `qos`: Message QoS.
- `retain`: Retain flag.
- `created_at`: Timestamp.

### `pgmqtt_retained`

Stores the latest retained message for each topic.

Columns:
- `topic`: MQTT topic (primary key).
- `message_id`: Reference to the message in `pgmqtt_messages`.

### `pgmqtt_sessions`

Tracks persistent session state for each client.

Columns:
- `client_id`: MQTT client identifier (primary key).
- `next_packet_id`: Next packet ID to assign for QoS 1 delivery.
- `expiry_interval`: Session Expiry Interval in seconds (0 = expires on disconnect).
- `disconnected_at`: Timestamp of last disconnect; `NULL` means currently connected.

### `pgmqtt_session_messages`

Per-session queue of pending messages for offline or slow clients.

Columns:
- `client_id`: References `pgmqtt_sessions`.
- `message_id`: References `pgmqtt_messages`.
- `packet_id`: Assigned packet ID when inflight (QoS 1); `NULL` when queued.
- `sent_at`: Timestamp of last send attempt (used for redelivery timing).
- `created_at`: When the message was enqueued.

### `pgmqtt_subscriptions`

Stores all active topic filter subscriptions.

Columns:
- `client_id`: References `pgmqtt_sessions`.
- `topic_filter`: MQTT topic filter (may include `+` and `#` wildcards).
- `qos`: Subscribed QoS level.

### `pgmqtt_inbound_mappings`

Stores inbound MQTT → PostgreSQL table mapping configurations. Managed via `pgmqtt_add_inbound_mapping` / `pgmqtt_remove_inbound_mapping`.

Columns:
- `mapping_name`: Unique mapping identifier (primary key).
- `topic_pattern`: Topic pattern with `{variable}` capture segments.
- `target_schema`: Schema of the target table.
- `target_table`: Target table name.
- `column_map`: JSONB object mapping column names to source expressions.
- `op`: Operation type (`'insert'`, `'upsert'`, or `'delete'`).
- `conflict_columns`: Array of column names for upsert (ON CONFLICT) or delete (WHERE clause).
- `template_type`: Template engine type (e.g., `'jsonpath'`).

The background worker loads mappings from this table at startup and reloads periodically (~80ms). Changes made via the SQL functions take effect on the next reload cycle.

### `pgmqtt_inbound_pending`

Tracks QoS 1 messages awaiting inbound table writes. Acts as a durable queue for the "virtual subscriber" — a background process that reads pending rows, executes the table write, and deletes the row on success.

Columns:
- `message_id`: Reference to the message in `pgmqtt_messages` (part of composite PK).
- `mapping_name`: The inbound mapping that matched (part of composite PK).
- `retry_count`: Number of retry attempts so far.
- `last_error`: Error message from the most recent failed attempt.
- `next_retry_at`: When the next retry should be attempted (exponential backoff: 1s, 2s, 4s… capped at 256s).
- `created_at`: When this pending entry was created.

Rows are inserted atomically with the message in `pgmqtt_messages` during QoS 1 publish handling, ensuring the PUBACK is only sent after both are durably committed. After 10 failed retries (or a non-retryable error), the row is moved to `pgmqtt_dead_letters`.

### `pgmqtt_dead_letters`

Stores inbound messages that failed permanently — either after exceeding the maximum retry count (10) or due to a non-retryable error.

Columns:
- `id`: Auto-incrementing primary key.
- `original_message_id`: The message ID from `pgmqtt_messages` (may no longer exist).
- `topic`: MQTT topic of the original message.
- `payload`: Original message payload (bytea).
- `mapping_name`: The inbound mapping that was being applied.
- `error_message`: The error that caused the dead-letter.
- `retry_count`: How many retries were attempted before dead-lettering.
- `dead_lettered_at`: When this entry was created.

---

## Admin Commands

The broker drains the `pgmqtt_admin_commands` table every tick and dispatches kick/reload actions against live connections. Three SQL functions enqueue commands:

### 9. `pgmqtt_disconnect_client`

Disconnect a single MQTT client by `client_id`.

**Signature:**
```sql
pgmqtt_disconnect_client(
    client_id text,
    reason_code int DEFAULT 135
) RETURNS bigint
```

Returns the inserted command row's `id`. Execution is asynchronous: the BGW drains the queue on a fixed ~100 ms cadence. The Will message, if any, fires — except when the client's current `pub_claims` / `pgmqtt_acls` no longer cover the Will topic (e.g. after a `pgmqtt_reload_acls`), in which case it is silently dropped.

The default `reason_code` is `0x87` (NOT_AUTHORIZED). Pass a different MQTT 5 reason code per [§3.14.2.1](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901208) if needed (e.g. `0x8E` SESSION_TAKEN_OVER). MQTT 3.1.1 has no server→client DISCONNECT packet, so 3.1.1 clients see the socket close without a reason code.

**Example:**
```sql
-- Compromised device — kick now, rotate password elsewhere.
SELECT pgmqtt_disconnect_client('device-42');
```

---

### 10. `pgmqtt_disconnect_role`

Disconnect every MQTT client that authenticated as the given Postgres role via password auth.

**Signature:**
```sql
pgmqtt_disconnect_role(
    role_name text,
    reason_code int DEFAULT 135
) RETURNS bigint
```

Returns the inserted command row's `id`. Anonymous and JWT-authenticated clients are unaffected.

Kicked clients with a persistent session (`session_expiry_interval > 0`) keep their session and subscriptions. On reconnect, persisted subscriptions are re-validated against the role's *current* `pgmqtt_acls` and any rows no longer covered are pruned. If you want to fully expire the session as well, also delete the row from `pgmqtt_sessions`.

**Example:**
```sql
-- Force-reconnect everyone using a role after rotating its password.
ALTER ROLE mqtt_devices PASSWORD 'new-secret';
SELECT pgmqtt_disconnect_role('mqtt_devices');
```

---

### 11. `pgmqtt_reload_acls`

Re-read `pgmqtt_acls` for the named client and overwrite its in-memory allowlist without disconnecting. Use `'*'` to refresh every password-authenticated client.

**Signature:**
```sql
pgmqtt_reload_acls(
    target text
) RETURNS bigint
```

Returns the inserted command row's `id`. No-op for clients that did not authenticate via password auth (JWT claims are immutable for the session lifetime).

Side effects of a refresh:

- **Subscriptions are pruned.** Any active subscription whose topic filter is no longer covered by the refreshed `sub` rules is removed from the in-memory tree and `pgmqtt_subscriptions`. The client is not notified — MQTT has no server-initiated UNSUBSCRIBE — so they will simply stop receiving traffic on that filter.
- **Stored Wills are re-checked.** If the client connected with a Will whose topic is no longer covered by the refreshed `pub` rules, the Will is dropped from the connection's state and will not fire on subsequent disconnect.

**Example:**
```sql
-- Revoked a role's topic permission and want it to take effect immediately.
DELETE FROM pgmqtt_acls WHERE role_name = 'mqtt_devices' AND topic_filter = 'private/#';
SELECT pgmqtt_reload_acls('*');
```

---

### Permissions

All three admin functions are revoked from `PUBLIC` at install time. Grant explicitly to operators:

```sql
GRANT EXECUTE ON FUNCTION pgmqtt_disconnect_client(text, int) TO oncall_role;
GRANT EXECUTE ON FUNCTION pgmqtt_disconnect_role(text, int)   TO oncall_role;
GRANT EXECUTE ON FUNCTION pgmqtt_reload_acls(text)            TO oncall_role;
```

---

### `pgmqtt_acls`

Per-role per-topic ACL allowlist. Consulted on CONNECT when the client authenticates via password auth. Enforced only when the active enterprise license includes the `acl` feature — see [enterprise.md → Topic-Level Access Control](enterprise.md#topic-level-access-control).

Columns:
- `role_name`: Postgres role name (part of composite PK).
- `topic_filter`: MQTT topic filter — supports `+` and `#` wildcards (part of composite PK).
- `can_publish`: Whether the role may PUBLISH to matching topics.
- `can_subscribe`: Whether the role may SUBSCRIBE to matching topics.

ACLs are loaded once on CONNECT and cached on the connection. Use `pgmqtt_reload_acls` to refresh a live connection without disconnecting it.

### `pgmqtt_admin_commands`

Unlogged command queue drained by the background worker every tick. Populated by the `pgmqtt_disconnect_client`, `pgmqtt_disconnect_role`, and `pgmqtt_reload_acls` SQL functions.

Columns:
- `id`: Auto-incrementing primary key (bigserial).
- `kind`: Command type — `'disconnect_client'`, `'disconnect_role'`, or `'reload_acls'`.
- `target`: Client ID, role name, or `'*'` depending on the command kind.
- `reason_code`: MQTT 5 reason code (smallint). NULL for `reload_acls`.
- `issued_at`: Timestamp when the command was enqueued.

Unlogged because the BGW loses in-memory state across a crash anyway — a dropped command simply needs to be reissued.
