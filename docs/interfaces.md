# pgmqtt Interfaces

This document describes the SQL-callable interfaces and configuration tables used to govern the `pgmqtt` extension.

## SQL Functions

`pgmqtt` introduces several functions to manage how changes in PostgreSQL tables map to MQTT topics.

### 1. `pgmqtt_add_mapping`

Registers a new logical decoding mapping for a given table.

**Signature:**
```sql
pgmqtt_add_mapping(
    schema_name text,
    table_name text,
    topic_template text,
    payload_template text,
    qos integer DEFAULT 0
) RETURNS text
```

**Parameters:**
- `schema_name`: The database schema of the table (e.g., `'public'`).
- `table_name`: The table name you want to capture changes from.
- `topic_template`: A Jinja2-compatible template string used to determine the MQTT topic for each change.
- `payload_template`: A Jinja2-compatible template string defining what the MQTT message body will contain.
- `qos`: Optional. The Quality of Service level for messages generated from this mapping (0 or 1). Defaults to 0.

**Example:**
```sql
SELECT pgmqtt_add_mapping(
    'public',
    'events',
    'events/{{ op | lower }}',
    '{{ columns | tojson }}',
    1
);
```

---

### 2. `pgmqtt_remove_mapping`

Removes an existing topic mapping. Note that any changes already in the internal ring buffer for this mapping may still be dispatched.

**Signature:**
```sql
pgmqtt_remove_mapping(
    schema_name text,
    table_name text
) RETURNS boolean
```
Returns `true` if the mapping was found and successfully deleted, or `false` otherwise.

**Example:**
```sql
SELECT pgmqtt_remove_mapping('public', 'events');
```

---

### 3. `pgmqtt_list_mappings`

Lists all currently active topic mappings.

**Signature:**
```sql
pgmqtt_list_mappings()
```
Returns a set of rows detailing the mappings.

**Example:**
```sql
SELECT * FROM pgmqtt_list_mappings();
```
Output:
```text
  schema_name | table_name |       topic_template      |    payload_template   | qos 
 -------------+------------+---------------------------+-----------------------+-----
  public      | events     | events/{{ op | lower }}   | {{ columns | tojson }} |   1
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

All counts are taken from a single SQL statement and reflect a consistent snapshot.

---

### 5. `pgmqtt_license_status` *(enterprise)*

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
