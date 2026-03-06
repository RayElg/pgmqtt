# pgmqtt Status & Limitations

## 1. Quality of Service (QoS) Level
**Supported: QoS 0 (At most once) and QoS 1 (At least once)**

- **QoS 0**: Messages are delivered immediately to online subscribers. No persistence or redelivery.
- **QoS 1**: Messages are persisted in the `pgmqtt_messages` table. If a client is offline, messages are stored and delivered upon reconnection (for persistent sessions). The broker handles `PUBACK` and redelivery for unacknowledged packets.
- **Downgrading**: Subscriptions with QoS 2 requests are currently downgraded to QoS 1.

## 2. Retained Messages
Retained messages are **partially supported**:
- **Client Publishes**: If a client publishes a message with the `Retain` flag set, the broker stores it in the `pgmqtt_retained` table and delivers it to matching new subscribers.
- **CDC Events**: Changes captured from PostgreSQL tables via CDC are not currently tagged as "retained".

## 3. Session Management
**Persistent Sessions**:
- **Clean Start**: Clients can specify `Clean Start` in the `CONNECT` packet. If `true`, any existing session and subscriptions for that client ID are removed.
- **Session Expiry**: Sessions persist until cleared by a `Clean Start=true` connection. Automatic session expiry (via Session Expiry Interval) is not yet implemented.
- **Persistence**: Subscriptions and inflight QoS 1 messages are stored in memory but indexed by Client ID, allowing them to survive individual connection drops.

## 4. Configuration and Mappings
Mappings between PostgreSQL tables and MQTT topics are managed via SQL functions. Mappings now support a `qos` parameter to specify the delivery level for CDC events.

## 5. Security and Authentication
- **No TLS/SSL**: MQTT connections are not encrypted natively by the extension.
- **No Authentication**: The broker does not yet validate MQTT credentials. Any client can connect.
- **Encryption**: For production use, it is recommended to proxy the MQTT/WS ports through a TLS-terminating load balancer or sidecar.

## 6. Supported Operations
`INSERT`, `UPDATE`, and `DELETE` operations are captured. `DELETE` requiring `REPLICA IDENTITY FULL` still applies. DDL changes and `TRUNCATE` are not captured.

## 7. Performance and Memory Bounds
- `pgmqtt` utilizes fixed-size in-memory ring buffers for CDC capture before persistence.
- High-throughput QoS 1 publishing involves database I/O for the `pgmqtt_messages` table, which may impact performance compared to transient QoS 0 delivery.

## 8. Replication and High Availability
**Primary-Only Execution**: 
- `pgmqtt` background workers (MQTT broker and CDC consumer) are configured to start only when the PostgreSQL instance is not in recovery mode (i.e., it is a primary node).
- **Read Replicas**: On read replicas, the background workers remain paused/shipped. No ports (1883, 8080, etc.) will be opened.
- **Failover/Promotion**: If a replica is promoted to primary (e.g., via `pg_ctl promote`), the background workers will automatically start, initialize the MQTT broker, and resume CDC processing.
