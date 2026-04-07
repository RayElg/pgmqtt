# pgmqtt Status & Limitations

## 1. Quality of Service (QoS) Level
**Supported: QoS 0 (At most once) and QoS 1 (At least once)**

- **QoS 0**: Messages are delivered immediately to online subscribers. No persistence or redelivery.
- **QoS 1**: Messages are persisted in the `pgmqtt_messages` table. If a client is offline, messages are stored and delivered upon reconnection (for persistent sessions). The broker handles `PUBACK` and redelivery for unacknowledged packets.
- **Downgrading**: Subscriptions with QoS 2 requests are currently downgraded to QoS 1.

## 2. Retained Messages
Retained messages are **supported**:
- **Live Publishes**: If a client publishes a message with the `Retain` flag set, the broker stores it and forwards it to matching subscribers.
- **Retained delivery on subscribe**: New subscribers receive matching retained messages immediately upon subscribing, with the RETAIN flag set to 1.
- **Clearing retained**: Publishing an empty payload with the Retain flag deletes the retained message. New subscribers will not receive anything for that topic.
- **CDC Events**: Changes captured from PostgreSQL tables via CDC are not currently tagged as "retained".



## 3. Security and Authentication
- **Native TLS/SSL (enterprise)**: MQTTS (port 8883) and WSS (port 9002) listeners are available in enterprise builds via `pgmqtt.mqtts_enabled` / `pgmqtt.wss_enabled`. Community builds should terminate TLS at a reverse proxy (nginx, HAProxy, AWS NLB) and forward plain MQTT or WebSocket traffic to pgmqtt.
- **No username/password authentication**: The broker does not validate MQTT credentials beyond JWT tokens.
- **JWT authentication**: Supported via `pgmqtt.jwt_public_key` / `pgmqtt.jwt_required` (enterprise feature).

## 4. Supported Operations
`INSERT`, `UPDATE`, and `DELETE` operations are captured. `DELETE` requiring `REPLICA IDENTITY FULL` still applies. DDL changes and `TRUNCATE` are not captured.

---

# pgmqtt MQTT Feature Matrix

Feature support status for the pgmqtt PostgreSQL extension MQTT broker.
Both MQTT 5.0 and MQTT 3.1.1 clients are supported and can interoperate on the
same broker. The tables below cover MQTT 5.0; the dedicated
[MQTT 3.1.1](#mqtt-311) section documents version-specific behaviour.

## Legend

| Symbol | Meaning |
|--------|---------|
| :white_check_mark: | Supported and tested |
| :construction: | Partial support |
| :x: | Not implemented |

## Connection & Session

| Feature | Status | Test |
|---------|--------|------|
| CONNECT / CONNACK | :white_check_mark: | [test_connect.py](tests/integration/test_connect.py) |
| Clean Start | :white_check_mark: | [test_session.py](tests/integration/test_session.py) |
| Session Expiry Interval | :white_check_mark: | [test_session.py](tests/integration/test_session.py) |
| Session persistence across restart | :white_check_mark: | [test_persistence.py](tests/integration/test_persistence.py) |
| Keep Alive / PINGREQ / PINGRESP | :white_check_mark: | [test_connect.py](tests/integration/test_connect.py) |
| Session Takeover | :white_check_mark: | [test_connect.py](tests/integration/test_connect.py) |
| Auto-generated Client IDs | :white_check_mark: | [test_connect.py](tests/integration/test_connect.py) |
| DISCONNECT packet | :white_check_mark: | [test_session.py](tests/integration/test_session.py) |

## Publish & Subscribe

| Feature | Status | Test |
|---------|--------|------|
| QoS 0 (At most once) | :white_check_mark: | [test_qos0.py](tests/integration/test_qos0.py) |
| QoS 1 (At least once) | :white_check_mark: | [test_qos1.py](tests/integration/test_qos1.py) |
| QoS 2 (Exactly once) | :x: | [test_qos2.py](tests/integration/test_qos2.py) |
| SUBSCRIBE / SUBACK | :white_check_mark: | [test_subscribe.py](tests/integration/test_subscribe.py) |
| UNSUBSCRIBE / UNSUBACK | :white_check_mark: | [test_subscribe.py](tests/integration/test_subscribe.py) |
| Wildcard subscriptions (`+`, `#`) | :white_check_mark: | [test_subscribe.py](tests/integration/test_subscribe.py) |
| Shared subscriptions (`$share/`) | :white_check_mark: | [test_shared_subscriptions.py](tests/integration/test_shared_subscriptions.py) |
| QoS downgrade (sub QoS < pub QoS) | :white_check_mark: | [test_qos0.py](tests/integration/test_qos0.py) |
| No QoS upgrade | :white_check_mark: | [test_qos0.py](tests/integration/test_qos0.py) |
| Packet ID wraparound | :white_check_mark: | [test_qos1.py](tests/integration/test_qos1.py) |
| DUP flag on redelivery | :white_check_mark: | [test_qos1.py](tests/integration/test_qos1.py) |

## Retained Messages

| Feature | Status | Test |
|---------|--------|------|
| Retain flag forwarding on live publish | :white_check_mark: | [test_retain.py](tests/integration/test_retain.py) |
| Retained message delivery on subscribe | :white_check_mark: | [test_retain.py](tests/integration/test_retain.py) |
| Clear retained with empty payload | :white_check_mark: | [test_retain.py](tests/integration/test_retain.py) |

## Will (LWT)

| Feature | Status | Test |
|---------|--------|------|
| Last Will on abrupt disconnect | :white_check_mark: | [test_will.py](tests/integration/test_will.py) |
| No LWT on clean disconnect | :white_check_mark: | [test_will.py](tests/integration/test_will.py) |
| Keep Alive timeout triggers LWT | :white_check_mark: | [test_will.py](tests/integration/test_will.py) |
| Will Delay Interval | :x: | [test_will.py](tests/integration/test_will.py) |
| Write error triggers LWT | :white_check_mark: | [test_persistence.py](tests/integration/test_persistence.py) |

## Flow Control

| Feature | Status | Test |
|---------|--------|------|
| Receive Maximum enforcement | :white_check_mark: | [test_flow_control.py](tests/integration/test_flow_control.py) |
| Default Receive Maximum (65535) | :white_check_mark: | [test_flow_control.py](tests/integration/test_flow_control.py) |
| Queuing beyond Receive Maximum | :white_check_mark: | [test_flow_control.py](tests/integration/test_flow_control.py) |

## MQTT 3.1.1

MQTT 3.1.1 (protocol level 4) clients connect alongside v5 clients on the
same listener. The broker auto-detects the protocol version from the CONNECT
packet and adapts all subsequent packets accordingly:

- **CONNACK** uses v3.1.1 return codes (0x00–0x05), not v5 reason codes.
- **SUBACK** return codes are 0x00/0x01/0x02 (granted QoS) or 0x80 (failure).
- **UNSUBACK** contains only the packet identifier (no reason codes, no properties).
- **PUBLISH** packets omit the properties section.
- **DISCONNECT** from the server is sent as an empty packet (no reason code).
- **Clean Session** (`clean_session=0`) persists the session indefinitely across
  disconnects, equivalent to `Session Expiry Interval = ∞` in v5.
- **Shared subscriptions** (`$share/`) are rejected for v3.1.1 clients (v5-only feature).
- **Session Expiry Interval**, **Receive Maximum**, **Will Delay Interval**,
  and **Message Expiry Interval** are v5 properties with no v3.1.1 equivalent.
  Sensible defaults are applied (expiry governed by Clean Session flag,
  Receive Maximum = 65535).

All features listed in the v5 tables above are supported for v3.1.1 clients
except where marked as v5-only.

| Feature | Status | Test |
|---------|--------|------|
| CONNECT / CONNACK (return codes) | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| CONNACK packet format (no properties) | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| Clean Session persistence (`clean_session=0`) | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| Clean Session clear (`clean_session=1`) | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| Keep Alive / PINGREQ / PINGRESP | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| Session Takeover | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| Auto-generated Client IDs | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| DISCONNECT (empty packet) | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| QoS 0 pub/sub | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| QoS 1 pub/sub with PUBACK | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| SUBSCRIBE / SUBACK (return codes) | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| UNSUBSCRIBE / UNSUBACK (no reason codes) | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| PUBLISH (no properties) | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| Wildcard subscriptions (`+`, `#`) | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| Shared subscriptions rejected | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| Retained messages | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| Last Will (abrupt + clean disconnect) | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| Unsupported version rejection (v3) | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| Cross-version interop (v5 ↔ v3.1.1) | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| Cross-version session resumption | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| Protocol state machine violations | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| Topic validation (wildcards, null, empty) | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |
| Malformed/truncated packet handling | :white_check_mark: | [test_v311.py](tests/integration/test_v311.py) |

## CDC (Change Data Capture)

| Feature | Status | Test |
|---------|--------|------|
| INSERT triggers MQTT publish | :white_check_mark: | [test_cdc.py](tests/integration/test_cdc.py) |
| Template-based topic/payload | :white_check_mark: | [test_cdc.py](tests/integration/test_cdc.py) |
| CDC QoS 0 | :white_check_mark: | [test_cdc.py](tests/integration/test_cdc.py) |
| CDC QoS 1 with persistence | :white_check_mark: | [test_cdc.py](tests/integration/test_cdc.py) |
| QoS switch (mapping update) | :white_check_mark: | [test_cdc.py](tests/integration/test_cdc.py) |
| WAL atomicity (LSN + message) | :white_check_mark: | [test_cdc.py](tests/integration/test_cdc.py) |
| CDC wildcard subscriptions | :white_check_mark: | [test_subscribe.py](tests/integration/test_subscribe.py) |
| Multi-table mappings | :white_check_mark: | [test_cdc.py](tests/integration/test_cdc.py) |

## Protocol Validation (Adversarial)

| Feature | Status | Test |
|---------|--------|------|
| Duplicate CONNECT rejection | :white_check_mark: | [test_adversarial.py](tests/integration/test_adversarial.py) |
| Subscribe before CONNECT rejection | :white_check_mark: | [test_adversarial.py](tests/integration/test_adversarial.py) |
| Unexpected PUBACK handling | :white_check_mark: | [test_adversarial.py](tests/integration/test_adversarial.py) |
| Invalid UTF-8 rejection | :x: | [test_adversarial.py](tests/integration/test_adversarial.py) |
| Null character in topic | :white_check_mark: | [test_adversarial.py](tests/integration/test_adversarial.py) |
| Wildcard in PUBLISH topic | :white_check_mark: | [test_adversarial.py](tests/integration/test_adversarial.py) |
| Invalid wildcard placement | :x: | [test_adversarial.py](tests/integration/test_adversarial.py) |
| QoS 0 with packet ID | :x: | [test_adversarial.py](tests/integration/test_adversarial.py) |
| QoS 1 without packet ID | :x: | [test_adversarial.py](tests/integration/test_adversarial.py) |
| Packet ID reuse detection | :x: | [test_adversarial.py](tests/integration/test_adversarial.py) |
| Reserved bit validation | :x: | [test_adversarial.py](tests/integration/test_adversarial.py) |
| Malformed varint detection | :x: | [test_adversarial.py](tests/integration/test_adversarial.py) |
| PUBREL without PUBREC | :x: | [test_adversarial.py](tests/integration/test_adversarial.py) |

## Message Expiry

| Feature | Status | Test |
|---------|--------|------|
| Message Expiry Interval | :x: | [test_session.py](tests/integration/test_session.py) |

## Performance

| Feature | Test |
|---------|------|
| QoS 0 throughput (200 msgs) | [test_perf.py](tests/integration/test_perf.py) |
| QoS 1 pipelined throughput (200 msgs) | [test_perf.py](tests/integration/test_perf.py) |
| CDC QoS 0 throughput (200 msgs) | [test_perf.py](tests/integration/test_perf.py) |
| CDC QoS 1 throughput (200 msgs) | [test_perf.py](tests/integration/test_perf.py) |
| Long-lived connection (40s) | [test_perf.py](tests/integration/test_perf.py) |
| High concurrency (50 clients x 1000 msgs) | [test_perf.py](tests/integration/test_perf.py) |

---

# Hard-Coded Limits

The following limits are compiled into the extension binary and cannot be changed at runtime.

## Connection & Client Limits

| Limit | Value | Description |
|-------|-------|-------------|
| Max concurrent connections (community) | **1,000** | Enterprise licenses can raise this via the license payload. |
| Max subscriptions per client | **100** | Additional SUBSCRIBE requests beyond this are rejected with an error reason code. |
| Client receive maximum (default) | **65,535** | MQTT 5.0 default; clients may negotiate a lower value via the CONNECT `Receive Maximum` property. |

## Message & Packet Limits

| Limit | Value | Description |
|-------|-------|-------------|
| Max MQTT packet size | **64 KB** (65,536 bytes) | Maximum bytes the broker will read from a single client request. Clients exceeding this are disconnected with `Malformed Packet`. |
| Max WebSocket frame payload | **64 KB** (65,536 bytes) | WebSocket frames larger than this are rejected and the connection is closed. |
| Max inflight QoS 1 messages per client | **800** | Unacknowledged QoS 1 PUBLISH packets queued for delivery. Additional messages are queued until the client ACKs. |
| Per-client pending queue warning | **10,000** messages | A warning is logged when a client's pending message queue exceeds this threshold. |
| Per-client pending queue hard cap | **50,000** messages | Clients exceeding this are disconnected to prevent unbounded memory growth. |

## Buffer & Throughput Limits

| Limit | Value | Description |
|-------|-------|-------------|
| CDC ring buffer capacity | **8,192** events | Fixed-capacity ring buffer for CDC change events. Oldest events are dropped on overflow (logged every 100 drops). |
| Per-topic QoS 0 buffer capacity | **4,096** messages | Bounded ring buffer per topic for QoS 0 CDC messages. Oldest messages are dropped on overflow. |
| Per-topic QoS 1+ buffer | **unbounded** | QoS 1+ messages are queued without a hard cap (a warning is logged every 1,000 messages). |
| CDC batch size | **256** rows | Maximum number of WAL changes consumed per poll cycle via `pg_logical_slot_get_changes`. |
| Inbound pending batch size | **50** rows | Maximum number of pending inbound writes processed per cycle. |

## Timing

| Limit | Value | Description |
|-------|-------|-------------|
| Poll interval (latch) | **80 ms** | How often the background worker wakes to accept connections, poll clients, and drain CDC. |
| Client read/write timeout | **2 seconds** | Timeout for individual client I/O operations. |
| CONNECT handshake timeout | **5 seconds** | Maximum time to wait for the initial MQTT CONNECT packet from a new connection. |
| Keep-alive enforcement | **1.5 &times; keep_alive** | Clients are disconnected if no packet is received within 1.5&times; their negotiated keep-alive interval (per MQTT 5.0 §3.1.2.10). A keep-alive of 0 disables the timeout. |
