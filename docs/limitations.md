# pgmqtt Status & Limitations

## 1. Quality of Service (QoS) Level
**Supported: QoS 0 (At most once) and QoS 1 (At least once)**

- **QoS 0**: Messages are delivered immediately to online subscribers. No persistence or redelivery.
- **QoS 1**: Messages are persisted in the `pgmqtt_messages` table. If a client is offline, messages are stored and delivered upon reconnection (for persistent sessions). The broker handles `PUBACK` and redelivery for unacknowledged packets.
- **Downgrading**: Subscriptions with QoS 2 requests are currently downgraded to QoS 1.

## 2. Retained Messages
Retained messages are **partially supported**:
- **Live Publishes**: If a client publishes a message with the `Retain` flag set, the broker forwards it to currently connected matching subscribers. However, delivery to new subscribers upon subscription is currently not supported.
- **CDC Events**: Changes captured from PostgreSQL tables via CDC are not currently tagged as "retained".



## 3. Security and Authentication
- **No TLS/SSL**: MQTT connections are not encrypted natively by the extension.
- **No Authentication**: The broker does not yet validate MQTT credentials. Any client can connect.
- **Encryption**: For production use, it is recommended to proxy the MQTT/WS ports through a TLS-terminating load balancer or sidecar.

## 4. Supported Operations
`INSERT`, `UPDATE`, and `DELETE` operations are captured. `DELETE` requiring `REPLICA IDENTITY FULL` still applies. DDL changes and `TRUNCATE` are not captured.

---

# pgmqtt MQTT 5.0 Feature Matrix

Feature support status for the pgmqtt PostgreSQL extension MQTT broker.

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
| QoS downgrade (sub QoS < pub QoS) | :white_check_mark: | [test_qos0.py](tests/integration/test_qos0.py) |
| No QoS upgrade | :white_check_mark: | [test_qos0.py](tests/integration/test_qos0.py) |
| Packet ID wraparound | :white_check_mark: | [test_qos1.py](tests/integration/test_qos1.py) |
| DUP flag on redelivery | :white_check_mark: | [test_qos1.py](tests/integration/test_qos1.py) |

## Retained Messages

| Feature | Status | Test |
|---------|--------|------|
| Retain flag forwarding on live publish | :white_check_mark: | [test_retain.py](tests/integration/test_retain.py) |
| Retained message delivery on subscribe | :x: | [test_retain.py](tests/integration/test_retain.py) |
| Clear retained with empty payload | :x: | [test_retain.py](tests/integration/test_retain.py) |

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
| Null character in topic | :x: | [test_adversarial.py](tests/integration/test_adversarial.py) |
| Wildcard in PUBLISH topic | :x: | [test_adversarial.py](tests/integration/test_adversarial.py) |
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
