"""
CDC (Change Data Capture) Specific Tests.

Tests WAL atomicity, QoS rendering, batch bursts, and multi-mapping demos.
Requires a module-level subscriber for WAL tests.
"""

import json
import socket
import struct
import time

from test_utils import run_sql
from proto_utils import (
    create_connect_packet,
    create_subscribe_packet,
    create_puback_packet,
    recv_packet,
    validate_connack,
    validate_suback,
    validate_publish,
    MQTTControlPacket,
    MQTT_HOST,
    MQTT_PORT,
    run_psql,
)

SLOT_NAME = "pgmqtt_slot"
TICK_WAIT = 2.0

# Module-level subscriber for WAL tests
_module_sub = None


def _encode_varlen(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            b |= 0x80
        out.append(b)
        if not n:
            break
    return out


def _connect_raw(client_id, clean_start=True):
    """Minimal MQTT connect for WAL tests."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet(client_id, clean_start=clean_start))
    recv_packet(s)
    return s


def _subscribe_raw(s, packet_id, topic, qos):
    s.sendall(create_subscribe_packet(packet_id, topic, qos=qos))
    recv_packet(s)


def _recv_raw(s, timeout=5.0):
    s.settimeout(timeout)
    try:
        hdr = s.recv(1)
        if not hdr:
            return None
        multiplier, length = 1, 0
        while True:
            b = s.recv(1)[0]
            length += (b & 0x7F) * multiplier
            if not (b & 0x80):
                break
            multiplier *= 128
        body = b""
        while len(body) < length:
            chunk = s.recv(length - len(body))
            if not chunk:
                return None
            body += chunk
        return (hdr[0], body)
    except socket.timeout:
        return None


def _get_slot_lsn():
    rows = run_sql(
        f"SELECT confirmed_flush_lsn::text FROM pg_replication_slots "
        f"WHERE slot_name = '{SLOT_NAME}'"
    )
    return rows[0][0] if rows else None


def _count_messages(topic_prefix):
    rows = run_sql(
        f"SELECT COUNT(*) FROM pgmqtt_messages WHERE topic LIKE '{topic_prefix}%'"
    )
    return rows[0][0] if rows else 0


# ---------------------------------------------------------------------------
# Module setup/teardown
# ---------------------------------------------------------------------------

def setup_module(module=None):
    global _module_sub

    run_sql("DROP TABLE IF EXISTS wct_qos1_table;")
    run_sql("DROP TABLE IF EXISTS wct_qos0_table;")
    run_sql("CREATE TABLE wct_qos1_table (id serial primary key, name text, val text);")
    run_sql("ALTER TABLE wct_qos1_table REPLICA IDENTITY FULL;")
    run_sql("CREATE TABLE wct_qos0_table (id serial primary key, name text, val text);")
    run_sql("ALTER TABLE wct_qos0_table REPLICA IDENTITY FULL;")
    run_sql(
        "SELECT pgmqtt_add_outbound_mapping('public', 'wct_qos1_table', "
        "'wct/qos1/{{ columns.name }}', '{{ columns.val }}', 1);"
    )
    run_sql(
        "SELECT pgmqtt_add_outbound_mapping('public', 'wct_qos0_table', "
        "'wct/qos0/{{ columns.name }}', '{{ columns.val }}', 0);"
    )
    time.sleep(6)

    _module_sub = _connect_raw("wct_module_sub", clean_start=True)
    _subscribe_raw(_module_sub, 1, "wct/#", qos=1)


def teardown_module(module=None):
    global _module_sub
    if _module_sub is not None:
        try:
            _module_sub.close()
        except Exception:
            pass
        _module_sub = None


# ---------------------------------------------------------------------------
# WAL atomicity tests
# ---------------------------------------------------------------------------

def test_qos1_persistence_atomic_with_wal_advance():
    """LSN and pgmqtt_messages row advance atomically."""
    before_lsn = _get_slot_lsn()
    before_count = _count_messages("wct/qos1/")

    run_sql("INSERT INTO wct_qos1_table (name, val) VALUES ('atom', 'v1');")
    time.sleep(TICK_WAIT)

    after_lsn = _get_slot_lsn()
    after_count = _count_messages("wct/qos1/")

    assert after_lsn != before_lsn, f"Slot LSN did not advance (still {after_lsn})"
    assert after_count == before_count + 1, (
        f"Expected {before_count + 1} messages, got {after_count} "
        "(LSN advanced but row missing — atomicity violation!)"
    )


def test_qos0_no_persistence():
    """QoS 0 CDC events do NOT create pgmqtt_messages rows."""
    before = _count_messages("wct/qos0/")
    run_sql("INSERT INTO wct_qos0_table (name, val) VALUES ('fire', 'forget');")
    time.sleep(TICK_WAIT)
    after = _count_messages("wct/qos0/")
    assert after == before, "QoS 0 should not persist to pgmqtt_messages"


def test_batch_burst_all_persisted():
    """300 burst inserts all persisted exactly once."""
    BURST = 300
    before = _count_messages("wct/qos1/burst")
    before_lsn = _get_slot_lsn()

    values = ", ".join(f"('burst{i}', 'v{i}')" for i in range(BURST))
    run_sql(f"INSERT INTO wct_qos1_table (name, val) VALUES {values};")
    time.sleep(TICK_WAIT * 3)

    after = _count_messages("wct/qos1/burst")
    after_lsn = _get_slot_lsn()

    assert after - before == BURST, (
        f"Expected {BURST} new messages, got {after - before}"
    )
    assert after_lsn != before_lsn, "Slot LSN did not advance"


def test_mqtt_delivery_after_persist():
    """MQTT delivery synchronized with DB persistence."""
    s = _connect_raw("wct_delivery_sub", clean_start=True)
    _subscribe_raw(s, 1, "wct/qos1/#", qos=1)

    before = _count_messages("wct/qos1/deliver")
    run_sql("INSERT INTO wct_qos1_table (name, val) VALUES ('deliver', 'payload1');")

    pkt = _recv_raw(s, timeout=10.0)
    assert pkt is not None, "No PUBLISH received"
    byte0, body = pkt
    qos = (byte0 & 0x06) >> 1
    assert qos == 1

    db_count = _count_messages("wct/qos1/deliver")
    assert db_count > before, (
        "PUBLISH delivered but NOT yet in pgmqtt_messages (atomicity violation!)"
    )
    s.close()


# ---------------------------------------------------------------------------
# CDC QoS rendering
# ---------------------------------------------------------------------------

def test_rendered_qos_switch():
    """Mapping QoS 0→1 reflects in delivered messages."""
    run_psql("DROP TABLE IF EXISTS qos_test_table;")
    run_psql("CREATE TABLE qos_test_table (id serial primary key, name text, val text);")
    run_psql("ALTER TABLE qos_test_table REPLICA IDENTITY FULL;")
    run_psql(
        "SELECT pgmqtt_add_outbound_mapping('public', 'qos_test_table', "
        "'test/{{ columns.name }}', '{{ columns.val }}', 0);"
    )
    time.sleep(6)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("cdc_qos_sub", clean_start=True))
    recv_packet(s)
    s.sendall(create_subscribe_packet(1, "test/#", qos=1))
    recv_packet(s)

    # QoS 0 insert
    run_psql("INSERT INTO qos_test_table (name, val) VALUES ('qos0', 'message0');")
    res = _recv_raw(s, timeout=5.0)
    assert res is not None, "Timeout waiting for QoS 0 message"
    header, body = res
    qos = (header & 0x06) >> 1
    assert qos == 0, f"Expected QoS 0, got {qos}"

    # Switch mapping to QoS 1
    run_psql(
        "SELECT pgmqtt_add_outbound_mapping('public', 'qos_test_table', "
        "'test/{{ columns.name }}', '{{ columns.val }}', 1);"
    )
    time.sleep(6)

    # QoS 1 insert
    run_psql("INSERT INTO qos_test_table (name, val) VALUES ('qos1', 'message1');")
    res = _recv_raw(s, timeout=5.0)
    assert res is not None, "Timeout waiting for QoS 1 message"
    header, body = res
    qos = (header & 0x06) >> 1
    assert qos == 1, f"Expected QoS 1, got {qos}"

    # ACK it
    tlen = struct.unpack("!H", body[:2])[0]
    packet_id = struct.unpack("!H", body[2 + tlen:2 + tlen + 2])[0]
    ack = bytearray([0x40, 0x02])
    ack.extend(struct.pack("!H", packet_id))
    s.sendall(ack)
    s.close()


# ---------------------------------------------------------------------------
# Multi-mapping demo
# ---------------------------------------------------------------------------

def test_comprehensive_cdc_demo():
    """Multiple CDC mappings (users/IoT) produce correct topics and JSON payloads."""
    run_psql("DROP TABLE IF EXISTS demo_users;")
    run_psql("DROP TABLE IF EXISTS demo_iot;")
    run_psql("CREATE TABLE demo_users (id serial PRIMARY KEY, username text, status text);")
    run_psql("ALTER TABLE demo_users REPLICA IDENTITY FULL;")
    run_psql("CREATE TABLE demo_iot (id serial PRIMARY KEY, device_id text, reading float, unit text);")
    run_psql("ALTER TABLE demo_iot REPLICA IDENTITY FULL;")

    run_psql(
        "SELECT pgmqtt_add_outbound_mapping('public', 'demo_users', "
        "'presence/{{columns.username}}', '{\"status\": \"{{columns.status}}\"}');"
    )
    run_psql(
        "SELECT pgmqtt_add_outbound_mapping('public', 'demo_iot', "
        "'telemetry/{{columns.device_id}}', '{\"val\": {{columns.reading}}, \"u\": \"{{columns.unit}}\"}');"
    )
    time.sleep(6)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("cdc_demo_client", clean_start=True))
    recv_packet(s)
    s.sendall(create_subscribe_packet(1, "#"))
    recv_packet(s)

    ops = [
        ("INSERT INTO demo_users (username, status) VALUES ('alice', 'online');",
         "presence/alice", {"status": "online"}),
        ("INSERT INTO demo_iot (device_id, reading, unit) VALUES ('sensor_01', 22.5, 'C');",
         "telemetry/sensor_01", {"val": 22.5, "u": "C"}),
    ]

    for sql, expected_topic, expected_json in ops:
        run_psql(sql)
        pkt = recv_packet(s, timeout=5)
        assert pkt is not None, f"Timeout waiting for {expected_topic}"
        topic, payload, *_ = validate_publish(pkt)
        data = json.loads(payload)
        assert topic == expected_topic, f"Topic mismatch: {topic}"
        assert data == expected_json, f"Payload mismatch: {data}"

    s.close()
