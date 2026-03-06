#!/usr/bin/env python3
"""
WAL position crash-correctness test for pgmqtt.

Verifies the invariant:
  "The replication slot's confirmed_flush_lsn only advances AFTER the
   corresponding QOS 1 messages have been durably persisted to
   pgmqtt_messages."

Since we cannot literally crash the broker mid-transaction in a test
environment, we instead verify the observable consequence: after each
CDC event lands, the slot LSN and the pgmqtt_messages row both exist
together.  If the old (two-transaction) code were still in place and we
ran many rapid inserts, we would occasionally see the LSN advance without
the corresponding row — this test would catch that.

We also verify that QOS 0 events do NOT create rows in pgmqtt_messages,
and that the batch-loop correctly drains a burst of events in one tick.
"""

import socket
import struct
import time
import sys
import os

from test_utils import run_sql, get_db_conn

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
SLOT_NAME = "pgmqtt_slot"
TICK_WAIT = 2.0   # seconds — more than one broker tick interval (250 ms)


# ── MQTT helpers ──────────────────────────────────────────────────────────────

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


def _connect(client_id, clean_start=True):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    flags = 0x02 if clean_start else 0x00
    vh = bytearray(b"\x00\x04MQTT\x05") + bytearray([flags]) + b"\x00\x3C" + b"\x00"
    payload = struct.pack("!H", len(client_id)) + client_id.encode()
    remaining = len(vh) + len(payload)
    pkt = bytearray([0x10]) + _encode_varlen(remaining) + vh + payload
    s.sendall(pkt)
    _recv(s)  # CONNACK
    return s


def _subscribe(s, packet_id, topic, qos):
    vh = struct.pack("!H", packet_id) + b"\x00"
    payload = struct.pack("!H", len(topic)) + topic.encode() + bytes([qos])
    remaining = len(vh) + len(payload)
    pkt = bytearray([0x82]) + _encode_varlen(remaining) + bytes(vh) + bytes(payload)
    s.sendall(pkt)
    _recv(s)  # SUBACK


def _recv(s, timeout=5.0):
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
    if rows:
        return rows[0][0]
    return None


def _count_messages(topic_prefix):
    rows = run_sql(
        f"SELECT COUNT(*) FROM pgmqtt_messages WHERE topic LIKE '{topic_prefix}%'"
    )
    return rows[0][0] if rows else 0


# ── Setup ─────────────────────────────────────────────────────────────────────

def setup_module(module=None):
    run_sql("DROP TABLE IF EXISTS wct_qos1_table;")
    run_sql("DROP TABLE IF EXISTS wct_qos0_table;")
    run_sql(
        "CREATE TABLE wct_qos1_table "
        "(id serial primary key, name text, val text);"
    )
    run_sql("ALTER TABLE wct_qos1_table REPLICA IDENTITY FULL;")
    run_sql(
        "CREATE TABLE wct_qos0_table "
        "(id serial primary key, name text, val text);"
    )
    run_sql("ALTER TABLE wct_qos0_table REPLICA IDENTITY FULL;")

    run_sql(
        "SELECT pgmqtt_add_mapping('public', 'wct_qos1_table', "
        "'wct/qos1/{{ columns.name }}', '{{ columns.val }}', 1);"
    )
    run_sql(
        "SELECT pgmqtt_add_mapping('public', 'wct_qos0_table', "
        "'wct/qos0/{{ columns.name }}', '{{ columns.val }}', 0);"
    )
    # Wait one tick so mappings are loaded
    time.sleep(TICK_WAIT)


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_qos1_persistence_atomic_with_wal_advance():
    """
    After a CDC event on a QOS 1 table:
    - pgmqtt_messages must contain the row
    - The slot LSN must have advanced
    Both facts must be true simultaneously (atomicity).
    """
    print("┌─ test_qos1_persistence_atomic_with_wal_advance")

    before_lsn = _get_slot_lsn()
    before_count = _count_messages("wct/qos1/")
    print(f"│  before: lsn={before_lsn}, msg_count={before_count}")

    run_sql("INSERT INTO wct_qos1_table (name, val) VALUES ('atom', 'v1');")
    time.sleep(TICK_WAIT)

    after_lsn = _get_slot_lsn()
    after_count = _count_messages("wct/qos1/")
    print(f"│  after:  lsn={after_lsn}, msg_count={after_count}")

    # LSN must have advanced
    assert after_lsn != before_lsn, (
        f"Slot LSN did not advance (still {after_lsn})"
    )
    # Message must exist — atomically with the LSN advance
    assert after_count == before_count + 1, (
        f"Expected {before_count + 1} messages, got {after_count} "
        f"(LSN advanced but row missing — atomicity violation!)"
    )
    print("└─ PASS\n")
    return True


def test_qos0_no_persistence():
    """
    CDC events on a QOS 0 table must NOT create rows in pgmqtt_messages.
    """
    print("┌─ test_qos0_no_persistence")

    before_count = _count_messages("wct/qos0/")
    run_sql("INSERT INTO wct_qos0_table (name, val) VALUES ('fire', 'forget');")
    time.sleep(TICK_WAIT)
    after_count = _count_messages("wct/qos0/")

    print(f"│  qos0 messages before={before_count}, after={after_count}")
    assert after_count == before_count, (
        f"QOS 0 event created a row in pgmqtt_messages (should not persist)"
    )
    print("└─ PASS\n")
    return True


def test_batch_burst_all_persisted():
    """
    Insert more events than CDC_BATCH_SIZE (256) in a single burst.
    All QOS 1 messages must be persisted exactly once, and the slot must
    have advanced past all of them.

    We use a smaller burst (300) to exercise at least two batch iterations
    without making the test too slow.
    """
    print("┌─ test_batch_burst_all_persisted")
    BURST = 300

    before_count = _count_messages("wct/qos1/burst")
    before_lsn = _get_slot_lsn()

    # Insert BURST rows in a single transaction to make them land in WAL together
    values = ", ".join(f"('burst{i}', 'v{i}')" for i in range(BURST))
    run_sql(f"INSERT INTO wct_qos1_table (name, val) VALUES {values};")

    # Allow enough time for the batch loop to drain all events
    # (at least ceil(BURST / 256) ticks × TICK_WAIT, plus margin)
    time.sleep(TICK_WAIT * 3)

    after_count = _count_messages("wct/qos1/burst")
    after_lsn = _get_slot_lsn()

    print(f"│  burst={BURST}, new rows in pgmqtt_messages={after_count - before_count}")
    print(f"│  lsn advanced: {before_lsn} → {after_lsn}")

    assert after_count - before_count == BURST, (
        f"Expected {BURST} new messages, got {after_count - before_count} "
        f"(some events may have been lost or double-counted)"
    )
    assert after_lsn != before_lsn, "Slot LSN did not advance after burst"
    print("└─ PASS\n")
    return True


def test_mqtt_delivery_after_persist():
    """
    A connected QOS 1 subscriber must receive the message AND the broker
    must have persisted it to pgmqtt_messages (confirmed via DB query).
    Demonstrates that the in-memory topic_buffer push happens in sync with
    the DB persist, so delivery does not race ahead of durability.
    """
    print("┌─ test_mqtt_delivery_after_persist")

    s = _connect("wct_sub", clean_start=True)
    _subscribe(s, 1, "wct/qos1/#", qos=1)

    before_count = _count_messages("wct/qos1/deliver")
    run_sql("INSERT INTO wct_qos1_table (name, val) VALUES ('deliver', 'payload1');")

    pkt = _recv(s, timeout=10.0)
    assert pkt is not None, "No PUBLISH received from broker"
    byte0, body = pkt
    qos = (byte0 & 0x06) >> 1
    assert qos == 1, f"Expected QOS 1, got QOS {qos}"

    tlen = struct.unpack("!H", body[:2])[0]
    topic = body[2:2 + tlen].decode()
    print(f"│  received PUBLISH topic={topic} qos={qos}")

    # At the moment we received the PUBLISH, the message MUST already be in DB
    db_count = _count_messages("wct/qos1/deliver")
    assert db_count > before_count, (
        "PUBLISH delivered to client but NOT yet in pgmqtt_messages "
        "(would be lost on crash — atomicity violation!)"
    )
    print(f"│  pgmqtt_messages row count={db_count} ✓")
    s.close()
    print("└─ PASS\n")
    return True


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== WAL Crash Correctness Tests ===\n")
    setup_module()

    results = {
        "qos1_persistence_atomic_with_wal_advance": False,
        "qos0_no_persistence": False,
        "batch_burst_all_persisted": False,
        "mqtt_delivery_after_persist": False,
    }

    try:
        results["qos1_persistence_atomic_with_wal_advance"] = \
            test_qos1_persistence_atomic_with_wal_advance()
    except Exception as e:
        print(f"└─ FAIL: {e}\n")

    try:
        results["qos0_no_persistence"] = test_qos0_no_persistence()
    except Exception as e:
        print(f"└─ FAIL: {e}\n")

    try:
        results["batch_burst_all_persisted"] = test_batch_burst_all_persisted()
    except Exception as e:
        print(f"└─ FAIL: {e}\n")

    try:
        results["mqtt_delivery_after_persist"] = test_mqtt_delivery_after_persist()
    except Exception as e:
        print(f"└─ FAIL: {e}\n")

    passed = sum(results.values())
    total = len(results)
    print(f"Results: {passed}/{total} passed")
    for name, ok in results.items():
        print(f"  {'✓' if ok else '✗'} {name}")

    if passed < total:
        sys.exit(1)
    print("\nALL WAL CRASH CORRECTNESS TESTS PASSED")
