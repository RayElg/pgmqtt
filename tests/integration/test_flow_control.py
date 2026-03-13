#!/usr/bin/env python3
"""
Integration tests for QoS 1 Flow Control in pgmqtt.

Tests:
  1. When the broker has MAX_INFLIGHT_MESSAGES in flight to a slow subscriber,
     additional messages are queued (not dropped) and delivered once the client
     starts ACKing — verifying at-least-once semantics under backpressure.
  2. A fast-ACKing client receives all messages without any drops.
"""

import socket
import time
import struct
import sys
import os
import threading


from proto_utils import (
    create_connect_packet,
    create_subscribe_packet,
    create_publish_packet,
    create_puback_packet,
    create_disconnect_packet,
    recv_packet,
    validate_connack,
    validate_suback,
    validate_publish,
    MQTTControlPacket,
    run_psql,
    MQTT_HOST,
    MQTT_PORT,
)

# ---------------------------------------------------------------------------
# Constants – must match server.rs MAX_INFLIGHT_MESSAGES
# ---------------------------------------------------------------------------
MAX_INFLIGHT = 800

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def setup_table(table="flow_control_test"):
    run_psql(f"DROP TABLE IF EXISTS {table};")
    run_psql(
        f"CREATE TABLE {table} (id serial primary key, val text);"
    )
    run_psql(f"ALTER TABLE {table} REPLICA IDENTITY FULL;")
    run_psql(
        f"SELECT pgmqtt_add_mapping('public', '{table}', "
        f"'test/flow/{{{{ columns.id }}}}', '{{{{ columns.val }}}}', 1);"
    )
    time.sleep(6)  # Wait for server's 5s mapping cache to expire


def drain_all_publish(s, count, timeout_per=3.0):
    """
    Read exactly `count` PUBLISH packets from socket `s`, returning their packet_ids.
    Raises AssertionError if fewer than `count` arrive.
    """
    pids = []
    while len(pids) < count:
        pkt = recv_packet(s, timeout=timeout_per)
        assert pkt is not None, (
            f"Expected {count} PUBLISH packets but only got {len(pids)}"
        )
        ptype = (pkt[0] & 0xF0) >> 4
        if ptype == MQTTControlPacket.PUBLISH:
            _, _, qos, _, _, pid, _ = validate_publish(pkt)
            assert qos == 1
            pids.append(pid)
    return pids


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_flow_control_queues_not_drops():
    """
    Scenario:
    - Subscriber connects but does NOT send PUBACKs (simulates a slow client).
    - Publisher sends N > MAX_INFLIGHT messages via CDC inserts.
    - The broker should fill up inflight and queue the rest.
    - After the subscriber starts ACKing, all N messages must be delivered.
    """
    print("\n[Flow Control] Test 1: queued messages are all delivered when client catches up")
    table = "flow_control_test"
    topic_prefix = "test/flow/"
    setup_table(table)

    TOTAL = MAX_INFLIGHT + 50  # intentionally exceed inflight window

    sub_id = "fc_slow_subscriber"

    # 1. Connect subscriber
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet(sub_id, clean_start=True, keep_alive=120))
    raw = recv_packet(s, timeout=5)
    validate_connack(raw)
    s.sendall(create_subscribe_packet(1, f"{topic_prefix}#", qos=1))
    validate_suback(recv_packet(s), 1)
    print(f"  ✓ Subscriber connected and subscribed to {topic_prefix}#")

    # 2. Insert TOTAL rows — each triggers a QoS 1 CDC PUBLISH
    print(f"  ! Inserting {TOTAL} rows into CDC table...")
    # Batch insert instead of one-by-one to speed up the test
    insert_sql = ", ".join([f"('msg-{i}')" for i in range(TOTAL)])
    run_psql(f"INSERT INTO {table} (val) VALUES {insert_sql};")

    # 3. Collect the first MAX_INFLIGHT PUBLISHes (do NOT ack them)
    print(f"  ! Collecting initial {MAX_INFLIGHT} publishes (no ACK)...")
    received_pids = []
    deadline = time.time() + 30
    while len(received_pids) < MAX_INFLIGHT and time.time() < deadline:
        pkt = recv_packet(s, timeout=2.0)
        if pkt is None:
            continue
        ptype = (pkt[0] & 0xF0) >> 4
        if ptype == MQTTControlPacket.PUBLISH:
            _, _, qos, _, _, pid, _ = validate_publish(pkt)
            assert qos == 1, f"Expected QoS 1, got {qos}"
            received_pids.append(pid)

    assert len(received_pids) == MAX_INFLIGHT, (
        f"Expected {MAX_INFLIGHT} initial publishes, got {len(received_pids)}"
    )
    print(f"  ✓ Received initial {MAX_INFLIGHT} publishes (inflight window full)")

    # Give the broker a moment to queue the overflow
    time.sleep(1)

    # 4. Now ACK every received message and collect queued ones
    print(f"  ! Sending PUBACKs and expecting queued messages to flow in...")
    all_received = list(received_pids)

    for pid in received_pids:
        s.sendall(create_puback_packet(pid))
        # The broker should immediately push one queued message per freed slot
        pkt = recv_packet(s, timeout=3.0)
        if pkt is not None:
            ptype = (pkt[0] & 0xF0) >> 4
            if ptype == MQTTControlPacket.PUBLISH:
                _, _, qos, _, _, new_pid, _ = validate_publish(pkt)
                all_received.append(new_pid)
                s.sendall(create_puback_packet(new_pid))

    # Drain any remaining
    remaining_deadline = time.time() + 10
    while time.time() < remaining_deadline:
        pkt = recv_packet(s, timeout=1.0)
        if pkt is None:
            break
        ptype = (pkt[0] & 0xF0) >> 4
        if ptype == MQTTControlPacket.PUBLISH:
            _, _, qos, _, _, pid, _ = validate_publish(pkt)
            all_received.append(pid)
            s.sendall(create_puback_packet(pid))

    print(f"  ! Total received: {len(all_received)} / {TOTAL}")
    assert len(all_received) >= TOTAL, (
        f"Expected {TOTAL} total messages, only received {len(all_received)}. "
        "Messages were dropped instead of queued!"
    )
    print(f"  ✓ All {TOTAL} messages delivered — no drops")
    s.close()


def test_fast_ack_receives_all():
    """
    A subscriber that ACKs immediately should receive all N messages
    without any queuing or drops.
    """
    print("\n[Flow Control] Test 2: fast-ACKing client receives all messages")
    table = "flow_control_test"
    topic_prefix = "test/flow/"
    setup_table(table)

    TOTAL = 100
    sub_id = "fc_fast_subscriber"

    # 1. Connect subscriber
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet(sub_id, clean_start=True, keep_alive=120))
    validate_connack(recv_packet(s))
    s.sendall(create_subscribe_packet(1, f"{topic_prefix}#", qos=1))
    validate_suback(recv_packet(s), 1)
    print(f"  ✓ Subscriber connected")

    # 2. Insert rows
    print(f"  ! Inserting {TOTAL} rows...")
    # Batch insert instead of one-by-one to speed up the test
    insert_sql = ", ".join([f"('fast-msg-{i}')" for i in range(TOTAL)])
    run_psql(f"INSERT INTO {table} (val) VALUES {insert_sql};")

    # 3. Receive and ACK everything
    received = 0
    deadline = time.time() + 30
    while received < TOTAL and time.time() < deadline:
        pkt = recv_packet(s, timeout=3.0)
        if pkt is None:
            continue
        ptype = (pkt[0] & 0xF0) >> 4
        if ptype == MQTTControlPacket.PUBLISH:
            _, _, qos, _, _, pid, _ = validate_publish(pkt)
            assert qos == 1
            s.sendall(create_puback_packet(pid))
            received += 1

    assert received == TOTAL, (
        f"Expected {TOTAL} messages, received {received}"
    )
    print(f"  ✓ Received all {TOTAL} messages with fast ACKs")
    s.close()


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    failures = []
    tests = [
        test_flow_control_queues_not_drops,
        test_fast_ack_receives_all,
    ]
    for t in tests:
        try:
            t()
            print(f"  PASS: {t.__name__}")
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"  FAIL: {t.__name__}: {e}")
            failures.append(t.__name__)

    if failures:
        print(f"\n{len(failures)} test(s) FAILED: {failures}")
        sys.exit(1)
    else:
        print("\nAll flow control tests PASSED")
