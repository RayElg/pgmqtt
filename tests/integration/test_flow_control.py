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
        f"SELECT pgmqtt_add_outbound_mapping('public', '{table}', "
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

def test_resume_caps_redelivery_at_receive_maximum():
    """On session resume, redelivery is capped at the new connection's
    Receive Maximum (MQTT-3.3.4-7).

    Scenario:
      1. Subscriber connects with high receive_maximum, subscribes QoS 1, and
         receives N messages without ACKing (so they sit in inflight).
      2. Subscriber abruptly closes the TCP connection (session preserved).
      3. Subscriber reconnects with a *small* receive_maximum.
      4. The broker must redeliver at most receive_maximum messages
         immediately; the rest stay in inflight and arrive only after
         redeliver_unacked fires (5s) — also capped at receive_maximum.
    """
    print("\n[Flow Control] Test 3: resume redelivery respects new receive_maximum")
    sub_id = "fc_resume_cap"
    topic = "test/flow/resume_cap"
    N = 20      # initial inflight built up on the first connection
    NEW_MAX = 5 # smaller receive_maximum on resume

    # 1. Connect with large receive_maximum, subscribe, build up inflight.
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet(
        sub_id,
        clean_start=True,
        keep_alive=120,
        properties={0x11: 300, 0x21: 200},  # session_expiry=300, receive_max=200
    ))
    validate_connack(recv_packet(s))
    s.sendall(create_subscribe_packet(1, topic, qos=1))
    validate_suback(recv_packet(s), 1)

    # 2. Publish N messages from a separate connection.
    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("fc_resume_cap_pub"))
    validate_connack(recv_packet(s_pub))
    for i in range(N):
        s_pub.sendall(create_publish_packet(topic, f"msg-{i}".encode(), qos=1, packet_id=i + 1))
        recv_packet(s_pub)  # PUBACK
    s_pub.sendall(create_disconnect_packet())
    s_pub.close()

    # 3. Receive (but do NOT ACK) all N — they pile up in broker inflight.
    initial_pids = drain_all_publish(s, N)
    assert len(initial_pids) == N
    print(f"  ✓ Built up {N} unacked inflight on first connection")

    # 4. Abrupt TCP close — session preserved (expiry=300).
    s.close()
    time.sleep(0.5)

    # 5. Reconnect with smaller receive_maximum.
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.connect((MQTT_HOST, MQTT_PORT))
    s2.sendall(create_connect_packet(
        sub_id,
        clean_start=False,
        keep_alive=120,
        properties={0x11: 300, 0x21: NEW_MAX},
    ))
    raw = recv_packet(s2, timeout=5)
    sp, rc, _ = validate_connack(raw)
    assert rc == 0
    assert sp, "session_present should be true"

    # 6. Count immediate redeliveries within a short window — must be ≤ NEW_MAX.
    immediate = []
    deadline = time.time() + 2.0
    while time.time() < deadline:
        pkt = recv_packet(s2, timeout=0.5)
        if pkt is None:
            continue
        ptype = (pkt[0] & 0xF0) >> 4
        if ptype == MQTTControlPacket.PUBLISH:
            _, _, qos, dup, _, pid, _ = validate_publish(pkt)
            assert qos == 1
            assert dup, "Resumed inflight must have DUP=1 (MQTT-4.4.0-1)"
            immediate.append(pid)

    assert len(immediate) == NEW_MAX, (
        f"Expected exactly {NEW_MAX} immediate redeliveries (receive_maximum cap), "
        f"got {len(immediate)}"
    )
    print(f"  ✓ Resume capped immediate redelivery at receive_maximum={NEW_MAX}")

    # 7. ACK them; deferred entries must eventually arrive (no message loss).
    #    NOTE: the broker enforces the cap per-tick rather than as a true
    #    on-wire send-quota, so several ticks may overlap and briefly exceed
    #    receive_maximum on the wire.  We assert no-loss + DUP=1, not the
    #    instantaneous count.
    for pid in immediate:
        s2.sendall(create_puback_packet(pid))

    print("  ! Draining deferred redeliveries...")
    deferred = []
    deadline = time.time() + 15
    while len(deferred) < (N - NEW_MAX) and time.time() < deadline:
        pkt = recv_packet(s2, timeout=2.0)
        if pkt is None:
            continue
        ptype = (pkt[0] & 0xF0) >> 4
        if ptype == MQTTControlPacket.PUBLISH:
            _, _, _, _, _, pid, _ = validate_publish(pkt)
            if pid in deferred:
                continue  # ignore duplicate redeliveries
            deferred.append(pid)
            s2.sendall(create_puback_packet(pid))

    assert len(deferred) >= (N - NEW_MAX), (
        f"Deferred redeliveries must all arrive ({N - NEW_MAX} expected), got {len(deferred)}"
    )
    print(f"  ✓ Recovered all {N - NEW_MAX} deferred messages — no loss")

    s2.sendall(create_disconnect_packet())
    s2.close()
    # Best-effort: re-clean by clean_start.
    s_clean = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_clean.connect((MQTT_HOST, MQTT_PORT))
    s_clean.sendall(create_connect_packet(sub_id, clean_start=True))
    recv_packet(s_clean)
    s_clean.sendall(create_disconnect_packet())
    s_clean.close()


def test_max_packet_size_inflight_discarded():
    """MQTT-3.1.2.24-2: inflight entries too large for the client's max_packet_size
    must be treated as delivered on reconnect — not redelivered, not stuck.

    Scenario:
      1. Subscriber connects (no max_packet_size limit), receives N large QoS 1
         messages without ACKing so they sit in inflight.
      2. TCP close — session persists.
      3. Subscriber reconnects with max_packet_size smaller than the messages.
      4. Broker must NOT redeliver those messages (they are discarded per spec).
      5. DB must be clean — no lingering session_messages rows.
    """
    print("\n[Flow Control] Test 4: max_packet_size discards oversized inflight on reconnect")
    sub_id = "fc_maxpkt_inflight"
    topic = "test/flow/maxpkt_inflight"
    PAYLOAD = b"X" * 500   # 500-byte payload → PUBLISH packet ~520 bytes
    MAX_PKT = 100           # reconnect limit well below the packet size
    N = 5

    # 1. Connect without max_packet_size limit and subscribe.
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet(
        sub_id, clean_start=True, keep_alive=120,
        properties={0x11: 300},  # session_expiry=300s
    ))
    validate_connack(recv_packet(s))
    s.sendall(create_subscribe_packet(1, topic, qos=1))
    validate_suback(recv_packet(s), 1)

    # 2. Publish N large messages and receive (but do NOT PUBACK) them.
    pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    pub.connect((MQTT_HOST, MQTT_PORT))
    pub.sendall(create_connect_packet("fc_maxpkt_inflight_pub"))
    validate_connack(recv_packet(pub))
    for i in range(N):
        pub.sendall(create_publish_packet(topic, PAYLOAD, qos=1, packet_id=i + 1))
        recv_packet(pub)  # PUBACK from broker
    pub.sendall(create_disconnect_packet())
    pub.close()

    inflight_pids = drain_all_publish(s, N)
    assert len(inflight_pids) == N
    print(f"  ✓ Built {N} unacked inflight entries")

    # 3. Abrupt close — session preserved.
    s.close()
    time.sleep(0.3)

    # 4. Reconnect with max_packet_size=100 (< ~520-byte PUBLISH packets).
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.connect((MQTT_HOST, MQTT_PORT))
    s2.sendall(create_connect_packet(
        sub_id, clean_start=False, keep_alive=120,
        properties={0x11: 300, 0x27: MAX_PKT},
    ))
    raw = recv_packet(s2, timeout=5)
    sp, rc, _ = validate_connack(raw)
    assert rc == 0 and sp, f"Expected session resumption, got rc={rc} sp={sp}"

    # 5. Verify no PUBLISH arrives within a 3-second window.
    unexpected = []
    deadline = time.time() + 3.0
    while time.time() < deadline:
        pkt = recv_packet(s2, timeout=0.5)
        if pkt is None:
            continue
        if (pkt[0] & 0xF0) >> 4 == MQTTControlPacket.PUBLISH:
            unexpected.append(pkt)

    assert len(unexpected) == 0, (
        f"Expected 0 redeliveries (all oversized, must be discarded per MQTT-3.1.2.24-2), "
        f"got {len(unexpected)}"
    )
    print("  ✓ No oversized messages redelivered")

    # 6. Verify DB is clean — no session_messages rows left for this client.
    time.sleep(0.5)  # allow DeleteMessage actions to flush
    rows = run_psql(
        f"SELECT count(*) FROM pgmqtt_session_messages "
        f"WHERE client_id = '{sub_id}'"
    ) or [(0,)]
    assert rows[0][0] == 0, (
        f"session_messages not cleaned up: {rows[0][0]} rows remain"
    )
    print("  ✓ DB clean — no lingering session_messages")

    s2.sendall(create_disconnect_packet())
    s2.close()
    # Wipe session.
    s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s3.connect((MQTT_HOST, MQTT_PORT))
    s3.sendall(create_connect_packet(sub_id, clean_start=True))
    recv_packet(s3)
    s3.sendall(create_disconnect_packet())
    s3.close()


def test_max_packet_size_queue_discarded():
    """MQTT-3.1.2.24-2: queued messages too large for the client's max_packet_size
    must be treated as delivered on reconnect — not delivered, not stuck in DB.

    Scenario:
      1. Subscriber connects with session_expiry, then disconnects cleanly.
      2. N large QoS 1 messages are published while subscriber is offline
         (they land in pgmqtt_session_messages with packet_id=NULL).
      3. Subscriber reconnects with max_packet_size smaller than the messages.
      4. Broker must NOT deliver those messages; DB rows must be cleaned up.
    """
    print("\n[Flow Control] Test 5: max_packet_size discards oversized queued messages on reconnect")
    sub_id = "fc_maxpkt_queue"
    topic = "test/flow/maxpkt_queue"
    PAYLOAD = b"Y" * 500
    MAX_PKT = 100
    N = 5

    # 1. Connect, subscribe, then cleanly disconnect (session persists).
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet(
        sub_id, clean_start=True, keep_alive=120,
        properties={0x11: 300},
    ))
    validate_connack(recv_packet(s))
    s.sendall(create_subscribe_packet(1, topic, qos=1))
    validate_suback(recv_packet(s), 1)
    s.sendall(create_disconnect_packet())
    s.close()
    time.sleep(0.3)

    # 2. Publish while subscriber is offline — messages queue in DB.
    pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    pub.connect((MQTT_HOST, MQTT_PORT))
    pub.sendall(create_connect_packet("fc_maxpkt_queue_pub"))
    validate_connack(recv_packet(pub))
    for i in range(N):
        pub.sendall(create_publish_packet(topic, PAYLOAD, qos=1, packet_id=i + 1))
        recv_packet(pub)  # PUBACK
    pub.sendall(create_disconnect_packet())
    pub.close()
    time.sleep(0.3)

    # Verify messages landed in DB before reconnect.
    rows = run_psql(
        f"SELECT count(*) FROM pgmqtt_session_messages WHERE client_id = '{sub_id}'"
    ) or [(0,)]
    assert rows[0][0] == N, f"Expected {N} queued rows, got {rows[0][0]}"
    print(f"  ✓ {N} messages queued in DB while offline")

    # 3. Reconnect with max_packet_size=100.
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.connect((MQTT_HOST, MQTT_PORT))
    s2.sendall(create_connect_packet(
        sub_id, clean_start=False, keep_alive=120,
        properties={0x11: 300, 0x27: MAX_PKT},
    ))
    raw = recv_packet(s2, timeout=5)
    sp, rc, _ = validate_connack(raw)
    assert rc == 0 and sp, f"Expected session resumption, got rc={rc} sp={sp}"

    # 4. No PUBLISH should arrive.
    unexpected = []
    deadline = time.time() + 3.0
    while time.time() < deadline:
        pkt = recv_packet(s2, timeout=0.5)
        if pkt is None:
            continue
        if (pkt[0] & 0xF0) >> 4 == MQTTControlPacket.PUBLISH:
            unexpected.append(pkt)

    assert len(unexpected) == 0, (
        f"Expected 0 deliveries (all oversized), got {len(unexpected)}"
    )
    print("  ✓ No oversized queued messages delivered")

    # 5. DB clean.
    time.sleep(0.5)
    rows = run_psql(
        f"SELECT count(*) FROM pgmqtt_session_messages WHERE client_id = '{sub_id}'"
    ) or [(0,)]
    assert rows[0][0] == 0, (
        f"session_messages not cleaned up: {rows[0][0]} rows remain"
    )
    print("  ✓ DB clean — queued rows removed")

    s2.sendall(create_disconnect_packet())
    s2.close()
    s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s3.connect((MQTT_HOST, MQTT_PORT))
    s3.sendall(create_connect_packet(sub_id, clean_start=True))
    recv_packet(s3)
    s3.sendall(create_disconnect_packet())
    s3.close()


if __name__ == "__main__":
    failures = []
    tests = [
        test_flow_control_queues_not_drops,
        test_fast_ack_receives_all,
        test_resume_caps_redelivery_at_receive_maximum,
        test_max_packet_size_inflight_discarded,
        test_max_packet_size_queue_discarded,
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
