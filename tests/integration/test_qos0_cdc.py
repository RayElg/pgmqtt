#!/usr/bin/env python3
"""
MQTT 5.0 QoS 0 Conformance Tests — adapted for pgmqtt

In pgmqtt, "PUBLISH" originates from CDC (database INSERT/UPDATE/DELETE).
Tests verify:
- QoS 0 message delivery from CDC to subscriber
- At-most-once semantics (no redelivery after disconnect)
- Correct fixed header flags (DUP=0, QoS=0, no packet ID)
- No PUBACK sent for QoS 0
"""
import socket
import sys
import time

from proto_utils import (
    ReasonCode,
    create_connect_packet, 
    create_subscribe_packet,
    recv_packet,
    validate_connack, 
    validate_suback, 
    validate_publish,
    run_psql,
    MQTT_HOST,
    MQTT_PORT
)

# ── Helpers ──────────────────────────────────────────────────────────────────

def setup_test_table(table_name, topic_pattern):
    """Create a test table and register a topic mapping."""
    run_psql(f"DROP TABLE IF EXISTS {table_name};")
    run_psql(f"CREATE TABLE {table_name} (id serial PRIMARY KEY, data text);")
    run_psql(f"ALTER TABLE {table_name} REPLICA IDENTITY FULL;")
    run_psql(
        f"SELECT pgmqtt_add_mapping('public', '{table_name}', "
        f"'{topic_pattern}', "
        f"'{{{{ columns | tojson }}}}');"
    )

def cdc_publish(table_name, data_value):
    """INSERT a row to trigger CDC → MQTT delivery."""
    run_psql(f"INSERT INTO {table_name} (data) VALUES ('{data_value}');")

# ── Tests ────────────────────────────────────────────────────────────────────

def test_qos0_cdc_delivery():
    """
    Test: Basic QoS 0 message delivery via CDC.
    """
    host, port = MQTT_HOST, MQTT_PORT
    table = "test_qos0_cdc"
    topic_filter = "qos0/cdc/#"

    setup_test_table(table, "qos0/cdc/{{ op | lower }}")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))

        # Connect consumer
        s_cons.sendall(create_connect_packet("qos0_cdc_consumer", clean_start=True))
        connack = recv_packet(s_cons)
        _, reason_code, _ = validate_connack(connack)
        assert reason_code == ReasonCode.SUCCESS, f"CONNACK failed: {reason_code}"

        # Subscribe
        s_cons.sendall(create_subscribe_packet(1, topic_filter, qos=0))
        suback = recv_packet(s_cons)
        reason_codes = validate_suback(suback, 1)
        assert reason_codes[0] == ReasonCode.GRANTED_QOS_0, \
            f"Expected QoS 0 grant, got {reason_codes}"

        # Trigger CDC by inserting a row
        time.sleep(0.5)
        cdc_publish(table, "hello_qos0")

        # Consumer should receive PUBLISH
        publish_packet = recv_packet(s_cons, timeout=10)
        assert publish_packet is not None, "No PUBLISH received after INSERT"

        recv_topic, recv_payload, qos, dup, _, packet_id, _ = \
            validate_publish(publish_packet)

        assert "qos0/cdc/" in recv_topic, f"Topic mismatch: {recv_topic}"
        assert qos == 0, f"Expected QoS 0, got {qos}"
        assert not dup, "DUP must be 0 for QoS 0"
        assert packet_id is None, "Packet ID must be None for QoS 0"

        print(f"✓ QoS 0 CDC delivery: topic='{recv_topic}' payload={recv_payload}")

        # Ensure no extra packets
        extra = recv_packet(s_cons, timeout=1.0)
        assert extra is None, f"Should not receive extra data after QoS 0, got: {extra}"

    print("✓ test_qos0_cdc_delivery passed")


def test_qos0_no_redelivery_cdc():
    """
    Test: QoS 0 messages are NOT redelivered after disconnect.
    """
    host, port = MQTT_HOST, MQTT_PORT
    table = "test_qos0_noredeliver"
    topic_filter = "qos0/noredeliver/#"
    client_id = "qos0_offline_cdc_consumer"

    setup_test_table(table, "qos0/noredeliver/{{ op | lower }}")

    # 1. Consumer connects, subscribes, then disconnects
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet(client_id, clean_start=True))
        recv_packet(s_cons)
        s_cons.sendall(create_subscribe_packet(5, topic_filter, qos=0))
        recv_packet(s_cons)

    # 2. INSERT while consumer is offline
    time.sleep(0.5)
    cdc_publish(table, "missed_while_offline")
    time.sleep(1)

    # 3. Consumer reconnects — should NOT receive the QoS 0 message
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet(client_id, clean_start=True))
        recv_packet(s_cons)

        # Re-subscribe
        s_cons.sendall(create_subscribe_packet(6, topic_filter, qos=0))
        recv_packet(s_cons)

        # Should not receive the old QoS 0 message
        pub = recv_packet(s_cons, timeout=2.0)
        assert pub is None, f"QoS 0 should NOT be redelivered, got: {pub}"

    print("✓ test_qos0_no_redelivery_cdc passed")


def test_qos0_multiple_subscribers():
    """
    Test: Multiple subscribers all receive the same QoS 0 message.
    """
    host, port = MQTT_HOST, MQTT_PORT
    table = "test_qos0_multi"
    topic_filter = "qos0/multi/#"

    setup_test_table(table, "qos0/multi/{{ op | lower }}")

    sockets = []
    try:
        for i in range(2):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host, port))
            s.sendall(create_connect_packet(f"qos0_multi_sub_{i}", clean_start=True))
            recv_packet(s)
            s.sendall(create_subscribe_packet(i + 1, topic_filter, qos=0))
            recv_packet(s)
            sockets.append(s)

        # Trigger CDC
        time.sleep(0.5)
        cdc_publish(table, "multi_test")

        # Both subscribers should receive
        for i, s in enumerate(sockets):
            pub = recv_packet(s, timeout=10)
            assert pub is not None, f"Subscriber {i} did not receive PUBLISH"
            recv_topic, _, qos, _, _, packet_id, _ = \
                validate_publish(pub)
            assert qos == 0, f"Subscriber {i}: Expected QoS 0, got {qos}"
            assert packet_id is None, f"Subscriber {i}: QoS 0 should have no packet ID"
            print(f"  ✓ Subscriber {i} received: topic='{recv_topic}'")

    finally:
        for s in sockets:
            s.close()

    print("✓ test_qos0_multiple_subscribers passed")


def test_qos0_wildcard_subscription():
    """
    Test: Wildcard subscription receives CDC messages.
    """
    host, port = MQTT_HOST, MQTT_PORT
    table = "test_qos0_wild"

    setup_test_table(table, "qos0/wild/{{ op | lower }}")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet("qos0_wild_consumer", clean_start=True))
        recv_packet(s_cons)

        # Subscribe with single-level wildcard
        s_cons.sendall(create_subscribe_packet(1, "qos0/wild/+", qos=0))
        suback = recv_packet(s_cons)
        reason_codes = validate_suback(suback, 1)
        assert reason_codes[0] == ReasonCode.GRANTED_QOS_0

        # INSERT triggers topic "qos0/wild/insert"
        time.sleep(0.5)
        cdc_publish(table, "wildcard_test")

        pub = recv_packet(s_cons, timeout=10)
        assert pub is not None, "No PUBLISH for wildcard subscription"
        recv_topic, _, qos, _, _, packet_id, _ = \
            validate_publish(pub)

        assert recv_topic == "qos0/wild/insert", f"Topic mismatch: {recv_topic}"
        assert qos == 0
        assert packet_id is None

        print(f"✓ Wildcard subscription received: topic='{recv_topic}'")

    print("✓ test_qos0_wildcard_subscription passed")


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== pgmqtt QoS 0 Conformance Tests (CDC mode) ===\n")
    tests = [
        test_qos0_cdc_delivery,
        test_qos0_no_redelivery_cdc,
        test_qos0_multiple_subscribers,
        test_qos0_wildcard_subscription,
    ]
    passed_count = 0
    failed_count = 0
    for t in tests:
        try:
            t()
            passed_count += 1
        except Exception as e:
            print(f"✗ {t.__name__} FAILED: {e}")
            failed_count += 1
    print(f"\n=== Results: {passed_count} passed, {failed_count} failed ===")
    sys.exit(1 if failed_count > 0 else 0)
