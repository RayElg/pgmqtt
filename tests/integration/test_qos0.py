"""
QoS 0 integration tests for pgmqtt.

Tests cover client-to-client publish, CDC delivery, no-redelivery semantics,
multiple subscribers, wildcard subscriptions, and QoS downgrade behavior.
"""
import socket
import time

from proto_utils import (
    create_connect_packet,
    create_subscribe_packet,
    create_publish_packet,
    create_disconnect_packet,
    recv_packet,
    validate_connack,
    validate_suback,
    validate_publish,
    MQTTControlPacket,
    ReasonCode,
    MQTT_HOST,
    MQTT_PORT,
    run_psql,
)


# -- Helpers ------------------------------------------------------------------

def setup_test_table(table_name, topic_pattern):
    """Create a test table and register a topic mapping for CDC."""
    run_psql(f"DROP TABLE IF EXISTS {table_name};")
    run_psql(f"CREATE TABLE {table_name} (id serial PRIMARY KEY, data text);")
    run_psql(f"ALTER TABLE {table_name} REPLICA IDENTITY FULL;")
    run_psql(
        f"SELECT pgmqtt_add_outbound_mapping('public', '{table_name}', "
        f"'{topic_pattern}', "
        f"'{{{{ columns | tojson }}}}');"
    )
    time.sleep(6)


def cdc_publish(table_name, data_value):
    """INSERT a row to trigger CDC delivery."""
    run_psql(f"INSERT INTO {table_name} (data) VALUES ('{data_value}');")


def mqtt_connect(client_id, clean_start=True, session_expiry=0):
    """Open TCP socket, send CONNECT, return (socket, session_present)."""
    props = {}
    if session_expiry > 0:
        props[0x11] = session_expiry
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet(
        client_id,
        clean_start=clean_start,
        properties=props if props else None,
    ))
    raw = recv_packet(s, timeout=5)
    assert raw is not None, f"[{client_id}] No CONNACK received"
    sp, rc, _ = validate_connack(raw)
    assert rc == 0, f"[{client_id}] CONNACK reason_code={rc:#04x}"
    return s, sp


# -- Tests --------------------------------------------------------------------

def test_qos0_client_publish():
    """Publisher sends QoS 0 via MQTT, subscriber receives it. No PUBACK."""
    # Connect subscriber
    s_sub, _ = mqtt_connect("qos0_cp_sub", clean_start=True)
    s_sub.sendall(create_subscribe_packet(1, "test/qos0/clientpub", qos=0))
    suback = recv_packet(s_sub)
    reason_codes = validate_suback(suback, 1)
    assert reason_codes[0] == ReasonCode.GRANTED_QOS_0

    # Connect publisher
    s_pub, _ = mqtt_connect("qos0_cp_pub", clean_start=True)
    s_pub.sendall(create_publish_packet("test/qos0/clientpub", b"hello_qos0", qos=0))

    # Subscriber receives
    pub_pkt = recv_packet(s_sub, timeout=5)
    assert pub_pkt is not None, "Subscriber did not receive PUBLISH"
    topic, payload, qos, dup, retain, packet_id, _ = validate_publish(pub_pkt)
    assert qos == 0, f"Expected QoS 0, got {qos}"
    assert packet_id is None, "QoS 0 must not have a packet ID"
    assert not dup, "DUP must be 0 for QoS 0"

    # No extra data
    extra = recv_packet(s_sub, timeout=1.0)
    assert extra is None, f"Should not receive extra data after QoS 0, got: {extra}"

    s_pub.close()
    s_sub.close()


def test_qos0_cdc_delivery():
    """CDC INSERT triggers QoS 0 delivery to a subscriber."""
    table = "test_qos0_cdc"
    setup_test_table(table, "qos0/cdc/{{ op | lower }}")

    s_sub, _ = mqtt_connect("qos0_cdc_consumer", clean_start=True)
    s_sub.sendall(create_subscribe_packet(1, "qos0/cdc/#", qos=0))
    suback = recv_packet(s_sub)
    reason_codes = validate_suback(suback, 1)
    assert reason_codes[0] == ReasonCode.GRANTED_QOS_0

    time.sleep(0.5)
    cdc_publish(table, "hello_qos0")

    pub_pkt = recv_packet(s_sub, timeout=10)
    assert pub_pkt is not None, "No PUBLISH received after INSERT"
    topic, payload, qos, dup, retain, packet_id, _ = validate_publish(pub_pkt)
    assert "qos0/cdc/" in topic, f"Topic mismatch: {topic}"
    assert qos == 0, f"Expected QoS 0, got {qos}"
    assert not dup, "DUP must be 0 for QoS 0"
    assert packet_id is None, "Packet ID must be None for QoS 0"

    extra = recv_packet(s_sub, timeout=1.0)
    assert extra is None, f"Should not receive extra data after QoS 0, got: {extra}"

    s_sub.close()


def test_qos0_no_redelivery_client_pub():
    """QoS 0 messages are not redelivered after disconnect (client publish path)."""
    client_id = "qos0_noredeliver_cp"
    topic = "test/qos0/noredeliver_cp"

    # Connect with persistent session, subscribe, disconnect
    s_sub, _ = mqtt_connect(client_id, clean_start=True, session_expiry=3600)
    s_sub.sendall(create_subscribe_packet(1, topic, qos=0))
    recv_packet(s_sub)
    s_sub.sendall(create_disconnect_packet())
    s_sub.close()
    time.sleep(0.5)

    # Publish while subscriber is offline
    s_pub, _ = mqtt_connect("qos0_noredeliver_cp_pub", clean_start=True)
    s_pub.sendall(create_publish_packet(topic, b"missed_msg", qos=0))
    s_pub.close()
    time.sleep(1)

    # Reconnect with clean_start=False
    s_sub2, _ = mqtt_connect(client_id, clean_start=False, session_expiry=3600)

    # Should NOT receive the QoS 0 message
    pub = recv_packet(s_sub2, timeout=2.0)
    assert pub is None, f"QoS 0 should NOT be redelivered, got: {pub}"

    s_sub2.close()


def test_qos0_no_redelivery_cdc():
    """QoS 0 CDC messages are not redelivered after disconnect."""
    table = "test_qos0_noredeliver"
    topic_filter = "qos0/noredeliver/#"
    client_id = "qos0_offline_cdc_consumer"

    setup_test_table(table, "qos0/noredeliver/{{ op | lower }}")

    # Connect, subscribe, disconnect
    s_sub, _ = mqtt_connect(client_id, clean_start=True)
    s_sub.sendall(create_subscribe_packet(5, topic_filter, qos=0))
    recv_packet(s_sub)
    s_sub.close()

    # INSERT while offline
    time.sleep(0.5)
    cdc_publish(table, "missed_while_offline")
    time.sleep(1)

    # Reconnect -- should NOT receive
    s_sub2, _ = mqtt_connect(client_id, clean_start=True)
    s_sub2.sendall(create_subscribe_packet(6, topic_filter, qos=0))
    recv_packet(s_sub2)

    pub = recv_packet(s_sub2, timeout=2.0)
    assert pub is None, f"QoS 0 should NOT be redelivered, got: {pub}"

    s_sub2.close()


def test_qos0_multiple_subscribers():
    """Multiple subscribers all receive the same QoS 0 CDC message."""
    table = "test_qos0_multi"
    topic_filter = "qos0/multi/#"

    setup_test_table(table, "qos0/multi/{{ op | lower }}")

    sockets = []
    try:
        for i in range(2):
            s, _ = mqtt_connect(f"qos0_multi_sub_{i}", clean_start=True)
            s.sendall(create_subscribe_packet(i + 1, topic_filter, qos=0))
            recv_packet(s)
            sockets.append(s)

        time.sleep(0.5)
        cdc_publish(table, "multi_test")

        for i, s in enumerate(sockets):
            pub = recv_packet(s, timeout=10)
            assert pub is not None, f"Subscriber {i} did not receive PUBLISH"
            topic, _, qos, _, _, packet_id, _ = validate_publish(pub)
            assert qos == 0, f"Subscriber {i}: Expected QoS 0, got {qos}"
            assert packet_id is None, f"Subscriber {i}: QoS 0 should have no packet ID"
    finally:
        for s in sockets:
            s.close()


def test_qos0_wildcard_subscription():
    """Wildcard subscription receives QoS 0 CDC messages."""
    table = "test_qos0_wild"

    setup_test_table(table, "qos0/wild/{{ op | lower }}")

    s_sub, _ = mqtt_connect("qos0_wild_consumer", clean_start=True)
    s_sub.sendall(create_subscribe_packet(1, "qos0/wild/+", qos=0))
    suback = recv_packet(s_sub)
    reason_codes = validate_suback(suback, 1)
    assert reason_codes[0] == ReasonCode.GRANTED_QOS_0

    time.sleep(0.5)
    cdc_publish(table, "wildcard_test")

    pub = recv_packet(s_sub, timeout=10)
    assert pub is not None, "No PUBLISH for wildcard subscription"
    topic, _, qos, _, _, packet_id, _ = validate_publish(pub)
    assert topic == "qos0/wild/insert", f"Topic mismatch: {topic}"
    assert qos == 0
    assert packet_id is None

    s_sub.close()


def test_qos0_downgrade_from_qos1():
    """Subscribe QoS 0, publish QoS 1 -- subscriber receives as QoS 0."""
    # Connect subscriber with QoS 0 subscription
    s_sub, _ = mqtt_connect("qos0_downgrade_sub", clean_start=True)
    s_sub.sendall(create_subscribe_packet(1, "test/qos0/downgrade", qos=0))
    suback = recv_packet(s_sub)
    reason_codes = validate_suback(suback, 1)
    assert reason_codes[0] == ReasonCode.GRANTED_QOS_0

    # Connect publisher, send QoS 1
    s_pub, _ = mqtt_connect("qos0_downgrade_pub", clean_start=True)
    s_pub.sendall(create_publish_packet(
        "test/qos0/downgrade", b"downgrade_payload", qos=1, packet_id=100
    ))
    # Publisher gets PUBACK
    puback = recv_packet(s_pub, timeout=5)
    assert puback is not None, "Publisher did not receive PUBACK"

    # Subscriber should receive as QoS 0
    pub_pkt = recv_packet(s_sub, timeout=5)
    assert pub_pkt is not None, "Subscriber did not receive PUBLISH"
    topic, payload, qos, dup, retain, packet_id, _ = validate_publish(pub_pkt)
    assert qos == 0, f"Expected downgraded QoS 0, got {qos}"
    assert packet_id is None, "QoS 0 must not have a packet ID"

    s_pub.close()
    s_sub.close()


def test_qos0_no_upgrade_after_disconnect():
    """QoS 0 is not redelivered after ungraceful disconnect (subscribe QoS 1, publish QoS 0)."""
    client_id = "qos0_no_upgrade_sub"
    topic = "test/qos0/no_upgrade"

    # Connect subscriber with QoS 1 subscription and persistent session
    s_sub, _ = mqtt_connect(client_id, clean_start=True, session_expiry=3600)
    s_sub.sendall(create_subscribe_packet(1, topic, qos=1))
    recv_packet(s_sub)

    # Publisher sends QoS 0
    s_pub, _ = mqtt_connect("qos0_no_upgrade_pub", clean_start=True)
    s_pub.sendall(create_publish_packet(topic, b"this is qos 0", qos=0))
    s_pub.close()

    time.sleep(0.5)
    msg = recv_packet(s_sub, timeout=5)
    assert msg is not None, "Subscriber did not receive message"

    # Ungraceful disconnect (no DISCONNECT packet)
    s_sub.close()
    time.sleep(3)

    # Reconnect with clean_start=False
    s_sub2, _ = mqtt_connect(client_id, clean_start=False, session_expiry=3600)

    # Should NOT receive redelivery -- QoS 0 is at-most-once
    redelivered = recv_packet(s_sub2, timeout=2.0)
    assert redelivered is None, f"QoS 0 message was redelivered after ungraceful disconnect"

    s_sub2.close()
