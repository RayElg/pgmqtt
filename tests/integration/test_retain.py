"""
MQTT 5.0 Retained Message Tests.
"""

import socket

from proto_utils import (
    create_connect_packet,
    create_subscribe_packet,
    create_publish_packet,
    create_disconnect_packet,
    recv_packet,
    validate_connack,
    validate_suback,
    validate_publish,
    ReasonCode,
    MQTT_HOST,
    MQTT_PORT,
)


def test_receive_retained_on_subscribe():
    """Subscriber receives retained message on subscribe with RETAIN=1."""
    topic = "test/retain/receive"
    payload = b"retained message"

    # Publish retained message
    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("retain_pub"))
    validate_connack(recv_packet(s_pub))
    s_pub.sendall(create_publish_packet(topic, payload, qos=1, packet_id=1, retain=True))
    recv_packet(s_pub)  # PUBACK
    s_pub.sendall(create_disconnect_packet())
    s_pub.close()

    # Subscribe — should receive retained
    s_cons = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_cons.connect((MQTT_HOST, MQTT_PORT))
    s_cons.sendall(create_connect_packet("retain_cons", clean_start=True))
    validate_connack(recv_packet(s_cons))
    s_cons.sendall(create_subscribe_packet(10, topic, qos=1))
    validate_suback(recv_packet(s_cons), 10)

    pub = recv_packet(s_cons)
    assert pub is not None, "Should receive retained message"
    t, p, qos, dup, retain, pid, props = validate_publish(pub)
    assert t == topic
    assert p == payload
    assert retain, "RETAIN flag should be 1"
    s_cons.close()

    # Cleanup
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("retain_cleaner"))
    recv_packet(s)
    s.sendall(create_publish_packet(topic, b"", qos=0, retain=True))
    s.close()


def test_clear_retained_with_empty_payload():
    """Empty payload with RETAIN=1 clears retained message."""
    topic = "test/retain/clear"

    # Set retained — QoS 1 so we can wait for PUBACK before proceeding
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("retain_setter"))
    recv_packet(s)
    s.sendall(create_publish_packet(topic, b"to be cleared", qos=1, packet_id=1, retain=True))
    recv_packet(s)  # PUBACK — retained message is now committed
    s.close()

    # Clear with empty payload — QoS 1 so we can wait for PUBACK before subscribing
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("retain_clearer"))
    recv_packet(s)
    s.sendall(create_publish_packet(topic, b"", qos=1, packet_id=2, retain=True))
    recv_packet(s)  # PUBACK — delete from pgmqtt_retained is now committed
    s.close()

    # Subscriber should NOT receive retained
    s_cons = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_cons.connect((MQTT_HOST, MQTT_PORT))
    s_cons.sendall(create_connect_packet("retain_checker", clean_start=True))
    recv_packet(s_cons)
    s_cons.sendall(create_subscribe_packet(11, topic, qos=0))
    recv_packet(s_cons)

    pub = recv_packet(s_cons, timeout=1.0)
    assert pub is None, "Should not receive after clearing retained"
    s_cons.close()


def test_retain_flag_zero_on_live_publish():
    """Live (non-retained) publish forwarded with RETAIN=0."""
    topic = "test/retain/live"
    payload = b"live message"

    s_cons = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_cons.connect((MQTT_HOST, MQTT_PORT))
    s_cons.sendall(create_connect_packet("retain_live_cons", clean_start=True))
    recv_packet(s_cons)
    s_cons.sendall(create_subscribe_packet(12, topic, qos=0))
    recv_packet(s_cons)

    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("retain_live_pub"))
    recv_packet(s_pub)
    s_pub.sendall(create_publish_packet(topic, payload, qos=0, retain=False))
    s_pub.close()

    pub = recv_packet(s_cons)
    assert pub is not None
    _, p, _, _, retain, _, _ = validate_publish(pub)
    assert p == payload
    assert not retain, "RETAIN should be 0 for live message"
    s_cons.close()
