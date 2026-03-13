"""
MQTT 5.0 QoS 1 Tests.
"""

import socket
import pytest

from proto_utils import (
    create_connect_packet,
    create_subscribe_packet,
    create_publish_packet,
    create_puback_packet,
    create_disconnect_packet,
    recv_packet,
    validate_publish,
    validate_connack,
    validate_suback,
    MQTT_HOST,
    MQTT_PORT,
)

@pytest.mark.xfail(reason="Partial acknowledgments not fully tracked during reconnect")
def test_qos1_partial_acknowledgments():
    """Test that a subset of acks is tracked individually during reconnect."""
    # Connect subscriber
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("qos1_partial", clean_start=True))
    validate_connack(recv_packet(s))

    s.sendall(create_subscribe_packet(1, "test/qos1/partial", qos=1))
    validate_suback(recv_packet(s), 1)

    # Publisher
    pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    pub.connect((MQTT_HOST, MQTT_PORT))
    pub.sendall(create_connect_packet("qos1_partial_pub", clean_start=True))
    validate_connack(recv_packet(pub))

    # Publish two messages
    pub.sendall(create_publish_packet("test/qos1/partial", b"msg1", qos=1, packet_id=101))
    recv_packet(pub)  # PUBACK
    pub.sendall(create_publish_packet("test/qos1/partial", b"msg2", qos=1, packet_id=102))
    recv_packet(pub)  # PUBACK
    pub.close()

    # Subscriber receives msg1 and msg2
    pkt1 = recv_packet(s)
    pkt2 = recv_packet(s)
    
    topic1, payload1, qos1, dup1, retain1, pid1, props1 = validate_publish(pkt1)
    topic2, payload2, qos2, dup2, retain2, pid2, props2 = validate_publish(pkt2)

    # Acknowledge only the first one
    s.sendall(create_puback_packet(pid1))

    # Disconnect and reconnect subscriber
    s.close()

    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.connect((MQTT_HOST, MQTT_PORT))
    s2.sendall(create_connect_packet("qos1_partial", clean_start=False))
    validate_connack(recv_packet(s2))

    # Receive remaining unacknowledged message (should be msg2, i.e., pid2)
    pkt3 = recv_packet(s2)
    topic3, payload3, qos3, dup3, retain3, pid3, props3 = validate_publish(pkt3)
    
    assert payload3 == payload2, "Expected unacknowledged message to be redelivered"

    # Make sure we don't receive msg1 again
    s2.settimeout(1.0)
    pkt4 = recv_packet(s2)
    assert pkt4 is None, "Should not receive acknowledged message"
    s2.close()


def test_qos_downgrade_2_to_1():
    """Test QoS downgrade behavior (e.g., publisher sends QoS 2, max sub is QoS 1)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("qos1_downgrade", clean_start=True))
    validate_connack(recv_packet(s))

    s.sendall(create_subscribe_packet(1, "test/qos/downgrade", qos=1))
    validate_suback(recv_packet(s), 1)

    # Publisher with QoS 2
    pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    pub.connect((MQTT_HOST, MQTT_PORT))
    pub.sendall(create_connect_packet("qos1_downgrade_pub", clean_start=True))
    validate_connack(recv_packet(pub))

    pub.sendall(create_publish_packet("test/qos/downgrade", b"msg", qos=2, packet_id=201))
    # Subscriber should receive it as QoS 1
    pkt = recv_packet(s, timeout=2.0)
    assert pkt is not None
    topic, payload, qos, dup, retain, pid, props = validate_publish(pkt)
    assert qos == 1, "Expected QoS to be downgraded to 1"
    
    s.sendall(create_puback_packet(pid))
    s.close()
    pub.close()


def test_packet_id_wraparound():
    """Test Handling of Packet ID wraparound."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("qos1_wrap", clean_start=True))
    validate_connack(recv_packet(s))

    # Subscribe
    s.sendall(create_subscribe_packet(1, "test/wrap", qos=1))
    validate_suback(recv_packet(s), 1)

    # Publisher
    pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    pub.connect((MQTT_HOST, MQTT_PORT))
    pub.sendall(create_connect_packet("qos1_wrap_pub", clean_start=True))
    validate_connack(recv_packet(pub))

    # Send publish with max packet ID
    pub.sendall(create_publish_packet("test/wrap", b"data1", qos=1, packet_id=65535))
    recv_packet(pub) # PUBACK
    
    # Send another with wrapped ID
    pub.sendall(create_publish_packet("test/wrap", b"data2", qos=1, packet_id=1))
    recv_packet(pub) # PUBACK
    
    pub.close()

    # Subscriber receives
    pkt1 = recv_packet(s)
    pkt2 = recv_packet(s)
    assert pkt1 is not None
    assert pkt2 is not None

    _, payload1, _, _, _, pid1, _ = validate_publish(pkt1)
    _, payload2, _, _, _, pid2, _ = validate_publish(pkt2)

    assert payload1 == b"data1"
    assert payload2 == b"data2"

    s.close()
