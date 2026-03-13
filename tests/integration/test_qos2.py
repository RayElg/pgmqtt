"""
MQTT 5.0 QoS 2 Exactly-Once Delivery Tests.

All tests are xfail because pgmqtt does not implement QoS 2.
"""

import socket
import struct
import pytest

from proto_utils import (
    create_connect_packet,
    create_subscribe_packet,
    create_publish_packet,
    create_pubrec_packet,
    create_pubrel_packet,
    create_pubcomp_packet,
    recv_packet,
    validate_connack,
    validate_suback,
    validate_publish,
    validate_pubrec,
    validate_pubrel,
    validate_pubcomp,
    MQTTControlPacket,
    ReasonCode,
    MQTT_HOST,
    MQTT_PORT,
)

pytestmark = pytest.mark.xfail(reason="QoS 2 not implemented in pgmqtt")


def test_qos2_consumer_flow():
    """Complete QoS 2 four-way handshake: PUBLISH→PUBREC→PUBREL→PUBCOMP."""
    topic = "test/qos2"
    payload = b"hello qos 2"

    s_cons = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_cons.connect((MQTT_HOST, MQTT_PORT))
    s_cons.sendall(create_connect_packet("qos2_cons", clean_start=True))
    validate_connack(recv_packet(s_cons))
    s_cons.sendall(create_subscribe_packet(4, topic, qos=2))
    validate_suback(recv_packet(s_cons), 4)

    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("qos2_pub"))
    recv_packet(s_pub)

    s_pub.sendall(create_publish_packet(topic, payload, qos=2, packet_id=20))
    validate_pubrec(recv_packet(s_pub), 20)
    s_pub.sendall(create_pubrel_packet(20))
    validate_pubcomp(recv_packet(s_pub), 20)
    s_pub.close()

    pub = recv_packet(s_cons)
    t, p, qos, dup, retain, pid, props = validate_publish(pub)
    assert t == topic
    assert p == payload
    assert qos == 2
    assert not dup

    s_cons.sendall(create_pubrec_packet(pid))
    validate_pubrel(recv_packet(s_cons), pid)
    s_cons.sendall(create_pubcomp_packet(pid))
    s_cons.close()


def test_qos2_redelivery_before_pubcomp():
    """Disconnect before PUBCOMP — broker resends PUBREL on reconnect."""
    topic = "test/qos2/redeliver"
    payload = b"qos2 redeliver"
    client_id = "qos2_redeliver_cons"

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet(client_id, clean_start=True, properties={0x11: 3600}))
    validate_connack(recv_packet(s))
    s.sendall(create_subscribe_packet(70, topic, qos=2))
    validate_suback(recv_packet(s), 70)

    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("qos2_redeliver_pub"))
    recv_packet(s_pub)
    s_pub.sendall(create_publish_packet(topic, payload, qos=2, packet_id=71))
    recv_packet(s_pub)
    s_pub.sendall(create_pubrel_packet(71))
    recv_packet(s_pub)
    s_pub.close()

    pub = recv_packet(s)
    _, _, _, _, _, pid, _ = validate_publish(pub)
    s.sendall(create_pubrec_packet(pid))
    validate_pubrel(recv_packet(s), pid)
    # Disconnect WITHOUT PUBCOMP
    s.close()

    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.connect((MQTT_HOST, MQTT_PORT))
    s2.sendall(create_connect_packet(client_id, clean_start=False))
    sp, rc, _ = validate_connack(recv_packet(s2))
    assert sp, "Session should be present"

    pkt = recv_packet(s2)
    ptype = pkt[0] >> 4
    if ptype == MQTTControlPacket.PUBREL:
        validate_pubrel(pkt, pid)
        s2.sendall(create_pubcomp_packet(pid))
    elif ptype == MQTTControlPacket.PUBLISH:
        _, _, _, dup, _, pid2, _ = validate_publish(pkt)
        assert dup
        s2.sendall(create_pubrec_packet(pid2))
        validate_pubrel(recv_packet(s2), pid2)
        s2.sendall(create_pubcomp_packet(pid2))
    s2.close()


def test_qos2_disconnect_before_pubrec():
    """Disconnect before PUBREC — PUBLISH redelivered with DUP=1."""
    topic = "test/qos2/before_pubrec"
    payload = b"before pubrec"
    client_id = "qos2_before_pubrec"

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet(client_id, clean_start=True, properties={0x11: 3600}))
    recv_packet(s)
    s.sendall(create_subscribe_packet(110, topic, qos=2))
    recv_packet(s)

    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("qos2_bp_pub"))
    recv_packet(s_pub)
    s_pub.sendall(create_publish_packet(topic, payload, qos=2, packet_id=111))
    recv_packet(s_pub)
    s_pub.sendall(create_pubrel_packet(111))
    recv_packet(s_pub)
    s_pub.close()

    pub = recv_packet(s)
    _, _, _, dup, _, pid, _ = validate_publish(pub)
    assert not dup
    s.close()  # Without PUBREC

    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.connect((MQTT_HOST, MQTT_PORT))
    s2.sendall(create_connect_packet(client_id, clean_start=False))
    recv_packet(s2)

    pub2 = recv_packet(s2)
    _, p, _, dup2, _, pid2, _ = validate_publish(pub2)
    assert dup2, "Redelivered PUBLISH must have DUP=1"
    assert p == payload

    s2.sendall(create_pubrec_packet(pid2))
    validate_pubrel(recv_packet(s2), pid2)
    s2.sendall(create_pubcomp_packet(pid2))
    s2.close()


def test_qos2_exactly_once_guarantee():
    """Message delivered exactly once even with reconnects."""
    topic = "test/qos2/exactly_once"
    payload = b"exactly once"
    client_id = "qos2_exactly_once"

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet(client_id, clean_start=True, properties={0x11: 3600}))
    recv_packet(s)
    s.sendall(create_subscribe_packet(140, topic, qos=2))
    recv_packet(s)

    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("qos2_eo_pub"))
    recv_packet(s_pub)
    s_pub.sendall(create_publish_packet(topic, payload, qos=2, packet_id=141))
    recv_packet(s_pub)
    s_pub.sendall(create_pubrel_packet(141))
    recv_packet(s_pub)
    s_pub.close()

    pub = recv_packet(s)
    _, _, _, _, _, pid, _ = validate_publish(pub)
    s.sendall(create_pubrec_packet(pid))
    validate_pubrel(recv_packet(s), pid)
    s.sendall(create_pubcomp_packet(pid))
    s.close()

    # Reconnect — should NOT receive again
    for _ in range(2):
        s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s2.connect((MQTT_HOST, MQTT_PORT))
        s2.sendall(create_connect_packet(client_id, clean_start=False))
        recv_packet(s2)
        extra = recv_packet(s2, timeout=1.0)
        assert extra is None, "Message delivered more than once"
        s2.close()
