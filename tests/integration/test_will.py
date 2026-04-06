"""
MQTT 5.0 Will Message (LWT) and Keep-Alive Tests.

Tests:
  - Last Will and Testament delivery on ungraceful disconnect
  - Will suppression on clean DISCONNECT
  - Keep-alive timeout triggers LWT
  - Will Delay Interval (xfail — not yet implemented)
"""

import socket
import time
import pytest

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
    MQTT_HOST,
    MQTT_PORT,
)


def test_lwt_abrupt_disconnect():
    """Ungraceful close delivers LWT to subscribers."""
    will_topic = "test/will/abrupt"
    will_payload = b"client died"

    # Subscriber
    s_sub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_sub.connect((MQTT_HOST, MQTT_PORT))
    s_sub.sendall(create_connect_packet("lwt_abrupt_sub", clean_start=True))
    validate_connack(recv_packet(s_sub))
    s_sub.sendall(create_subscribe_packet(1, will_topic, qos=1))
    validate_suback(recv_packet(s_sub), 1)

    # Client with LWT
    s_will = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_will.connect((MQTT_HOST, MQTT_PORT))
    s_will.sendall(create_connect_packet(
        "lwt_abrupt_client",
        clean_start=True,
        will_topic=will_topic,
        will_payload=will_payload,
        will_qos=1,
        will_retain=False,
    ))
    validate_connack(recv_packet(s_will))

    # Ungraceful disconnect
    s_will.close()

    # Subscriber should receive LWT
    pkt = recv_packet(s_sub, timeout=5.0)
    assert pkt is not None, "Should receive LWT after ungraceful disconnect"
    topic, payload, qos, dup, retain, pid, props = validate_publish(pkt)
    assert topic == will_topic
    assert payload == will_payload
    if pid:
        s_sub.sendall(create_puback_packet(pid))
    s_sub.close()


def test_lwt_clean_disconnect():
    """Clean DISCONNECT suppresses LWT delivery."""
    will_topic = "test/will/clean"
    will_payload = b"should not see this"

    s_sub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_sub.connect((MQTT_HOST, MQTT_PORT))
    s_sub.sendall(create_connect_packet("lwt_clean_sub", clean_start=True))
    validate_connack(recv_packet(s_sub))
    s_sub.sendall(create_subscribe_packet(1, will_topic, qos=0))
    validate_suback(recv_packet(s_sub), 1)

    s_will = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_will.connect((MQTT_HOST, MQTT_PORT))
    s_will.sendall(create_connect_packet(
        "lwt_clean_client",
        clean_start=True,
        will_topic=will_topic,
        will_payload=will_payload,
        will_qos=0,
    ))
    validate_connack(recv_packet(s_will))

    # Clean disconnect
    s_will.sendall(create_disconnect_packet(reason_code=0))
    s_will.close()

    pkt = recv_packet(s_sub, timeout=2.0)
    assert pkt is None, "LWT should NOT be delivered on clean disconnect"
    s_sub.close()


def test_keepalive_timeout_triggers_lwt():
    """LWT published after keep-alive timeout (client goes silent)."""
    will_topic = "test/will/keepalive"
    will_payload = b"keepalive timeout"

    s_sub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_sub.connect((MQTT_HOST, MQTT_PORT))
    s_sub.sendall(create_connect_packet("lwt_ka_sub", clean_start=True))
    validate_connack(recv_packet(s_sub))
    s_sub.sendall(create_subscribe_packet(1, will_topic, qos=0))
    validate_suback(recv_packet(s_sub), 1)

    # Client with short keep-alive and LWT
    s_will = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_will.connect((MQTT_HOST, MQTT_PORT))
    s_will.sendall(create_connect_packet(
        "lwt_ka_client",
        clean_start=True,
        keep_alive=2,
        will_topic=will_topic,
        will_payload=will_payload,
        will_qos=0,
    ))
    validate_connack(recv_packet(s_will))

    # Go silent — broker should disconnect after 1.5x keep-alive = 3s
    time.sleep(5)

    pkt = recv_packet(s_sub, timeout=3.0)
    assert pkt is not None, "Should receive LWT after keep-alive timeout"
    topic, payload, *_ = validate_publish(pkt)
    assert topic == will_topic
    assert payload == will_payload
    s_will.close()
    s_sub.close()


def test_session_takeover_fires_will():
    """Session takeover disconnects old client and fires its Will message."""
    will_topic = "test/will/takeover"

    # Subscriber
    s_sub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_sub.connect((MQTT_HOST, MQTT_PORT))
    s_sub.sendall(create_connect_packet("lwt_takeover_sub", clean_start=True))
    validate_connack(recv_packet(s_sub))
    s_sub.sendall(create_subscribe_packet(1, will_topic, qos=1))
    validate_suback(recv_packet(s_sub), 1)

    # Client with Will message
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s1.connect((MQTT_HOST, MQTT_PORT))
    s1.sendall(create_connect_packet(
        "lwt_takeover_client",
        will_topic=will_topic,
        will_payload=b"taken-over",
        will_qos=0,
    ))
    recv_packet(s1)

    time.sleep(0.5)

    # Takeover: new connection with same client_id
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.connect((MQTT_HOST, MQTT_PORT))
    s2.sendall(create_connect_packet("lwt_takeover_client", clean_start=True))
    validate_connack(recv_packet(s2))

    # Subscriber should receive the Will message from takeover
    pkt = recv_packet(s_sub, timeout=5)
    assert pkt is not None, "Should receive Will message from session takeover"
    topic, payload, *_ = validate_publish(pkt)
    assert topic == will_topic
    assert payload == b"taken-over"

    s2.close()
    s_sub.close()


@pytest.mark.xfail(reason="Will Delay Interval not yet implemented")
def test_will_delay_cancelled_on_reconnect():
    """Will with delay cancelled by reconnect within delay period."""
    will_topic = "test/will/delayed"
    will_payload = b"delayed will"
    client_id = "lwt_delay_client"

    s_sub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_sub.connect((MQTT_HOST, MQTT_PORT))
    s_sub.sendall(create_connect_packet("lwt_delay_sub", clean_start=True))
    validate_connack(recv_packet(s_sub))
    s_sub.sendall(create_subscribe_packet(1, will_topic, qos=0))
    validate_suback(recv_packet(s_sub), 1)

    # Client with Will Delay = 5 seconds
    s_will = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_will.connect((MQTT_HOST, MQTT_PORT))
    s_will.sendall(create_connect_packet(
        client_id,
        clean_start=True,
        will_topic=will_topic,
        will_payload=will_payload,
        will_qos=0,
        will_properties={0x18: 5},  # Will Delay Interval = 5 seconds
    ))
    validate_connack(recv_packet(s_will))

    # Ungraceful disconnect
    s_will.close()

    # Reconnect within delay
    time.sleep(1)
    s_reconn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_reconn.connect((MQTT_HOST, MQTT_PORT))
    s_reconn.sendall(create_connect_packet(client_id, clean_start=True))
    validate_connack(recv_packet(s_reconn))
    s_reconn.close()

    # Will should NOT be delivered
    pkt = recv_packet(s_sub, timeout=2.0)
    assert pkt is None, "Will should be cancelled on reconnect within delay"
    s_sub.close()
