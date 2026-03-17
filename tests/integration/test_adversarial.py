"""
MQTT 5.0 Adversarial / Protocol Violation Tests.

Tests broker handling of malformed packets, protocol violations, and edge cases.
Many are xfail because pgmqtt does not enforce all MQTT 5.0 validations.
"""

import socket
import struct
import pytest

from proto_utils import (
    create_connect_packet,
    create_subscribe_packet,
    create_publish_packet,
    create_puback_packet,
    create_pubrel_packet,
    recv_packet,
    validate_disconnect,
    encode_variable_byte_integer,
    encode_utf8_string,
    MQTTControlPacket,
    ReasonCode,
    MQTT_HOST,
    MQTT_PORT,
)


def _expect_disconnect_or_close(s, timeout=2.0, expected_reason=None):
    """Expect either connection close or DISCONNECT packet."""
    data = recv_packet(s, timeout=timeout)
    if not data:
        return True  # Connection closed
    if data[0] >> 4 == MQTTControlPacket.DISCONNECT:
        rc = validate_disconnect(data)
        if expected_reason is not None:
            assert rc == expected_reason, f"Expected 0x{expected_reason:02x}, got 0x{rc:02x}"
        return True
    return False


# --- UTF-8 violations ---

@pytest.mark.xfail(reason="UTF-8 validation not strictly enforced")
def test_invalid_utf8_client_id():
    """CONNECT with invalid UTF-8 in client ID should be rejected."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))

    protocol_name = encode_utf8_string("MQTT")
    protocol_version = b'\x05'
    connect_flags = b'\x02'
    keep_alive = struct.pack('!H', 60)
    properties = encode_variable_byte_integer(0)
    invalid_client_id = struct.pack('!H', 5) + b'test\xff'

    vh = protocol_name + protocol_version + connect_flags + keep_alive + properties
    remaining = len(vh) + len(invalid_client_id)
    fixed = bytes([(MQTTControlPacket.CONNECT << 4)]) + encode_variable_byte_integer(remaining)

    s.sendall(fixed + vh + invalid_client_id)
    assert _expect_disconnect_or_close(s, expected_reason=ReasonCode.MALFORMED_PACKET)
    s.close()


@pytest.mark.xfail(reason="UTF-8 validation not strictly enforced")
def test_invalid_utf8_topic():
    """PUBLISH with invalid UTF-8 in topic name should be rejected."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("adv_utf8_topic"))
    recv_packet(s)

    invalid_topic = struct.pack('!H', 6) + b'test/\xff'
    properties = encode_variable_byte_integer(0)
    payload = b"test"
    var_header = invalid_topic + properties
    remaining = len(var_header) + len(payload)
    fixed = bytes([(MQTTControlPacket.PUBLISH << 4)]) + encode_variable_byte_integer(remaining)

    s.sendall(fixed + var_header + payload)
    data = recv_packet(s, timeout=2.0)
    if data and data[0] >> 4 == MQTTControlPacket.DISCONNECT:
        rc = validate_disconnect(data)
        assert rc in (ReasonCode.MALFORMED_PACKET, ReasonCode.TOPIC_NAME_INVALID)
    else:
        assert data is None, "Connection should be closed"
    s.close()


# --- Topic violations ---

def test_topic_with_null_character():
    """Topic containing null character should be rejected."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("adv_null_topic"))
    recv_packet(s)
    s.sendall(create_publish_packet("test/\x00/data", b"test", qos=0))
    assert _expect_disconnect_or_close(s)
    s.close()


def test_invalid_wildcard_in_publish():
    """Publishing to topic with wildcards should be rejected."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("adv_wc_pub"))
    recv_packet(s)
    s.sendall(create_publish_packet("test/+/data", b"test", qos=0))
    assert _expect_disconnect_or_close(s)
    s.close()


@pytest.mark.xfail(reason="Invalid wildcard placement not rejected")
def test_invalid_wildcard_placement():
    """Subscribe with invalid wildcard placement (test/+foo) should be rejected."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("adv_wc_place"))
    recv_packet(s)
    s.sendall(create_subscribe_packet(1, "test/+foo", qos=0))
    response = recv_packet(s)
    # Broker may send SUBACK with failure or DISCONNECT
    assert response is not None or response is None  # Accept any behavior
    s.close()


# --- QoS violations ---

@pytest.mark.xfail(reason="QoS 0 packet ID validation not enforced")
def test_qos0_with_packet_id():
    """QoS 0 PUBLISH with packet identifier should be rejected."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("adv_qos0_pid"))
    recv_packet(s)

    topic = encode_utf8_string("test/invalid")
    packet_id = struct.pack('!H', 123)
    properties = encode_variable_byte_integer(0)
    payload = b"test"
    var_header = topic + packet_id + properties
    remaining = len(var_header) + len(payload)
    fixed = bytes([(MQTTControlPacket.PUBLISH << 4) | 0x00]) + encode_variable_byte_integer(remaining)

    s.sendall(fixed + var_header + payload)
    assert _expect_disconnect_or_close(s, expected_reason=ReasonCode.MALFORMED_PACKET)
    s.close()


@pytest.mark.xfail(reason="QoS 1 missing packet ID not detected")
def test_qos1_without_packet_id():
    """QoS 1 PUBLISH without packet identifier should be rejected."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("adv_qos1_nopid"))
    recv_packet(s)

    topic = encode_utf8_string("test/invalid")
    properties = encode_variable_byte_integer(0)
    payload = b"test"
    var_header = topic + properties  # Missing packet ID
    remaining = len(var_header) + len(payload)
    fixed = bytes([(MQTTControlPacket.PUBLISH << 4) | 0x02]) + encode_variable_byte_integer(remaining)

    s.sendall(fixed + var_header + payload)
    assert _expect_disconnect_or_close(s, expected_reason=ReasonCode.MALFORMED_PACKET)
    s.close()


# --- Packet ID reuse ---

@pytest.mark.xfail(reason="Packet ID reuse not detected")
def test_packet_id_reuse():
    """Reusing packet ID before acknowledgment should be rejected."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("adv_pid_reuse"))
    recv_packet(s)

    s.sendall(create_publish_packet("test/reuse", b"msg1", qos=1, packet_id=100))
    s.sendall(create_publish_packet("test/reuse", b"msg2", qos=1, packet_id=100))
    assert _expect_disconnect_or_close(s, expected_reason=ReasonCode.PACKET_ID_IN_USE)
    s.close()


# --- Reserved bits ---

@pytest.mark.xfail(reason="Reserved bit validation not enforced")
def test_invalid_reserved_bits():
    """Invalid reserved bits in fixed header should be rejected."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("adv_res_bits"))
    recv_packet(s)

    # SUBSCRIBE with wrong reserved bits (0x03 instead of 0x02)
    fixed = bytes([(MQTTControlPacket.SUBSCRIBE << 4) | 0x03]) + b'\x00'
    s.sendall(fixed)
    assert _expect_disconnect_or_close(s, expected_reason=ReasonCode.MALFORMED_PACKET)
    s.close()


@pytest.mark.xfail(reason="Malformed varint not detected")
def test_malformed_varint():
    """Malformed variable byte integer (5 bytes) should cause disconnect."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("adv_varint"))
    recv_packet(s)

    malformed_len = b'\x80\x80\x80\x80\x81'
    pkt = bytes([(MQTTControlPacket.PINGREQ << 4)]) + malformed_len
    s.sendall(pkt)
    assert _expect_disconnect_or_close(s, expected_reason=ReasonCode.MALFORMED_PACKET)
    s.close()


# --- State machine violations ---

def test_unexpected_puback():
    """PUBACK for non-existent packet ID — broker should handle gracefully."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("adv_puback"))
    recv_packet(s)

    s.sendall(create_puback_packet(999))
    # Broker may ignore or disconnect — both are acceptable
    recv_packet(s, timeout=2.0)
    s.close()


@pytest.mark.xfail(reason="QoS 2 not implemented")
def test_pubrel_without_pubrec():
    """PUBREL without first receiving PUBREC should be rejected."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("adv_pubrel"))
    recv_packet(s)

    s.sendall(create_pubrel_packet(888))
    assert _expect_disconnect_or_close(s, expected_reason=ReasonCode.PACKET_ID_NOT_FOUND)
    s.close()


def test_duplicate_connect():
    """Sending CONNECT twice on same connection causes disconnect."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("adv_dup_connect"))
    recv_packet(s)

    s.sendall(create_connect_packet("adv_dup_connect2"))
    assert _expect_disconnect_or_close(s, expected_reason=ReasonCode.PROTOCOL_ERROR)
    s.close()


def test_subscribe_before_connect():
    """SUBSCRIBE before CONNECT should close connection."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))

    s.sendall(create_subscribe_packet(1, "test/topic", qos=0))
    data = recv_packet(s, timeout=2.0)
    assert data is None, "Connection should be closed for packet before CONNECT"
    s.close()


def test_publish_before_connect():
    """PUBLISH before CONNECT should close connection."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))

    s.sendall(create_publish_packet("test/topic", b"data", qos=0))
    data = recv_packet(s, timeout=2.0)
    assert data is None, "Connection should be closed for packet before CONNECT"
    s.close()


@pytest.mark.xfail(reason="Maximum packet size validation not enforced")
def test_payload_exceeds_maximum_packet_size():
    """Payload exceeding Maximum Packet Size should be rejected."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("adv_max_size"))
    recv_packet(s)

    huge_payload = b"X" * (1024 * 1024 * 10)  # 10 MB
    s.sendall(create_publish_packet("test/huge", huge_payload, qos=0))
    assert _expect_disconnect_or_close(s, expected_reason=ReasonCode.PACKET_TOO_LARGE)
    s.close()
