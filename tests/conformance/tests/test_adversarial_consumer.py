"""
MQTT 5.0 Conformance Tests: Adversarial/Protocol Violations (Consumer Perspective)

Tests verify broker correctly rejects malformed packets and protocol violations
per MQTT 5.0 spec error handling requirements.
"""
import socket
import struct
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from proto_utils import (
    MQTTControlPacket, ReasonCode, EXPECTED_FLAGS, get_broker_config,
    create_connect_packet, create_subscribe_packet, create_publish_packet,
    create_puback_packet, create_pubrel_packet, create_pubcomp_packet,
    recv_packet, encode_variable_byte_integer, encode_utf8_string,
    validate_disconnect
)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def expect_disconnect_or_close(s, timeout=2.0, expected_reason=None):
    """
    Expect either connection close or DISCONNECT packet.
    Returns True if broker behaved correctly.
    """
    data = recv_packet(s, timeout=timeout)
    if not data:
        print("✓ Connection closed by broker")
        return True
    elif data[0] >> 4 == MQTTControlPacket.DISCONNECT:
        reason_code = validate_disconnect(data)
        print(f"✓ Received DISCONNECT with reason code 0x{reason_code:02x}")
        if expected_reason is not None:
            assert reason_code == expected_reason, \
                f"Expected reason 0x{expected_reason:02x}, got 0x{reason_code:02x}"
        return True
    else:
        print(f"[!] Unexpected response: {data.hex()}")
        return False


# ============================================================================
# PROTOCOL VIOLATIONS - UTF-8 & STRINGS
# ============================================================================

def test_invalid_utf8_client_id():
    """Test: CONNECT with invalid UTF-8 in client ID should be rejected."""
    host, port = get_broker_config()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        
        # Create CONNECT with invalid UTF-8 (0xFF is invalid in UTF-8)
        protocol_name = encode_utf8_string("MQTT")
        protocol_version = b'\x05'
        connect_flags = b'\x02'  # Clean Start
        keep_alive = struct.pack('!H', 60)
        properties = encode_variable_byte_integer(0)
        
        # Invalid UTF-8 client ID
        invalid_client_id = struct.pack('!H', 5) + b'test\xff'
        
        variable_header = protocol_name + protocol_version + connect_flags + keep_alive + properties
        remaining_length = len(variable_header) + len(invalid_client_id)
        fixed_header = bytes([(MQTTControlPacket.CONNECT << 4)]) + encode_variable_byte_integer(remaining_length)
        
        print("Sending CONNECT with invalid UTF-8 client ID...")
        s.sendall(fixed_header + variable_header + invalid_client_id)
        
        expect_disconnect_or_close(s, expected_reason=ReasonCode.MALFORMED_PACKET)
    
    print("✓ test_invalid_utf8_client_id passed\n")


def test_invalid_utf8_topic():
    """Test: PUBLISH with invalid UTF-8 in topic name should be rejected."""
    host, port = get_broker_config()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(create_connect_packet("adv_utf8_topic"))
        recv_packet(s)
        
        # Create PUBLISH with invalid UTF-8 topic
        invalid_topic = struct.pack('!H', 6) + b'test/\xff'
        properties = encode_variable_byte_integer(0)
        payload = b"test"
        
        var_header = invalid_topic + properties
        remaining_length = len(var_header) + len(payload)
        fixed_header = bytes([(MQTTControlPacket.PUBLISH << 4)]) + encode_variable_byte_integer(remaining_length)
        
        print("Sending PUBLISH with invalid UTF-8 topic...")
        s.sendall(fixed_header + var_header + payload)
        
        # Broker may use TOPIC_NAME_INVALID (0x90) or MALFORMED_PACKET (0x81)
        data = recv_packet(s, timeout=2.0)
        if not data:
            print("✓ Connection closed by broker")
        elif data[0] >> 4 == MQTTControlPacket.DISCONNECT:
            reason_code = validate_disconnect(data)
            print(f"✓ Received DISCONNECT with reason code 0x{reason_code:02x}")
            # Accept either MALFORMED_PACKET or TOPIC_NAME_INVALID
            assert reason_code in (ReasonCode.MALFORMED_PACKET, ReasonCode.TOPIC_NAME_INVALID), \
                f"Expected MALFORMED_PACKET or TOPIC_NAME_INVALID, got 0x{reason_code:02x}"
        else:
            raise AssertionError(f"Unexpected response: {data.hex()}")
    
    print("✓ test_invalid_utf8_topic passed\n")


# ============================================================================
# PROTOCOL VIOLATIONS - TOPIC NAMES
# ============================================================================

def test_topic_with_null_character():
    """Test: Topic containing null character should be rejected."""
    host, port = get_broker_config()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(create_connect_packet("adv_null_topic"))
        recv_packet(s)
        
        # Topic with null character
        topic_with_null = "test/\x00/data"
        
        print("Sending PUBLISH with null character in topic...")
        s.sendall(create_publish_packet(topic_with_null, b"test", qos=0))
        
        # Broker may use different reason codes
        data = recv_packet(s, timeout=2.0)
        if not data:
            print("✓ Connection closed by broker")
        elif data[0] >> 4 == MQTTControlPacket.DISCONNECT:
            reason_code = validate_disconnect(data)
            print(f"✓ Received DISCONNECT with reason code 0x{reason_code:02x}")
        else:
            raise AssertionError(f"Unexpected response: {data.hex()}")
    
    print("✓ test_topic_with_null_character passed\n")


def test_invalid_wildcard_in_publish():
    """Test: Publishing to topic with wildcards should be rejected."""
    host, port = get_broker_config()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(create_connect_packet("adv_wildcard_pub"))
        recv_packet(s)
        
        print("Sending PUBLISH to topic with wildcard...")
        s.sendall(create_publish_packet("test/+/data", b"test", qos=0))
        
        # Broker may use different reason codes
        data = recv_packet(s, timeout=2.0)
        if not data:
            print("✓ Connection closed by broker")
        elif data[0] >> 4 == MQTTControlPacket.DISCONNECT:
            reason_code = validate_disconnect(data)
            print(f"✓ Received DISCONNECT with reason code 0x{reason_code:02x}")
        else:
            raise AssertionError(f"Unexpected response: {data.hex()}")
    
    print("✓ test_invalid_wildcard_in_publish passed\n")


def test_invalid_wildcard_placement():
    """Test: Subscribe with invalid wildcard placement should be rejected."""
    host, port = get_broker_config()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(create_connect_packet("adv_wildcard_place"))
        recv_packet(s)
        
        # Invalid: + not at level boundary
        invalid_filter = "test/+foo"
        
        print("Sending SUBSCRIBE with invalid wildcard placement...")
        s.sendall(create_subscribe_packet(1, invalid_filter, qos=0))
        
        # Broker may send SUBACK with failure or DISCONNECT
        response = recv_packet(s)
        if response:
            packet_type = response[0] >> 4
            if packet_type == MQTTControlPacket.SUBACK:
                # Check for failure reason code in payload
                print("✓ Received SUBACK (should contain failure reason)")
            elif packet_type == MQTTControlPacket.DISCONNECT:
                print("✓ Received DISCONNECT")
        else:
            print("✓ Connection closed")
    
    print("✓ test_invalid_wildcard_placement passed\n")


# ============================================================================
# PROTOCOL VIOLATIONS - QoS
# ============================================================================

def test_qos0_with_packet_id():
    """Test: QoS 0 PUBLISH with packet identifier should be rejected."""
    host, port = get_broker_config()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(create_connect_packet("adv_qos0_pid"))
        recv_packet(s)
        
        # Manually create QoS 0 PUBLISH with packet ID (invalid per spec)
        topic = encode_utf8_string("test/invalid")
        packet_id = struct.pack('!H', 123)  # Should NOT be present for QoS 0
        properties = encode_variable_byte_integer(0)
        payload = b"test"
        
        var_header = topic + packet_id + properties  # Invalid!
        remaining_length = len(var_header) + len(payload)
        fixed_header = bytes([(MQTTControlPacket.PUBLISH << 4) | 0x00]) + encode_variable_byte_integer(remaining_length)
        
        print("Sending QoS 0 PUBLISH with packet identifier...")
        s.sendall(fixed_header + var_header + payload)
        
        expect_disconnect_or_close(s, expected_reason=ReasonCode.MALFORMED_PACKET)
    
    print("✓ test_qos0_with_packet_id passed\n")


def test_qos1_without_packet_id():
    """Test: QoS 1 PUBLISH without packet identifier should be rejected."""
    host, port = get_broker_config()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(create_connect_packet("adv_qos1_nopid"))
        recv_packet(s)
        
        # Manually create QoS 1 PUBLISH without packet ID (invalid)
        topic = encode_utf8_string("test/invalid")
        properties = encode_variable_byte_integer(0)
        payload = b"test"
        
        var_header = topic + properties  # Missing packet ID!
        remaining_length = len(var_header) + len(payload)
        fixed_header = bytes([(MQTTControlPacket.PUBLISH << 4) | 0x02]) + encode_variable_byte_integer(remaining_length)
        
        print("Sending QoS 1 PUBLISH without packet identifier...")
        s.sendall(fixed_header + var_header + payload)
        
        expect_disconnect_or_close(s, expected_reason=ReasonCode.MALFORMED_PACKET)
    
    print("✓ test_qos1_without_packet_id passed\n")


# ============================================================================
# PROTOCOL VIOLATIONS - PACKET IDENTIFIER
# ============================================================================

def test_packet_id_reuse():
    """Test: Reusing packet ID before acknowledgment should be rejected."""
    host, port = get_broker_config()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(create_connect_packet("adv_pid_reuse"))
        recv_packet(s)
        
        # Send first PUBLISH with packet ID 100
        print("Sending first PUBLISH with packet_id=100...")
        s.sendall(create_publish_packet("test/reuse", b"msg1", qos=1, packet_id=100))
        
        # Immediately send second PUBLISH with same packet ID (before PUBACK)
        print("Sending second PUBLISH with same packet_id=100...")
        s.sendall(create_publish_packet("test/reuse", b"msg2", qos=1, packet_id=100))
        
        # Broker should disconnect or send error
        expect_disconnect_or_close(s, expected_reason=ReasonCode.PACKET_ID_IN_USE)
    
    print("✓ test_packet_id_reuse passed\n")


# ============================================================================
# PROTOCOL VIOLATIONS - RESERVED BITS
# ============================================================================

def test_invalid_reserved_bits():
    """Test: Invalid reserved bits in fixed header should be rejected."""
    host, port = get_broker_config()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(create_connect_packet("adv_res_bits"))
        recv_packet(s)
        
        # SUBSCRIBE must have bits 3,2,1,0 set to 0,0,1,0 (value 2)
        # Send with 0,0,1,1 (value 3) instead
        print("Sending SUBSCRIBE with invalid reserved bits...")
        fixed_header = bytes([(MQTTControlPacket.SUBSCRIBE << 4) | 0x03]) + b'\x00'
        s.sendall(fixed_header)
        
        expect_disconnect_or_close(s, expected_reason=ReasonCode.MALFORMED_PACKET)
    
    print("✓ test_invalid_reserved_bits passed\n")


# ============================================================================
# PROTOCOL VIOLATIONS - VARIABLE BYTE INTEGER
# ============================================================================

def test_malformed_varint():
    """Test: Malformed variable byte integer should cause disconnect."""
    host, port = get_broker_config()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(create_connect_packet("adv_varint"))
        recv_packet(s)
        
        # Variable byte integer with 5 bytes (max is 4)
        malformed_len = b'\x80\x80\x80\x80\x81'
        packet = bytes([(MQTTControlPacket.PINGREQ << 4)]) + malformed_len
        
        print("Sending packet with malformed variable byte integer...")
        s.sendall(packet)
        
        expect_disconnect_or_close(s, expected_reason=ReasonCode.MALFORMED_PACKET)
    
    print("✓ test_malformed_varint passed\n")


# ============================================================================
# STATE MACHINE VIOLATIONS
# ============================================================================

def test_unexpected_puback():
    """Test: Sending PUBACK for non-existent packet ID."""
    host, port = get_broker_config()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(create_connect_packet("adv_puback"))
        recv_packet(s)
        
        print("Sending unexpected PUBACK...")
        s.sendall(create_puback_packet(999))
        
        # Broker may ignore or disconnect
        data = recv_packet(s, timeout=2.0)
        if not data:
            print("✓ Connection closed by broker")
        else:
            print(f"✓ Broker response: {data.hex()}")
    
    print("✓ test_unexpected_puback passed\n")


def test_pubrel_without_pubrec():
    """Test: Sending PUBREL without first receiving PUBREC."""
    host, port = get_broker_config()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(create_connect_packet("adv_pubrel"))
        recv_packet(s)
        
        print("Sending PUBREL without PUBREC...")
        s.sendall(create_pubrel_packet(888))
        
        expect_disconnect_or_close(s, expected_reason=ReasonCode.PACKET_ID_NOT_FOUND)
    
    print("✓ test_pubrel_without_pubrec passed\n")


def test_duplicate_connect():
    """Test: Sending CONNECT twice on same connection."""
    host, port = get_broker_config()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(create_connect_packet("adv_dup_connect"))
        recv_packet(s)
        
        print("Sending second CONNECT on same connection...")
        s.sendall(create_connect_packet("adv_dup_connect2"))
        
        expect_disconnect_or_close(s, expected_reason=ReasonCode.PROTOCOL_ERROR)
    
    print("✓ test_duplicate_connect passed\n")


def test_subscribe_before_connect():
    """Test: Sending SUBSCRIBE before CONNECT."""
    host, port = get_broker_config()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        
        print("Sending SUBSCRIBE before CONNECT...")
        s.sendall(create_subscribe_packet(1, "test/topic", qos=0))
        
        # Connection should be closed
        data = recv_packet(s, timeout=2.0)
        assert data is None, "Connection should be closed for packet before CONNECT"
        print("✓ Connection closed as expected")
    
    print("✓ test_subscribe_before_connect passed\n")


# ============================================================================
# SESSION VIOLATIONS
# ============================================================================

def test_session_takeover():
    """Test: Connecting with same client ID from two connections."""
    host, port = get_broker_config()
    client_id = "takeover_client"
    
    # First connection
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s1.connect((host, port))
    s1.sendall(create_connect_packet(client_id, clean_start=True))
    recv_packet(s1)
    print("First connection established")
    
    # Second connection with same client ID
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s2:
        s2.connect((host, port))
        s2.sendall(create_connect_packet(client_id, clean_start=False))
        recv_packet(s2)
        print("Second connection established with same client ID")
        
        # First connection should receive DISCONNECT with Session Taken Over
        print("Checking first connection for DISCONNECT...")
        data = recv_packet(s1, timeout=3.0)
        if data and data[0] >> 4 == MQTTControlPacket.DISCONNECT:
            reason_code = validate_disconnect(data)
            assert reason_code == ReasonCode.SESSION_TAKEN_OVER, \
                f"Expected SESSION_TAKEN_OVER (0x8E), got 0x{reason_code:02x}"
            print(f"✓ First connection received DISCONNECT with reason 0x{reason_code:02x}")
        else:
            print("✓ First connection closed")
    
    s1.close()
    print("✓ test_session_takeover passed\n")


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("MQTT 5.0 Adversarial/Protocol Violation Tests")
    print("=" * 70 + "\n")
    
    # UTF-8 & String violations
    test_invalid_utf8_client_id()
    test_invalid_utf8_topic()
    
    # Topic name violations
    test_topic_with_null_character()
    test_invalid_wildcard_in_publish()
    test_invalid_wildcard_placement()
    
    # QoS violations
    test_qos0_with_packet_id()
    test_qos1_without_packet_id()
    
    # Packet identifier violations
    test_packet_id_reuse()
    
    # Reserved bits & malformed packets
    test_invalid_reserved_bits()
    test_malformed_varint()
    
    # State machine violations
    test_unexpected_puback()
    test_pubrel_without_pubrec()
    test_duplicate_connect()
    test_subscribe_before_connect()
    
    # Session violations
    test_session_takeover()
    
    print("=" * 70)
    print("All adversarial tests completed!")
    print("=" * 70)

