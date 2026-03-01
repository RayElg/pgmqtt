#!/usr/bin/env python3
"""
Basic MQTT 5.0 protocol integration tests for pgmqtt.
Verifies CONNECT, SUBSCRIBE, and PINGREQ functionality.
"""

import socket
import sys
from proto_utils import (
    create_connect_packet,
    create_subscribe_packet,
    recv_packet,
    validate_connack,
    validate_suback,
    MQTT_HOST,
    MQTT_PORT,
    ReasonCode
)

def test_mqtt_connect():
    """Test: MQTT CONNECT → CONNACK on port 1883."""
    print("\n[MQTT Protocol] CONNECT → CONNACK")
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(5)
        s.connect((MQTT_HOST, MQTT_PORT))
        s.sendall(create_connect_packet("test_connect"))
        
        resp = recv_packet(s, timeout=5)
        assert resp is not None, "No CONNACK received"
        
        _, reason_code, _ = validate_connack(resp)
        assert reason_code == ReasonCode.SUCCESS, f"CONNACK failed: {reason_code}"
        print("  ✓ CONNACK received (Success)")

def test_mqtt_subscribe():
    """Test: MQTT SUBSCRIBE → SUBACK."""
    print("\n[MQTT Protocol] SUBSCRIBE → SUBACK")
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(5)
        s.connect((MQTT_HOST, MQTT_PORT))
        s.sendall(create_connect_packet("test_subscribe"))
        recv_packet(s, timeout=5)
        
        packet_id = 77
        s.sendall(create_subscribe_packet(packet_id, "test/topic", qos=0))
        suback = recv_packet(s, timeout=5)
        assert suback is not None, "No SUBACK received"
        
        reason_codes = validate_suback(suback, packet_id)
        assert reason_codes[0] == ReasonCode.GRANTED_QOS_0, f"SUBACK reason_code={reason_codes[0]}"
        print(f"  ✓ SUBACK received (Granted QoS 0)")

def test_mqtt_pingreq():
    """Test: MQTT PINGREQ → PINGRESP."""
    print("\n[MQTT Protocol] PINGREQ → PINGRESP")
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(5)
        s.connect((MQTT_HOST, MQTT_PORT))
        s.sendall(create_connect_packet("test_ping"))
        recv_packet(s, timeout=5)
        
        # Send PINGREQ
        s.sendall(bytes([0xC0, 0x00]))
        resp = recv_packet(s, timeout=5)
        assert resp == bytes([0xD0, 0x00]), "Unexpected PINGRESP"
        print("  ✓ PINGRESP received")

if __name__ == "__main__":
    test_mqtt_connect()
    test_mqtt_subscribe()
    test_mqtt_pingreq()
    print("\n✓ All MQTT protocol tests passed")
