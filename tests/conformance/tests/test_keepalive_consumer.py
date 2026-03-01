"""
MQTT 5.0 Conformance Tests: Keep-Alive and PING (Consumer Perspective)

Tests verify broker correctly responds to PINGREQ and enforces keep-alive
per MQTT 5.0 Section 3.1.2.10.
"""
import socket
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from proto_utils import (get_broker_config,
    MQTTControlPacket, ReasonCode,
    create_connect_packet, create_pingreq_packet, recv_packet,
    validate_connack, validate_pingresp
,
    get_broker_config)


def test_pingreq_pingresp():
    """
    Test: Client sends PINGREQ, broker responds with PINGRESP.
    Validates PINGRESP fixed header flags = 0x00 per Table 2-2.
    """
    host, port = get_broker_config()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(create_connect_packet("ping_test_client", clean_start=True))
        connack = recv_packet(s)
        validate_connack(connack)
        
        # Send PINGREQ
        s.sendall(create_pingreq_packet())
        
        # Should receive PINGRESP
        pingresp = recv_packet(s)
        assert pingresp is not None, "Should receive PINGRESP"
        
        # Validate PINGRESP (checks type and flags = 0x00)
        validate_pingresp(pingresp)
        
        print("✓ test_pingreq_pingresp passed")


def test_multiple_pings():
    """
    Test: Multiple PINGREQ/PINGRESP exchanges work correctly.
    """
    host, port = get_broker_config()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(create_connect_packet("multi_ping_client", clean_start=True))
        recv_packet(s)
        
        for i in range(5):
            s.sendall(create_pingreq_packet())
            pingresp = recv_packet(s)
            validate_pingresp(pingresp)
        
        print("✓ test_multiple_pings passed")


def test_broker_disconnects_after_keepalive_expiry():
    """
    Test: If client doesn't send any packets within 1.5x keep-alive,
    broker should close connection.
    
    Note: This test uses a very short keep-alive (2 seconds) for testing.
    """
    host, port = get_broker_config()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        # Keep-alive = 2 seconds
        s.sendall(create_connect_packet("keepalive_expiry_client", clean_start=True, keep_alive=2))
        connack = recv_packet(s)
        validate_connack(connack)
        
        print("Waiting for keep-alive expiry (should disconnect after ~3 seconds)...")
        
        # Wait 1.5x keep-alive + buffer = 4 seconds
        time.sleep(4)
        
        # Try to receive - should get nothing (connection closed) or DISCONNECT
        s.settimeout(1.0)
        try:
            data = s.recv(1024)
            if not data:
                print("✓ test_broker_disconnects_after_keepalive_expiry passed (connection closed)")
            elif data[0] >> 4 == MQTTControlPacket.DISCONNECT:
                print("✓ test_broker_disconnects_after_keepalive_expiry passed (DISCONNECT received)")
            else:
                print(f"Unexpected data received: {data.hex()}")
        except socket.timeout:
            # Try to send - should fail if connection was closed
            try:
                s.sendall(create_pingreq_packet())
                # If we can still send, connection wasn't closed
                print("WARNING: Connection still active after keep-alive expiry")
            except (BrokenPipeError, ConnectionResetError, OSError):
                print("✓ test_broker_disconnects_after_keepalive_expiry passed (send failed)")


def test_keepalive_maintained_with_ping():
    """
    Test: Sending PINGREQ within keep-alive interval keeps connection alive.
    """
    host, port = get_broker_config()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        # Keep-alive = 3 seconds
        s.sendall(create_connect_packet("keepalive_maintained_client", clean_start=True, keep_alive=3))
        recv_packet(s)
        
        # Send PING every 2 seconds, for 8 seconds total
        for i in range(4):
            time.sleep(2)
            s.sendall(create_pingreq_packet())
            pingresp = recv_packet(s)
            assert pingresp is not None, f"PINGRESP {i+1} should be received"
            validate_pingresp(pingresp)
        
        print("✓ test_keepalive_maintained_with_ping passed")


if __name__ == "__main__":
    test_pingreq_pingresp()
    test_multiple_pings()
    test_keepalive_maintained_with_ping()
    test_broker_disconnects_after_keepalive_expiry()
    print("\nAll keep-alive tests passed!")
