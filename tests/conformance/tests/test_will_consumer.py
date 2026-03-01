"""
MQTT 5.0 Conformance Tests: Will Message (Consumer Perspective)

Tests verify broker delivers Will Message on ungraceful disconnect
per MQTT 5.0 Section 3.1.2.5.
"""
import socket
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from proto_utils import (get_broker_config,
    MQTTControlPacket, ReasonCode,
    create_connect_packet, create_subscribe_packet,
    create_disconnect_packet, recv_packet,
    validate_connack, validate_suback, validate_publish
,
    get_broker_config)


def test_will_delivered_on_disconnect():
    """
    Test: Client connects with Will Message, then disconnects ungracefully.
    Expected: Will Message is delivered to subscribers.
    """
    host, port = get_broker_config()
    will_topic = "test/will/delivered"
    will_payload = b"client died unexpectedly"
    
    # 1. Consumer subscribes to will topic
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet("will_consumer", clean_start=True))
        connack = recv_packet(s_cons)
        validate_connack(connack)
        
        s_cons.sendall(create_subscribe_packet(40, will_topic, qos=1))
        suback = recv_packet(s_cons)
        validate_suback(suback, 40)
        
        # 2. Another client connects with Will Message
        s_will = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s_will.connect((host, port))
        s_will.sendall(create_connect_packet(
            "will_client",
            clean_start=True,
            will_topic=will_topic,
            will_payload=will_payload,
            will_qos=1,
            will_retain=False
        ))
        recv_packet(s_will)
        
        # 3. Close socket without DISCONNECT (ungraceful)
        s_will.close()
        
        # 4. Consumer should receive Will Message
        # May need a short delay for broker to detect disconnect
        pub = recv_packet(s_cons, timeout=5.0)
        assert pub is not None, "Should receive Will Message after ungraceful disconnect"
        
        topic, payload, qos, dup, retain, packet_id, props = validate_publish(pub)
        assert topic == will_topic, f"Will topic mismatch: {topic}"
        assert payload == will_payload, f"Will payload mismatch: {payload}"
        
        print("✓ test_will_delivered_on_disconnect passed")


def test_will_not_delivered_on_clean_disconnect():
    """
    Test: Client connects with Will Message, then disconnects cleanly with DISCONNECT.
    Expected: Will Message is NOT delivered.
    """
    host, port = get_broker_config()
    will_topic = "test/will/suppressed"
    will_payload = b"should never see this"
    
    # 1. Consumer subscribes to will topic
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet("will_suppress_consumer", clean_start=True))
        recv_packet(s_cons)
        
        s_cons.sendall(create_subscribe_packet(41, will_topic, qos=0))
        recv_packet(s_cons)
        
        # 2. Client connects with Will Message
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_will:
            s_will.connect((host, port))
            s_will.sendall(create_connect_packet(
                "will_clean_client",
                clean_start=True,
                will_topic=will_topic,
                will_payload=will_payload,
                will_qos=0
            ))
            recv_packet(s_will)
            
            # 3. Send clean DISCONNECT with reason code 0x00
            s_will.sendall(create_disconnect_packet(reason_code=0))
        
        # 4. Consumer should NOT receive Will Message
        pub = recv_packet(s_cons, timeout=2.0)
        assert pub is None, f"Will Message should NOT be delivered on clean disconnect, got: {pub}"
        
        print("✓ test_will_not_delivered_on_clean_disconnect passed")


def test_will_with_delay():
    """
    Test: Will Delay Interval property.
    Client connects with Will Delay, disconnects ungracefully, reconnects before delay.
    Expected: Will is NOT sent if client reconnects within delay.
    """
    host, port = get_broker_config()
    will_topic = "test/will/delayed"
    will_payload = b"delayed will"
    client_id = "will_delay_client"
    
    # Subscribe to will topic
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet("will_delay_consumer", clean_start=True))
        recv_packet(s_cons)
        
        s_cons.sendall(create_subscribe_packet(42, will_topic, qos=0))
        recv_packet(s_cons)
        
        # Client connects with Will Delay = 5 seconds
        s_will = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s_will.connect((host, port))
        s_will.sendall(create_connect_packet(
            client_id,
            clean_start=True,
            will_topic=will_topic,
            will_payload=will_payload,
            will_qos=0,
            will_properties={0x18: 5}  # Will Delay Interval = 5 seconds
        ))
        recv_packet(s_will)
        
        # Ungraceful disconnect
        s_will.close()
        
        # Immediately reconnect (within delay)
        time.sleep(1)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_reconnect:
            s_reconnect.connect((host, port))
            s_reconnect.sendall(create_connect_packet(client_id, clean_start=True))
            recv_packet(s_reconnect)
        
        # Will should NOT be delivered
        pub = recv_packet(s_cons, timeout=2.0)
        assert pub is None, f"Will should be cancelled on reconnect, got: {pub}"
        
        print("✓ test_will_with_delay passed")


if __name__ == "__main__":
    test_will_delivered_on_disconnect()
    test_will_not_delivered_on_clean_disconnect()
    test_will_with_delay()
    print("\nAll will message tests passed!")
