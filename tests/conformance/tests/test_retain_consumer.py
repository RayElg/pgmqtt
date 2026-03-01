"""
MQTT 5.0 Conformance Tests: Retained Messages (Consumer Perspective)

Tests verify broker correctly delivers retained messages to subscribing consumers
per MQTT 5.0 Section 3.3.1.3.
"""
import socket
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from proto_utils import (get_broker_config,
    MQTTControlPacket, ReasonCode,
    create_connect_packet, create_subscribe_packet, create_publish_packet,
    create_disconnect_packet, recv_packet,
    validate_connack, validate_suback, validate_publish
,
    get_broker_config)


def test_receive_retained_on_subscribe():
    """
    Test: Consumer subscribes to topic with existing retained message.
    Expected: Consumer receives PUBLISH with RETAIN flag = 1.
    """
    host, port = get_broker_config()
    topic = "test/retain/receive"
    payload = b"retained message"
    
    # 1. Publish retained message
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
        s_pub.connect((host, port))
        s_pub.sendall(create_connect_packet("retain_publisher"))
        connack = recv_packet(s_pub)
        session_present, reason_code, _ = validate_connack(connack)
        assert reason_code == ReasonCode.SUCCESS
        
        s_pub.sendall(create_publish_packet(topic, payload, qos=1, packet_id=1, retain=True))
        recv_packet(s_pub)  # PUBACK
        s_pub.sendall(create_disconnect_packet())
    
    # 2. Consumer subscribes and should receive retained message
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet("retain_consumer", clean_start=True))
        connack = recv_packet(s_cons)
        validate_connack(connack)
        
        s_cons.sendall(create_subscribe_packet(10, topic, qos=1))
        suback = recv_packet(s_cons)
        reason_codes = validate_suback(suback, 10)
        assert reason_codes[0] <= ReasonCode.GRANTED_QOS_2
        
        # Should receive retained message
        pub = recv_packet(s_cons)
        recv_topic, recv_payload, qos, dup, retain, packet_id, props = validate_publish(pub)
        
        assert recv_topic == topic, f"Topic mismatch: {recv_topic}"
        assert recv_payload == payload, f"Payload mismatch: {recv_payload}"
        assert retain, "RETAIN flag should be 1 for retained message delivery"
        
        print("✓ test_receive_retained_on_subscribe passed")
        
        # Cleanup: clear retained message
        s_cons.sendall(create_disconnect_packet())
    
    # Clear retained message
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(create_connect_packet("retain_cleaner"))
        recv_packet(s)
        s.sendall(create_publish_packet(topic, b"", qos=0, retain=True))
        s.sendall(create_disconnect_packet())


def test_clear_retained_with_empty_payload():
    """
    Test: Publishing empty payload with RETAIN=1 clears retained message.
    Expected: Subsequent subscriber receives no retained message.
    """
    host, port = get_broker_config()
    topic = "test/retain/clear"
    
    # 1. Publish retained message
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(create_connect_packet("retain_setter"))
        recv_packet(s)
        s.sendall(create_publish_packet(topic, b"to be cleared", qos=0, retain=True))
        s.sendall(create_disconnect_packet())
    
    # 2. Clear retained message with empty payload
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(create_connect_packet("retain_clearer"))
        recv_packet(s)
        s.sendall(create_publish_packet(topic, b"", qos=0, retain=True))
        s.sendall(create_disconnect_packet())
    
    # 3. New subscriber should NOT receive retained message
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet("retain_checker", clean_start=True))
        recv_packet(s_cons)
        
        s_cons.sendall(create_subscribe_packet(11, topic, qos=0))
        recv_packet(s_cons)  # SUBACK
        
        # Should NOT receive any retained message
        pub = recv_packet(s_cons, timeout=1.0)
        assert pub is None, f"Should not receive retained message after clear, got: {pub.hex() if pub else None}"
        
        print("✓ test_clear_retained_with_empty_payload passed")


def test_retain_flag_preserved_on_live_publish():
    """
    Test: When broker forwards a non-retained PUBLISH to a subscriber,
    the RETAIN flag should be 0 (unless it's a retained message delivery on subscribe).
    """
    host, port = get_broker_config()
    topic = "test/retain/live"
    payload = b"live message"
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet("retain_live_consumer", clean_start=True))
        recv_packet(s_cons)
        
        s_cons.sendall(create_subscribe_packet(12, topic, qos=0))
        recv_packet(s_cons)  # SUBACK
        
        # Publish live message (not retained)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
            s_pub.connect((host, port))
            s_pub.sendall(create_connect_packet("retain_live_publisher"))
            recv_packet(s_pub)
            s_pub.sendall(create_publish_packet(topic, payload, qos=0, retain=False))
        
        # Consumer receives message
        pub = recv_packet(s_cons)
        recv_topic, recv_payload, qos, dup, retain, packet_id, props = validate_publish(pub)
        
        assert recv_payload == payload
        assert not retain, "RETAIN flag should be 0 for live (non-retained) message"
        
        print("✓ test_retain_flag_preserved_on_live_publish passed")


if __name__ == "__main__":
    test_receive_retained_on_subscribe()
    test_clear_retained_with_empty_payload()
    test_retain_flag_preserved_on_live_publish()
    print("\nAll retain tests passed!")
