"""
MQTT 5.0 Conformance Tests: Unsubscribe (Consumer Perspective)

Tests verify broker correctly handles UNSUBSCRIBE and stops message delivery
per MQTT 5.0 Section 3.10-3.11.
"""
import socket
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from proto_utils import (get_broker_config,
    MQTTControlPacket, ReasonCode,
    create_connect_packet, create_subscribe_packet, create_unsubscribe_packet,
    create_publish_packet, create_disconnect_packet, recv_packet,
    validate_connack, validate_suback, validate_unsuback, validate_publish
,
    get_broker_config)


def test_unsubscribe_stops_messages():
    """
    Test: After UNSUBSCRIBE, broker stops delivering messages to that subscription.
    """
    host, port = get_broker_config()
    topic = "test/unsub/stop"
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet("unsub_consumer", clean_start=True))
        connack = recv_packet(s_cons)
        validate_connack(connack)
        
        # Subscribe
        s_cons.sendall(create_subscribe_packet(30, topic, qos=0))
        suback = recv_packet(s_cons)
        validate_suback(suback, 30)
        
        # Verify subscription works
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
            s_pub.connect((host, port))
            s_pub.sendall(create_connect_packet("unsub_publisher1"))
            recv_packet(s_pub)
            s_pub.sendall(create_publish_packet(topic, b"before unsub", qos=0))
        
        pub = recv_packet(s_cons)
        assert pub is not None, "Should receive message before unsubscribe"
        validate_publish(pub)
        
        # Unsubscribe
        s_cons.sendall(create_unsubscribe_packet(31, topic))
        unsuback = recv_packet(s_cons)
        reason_codes = validate_unsuback(unsuback, 31)
        assert reason_codes[0] in (ReasonCode.SUCCESS, ReasonCode.NO_SUBSCRIPTION_EXISTED), \
            f"Unexpected UNSUBACK reason: {reason_codes[0]}"
        
        # Publish again - should NOT be delivered
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
            s_pub.connect((host, port))
            s_pub.sendall(create_connect_packet("unsub_publisher2"))
            recv_packet(s_pub)
            s_pub.sendall(create_publish_packet(topic, b"after unsub", qos=0))
        
        pub2 = recv_packet(s_cons, timeout=1.0)
        assert pub2 is None, f"Should NOT receive message after unsubscribe, got: {pub2}"
        
        print("✓ test_unsubscribe_stops_messages passed")


def test_unsuback_header_and_reason():
    """
    Test: UNSUBACK has correct fixed header flags (0x00) and valid reason codes.
    """
    host, port = get_broker_config()
    topic = "test/unsub/header"
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(create_connect_packet("unsuback_test", clean_start=True))
        recv_packet(s)
        
        # Subscribe first
        s.sendall(create_subscribe_packet(32, topic, qos=0))
        recv_packet(s)
        
        # Unsubscribe from existing subscription
        s.sendall(create_unsubscribe_packet(33, topic))
        unsuback = recv_packet(s)
        
        # validate_unsuback checks fixed header flags = 0x00
        reason_codes = validate_unsuback(unsuback, 33)
        assert reason_codes[0] == ReasonCode.SUCCESS, \
            f"Expected SUCCESS (0x00), got {reason_codes[0]}"
        
        print("✓ test_unsuback_header_and_reason passed (existing subscription)")
        
        # Unsubscribe from non-existent subscription
        s.sendall(create_unsubscribe_packet(34, "test/unsub/nonexistent"))
        unsuback2 = recv_packet(s)
        reason_codes2 = validate_unsuback(unsuback2, 34)
        
        # Broker may return SUCCESS or NO_SUBSCRIPTION_EXISTED
        assert reason_codes2[0] in (ReasonCode.SUCCESS, ReasonCode.NO_SUBSCRIPTION_EXISTED), \
            f"Unexpected reason code: {reason_codes2[0]}"
        
        print("✓ test_unsuback_header_and_reason passed (non-existent subscription)")


if __name__ == "__main__":
    test_unsubscribe_stops_messages()
    test_unsuback_header_and_reason()
    print("\nAll unsubscribe tests passed!")
