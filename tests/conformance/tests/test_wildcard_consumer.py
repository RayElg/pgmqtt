"""
MQTT 5.0 Conformance Tests: Topic Wildcards (Consumer Perspective)

Tests verify broker correctly matches wildcard subscriptions and delivers
messages per MQTT 5.0 Section 4.7.
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


def test_single_level_wildcard():
    """
    Test: Subscribe with single-level wildcard (+).
    Pattern: test/+/data matches test/a/data, test/b/data, not test/a/b/data
    """
    host, port = get_broker_config()
    filter_topic = "test/+/data"
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet("wildcard_single_consumer", clean_start=True))
        connack = recv_packet(s_cons)
        validate_connack(connack)
        
        s_cons.sendall(create_subscribe_packet(20, filter_topic, qos=0))
        suback = recv_packet(s_cons)
        reason_codes = validate_suback(suback, 20)
        assert reason_codes[0] <= ReasonCode.GRANTED_QOS_2, f"Subscribe failed: {reason_codes}"
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
            s_pub.connect((host, port))
            s_pub.sendall(create_connect_packet("wildcard_single_publisher"))
            recv_packet(s_pub)
            
            # Should match
            s_pub.sendall(create_publish_packet("test/a/data", b"match1", qos=0))
            s_pub.sendall(create_publish_packet("test/sensor1/data", b"match2", qos=0))
            
            # Should NOT match (too many levels)
            s_pub.sendall(create_publish_packet("test/a/b/data", b"nomatch", qos=0))
        
        # Should receive first two matches
        pub1 = recv_packet(s_cons)
        topic1, payload1, _, _, _, _, _ = validate_publish(pub1)
        assert topic1 == "test/a/data", f"Expected test/a/data, got {topic1}"
        
        pub2 = recv_packet(s_cons)
        topic2, payload2, _, _, _, _, _ = validate_publish(pub2)
        assert topic2 == "test/sensor1/data", f"Expected test/sensor1/data, got {topic2}"
        
        # Should NOT receive non-matching
        pub3 = recv_packet(s_cons, timeout=1.0)
        assert pub3 is None, f"Should not match test/a/b/data, but received: {pub3}"
        
        print("✓ test_single_level_wildcard passed")


def test_multi_level_wildcard():
    """
    Test: Subscribe with multi-level wildcard (#).
    Pattern: test/# matches test, test/a, test/a/b, test/a/b/c
    """
    host, port = get_broker_config()
    filter_topic = "test/multi/#"
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet("wildcard_multi_consumer", clean_start=True))
        recv_packet(s_cons)
        
        s_cons.sendall(create_subscribe_packet(21, filter_topic, qos=0))
        suback = recv_packet(s_cons)
        validate_suback(suback, 21)
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
            s_pub.connect((host, port))
            s_pub.sendall(create_connect_packet("wildcard_multi_publisher"))
            recv_packet(s_pub)
            
            # All should match
            s_pub.sendall(create_publish_packet("test/multi", b"m1", qos=0))
            s_pub.sendall(create_publish_packet("test/multi/a", b"m2", qos=0))
            s_pub.sendall(create_publish_packet("test/multi/a/b/c", b"m3", qos=0))
        
        # Receive all three
        topics_received = []
        for _ in range(3):
            pub = recv_packet(s_cons, timeout=2.0)
            assert pub is not None, "Expected to receive message"
            topic, _, _, _, _, _, _ = validate_publish(pub)
            topics_received.append(topic)
        
        assert "test/multi" in topics_received
        assert "test/multi/a" in topics_received
        assert "test/multi/a/b/c" in topics_received
        
        print("✓ test_multi_level_wildcard passed")


def test_combined_wildcards():
    """
    Test: Subscribe with combined wildcards.
    Pattern: +/sensor/# matches home/sensor, office/sensor/temp/1
    """
    host, port = get_broker_config()
    filter_topic = "+/sensor/#"
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet("wildcard_combined_consumer", clean_start=True))
        recv_packet(s_cons)
        
        s_cons.sendall(create_subscribe_packet(22, filter_topic, qos=0))
        suback = recv_packet(s_cons)
        validate_suback(suback, 22)
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
            s_pub.connect((host, port))
            s_pub.sendall(create_connect_packet("wildcard_combined_publisher"))
            recv_packet(s_pub)
            
            # Should match
            s_pub.sendall(create_publish_packet("home/sensor", b"c1", qos=0))
            s_pub.sendall(create_publish_packet("office/sensor/temp", b"c2", qos=0))
            s_pub.sendall(create_publish_packet("garage/sensor/humidity/1", b"c3", qos=0))
            
            # Should NOT match
            s_pub.sendall(create_publish_packet("home/device/temp", b"nomatch", qos=0))
        
        topics_received = []
        for _ in range(3):
            pub = recv_packet(s_cons, timeout=2.0)
            assert pub, "Expected message"
            topic, _, _, _, _, _, _ = validate_publish(pub)
            topics_received.append(topic)
        
        assert "home/sensor" in topics_received
        assert "office/sensor/temp" in topics_received
        assert "garage/sensor/humidity/1" in topics_received
        
        # Should not receive non-matching
        pub_extra = recv_packet(s_cons, timeout=1.0)
        assert pub_extra is None, "Should not receive non-matching topic"
        
        print("✓ test_combined_wildcards passed")


if __name__ == "__main__":
    test_single_level_wildcard()
    test_multi_level_wildcard()
    test_combined_wildcards()
    print("\nAll wildcard tests passed!")
