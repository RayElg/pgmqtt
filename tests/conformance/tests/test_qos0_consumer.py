"""
MQTT 5.0 Conformance Tests: QoS 0 (Consumer Perspective)

Tests verify broker correctly handles QoS 0 message delivery:
- At-most-once delivery semantics
- No PUBACK for QoS 0
- Correct fixed header flags (DUP=0, QoS=0)
"""
import socket
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from proto_utils import (
    MQTTControlPacket, ReasonCode, get_broker_config,
    create_connect_packet, create_subscribe_packet, create_publish_packet,
    recv_packet,
    validate_connack, validate_suback, validate_publish
)


def test_qos0_consumer():
    """
    Test: Basic QoS 0 message delivery.
    Consumer subscribes, publisher sends QoS 0, consumer receives message.
    No PUBACK expected for QoS 0.
    """
    host, port = get_broker_config()
    topic = "test/qos0"
    payload = b"hello qos 0"
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        
        # Connect consumer
        s_cons.sendall(create_connect_packet("consumer_test", clean_start=True))
        connack = recv_packet(s_cons)
        session_present, reason_code, props = validate_connack(connack)
        assert reason_code == ReasonCode.SUCCESS
        
        # Subscribe
        s_cons.sendall(create_subscribe_packet(1, topic, qos=0))
        suback = recv_packet(s_cons)
        reason_codes = validate_suback(suback, 1)
        assert reason_codes[0] == ReasonCode.GRANTED_QOS_0, f"Expected QoS 0 grant, got {reason_codes}"
        
        # Now publish from another connection
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
            s_pub.connect((host, port))
            s_pub.sendall(create_connect_packet("publisher_test"))
            recv_packet(s_pub)
            
            # Publish QoS 0
            s_pub.sendall(create_publish_packet(topic, payload, qos=0))
            
        # Consumer should receive it
        publish_packet = recv_packet(s_cons)
        recv_topic, recv_payload, qos, dup, retain, packet_id, props = validate_publish(publish_packet)
        
        assert recv_topic == topic, f"Topic mismatch: {recv_topic}"
        assert recv_payload == payload, f"Payload mismatch: {recv_payload}"
        assert qos == 0, f"Expected QoS 0, got {qos}"
        assert not dup, "DUP must be 0 for QoS 0"
        assert packet_id is None, "Packet ID must be None for QoS 0"
        
        print("✓ QoS 0 Publish packet validated successfully")
        
        # Ensure no more data (like a PUBACK which would be wrong for QoS 0)
        extra = recv_packet(s_cons, timeout=1.0)
        assert extra is None, f"Should not receive extra data after QoS 0, got: {extra}"
            
    print("✓ test_qos0_consumer passed")


def test_qos0_no_redelivery():
    """
    Test: QoS 0 messages are NOT redelivered after disconnect.
    At-most-once semantics means messages are lost if consumer is offline.
    """
    host, port = get_broker_config()
    topic = "test/qos0_noredeliver"
    payload = b"qos0 no redeliver"
    client_id = "qos0_offline_consumer"
    
    # 1. Consumer connects with session, subscribes, then disconnects
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet(client_id, clean_start=True, properties={0x11: 3600}))
        recv_packet(s_cons)
        s_cons.sendall(create_subscribe_packet(5, topic, qos=0))
        recv_packet(s_cons)
    
    # 2. Publish while consumer is offline
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
        s_pub.connect((host, port))
        s_pub.sendall(create_connect_packet("qos0_publisher"))
        recv_packet(s_pub)
        s_pub.sendall(create_publish_packet(topic, payload, qos=0))
    
    # 3. Consumer reconnects - should NOT receive the QoS 0 message
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet(client_id, clean_start=False))
        connack = recv_packet(s_cons)
        session_present, reason_code, props = validate_connack(connack)
        
        # Should not receive QoS 0 message (it was not queued)
        pub = recv_packet(s_cons, timeout=1.0)
        assert pub is None, f"QoS 0 should NOT be redelivered, got: {pub}"
    
    print("✓ test_qos0_no_redelivery passed")


def test_qos0_downgrade_from_qos1():
    """
    Test: Subscribe QoS 0, publish QoS 1, verify received as QoS 0.
    Broker should downgrade to match subscription QoS.
    """
    host, port = get_broker_config()
    topic = "test/qos0_downgrade"
    payload = b"downgrade test"
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet("qos0_downgrade_cons", clean_start=True))
        recv_packet(s_cons)
        
        # Subscribe with QoS 0
        s_cons.sendall(create_subscribe_packet(10, topic, qos=0))
        suback = recv_packet(s_cons)
        reason_codes = validate_suback(suback, 10)
        assert reason_codes[0] == ReasonCode.GRANTED_QOS_0
        
        # Publish with QoS 1
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
            s_pub.connect((host, port))
            s_pub.sendall(create_connect_packet("qos0_downgrade_pub"))
            recv_packet(s_pub)
            s_pub.sendall(create_publish_packet(topic, payload, qos=1, packet_id=100))
            recv_packet(s_pub)  # PUBACK from broker
        
        # Consumer should receive as QoS 0
        pub = recv_packet(s_cons)
        recv_topic, recv_payload, qos, dup, retain, packet_id, props = validate_publish(pub)
        
        assert recv_topic == topic
        assert recv_payload == payload
        assert qos == 0, f"Expected QoS 0 (downgraded), got QoS {qos}"
        assert packet_id is None, "QoS 0 should not have packet ID"
        
        print("✓ QoS 1 publish correctly downgraded to QoS 0")
    
    print("✓ test_qos0_downgrade_from_qos1 passed")


if __name__ == "__main__":
    test_qos0_consumer()
    test_qos0_no_redelivery()
    test_qos0_downgrade_from_qos1()
    print("\nAll QoS 0 tests passed!")
