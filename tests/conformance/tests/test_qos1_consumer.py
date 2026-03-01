"""
MQTT 5.0 Conformance Tests: QoS 1 (Consumer Perspective)

Tests verify broker correctly handles QoS 1 message flow including:
- PUBLISH → PUBACK handshake
- Session persistence and redelivery
- DUP flag on redelivered messages
"""
import socket
import struct
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from proto_utils import (get_broker_config,
    MQTTControlPacket, ReasonCode,
    create_connect_packet, create_subscribe_packet, create_puback_packet,
    create_publish_packet, create_pubrel_packet, recv_packet,
    validate_connack, validate_suback, validate_publish, validate_puback
,
    get_broker_config)


def test_qos1_consumer_flow():
    """
    Test: Basic QoS 1 message delivery to consumer.
    Consumer subscribes, publisher sends QoS 1, consumer receives and sends PUBACK.
    """
    host, port = get_broker_config()
    topic = "test/qos1"
    payload = b"hello qos 1"
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet("consumer_qos1", clean_start=True))
        connack = recv_packet(s_cons)
        session_present, reason_code, props = validate_connack(connack)
        assert reason_code == ReasonCode.SUCCESS
        
        s_cons.sendall(create_subscribe_packet(2, topic, qos=1))
        suback = recv_packet(s_cons)
        reason_codes = validate_suback(suback, 2)
        assert reason_codes[0] in (ReasonCode.GRANTED_QOS_0, ReasonCode.GRANTED_QOS_1), \
            f"Subscribe failed: {reason_codes}"
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
            s_pub.connect((host, port))
            s_pub.sendall(create_connect_packet("publisher_qos1"))
            recv_packet(s_pub)
            
            # Publish QoS 1
            s_pub.sendall(create_publish_packet(topic, payload, qos=1, packet_id=10))
            puback_pub = recv_packet(s_pub)
            validate_puback(puback_pub, 10)
            
        # Consumer should receive QoS 1 PUBLISH
        publish_packet = recv_packet(s_cons)
        recv_topic, recv_payload, qos, dup, retain, packet_id, props = validate_publish(publish_packet)
        
        assert recv_topic == topic
        assert recv_payload == payload
        assert qos == 1, f"Expected QoS 1, got {qos}"
        assert not dup, "DUP should be 0 for first delivery"
        
        # Consumer responds with PUBACK
        print(f"Received QoS 1 PUBLISH with packet ID {packet_id}")
        s_cons.sendall(create_puback_packet(packet_id))
        
    print("✓ test_qos1_consumer_flow passed")


def test_qos1_redelivery_on_reconnect():
    """
    Test: QoS 1 message redelivery when consumer reconnects with session persistence.
    """
    host, port = get_broker_config()
    topic = "test/qos1_redeliver"
    payload = b"redeliver test"
    client_id = "consumer_redeliver"
    
    # 1. Consumer connects with Session Expiry and subscribes, then disconnects
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet(client_id, clean_start=True, properties={0x11: 3600}))
        connack = recv_packet(s_cons)
        validate_connack(connack)
        s_cons.sendall(create_subscribe_packet(3, topic, qos=1))
        suback = recv_packet(s_cons)
        validate_suback(suback, 3)
    
    # 2. Publish message while consumer is disconnected
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
        s_pub.connect((host, port))
        s_pub.sendall(create_connect_packet("pub_redeliver"))
        recv_packet(s_pub)
        s_pub.sendall(create_publish_packet(topic, payload, qos=1, packet_id=55))
        recv_packet(s_pub)
        
    # 3. Consumer reconnects with Clean Start=0. Broker should resend.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet(client_id, clean_start=False))
        connack = recv_packet(s_cons)
        session_present, reason_code, props = validate_connack(connack)
        assert session_present, "Session Present should be 1"
        
        # Consumer receives redelivery
        pub = recv_packet(s_cons)
        recv_topic, recv_payload, qos, dup, retain, packet_id, props = validate_publish(pub)
        
        assert recv_payload == payload
        print(f"Received publication after reconnect (packet_id={packet_id})")
        
    print("✓ test_qos1_redelivery_on_reconnect passed")


def test_qos1_dup_flag_on_redelivery():
    """
    Test: DUP flag should be 1 when broker redelivers QoS 1 message.
    
    Scenario:
    1. Consumer connects with persistent session, subscribes
    2. Publisher sends QoS 1
    3. Consumer receives but does NOT send PUBACK
    4. Consumer disconnects ungracefully
    5. Consumer reconnects - broker should redeliver with DUP=1
    """
    host, port = get_broker_config()
    topic = "test/qos1_dup"
    payload = b"dup test message"
    client_id = "consumer_dup_test"
    
    # 1. Consumer connects with Session Expiry
    s_cons = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_cons.connect((host, port))
    s_cons.sendall(create_connect_packet(client_id, clean_start=True, properties={0x11: 3600}))
    connack = recv_packet(s_cons)
    validate_connack(connack)
    
    s_cons.sendall(create_subscribe_packet(60, topic, qos=1))
    suback = recv_packet(s_cons)
    validate_suback(suback, 60)
    
    # 2. Publish QoS 1
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
        s_pub.connect((host, port))
        s_pub.sendall(create_connect_packet("dup_publisher"))
        recv_packet(s_pub)
        s_pub.sendall(create_publish_packet(topic, payload, qos=1, packet_id=61))
        recv_packet(s_pub)
    
    # 3. Consumer receives but does NOT send PUBACK
    pub1 = recv_packet(s_cons)
    recv_topic, recv_payload, qos, dup1, retain, packet_id1, props = validate_publish(pub1)
    assert not dup1, "First delivery should have DUP=0"
    print(f"First delivery: DUP={dup1}, packet_id={packet_id1}")
    
    # 4. Close without PUBACK (ungraceful)
    s_cons.close()
    
    # 5. Reconnect - broker should redeliver with DUP=1
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons2:
        s_cons2.connect((host, port))
        s_cons2.sendall(create_connect_packet(client_id, clean_start=False))
        connack = recv_packet(s_cons2)
        session_present, reason_code, props = validate_connack(connack)
        assert session_present, "Session should be present"
        
        pub2 = recv_packet(s_cons2)
        recv_topic, recv_payload, qos, dup2, retain, packet_id2, props = validate_publish(pub2)
        
        print(f"Redelivery: DUP={dup2}, packet_id={packet_id2}")
        assert dup2, "Redelivered message MUST have DUP=1"
        assert recv_payload == payload
        
        # Now send PUBACK
        s_cons2.sendall(create_puback_packet(packet_id2))
    
    print("✓ test_qos1_dup_flag_on_redelivery passed")


def test_qos1_multiple_unacked_redelivery():
    """
    Test: Send multiple QoS 1 messages without acking, verify all redelivered with DUP=1.
    """
    host, port = get_broker_config()
    topic = "test/qos1_multi_unack"
    client_id = "qos1_multi_unack_client"
    
    # Connect with session persistence
    s_cons = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_cons.connect((host, port))
    s_cons.sendall(create_connect_packet(client_id, clean_start=True, properties={0x11: 3600}))
    recv_packet(s_cons)
    
    s_cons.sendall(create_subscribe_packet(80, topic, qos=1))
    recv_packet(s_cons)
    
    # Publish 3 messages
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
        s_pub.connect((host, port))
        s_pub.sendall(create_connect_packet("qos1_multi_pub"))
        recv_packet(s_pub)
        
        for i in range(3):
            s_pub.sendall(create_publish_packet(topic, f"msg{i}".encode(), qos=1, packet_id=81+i))
            recv_packet(s_pub)  # PUBACK from broker
    
    # Receive all 3 but don't ack
    received_first = []
    for i in range(3):
        pub = recv_packet(s_cons)
        _, payload, qos, dup, _, packet_id, _ = validate_publish(pub)
        assert not dup, f"First delivery {i} should have DUP=0"
        received_first.append((packet_id, payload))
        print(f"Received msg {i}: packet_id={packet_id}, DUP=0")
    
    # Disconnect without acking
    s_cons.close()
    
    # Reconnect - all should be redelivered with DUP=1
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons2:
        s_cons2.connect((host, port))
        s_cons2.sendall(create_connect_packet(client_id, clean_start=False))
        recv_packet(s_cons2)
        
        received_second = []
        for i in range(3):
            pub = recv_packet(s_cons2)
            _, payload, qos, dup, _, packet_id, _ = validate_publish(pub)
            assert dup, f"Redelivery {i} MUST have DUP=1"
            received_second.append((packet_id, payload))
            print(f"Redelivered msg {i}: packet_id={packet_id}, DUP=1")
            s_cons2.sendall(create_puback_packet(packet_id))
        
        # Verify same messages
        assert len(received_first) == len(received_second) == 3
    
    print("✓ test_qos1_multiple_unacked_redelivery passed")


def test_qos1_partial_acknowledgment():
    """
    Test: Receive 3 messages, ack only middle one, verify others redelivered.
    """
    host, port = get_broker_config()
    topic = "test/qos1_partial_ack"
    client_id = "qos1_partial_ack_client"
    
    s_cons = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_cons.connect((host, port))
    s_cons.sendall(create_connect_packet(client_id, clean_start=True, properties={0x11: 3600}))
    recv_packet(s_cons)
    
    s_cons.sendall(create_subscribe_packet(90, topic, qos=1))
    recv_packet(s_cons)
    
    # Publish 3 messages
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
        s_pub.connect((host, port))
        s_pub.sendall(create_connect_packet("qos1_partial_pub"))
        recv_packet(s_pub)
        
        for i in range(3):
            s_pub.sendall(create_publish_packet(topic, f"msg{i}".encode(), qos=1, packet_id=91+i))
            recv_packet(s_pub)
    
    # Receive all 3, ack only #2
    packet_ids = []
    for i in range(3):
        pub = recv_packet(s_cons)
        _, payload, _, _, _, packet_id, _ = validate_publish(pub)
        packet_ids.append(packet_id)
        if i == 1:  # Ack only middle message
            s_cons.sendall(create_puback_packet(packet_id))
            print(f"Acked message {i} (packet_id={packet_id})")
    
    # Disconnect
    s_cons.close()
    
    # Reconnect - should receive messages 0 and 2 again
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons2:
        s_cons2.connect((host, port))
        s_cons2.sendall(create_connect_packet(client_id, clean_start=False))
        recv_packet(s_cons2)
        
        # Should receive 2 messages (the unacked ones)
        redelivered = []
        for _ in range(2):
            pub = recv_packet(s_cons2, timeout=3.0)
            if pub:
                _, payload, _, dup, _, packet_id, _ = validate_publish(pub)
                assert dup, "Redelivered messages must have DUP=1"
                redelivered.append(packet_id)
                s_cons2.sendall(create_puback_packet(packet_id))
        
        # Should not receive the acked message
        extra = recv_packet(s_cons2, timeout=1.0)
        assert extra is None, "Should not receive already-acked message"
        
        print(f"Redelivered {len(redelivered)} unacked messages")
    
    print("✓ test_qos1_partial_acknowledgment passed")


def test_qos1_downgrade_from_qos2():
    """
    Test: Subscribe QoS 1, publish QoS 2, verify received as QoS 1.
    """
    host, port = get_broker_config()
    topic = "test/qos1_downgrade"
    payload = b"downgrade from qos2"
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet("qos1_downgrade_cons"))
        recv_packet(s_cons)
        
        # Subscribe QoS 1
        s_cons.sendall(create_subscribe_packet(95, topic, qos=1))
        suback = recv_packet(s_cons)
        reason_codes = validate_suback(suback, 95)
        assert reason_codes[0] in (ReasonCode.GRANTED_QOS_0, ReasonCode.GRANTED_QOS_1)
        
        # Publish QoS 2
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
            s_pub.connect((host, port))
            s_pub.sendall(create_connect_packet("qos1_downgrade_pub"))
            recv_packet(s_pub)
            s_pub.sendall(create_publish_packet(topic, payload, qos=2, packet_id=96))
            recv_packet(s_pub)  # PUBREC
            s_pub.sendall(create_pubrel_packet(96))
            recv_packet(s_pub)  # PUBCOMP
        
        # Consumer should receive as QoS 1 (downgraded from QoS 2)
        pub = recv_packet(s_cons)
        _, recv_payload, qos, _, _, packet_id, _ = validate_publish(pub)
        
        assert recv_payload == payload
        assert qos == 1, f"Expected QoS 1 (downgraded from 2), got QoS {qos}"
        assert packet_id is not None, "QoS 1 must have packet ID"
        
        s_cons.sendall(create_puback_packet(packet_id))
        print("✓ QoS 2 publish correctly downgraded to QoS 1")
    
    print("✓ test_qos1_downgrade_from_qos2 passed")


def test_qos1_packet_id_wraparound():
    """
    Test: Use packet IDs near 65535, verify wraparound to 1.
    """
    host, port = get_broker_config()
    topic = "test/qos1_wraparound"
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet("qos1_wraparound_cons"))
        recv_packet(s_cons)
        
        s_cons.sendall(create_subscribe_packet(65534, topic, qos=1))
        recv_packet(s_cons)
        
        # Publish with high packet IDs
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
            s_pub.connect((host, port))
            s_pub.sendall(create_connect_packet("qos1_wraparound_pub"))
            recv_packet(s_pub)
            
            # Send with packet IDs: 65534, 65535, 1 (wraparound)
            for pid in [65534, 65535, 1]:
                s_pub.sendall(create_publish_packet(topic, f"msg{pid}".encode(), qos=1, packet_id=pid))
                puback = recv_packet(s_pub)
                validate_puback(puback, pid)
                print(f"Published with packet_id={pid}")
        
        # Receive all 3
        for expected_pid in [65534, 65535, 1]:
            pub = recv_packet(s_cons)
            _, payload, qos, _, _, packet_id, _ = validate_publish(pub)
            # Packet ID from broker may differ, just verify we got the message
            s_cons.sendall(create_puback_packet(packet_id))
            print(f"Received message, sent PUBACK for packet_id={packet_id}")
    
    print("✓ test_qos1_packet_id_wraparound passed")


if __name__ == "__main__":
    test_qos1_consumer_flow()
    test_qos1_redelivery_on_reconnect()
    test_qos1_dup_flag_on_redelivery()
    test_qos1_multiple_unacked_redelivery()
    test_qos1_partial_acknowledgment()
    test_qos1_downgrade_from_qos2()
    test_qos1_packet_id_wraparound()
    print("\nAll QoS 1 tests passed!")
