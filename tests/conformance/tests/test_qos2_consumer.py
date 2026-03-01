"""
MQTT 5.0 Conformance Tests: QoS 2 (Consumer Perspective)

Tests verify broker correctly handles QoS 2 message flow including:
- Complete 4-way handshake: PUBLISH → PUBREC → PUBREL → PUBCOMP
- Fixed header flags validation per Table 2-2
- DUP flag on redelivered messages
"""
import socket
import struct
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from proto_utils import (get_broker_config,
    MQTTControlPacket, ReasonCode,
    create_connect_packet, create_subscribe_packet, create_publish_packet,
    create_pubrec_packet, create_pubrel_packet, create_pubcomp_packet,
    recv_packet,
    validate_connack, validate_suback, validate_publish,
    validate_pubrec, validate_pubrel, validate_pubcomp
,
    get_broker_config)


def test_qos2_consumer_flow():
    """
    Test: Complete QoS 2 message flow from consumer's perspective.
    Publisher → Broker: PUBLISH → PUBREC → PUBREL → PUBCOMP
    Broker → Consumer: PUBLISH → PUBREC → PUBREL → PUBCOMP
    """
    host, port = get_broker_config()
    topic = "test/qos2"
    payload = b"hello qos 2"
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet("consumer_qos2", clean_start=True))
        connack = recv_packet(s_cons)
        session_present, reason_code, props = validate_connack(connack)
        assert reason_code == ReasonCode.SUCCESS
        
        s_cons.sendall(create_subscribe_packet(4, topic, qos=2))
        suback = recv_packet(s_cons)
        reason_codes = validate_suback(suback, 4)
        assert reason_codes[0] in (ReasonCode.GRANTED_QOS_0, ReasonCode.GRANTED_QOS_1, ReasonCode.GRANTED_QOS_2)
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
            s_pub.connect((host, port))
            s_pub.sendall(create_connect_packet("publisher_qos2"))
            recv_packet(s_pub)
            
            # Publish QoS 2
            packet_id = 20
            s_pub.sendall(create_publish_packet(topic, payload, qos=2, packet_id=packet_id))
            
            # Publisher expects PUBREC from broker (flags must be 0x00)
            pubrec = recv_packet(s_pub)
            validate_pubrec(pubrec, packet_id)
            
            # Publisher sends PUBREL to broker (flags must be 0x02)
            s_pub.sendall(create_pubrel_packet(packet_id))
            
            # Publisher expects PUBCOMP from broker (flags must be 0x00)
            pubcomp = recv_packet(s_pub)
            validate_pubcomp(pubcomp, packet_id)
            
        # Consumer should receive QoS 2 PUBLISH
        publish_packet = recv_packet(s_cons)
        recv_topic, recv_payload, qos, dup, retain, recv_packet_id, props = validate_publish(publish_packet)
        
        assert recv_topic == topic
        assert recv_payload == payload
        assert qos == 2, f"Expected QoS 2, got {qos}"
        assert not dup, "DUP should be 0 for first delivery"
        
        print(f"Received QoS 2 PUBLISH with packet ID {recv_packet_id}")
        
        # Consumer responds with PUBREC
        s_cons.sendall(create_pubrec_packet(recv_packet_id))
        
        # Consumer expects PUBREL (flags must be 0x02)
        pubrel_received = recv_packet(s_cons)
        validate_pubrel(pubrel_received, recv_packet_id)
        print(f"Received PUBREL for packet ID {recv_packet_id}")
        
        # Consumer responds with PUBCOMP
        s_cons.sendall(create_pubcomp_packet(recv_packet_id))
        
    print("✓ test_qos2_consumer_flow passed")


def test_qos2_redelivery_before_pubcomp():
    """
    Test: QoS 2 redelivery when consumer disconnects before completing handshake.
    
    Scenario:
    1. Consumer subscribes with persistent session
    2. Publisher sends QoS 2
    3. Consumer receives PUBLISH, sends PUBREC, receives PUBREL
    4. Consumer disconnects WITHOUT sending PUBCOMP
    5. Consumer reconnects - broker should resend PUBREL (or PUBLISH with DUP=1)
    """
    host, port = get_broker_config()
    topic = "test/qos2_redeliver"
    payload = b"qos2 redeliver"
    client_id = "consumer_qos2_redeliver"
    
    # 1. Consumer connects with session persistence
    s_cons = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_cons.connect((host, port))
    s_cons.sendall(create_connect_packet(client_id, clean_start=True, properties={0x11: 3600}))
    connack = recv_packet(s_cons)
    validate_connack(connack)
    
    s_cons.sendall(create_subscribe_packet(70, topic, qos=2))
    suback = recv_packet(s_cons)
    validate_suback(suback, 70)
    
    # 2. Publisher sends QoS 2
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
        s_pub.connect((host, port))
        s_pub.sendall(create_connect_packet("qos2_redeliver_pub"))
        recv_packet(s_pub)
        s_pub.sendall(create_publish_packet(topic, payload, qos=2, packet_id=71))
        recv_packet(s_pub)  # PUBREC
        s_pub.sendall(create_pubrel_packet(71))
        recv_packet(s_pub)  # PUBCOMP
    
    # 3. Consumer receives PUBLISH
    pub1 = recv_packet(s_cons)
    recv_topic, recv_payload, qos, dup1, retain, packet_id1, props = validate_publish(pub1)
    assert not dup1, "First delivery should have DUP=0"
    print(f"First delivery: DUP={dup1}, packet_id={packet_id1}")
    
    # Send PUBREC
    s_cons.sendall(create_pubrec_packet(packet_id1))
    
    # Receive PUBREL
    pubrel = recv_packet(s_cons)
    validate_pubrel(pubrel, packet_id1)
    
    # 4. Disconnect WITHOUT sending PUBCOMP
    s_cons.close()
    
    # 5. Reconnect - broker should resend PUBREL (state is at PUBREL stage)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons2:
        s_cons2.connect((host, port))
        s_cons2.sendall(create_connect_packet(client_id, clean_start=False))
        connack = recv_packet(s_cons2)
        session_present, reason_code, props = validate_connack(connack)
        assert session_present, "Session should be present"
        
        # Broker should resend PUBREL since we didn't complete with PUBCOMP
        packet = recv_packet(s_cons2)
        
        # Could be PUBREL (if broker tracks that PUBREC was sent) or PUBLISH with DUP=1
        packet_type = packet[0] >> 4
        
        if packet_type == MQTTControlPacket.PUBREL:
            validate_pubrel(packet, packet_id1)
            print("Broker resent PUBREL as expected")
            s_cons2.sendall(create_pubcomp_packet(packet_id1))
        elif packet_type == MQTTControlPacket.PUBLISH:
            recv_topic, recv_payload, qos, dup2, retain, packet_id2, props = validate_publish(packet)
            print(f"Broker resent PUBLISH: DUP={dup2}, packet_id={packet_id2}")
            assert dup2, "Redelivered PUBLISH MUST have DUP=1"
            # Complete handshake
            s_cons2.sendall(create_pubrec_packet(packet_id2))
            pubrel = recv_packet(s_cons2)
            validate_pubrel(pubrel, packet_id2)
            s_cons2.sendall(create_pubcomp_packet(packet_id2))
        else:
            raise AssertionError(f"Unexpected packet type: {packet_type}")
    
    print("✓ test_qos2_redelivery_before_pubcomp passed")


def test_qos2_disconnect_before_pubrec():
    """
    Test: Consumer disconnects after receiving PUBLISH but before sending PUBREC.
    On reconnect, PUBLISH should be redelivered with DUP=1.
    """
    host, port = get_broker_config()
    topic = "test/qos2_before_pubrec"
    payload = b"disconnect before pubrec"
    client_id = "qos2_before_pubrec_client"
    
    # Connect and subscribe
    s_cons = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_cons.connect((host, port))
    s_cons.sendall(create_connect_packet(client_id, clean_start=True, properties={0x11: 3600}))
    recv_packet(s_cons)
    
    s_cons.sendall(create_subscribe_packet(110, topic, qos=2))
    recv_packet(s_cons)
    
    # Publish QoS 2
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
        s_pub.connect((host, port))
        s_pub.sendall(create_connect_packet("qos2_before_pubrec_pub"))
        recv_packet(s_pub)
        s_pub.sendall(create_publish_packet(topic, payload, qos=2, packet_id=111))
        recv_packet(s_pub)  # PUBREC
        s_pub.sendall(create_pubrel_packet(111))
        recv_packet(s_pub)  # PUBCOMP
    
    # Receive PUBLISH but don't send PUBREC
    pub1 = recv_packet(s_cons)
    _, _, _, dup1, _, packet_id1, _ = validate_publish(pub1)
    assert not dup1, "First delivery should have DUP=0"
    print(f"Received PUBLISH (packet_id={packet_id1}), disconnecting before PUBREC")
    
    # Disconnect without sending PUBREC
    s_cons.close()
    
    # Reconnect - should receive PUBLISH again with DUP=1
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons2:
        s_cons2.connect((host, port))
        s_cons2.sendall(create_connect_packet(client_id, clean_start=False))
        recv_packet(s_cons2)
        
        pub2 = recv_packet(s_cons2)
        _, recv_payload, qos, dup2, _, packet_id2, _ = validate_publish(pub2)
        
        assert dup2, "Redelivered PUBLISH MUST have DUP=1"
        assert recv_payload == payload
        print(f"Redelivered PUBLISH with DUP=1 (packet_id={packet_id2})")
        
        # Complete handshake
        s_cons2.sendall(create_pubrec_packet(packet_id2))
        pubrel = recv_packet(s_cons2)
        validate_pubrel(pubrel, packet_id2)
        s_cons2.sendall(create_pubcomp_packet(packet_id2))
    
    print("✓ test_qos2_disconnect_before_pubrec passed")


def test_qos2_duplicate_pubrec():
    """
    Test: Send PUBREC twice for same message, verify broker handles gracefully.
    """
    host, port = get_broker_config()
    topic = "test/qos2_dup_pubrec"
    payload = b"duplicate pubrec test"
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet("qos2_dup_pubrec_cons"))
        recv_packet(s_cons)
        
        s_cons.sendall(create_subscribe_packet(120, topic, qos=2))
        recv_packet(s_cons)
        
        # Publish QoS 2
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
            s_pub.connect((host, port))
            s_pub.sendall(create_connect_packet("qos2_dup_pubrec_pub"))
            recv_packet(s_pub)
            s_pub.sendall(create_publish_packet(topic, payload, qos=2, packet_id=121))
            recv_packet(s_pub)
            s_pub.sendall(create_pubrel_packet(121))
            recv_packet(s_pub)
        
        # Receive PUBLISH
        pub = recv_packet(s_cons)
        _, _, _, _, _, packet_id, _ = validate_publish(pub)
        
        # Send PUBREC twice
        s_cons.sendall(create_pubrec_packet(packet_id))
        s_cons.sendall(create_pubrec_packet(packet_id))
        print("Sent PUBREC twice")
        
        # Should receive PUBREL (broker may ignore duplicate PUBREC)
        pubrel = recv_packet(s_cons, timeout=3.0)
        if pubrel:
            validate_pubrel(pubrel, packet_id)
            s_cons.sendall(create_pubcomp_packet(packet_id))
            print("✓ Broker handled duplicate PUBREC gracefully")
        
        # Check for any extra PUBREL
        extra = recv_packet(s_cons, timeout=1.0)
        if extra and extra[0] >> 4 == MQTTControlPacket.PUBREL:
            print("⚠ Broker sent duplicate PUBREL (acceptable)")
            s_cons.sendall(create_pubcomp_packet(packet_id))
    
    print("✓ test_qos2_duplicate_pubrec passed")


def test_qos2_multiple_inflight():
    """
    Test: Send multiple QoS 2 messages, complete handshakes in different orders.
    """
    host, port = get_broker_config()
    topic = "test/qos2_multi"
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet("qos2_multi_cons"))
        recv_packet(s_cons)
        
        s_cons.sendall(create_subscribe_packet(130, topic, qos=2))
        recv_packet(s_cons)
        
        # Publish 3 QoS 2 messages
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
            s_pub.connect((host, port))
            s_pub.sendall(create_connect_packet("qos2_multi_pub"))
            recv_packet(s_pub)
            
            for i in range(3):
                s_pub.sendall(create_publish_packet(topic, f"msg{i}".encode(), qos=2, packet_id=131+i))
                recv_packet(s_pub)  # PUBREC
                s_pub.sendall(create_pubrel_packet(131+i))
                recv_packet(s_pub)  # PUBCOMP
        
        # Receive all 3 PUBLISH packets
        received = []
        for _ in range(3):
            pub = recv_packet(s_cons)
            _, payload, _, _, _, packet_id, _ = validate_publish(pub)
            received.append((packet_id, payload))
        
        # Send PUBREC in reverse order
        for packet_id, _ in reversed(received):
            s_cons.sendall(create_pubrec_packet(packet_id))
            print(f"Sent PUBREC for packet_id={packet_id}")
        
        # Receive PUBREL and send PUBCOMP
        for _ in range(3):
            pubrel = recv_packet(s_cons)
            packet_id = struct.unpack_from('!H', pubrel, 2)[0]
            validate_pubrel(pubrel, packet_id)
            s_cons.sendall(create_pubcomp_packet(packet_id))
            print(f"Completed handshake for packet_id={packet_id}")
    
    print("✓ test_qos2_multiple_inflight passed")


def test_qos2_exactly_once_guarantee():
    """
    Test: Verify message delivered exactly once even with reconnects.
    """
    host, port = get_broker_config()
    topic = "test/qos2_exactly_once"
    payload = b"exactly once test"
    client_id = "qos2_exactly_once_client"
    
    # Track received messages
    received_payloads = []
    
    # First connection
    s_cons = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_cons.connect((host, port))
    s_cons.sendall(create_connect_packet(client_id, clean_start=True, properties={0x11: 3600}))
    recv_packet(s_cons)
    
    s_cons.sendall(create_subscribe_packet(140, topic, qos=2))
    recv_packet(s_cons)
    
    # Publish message
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
        s_pub.connect((host, port))
        s_pub.sendall(create_connect_packet("qos2_exactly_once_pub"))
        recv_packet(s_pub)
        s_pub.sendall(create_publish_packet(topic, payload, qos=2, packet_id=141))
        recv_packet(s_pub)
        s_pub.sendall(create_pubrel_packet(141))
        recv_packet(s_pub)
    
    # Receive and complete handshake
    pub = recv_packet(s_cons)
    _, recv_payload, _, _, _, packet_id, _ = validate_publish(pub)
    received_payloads.append(recv_payload)
    
    s_cons.sendall(create_pubrec_packet(packet_id))
    pubrel = recv_packet(s_cons)
    validate_pubrel(pubrel, packet_id)
    s_cons.sendall(create_pubcomp_packet(packet_id))
    s_cons.close()
    
    # Reconnect multiple times - should NOT receive message again
    for attempt in range(2):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons2:
            s_cons2.connect((host, port))
            s_cons2.sendall(create_connect_packet(client_id, clean_start=False))
            recv_packet(s_cons2)
            
            # Should not receive the message again
            pub = recv_packet(s_cons2, timeout=1.0)
            assert pub is None, f"Message delivered more than once (attempt {attempt+1})"
    
    assert len(received_payloads) == 1, f"Expected exactly 1 delivery, got {len(received_payloads)}"
    print("✓ Message delivered exactly once")
    
    print("✓ test_qos2_exactly_once_guarantee passed")


if __name__ == "__main__":
    test_qos2_consumer_flow()
    test_qos2_redelivery_before_pubcomp()
    test_qos2_disconnect_before_pubrec()
    test_qos2_duplicate_pubrec()
    test_qos2_multiple_inflight()
    test_qos2_exactly_once_guarantee()
    print("\nAll QoS 2 tests passed!")
