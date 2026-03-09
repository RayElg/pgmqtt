#!/usr/bin/env python3
"""
QoS 1 Integration Test for pgmqtt.
"""

import socket
import time
import json
from proto_utils import (
    create_connect_packet,
    create_subscribe_packet,
    create_publish_packet,
    create_puback_packet,
    recv_packet,
    validate_publish,
    validate_puback,
    run_psql,
    MQTT_HOST,
    MQTT_PORT,
    ReasonCode
)

# -- Tests --

def test_qos1_client_publish():
    print("Testing Client PUBLISH QoS 1 -> Broker PUBACK")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((MQTT_HOST, MQTT_PORT))
        s.sendall(create_connect_packet("pub_client"))
        recv_packet(s)
        
        # Subscribe to ensure persistence path is considered (optional but good for consistency)
        s.sendall(create_subscribe_packet(1, "test/qos1", qos=1))
        recv_packet(s)
        
        packet_id = 123
        s.sendall(create_publish_packet("test/qos1", b"hello", qos=1, packet_id=packet_id))
        resp = recv_packet(s)
        
        if resp:
            try:
                rc = validate_puback(resp, packet_id)
                if rc == ReasonCode.SUCCESS:
                    print(f"  ✓ Received PUBACK for packet_id {packet_id}")
                    return True
            except Exception as e:
                print(f"  ✗ PUBACK validation failed: {e}")
    print("  ✗ Failed to receive correct PUBACK")
    return False

def test_qos1_broker_publish():
    print("Testing Broker PUBLISH QoS 1 -> Client PUBACK")
    # Setup mapping
    run_psql("DROP TABLE IF EXISTS qos1_test;")
    run_psql("CREATE TABLE qos1_test (id serial primary key, name text, value text);")
    run_psql("ALTER TABLE qos1_test REPLICA IDENTITY FULL;")
    run_psql("SELECT pgmqtt_add_mapping('public', 'qos1_test', 'test/{{ columns.name }}', '{{ columns.value }}', 1);")
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((MQTT_HOST, MQTT_PORT))
        s.sendall(create_connect_packet("sub_client"))
        recv_packet(s)
        s.sendall(create_subscribe_packet(1, "test/#", qos=1))
        recv_packet(s)
        
        # Trigger publish
        run_psql("INSERT INTO qos1_test (name, value) VALUES ('qos1', 'data1');")
        
        pub_pkt = recv_packet(s, timeout=10)
        if not pub_pkt:
            print("  ✗ No PUBLISH received")
            return False
            
        topic, payload, qos, _, _, pid, _ = validate_publish(pub_pkt)
        print(f"  Received PUBLISH: topic={topic}, qos={qos}, pid={pid}")
        
        if qos == 1 and pid is not None:
            # Send PUBACK
            s.sendall(create_puback_packet(pid))
            print(f"  ✓ Sent PUBACK for packet_id {pid}")
            time.sleep(1)
            return True
            
    print("  ✗ Failed QoS 1 handshake")
    return False

def test_qos1_intermittent_connectivity():
    print("Testing Intermittent Connectivity (Disconnect before PUBACK) -> Redelivery")
    cid = "intermittent_client"
    
    # 1. Connect and subscribe with Clean Start = 1 to ensure a fresh session
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((MQTT_HOST, MQTT_PORT))
        s.sendall(create_connect_packet(cid, clean_start=True))
        recv_packet(s)
        s.sendall(create_subscribe_packet(1, "test/intermittent", qos=1))
        recv_packet(s)
        
        # Trigger publish
        run_psql("INSERT INTO qos1_test (name, value) VALUES ('intermittent', 'data2');")
        
        # Receive PUBLISH but DISCONNECT immediately (no PUBACK)
        pub_pkt = recv_packet(s, timeout=5)
        if not pub_pkt:
            print("  ✗ No PUBLISH received")
            return False
        
        _, _, _, _, _, pid, _ = validate_publish(pub_pkt)
        print(f"  Received PUBLISH: pid={pid}. Disconnecting now...")
    
    time.sleep(2)
    
    # 2. Reconnect with Clean Start = 0 to resume session
    print("  Reconnecting with Clean Start=0...")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s2:
        s2.connect((MQTT_HOST, MQTT_PORT))
        s2.sendall(create_connect_packet(cid, clean_start=False))
        connack = recv_packet(s2)
        
        # The broker should redeliver the message
        redelivered = recv_packet(s2, timeout=10)
        if redelivered:
            topic, payload, qos, dup, _, pid, _ = validate_publish(redelivered)
            print(f"  Received Packet: topic={topic}, qos={qos}, pid={pid}, dup={dup}")
            if pid is not None and dup:
                s2.sendall(create_puback_packet(pid))
                print("  ✓ Received redelivery with DUP=1 and acked it")
                return True
        else:
            print("  ✗ No redelivery received")
            
    return False

if __name__ == "__main__":
    c_pub = test_qos1_client_publish()
    b_pub = test_qos1_broker_publish()
    i_con = test_qos1_intermittent_connectivity()
    if c_pub and b_pub and i_con:
        print("\nALL QoS 1 TESTS PASSED")
    else:
        print("\nSOME TESTS FAILED")
        exit(1)
