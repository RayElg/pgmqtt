#!/usr/bin/env python3
"""
Integration test for MQTT Last Will and Testament (LWT).
"""

import socket
import time
import sys
import os

# Add the current directory to sys.path to import proto_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from proto_utils import (
    create_connect_packet,
    create_subscribe_packet,
    create_disconnect_packet,
    recv_packet,
    validate_connack,
    validate_suback,
    validate_publish,
    MQTT_HOST,
    MQTT_PORT,
    ReasonCode
)

def test_lwt_abrupt_disconnect():
    print("\n[LWT Test] Verifying LWT publication on abrupt disconnect")
    
    topic = "lwt/abrupt"
    payload = b"client-a-died-abruptly"
    qos = 0
    
    # 1. Start Client B (subscriber)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_b:
        s_b.settimeout(5)
        s_b.connect((MQTT_HOST, MQTT_PORT))
        s_b.sendall(create_connect_packet("client_b_abrupt"))
        resp = recv_packet(s_b)
        validate_connack(resp)
        
        # Subscribe to LWT topic
        pid = 1
        s_b.sendall(create_subscribe_packet(pid, topic, qos=0))
        resp = recv_packet(s_b)
        validate_suback(resp, pid)
        print("  ✓ Client B subscribed to LWT topic")
        
        # 2. Start Client A with LWT
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_a:
            s_a.settimeout(5)
            s_a.connect((MQTT_HOST, MQTT_PORT))
            
            # Connect with Will
            connect_pkt = create_connect_packet(
                "client_a_abrupt",
                will_topic=topic,
                will_payload=payload,
                will_qos=qos,
                will_retain=False
            )
            s_a.sendall(connect_pkt)
            resp = recv_packet(s_a)
            validate_connack(resp)
            print("  ✓ Client A connected with Will message")
            
            # 3. Abruptly close Client A
            print("  ! Abruptly closing Client A (socket close)...")
            s_a.close()
            
        # 4. Wait for Client B to receive LWT
        print("  ! Waiting for Client B to receive Will...")
        start_time = time.time()
        will_received = False
        while time.time() - start_time < 5:
            resp = recv_packet(s_b, timeout=2)
            if resp:
                try:
                    t, p, q, d, r, _, _ = validate_publish(resp)
                    if t == topic and p == payload:
                        print(f"  ✓ Client B received Will: topic={t}, payload={p}")
                        will_received = True
                        break
                except AssertionError:
                    continue
        
        assert will_received, "Client B did not receive Will message after abrupt disconnect"

def test_lwt_clean_disconnect():
    print("\n[LWT Test] Verifying LWT NOT published on clean DISCONNECT")
    
    topic = "lwt/clean"
    payload = b"should-not-see-this"
    qos = 0
    
    # 1. Start Client B (subscriber)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_b:
        s_b.settimeout(5)
        s_b.connect((MQTT_HOST, MQTT_PORT))
        s_b.sendall(create_connect_packet("client_b_clean"))
        resp = recv_packet(s_b)
        validate_connack(resp)
        
        # Subscribe to LWT topic
        pid = 1
        s_b.sendall(create_subscribe_packet(pid, topic, qos=0))
        resp = recv_packet(s_b)
        validate_suback(resp, pid)
        
        # 2. Start Client A with LWT
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_a:
            s_a.settimeout(5)
            s_a.connect((MQTT_HOST, MQTT_PORT))
            
            # Connect with Will
            connect_pkt = create_connect_packet(
                "client_a_clean",
                will_topic=topic,
                will_payload=payload,
                will_qos=qos,
                will_retain=False
            )
            s_a.sendall(connect_pkt)
            resp = recv_packet(s_a)
            validate_connack(resp)
            
            # 3. Send clean DISCONNECT
            print("  ! Sending clean DISCONNECT from Client A...")
            s_a.sendall(create_disconnect_packet(ReasonCode.NORMAL_DISCONNECTION))
            time.sleep(0.1)
            s_a.close()
            
        # 4. Wait to ensure Client B receives NOTHING
        print("  ! Waiting to ensure Client B receives NOTHING...")
        resp = recv_packet(s_b, timeout=2)
        assert resp is None, f"Client B received unexpected packet: {resp}"
        print("  ✓ Client B received nothing (as expected)")

if __name__ == "__main__":
    try:
        test_lwt_abrupt_disconnect()
        test_lwt_clean_disconnect()
        print("\n✓ All LWT tests passed")
    except Exception as e:
        print(f"\n✖ LWT test failed: {e}")
        sys.exit(1)
