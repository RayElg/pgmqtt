#!/usr/bin/env python3
"""
Integration test for MQTT Keep-Alive timeouts.
"""

import socket
import time
import sys
import os

# Add the current directory to sys.path to import proto_utils

from proto_utils import (
    create_connect_packet,
    create_subscribe_packet,
    recv_packet,
    validate_connack,
    validate_suback,
    validate_publish,
    MQTT_HOST,
    MQTT_PORT
)

def test_keepalive_timeout():
    print("\n[Keep-Alive Test] Verifying LWT publication on keep-alive timeout")
    
    topic = "lwt/keepalive"
    payload = b"client-a-timed-out"
    # Set a very short keep-alive for testing: 2 seconds
    keep_alive = 2
    
    # 1. Start Client B (subscriber)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_b:
        s_b.settimeout(10)
        s_b.connect((MQTT_HOST, MQTT_PORT))
        s_b.sendall(create_connect_packet("client_b_ka"))
        validate_connack(recv_packet(s_b))
        
        # Subscribe to LWT topic
        pid = 1
        s_b.sendall(create_subscribe_packet(pid, topic, qos=0))
        validate_suback(recv_packet(s_b), pid)
        print("  ✓ Client B subscribed to LWT topic")
        
        # 2. Start Client A with short Keep-Alive and LWT
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_a:
            s_a.settimeout(5)
            s_a.connect((MQTT_HOST, MQTT_PORT))
            
            # Connect with Will and 2s Keep-Alive
            connect_pkt = create_connect_packet(
                "client_a_ka",
                keep_alive=keep_alive,
                will_topic=topic,
                will_payload=payload,
                will_qos=0,
                will_retain=False
            )
            s_a.sendall(connect_pkt)
            validate_connack(recv_packet(s_a))
            print(f"  ✓ Client A connected (Keep-Alive={keep_alive}s) with Will message")
            
            # 3. Do nothing and wait for timeout (1.5 * 2s = 3s)
            print("  ! Client A going silent, waiting for broker to timeout (should take ~3s)...")
            time.sleep(4)
            
        # 4. Verify Client B receives LWT
        print("  ! Checking if Client B received Will...")
        will_received = False
        start_time = time.time()
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
        
        assert will_received, "Client B did not receive Will message after Keep-Alive timeout"

if __name__ == "__main__":
    try:
        test_keepalive_timeout()
        print("\n✓ Keep-Alive test passed")
    except Exception as e:
        print(f"\n✖ Keep-Alive test failed: {e}")
        sys.exit(1)
