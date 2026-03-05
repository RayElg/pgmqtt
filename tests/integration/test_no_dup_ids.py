#!/usr/bin/env python3
import socket
import sys
import time
import os
import concurrent.futures

# Add integration tests to path to import proto_utils
sys.path.append(os.path.join(os.path.dirname(__file__), 'integration'))

from proto_utils import (
    create_connect_packet,
    recv_packet,
    validate_connack,
    MQTT_HOST,
    MQTT_PORT,
    ReasonCode
)

def test_duplicate_ids():
    print("\n[Verification] Testing for unique auto-generated client IDs")
    
    # 1. Connect Client A
    print("Connecting Client A (auto ID)...")
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s1.settimeout(5)
    s1.connect((MQTT_HOST, MQTT_PORT))
    s1.sendall(create_connect_packet("")) # empty ID -> auto
    resp1 = recv_packet(s1)
    assert resp1 is not None, "Client A failed to get CONNACK"
    _, reason1, _ = validate_connack(resp1)
    assert reason1 == ReasonCode.SUCCESS
    print("  ✓ Client A connected")

    # 2. Connect Client B
    print("Connecting Client B (auto ID)...")
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.settimeout(5)
    s2.connect((MQTT_HOST, MQTT_PORT))
    s2.sendall(create_connect_packet("")) # empty ID -> auto
    resp2 = recv_packet(s2)
    assert resp2 is not None, "Client B failed to get CONNACK"
    _, reason2, _ = validate_connack(resp2)
    assert reason2 == ReasonCode.SUCCESS
    print("  ✓ Client B connected")

    # 3. Disconnect Client A
    print("Disconnecting Client A...")
    s1.close()
    time.sleep(1) # Wait for server to process disconnect

    # 4. Connect Client C
    print("Connecting Client C (auto ID)...")
    s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s3.settimeout(5)
    s3.connect((MQTT_HOST, MQTT_PORT))
    s3.sendall(create_connect_packet("")) # empty ID -> auto
    resp3 = recv_packet(s3)
    assert resp3 is not None, "Client C failed to get CONNACK"
    _, reason3, _ = validate_connack(resp3)
    assert reason3 == ReasonCode.SUCCESS
    print("  ✓ Client C connected")

    # 5. Check if Client B is still alive
    print("Verifying Client B is still connected...")
    # We can send a PINGREQ to check
    try:
        s2.sendall(bytes([0xC0, 0x00]))
        pingresp = recv_packet(s2, timeout=2)
        if pingresp == bytes([0xD0, 0x00]):
            print("  ✓ Client B is still alive. No ID collision occurred.")
        else:
            print("  ✗ Client B did not respond to PING. Potential ID collision!")
            sys.exit(1)
    except Exception as e:
        print(f"  ✗ Error communicating with Client B: {e}. Potential ID collision!")
        sys.exit(1)

    s2.close()
    s3.close()
    print("\n✓ Verification successful: Auto-generated client IDs are unique.")

def connect_client(client_index):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(10)
        s.connect((MQTT_HOST, MQTT_PORT))
        s.sendall(create_connect_packet("")) # empty ID -> auto
        resp = recv_packet(s)
        if resp is None:
            return None, f"Client {client_index} failed to get CONNACK"
        _, reason, _ = validate_connack(resp)
        if reason != ReasonCode.SUCCESS:
            return None, f"Client {client_index} failed with reason {reason}"
        return s, None
    except Exception as e:
        return None, f"Error connecting client {client_index}: {e}"

def test_many_concurrent_clients(num_clients=200):
    print(f"\n[Verification] Testing {num_clients} clients connecting concurrently")
    sockets = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        future_to_idx = {executor.submit(connect_client, i): i for i in range(num_clients)}
        for future in concurrent.futures.as_completed(future_to_idx):
            s, err = future.result()
            if err:
                print(f"  ✗ {err}")
            else:
                sockets.append(s)
                
    print(f"  ✓ {len(sockets)} clients connected successfully")
    assert len(sockets) == num_clients, f"Only {len(sockets)}/{num_clients} connected"

    print("Verifying all clients are still alive (no ID collisions caused disconnects)...")
    alive_count = 0
    for i, s in enumerate(sockets):
        try:
            s.sendall(bytes([0xC0, 0x00]))
            pingresp = recv_packet(s, timeout=5)
            if pingresp == bytes([0xD0, 0x00]):
                alive_count += 1
            else:
                print(f"  ✗ Client {i} socket did not respond properly to PING")
        except Exception as e:
            print(f"  ✗ Error communicating with Client {i}: {e}")
            
    print(f"  ✓ {alive_count} clients alive")
    assert alive_count == num_clients, f"Only {alive_count}/{num_clients} alive"

    print("Disconnecting all clients...")
    for s in sockets:
        try:
            s.close()
        except:
            pass

    print(f"✓ Verification successful: {num_clients} concurrent clients handled correctly.")

if __name__ == "__main__":
    test_duplicate_ids()
    test_many_concurrent_clients()
