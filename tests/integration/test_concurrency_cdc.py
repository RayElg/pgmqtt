#!/usr/bin/env python3
"""
High Concurrency Stress Test for pgmqtt (CDC mode).

Scenario:
1. Create a `load_test` table.
2. Spawn N concurrent MQTT clients (subscribers).
3. Perform bulk INSERTs (M rows).
4. Verify every client received M messages.
"""

import socket
import threading
import time
import struct
import sys
import subprocess

# ── Configuration ─────────────────────────────────────────────────────────────

MQTT_HOST = "localhost"
MQTT_PORT = 1883
NUM_CLIENTS = 50
NUM_MESSAGES = 1000
BATCH_SIZE = 100 # Insert in batches to simulate burst
MAX_WAIT_TIME = 60 # Seconds to wait for completion

# ── MQTT Helpers ──────────────────────────────────────────────────────────────

def encode_utf8(s):
    try:
        b = s.encode("utf-8")
        return struct.pack("!H", len(b)) + b
    except:
        return b""

def encode_variable_byte_int(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value > 0:
            byte |= 0x80
        out.append(byte)
        if value == 0:
            break
    return bytes(out)

def decode_variable_byte_int(sock):
    value = 0
    multiplier = 1
    for i in range(4):
        try:
            b = sock.recv(1)
        except:
            return None, 0
            
        if not b: return None, 0
        value += (b[0] & 0x7F) * multiplier
        if b[0] & 0x80 == 0:
            return value, i + 1
        multiplier *= 128
    return None, 0

def build_connect(client_id):
    vh = encode_utf8("MQTT") + bytes([5, 0x02]) + struct.pack("!H", 60) + bytes([0x00])
    vh += encode_utf8(client_id)
    return bytes([0x10]) + encode_variable_byte_int(len(vh)) + vh

def build_subscribe(packet_id, topic, qos=0):
    vh = struct.pack("!H", packet_id) + bytes([0x00]) + encode_utf8(topic) + bytes([qos])
    return bytes([0x82]) + encode_variable_byte_int(len(vh)) + vh

def recv_packet_blocking(sock):
    """Blocking receive of a full MQTT packet."""
    try:
        header = sock.recv(1)
        if not header: return None
        
        # Read Remaining Length
        remaining_len = 0
        multiplier = 1
        while True:
            byte_data = sock.recv(1)
            if not byte_data: return None
            byte = byte_data[0]
            remaining_len += (byte & 0x7F) * multiplier
            if (byte & 0x80) == 0:
                break
            multiplier *= 128
            
        # Read payload
        data = bytearray()
        while len(data) < remaining_len:
            chunk = sock.recv(remaining_len - len(data))
            if not chunk: return None
            data.extend(chunk)
            
        return header + encode_variable_byte_int(remaining_len) + bytes(data)
    except Exception:
        return None

# ── SQL Helpers ──────────────────────────────────────────────────────────────

from test_utils import run_sql

def setup_db():
    print("  [DB] Setting up table 'load_test'...")
    run_sql("DROP TABLE IF EXISTS load_test;")
    run_sql("CREATE TABLE load_test (id serial PRIMARY KEY, val int);")
    run_sql("ALTER TABLE load_test REPLICA IDENTITY FULL;")
    # Map: load/test/{{val}}
    run_sql("SELECT pgmqtt_add_mapping('public', 'load_test', 'load/test/{{columns.val}}', '{\"val\": {{columns.val}}}');")

def generate_load():
    print(f"  [DB] Generating {NUM_MESSAGES} messages in batches of {BATCH_SIZE}...")
    sent = 0
    while sent < NUM_MESSAGES:
        count = min(BATCH_SIZE, NUM_MESSAGES - sent)
        start = sent + 1
        end = sent + count
        # INSERT INTO load_test (val) SELECT generate_series(start, end)
        # Note: Using simple INSERT here.
        run_sql(f"INSERT INTO load_test (val) SELECT generate_series({start}, {end});")
        sent += count
        time.sleep(0.5) # Give DB/CDC worker a slight break
    print("  [DB] Load generation complete.")

# ── Client Thread ────────────────────────────────────────────────────────────

def client_task(client_index, results):
    client_id = f"load_client_{client_index}"
    received_count = 0
    connected = False
    
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((MQTT_HOST, MQTT_PORT))
        s.settimeout(MAX_WAIT_TIME)
        
        s.sendall(build_connect(client_id))
        connack = recv_packet_blocking(s) # CONNACK
        if connack and (connack[0] >> 4) == 2:
            connected = True
        else:
            print(f"Client {client_index}: Failed to connect")
            return
        
        s.sendall(build_subscribe(1, "load/test/#"))
        suback = recv_packet_blocking(s) # SUBACK
        if not suback or (suback[0] >> 4) != 9:
             print(f"Client {client_index}: Failed to subscribe")
             return
             
        start_time = time.time()
        
        while received_count < NUM_MESSAGES:
            if time.time() - start_time > MAX_WAIT_TIME:
                print(f"Client {client_index} timed out waiting for messages")
                break
                
            pkt = recv_packet_blocking(s)
            if not pkt: 
                print(f"Client {client_index} disconnected unexpectedly")
                break
            
            # Check if it's a PUBLISH (0x30)
            packet_type = (pkt[0] >> 4) & 0x0F
            if packet_type == 3:
                received_count += 1
            # Handle PINGRESP (0xD0) or others if needed, but for now just count PUBLISH
                
        s.close()
        results[client_index] = received_count
    except Exception as e:
        print(f"Client {client_index} error: {e}")
        results[client_index] = received_count

# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"=== High Concurrency Stress Test ===")
    print(f"Clients: {NUM_CLIENTS}, Messages: {NUM_MESSAGES}")
    
    setup_db()
    
    threads = []
    results = [0] * NUM_CLIENTS
    
    print(f"  [MQTT] Spawning {NUM_CLIENTS} clients...")
    for i in range(NUM_CLIENTS):
        t = threading.Thread(target=client_task, args=(i, results))
        t.start()
        threads.append(t)
        
    # Give clients a moment to connect and subscribe
    time.sleep(5)
    
    print("  [Main] Starting load generation...")
    start_time = time.time()
    generate_load_thread = threading.Thread(target=generate_load)
    generate_load_thread.start()
    
    generate_load_thread.join()
    
    print("  [MQTT] Waiting for clients to finish receiving...")
    for t in threads:
        t.join()
        
    end_time = time.time()
    total_time = end_time - start_time
    
    # Verification
    total_expected = NUM_CLIENTS * NUM_MESSAGES
    total_received = sum(results)
    
    print("-" * 40)
    print(f"Total Messages Expected: {total_expected}")
    print(f"Total Messages Received: {total_received}")
    print(f"Total Time: {total_time:.2f} seconds")
    print(f"Throughput: {total_received / total_time:.2f} messages/sec")
    print("-" * 40)
    
    failures = 0
    for i, count in enumerate(results):
        if count != NUM_MESSAGES:
            print(f"  Client {i}: Suspicious count {count}/{NUM_MESSAGES}")
            failures += 1
            
    if failures == 0:
        print(f"✓ SUCCESS: All {NUM_CLIENTS} clients received {NUM_MESSAGES} messages.")
        sys.exit(0)
    else:
        print(f"✗ FAILURE: {failures} clients missed messages.")
        sys.exit(1)
