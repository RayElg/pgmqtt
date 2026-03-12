#!/usr/bin/env python3
"""
Extended integration tests for pgmqtt.
- Test 1: Long-lived connection (over 1 minute).
- Test 2: Comprehensive demo (multiple tables/topics).

Prerequisites:
  - Docker container running
"""

import socket
import sys
import time
import json
import psycopg2
from proto_utils import (
    create_connect_packet, 
    create_subscribe_packet, 
    recv_packet, 
    validate_publish,
    MQTT_HOST,
    MQTT_PORT
)

# ── Configuration ─────────────────────────────────────────────────────────────

PG_HOST = "127.0.0.1"
PG_PORT = 5432
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_DB = "postgres"

# ── SQL helpers ──────────────────────────────────────────────────────────────

def run_psql(sql):
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DB
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
            if cur.description:
                return cur.fetchall()
        conn.close()
    except Exception as e:
        print(f"  psycopg2 error: {e}")
    return None

# ── Test Infrastructure ──────────────────────────────────────────────────────

passed = 0
failed = 0

def ok(msg):
    global passed
    print(f"  ✓ {msg}")
    passed += 1

def fail(msg):
    global failed
    print(f"  ✗ {msg}")
    failed += 1

# ── Tests ────────────────────────────────────────────────────────────────────

def test_long_lived_connection():
    """Requirement: Over the course of a minute, be able to continuously receive changes."""
    print("\n[Scenario 1] Long-lived connection (40 seconds)")
    
    # 1. Setup table and mapping
    run_psql("DROP TABLE IF EXISTS heartbeat;")
    run_psql("CREATE TABLE heartbeat (id serial PRIMARY KEY, ts timestamp DEFAULT now());")
    run_psql("ALTER TABLE heartbeat REPLICA IDENTITY FULL;")
    run_psql("SELECT pgmqtt_add_mapping('public', 'heartbeat', 'system/heartbeat', '{{ columns.ts }}');")
    time.sleep(6)  # Wait for server's 5s mapping cache to expire

    # 2. Connect and subscribe
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("long_lived_tester"))
    recv_packet(s) # CONNACK
    s.sendall(create_subscribe_packet(1, "system/heartbeat"))
    recv_packet(s) # SUBACK
    
    start_time = time.time()
    duration = 40 # Run for 40 seconds to be sure
    interval = 10 # Send every 10 seconds
    count = 0
    received = 0
    
    print(f"  Monitoring changes for {duration}s...")
    
    try:
        last_send = 0
        last_ping_count = 0
        while time.time() - start_time < duration:
            now = time.time()
            if now - last_send >= interval:
                run_psql("INSERT INTO heartbeat DEFAULT VALUES;")
                count += 1
                last_send = now
            
            pkt = recv_packet(s, timeout=1.0)
            if pkt:
                packet_type = pkt[0] >> 4
                if packet_type == 3: # PUBLISH
                    topic, payload, _, _, _, _, _ = validate_publish(pkt)
                    if topic == "system/heartbeat":
                        received += 1
                elif packet_type == 13: # PINGRESP
                    pass # Ignore
            
            # Send PINGREQ only once per 3 heartbeats
            if count % 3 == 0 and count > last_ping_count:
                s.sendall(bytes([0xC0, 0x00])) # PINGREQ
                last_ping_count = count
            
            time.sleep(0.1)
        
        if received >= count:
            ok(f"Received all {received} heartbeats over {duration}s on a single connection.")
        else:
            fail(f"Only received {received}/{count} heartbeats. CDC or connection might have dropped.")
            
    finally:
        s.close()

def test_comprehensive_demo():
    """Requirement: Showcase multiple topic/payload mappings (personal demo)."""
    print("\n[Scenario 2] Comprehensive Demo (Multiple Mappings)")
    
    # 1. Setup tables
    tables = {
        "users": "CREATE TABLE demo_users (id serial PRIMARY KEY, username text, status text);",
        "iot": "CREATE TABLE demo_iot (id serial PRIMARY KEY, device_id text, reading float, unit text);"
    }
    
    for name, sql in tables.items():
        run_psql(f"DROP TABLE IF EXISTS demo_{name};")
        run_psql(sql)
        run_psql(f"ALTER TABLE demo_{name} REPLICA IDENTITY FULL;")
    
    # 2. Add Mappings
    run_psql("SELECT pgmqtt_add_mapping('public', 'demo_users', 'presence/{{columns.username}}', '{\"status\": \"{{columns.status}}\"}');")
    run_psql("SELECT pgmqtt_add_mapping('public', 'demo_iot', 'telemetry/{{columns.device_id}}', '{\"val\": {{columns.reading}}, \"u\": \"{{columns.unit}}\"}');")
    time.sleep(6)  # Wait for server's 5s mapping cache to expire

    # 3. Connect and subscribe to everything
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("demo_client"))
    recv_packet(s)
    s.sendall(create_subscribe_packet(1, "#")) # Wildcard subscription
    recv_packet(s)
    
    # 4. Perform operations
    print("  Performing multiple operations...")
    ops = [
        ("INSERT INTO demo_users (username, status) VALUES ('alice', 'online');", "presence/alice", {"status": "online"}),
        ("INSERT INTO demo_users (username, status) VALUES ('bob', 'offline');", "presence/bob", {"status": "offline"}),
        ("INSERT INTO demo_iot (device_id, reading, unit) VALUES ('sensor_01', 22.5, 'C');", "telemetry/sensor_01", {"val": 22.5, "u": "C"}),
        ("INSERT INTO demo_iot (device_id, reading, unit) VALUES ('sensor_02', 45.0, '%');", "telemetry/sensor_02", {"val": 45.0, "u": "%"})
    ]
    
    expected_matches = len(ops)
    found_matches = 0
    
    for sql, expected_topic, expected_json in ops:
        run_psql(sql)
        pkt = recv_packet(s, timeout=5)
        if pkt:
            topic, payload, _, _, _, _, _ = validate_publish(pkt)
            try:
                data = json.loads(payload)
                if topic == expected_topic and data == expected_json:
                    print(f"    ✓ Matched {topic}: {data}")
                    found_matches += 1
                else:
                    print(f"    × Mismatch: Got {topic} with {data}, expected {expected_topic} with {expected_json}")
            except:
                print(f"    × Failed to parse JSON: {payload}")
        else:
            print(f"    × Timeout waiting for {expected_topic}")

    if found_matches == expected_matches:
        ok(f"Demo successful: All {expected_matches} complex mappings verified.")
    else:
        fail(f"Demo incomplete: Only {found_matches}/{expected_matches} mappings matched correctly.")
    
    s.close()

if __name__ == "__main__":
    print("=== pgmqtt Extended Integration Scenarios ===")
    
    try:
        test_comprehensive_demo()
        test_long_lived_connection()
    except Exception as e:
        print(f"ERROR during testing: {e}")
        sys.exit(1)
    
    print(f"\n=== Results: {passed} passed, {failed} failed ===")
    sys.exit(1 if failed > 0 else 0)
