#!/usr/bin/env python3
"""
MQTT 5.0 Conformance Tests: CDC Unsubscribe Adaptation
Verifies that UNSUBSCRIBE stops CDC message delivery.
"""

import socket
import sys
import os
import time
import subprocess
import struct


from proto_utils import (
    ReasonCode, create_connect_packet, create_subscribe_packet, create_unsubscribe_packet,
    recv_packet, validate_connack, validate_suback, validate_unsuback, validate_publish,
    encode_variable_byte_integer, decode_variable_byte_integer
)

# ── Configuration ─────────────────────────────────────────────────────────────

MQTT_HOST = "localhost"
MQTT_PORT = 1883
PG_CONTAINER = "pgmqtt-postgres-1"

# ── Helpers ──────────────────────────────────────────────────────────────────

def psql(sql):
    """Run SQL inside the pgmqtt container."""
    result = subprocess.run(
        ["docker", "exec", PG_CONTAINER, "psql", "-U", "postgres", "-d", "postgres", "-c", sql],
        capture_output=True, text=True, timeout=10
    )
    if result.returncode != 0:
        raise RuntimeError(f"psql failed: {result.stderr.strip()}")
    return result.stdout

def setup_table_and_mapping(table_name, topic_pattern):
    psql(f"DROP TABLE IF EXISTS {table_name};")
    psql(f"CREATE TABLE {table_name} (id serial PRIMARY KEY, data text);")
    psql(f"ALTER TABLE {table_name} REPLICA IDENTITY FULL;")
    psql(f"SELECT pgmqtt_add_mapping('public', '{table_name}', '{topic_pattern}', 'payload');")

def cdc_publish(table_name, data):
    psql(f"INSERT INTO {table_name} (data) VALUES ('{data}');")
    time.sleep(0.5) # Give CDC worker a moment

# ── Tests ────────────────────────────────────────────────────────────────────

def test_cdc_unsubscribe_stops_messages():
    """
    Test: After UNSUBSCRIBE, broker stops delivering CDC messages.
    """
    print("\n[Test] CDC Unsubscribe Stops Messages")
    table = "test_cdc_unsub"
    # Mapping: cdc/unsub/{{data}}
    setup_table_and_mapping(table, "cdc/unsub/{{columns.data}}")
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((MQTT_HOST, MQTT_PORT))
        s.settimeout(5)
        s.sendall(create_connect_packet("cdc_unsub_test", clean_start=True))
        recv_packet(s) # CONNACK
        
        # Subscribe to cdc/unsub/+
        topic_filter = "cdc/unsub/+"
        s.sendall(create_subscribe_packet(1, topic_filter, qos=0))
        recv_packet(s) # SUBACK
        
        # 1. Trigger CDC (Should Receive)
        print("  Inserting 'received'...")
        cdc_publish(table, "received")
        
        pkt = recv_packet(s, timeout=5)
        assert pkt is not None, "Did not receive message before unsubscribe"
        topic, _, _, _, _, _, _ = validate_publish(pkt)
        assert topic == "cdc/unsub/received"
        print(f"  ✓ Received {topic}")
        
        # 2. Unsubscribe
        print("  Unsubscribing...")
        s.sendall(create_unsubscribe_packet(2, topic_filter))
        unsuback = recv_packet(s)
        reason_codes = validate_unsuback(unsuback, 2)
        assert reason_codes[0] == ReasonCode.SUCCESS
        print("  ✓ Unsubscribe successful")
        
        # 3. Trigger CDC (Should NOT Receive)
        print("  Inserting 'ignored'...")
        cdc_publish(table, "ignored")
        
        pkt = recv_packet(s, timeout=2)
        assert pkt is None, f"Should NOT receive message after unsubscribe, got: {pkt}"
        print("  ✓ No message received (as expected)")

    print("✓ test_cdc_unsubscribe_stops_messages passed")


if __name__ == "__main__":
    print("=== CDC Unsubscribe Test ===")
    try:
        test_cdc_unsubscribe_stops_messages()
        print("\nAll CDC Unsubscribe Tests Passed")
    except Exception as e:
        print(f"\n✗ FAILED: {e}")
        sys.exit(1)
