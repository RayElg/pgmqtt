#!/usr/bin/env python3
"""
MQTT 5.0 Conformance Tests: CDC Adaptation
Adapts standard conformance tests to use CDC as the publisher.

Focus:
- Wildcard subscriptions (+, #) matching CDC-generated topics.
"""

import socket
import sys
import os
import time
import subprocess
import struct


from proto_utils import (
    ReasonCode, create_connect_packet, create_subscribe_packet,
    recv_packet, validate_connack, validate_suback, validate_publish,
    encode_variable_byte_integer, decode_variable_byte_integer
)

# ── Configuration ─────────────────────────────────────────────────────────────

MQTT_HOST = "127.0.0.1"
MQTT_PORT = 1883
from test_utils import run_sql

# ── Helpers ──────────────────────────────────────────────────────────────────

def setup_table_and_mapping(table_name, topic_pattern):
    run_sql(f"DROP TABLE IF EXISTS {table_name};")
    run_sql(f"CREATE TABLE {table_name} (id serial PRIMARY KEY, data text);")
    run_sql(f"ALTER TABLE {table_name} REPLICA IDENTITY FULL;")
    run_sql(f"SELECT pgmqtt_add_mapping('public', '{table_name}', '{topic_pattern}', 'payload');")

def cdc_publish(table_name, data):
    run_sql(f"INSERT INTO {table_name} (data) VALUES ('{data}');")
    time.sleep(0.5) # Give CDC worker a moment

# ── Tests ────────────────────────────────────────────────────────────────────

def test_cdc_wildcard_single_level():
    """
    Test: Subscribe to 'cdc/+/data' and verify it matches 'cdc/a/data' via CDC.
    """
    print("\n[Test] CDC Single Level Wildcard (+)")
    table = "test_cdc_wild_single"
    # Mapping: cdc/{{data}}/data
    # If we insert data='a', topic becomes 'cdc/a/data'
    setup_table_and_mapping(table, "cdc/{{columns.data}}/data")
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((MQTT_HOST, MQTT_PORT))
        s.settimeout(5)
        s.sendall(create_connect_packet("cdc_wild_single", clean_start=True))
        recv_packet(s) # CONNACK
        
        # Subscribe to cdc/+/data
        s.sendall(create_subscribe_packet(1, "cdc/+/data", qos=0))
        recv_packet(s) # SUBACK
        
        # 1. Insert 'a' -> cdc/a/data (Should Match)
        print("  Inserting 'a'...")
        cdc_publish(table, "a")
        
        # 2. Insert 'b' -> cdc/b/data (Should Match)
        print("  Inserting 'b'...")
        cdc_publish(table, "b")
        
        # 3. Verify reception
        # Expecting 2 messages
        for expected_val in ["a", "b"]:
            pkt = recv_packet(s, timeout=5)
            if pkt is None:
                raise TimeoutError(f"Timed out waiting for {expected_val}")
            topic, payload, _, _, _, _, _ = validate_publish(pkt)
            expected_topic = f"cdc/{expected_val}/data"
            assert topic == expected_topic, f"Expected {expected_topic}, got {topic}"
            print(f"  ✓ Received {topic}")
            
    print("✓ test_cdc_wildcard_single_level passed")


def test_cdc_wildcard_multi_level():
    """
    Test: Subscribe to 'cdc/multi/#' and verify matching.
    """
    print("\n[Test] CDC Multi Level Wildcard (#)")
    table = "test_cdc_wild_multi"
    # Mapping: cdc/multi/{{data}}
    setup_table_and_mapping(table, "cdc/multi/{{columns.data}}")
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((MQTT_HOST, MQTT_PORT))
        s.settimeout(5)
        s.sendall(create_connect_packet("cdc_wild_multi", clean_start=True))
        recv_packet(s)
        
        s.sendall(create_subscribe_packet(1, "cdc/multi/#", qos=0))
        recv_packet(s)
        
        # 1. Insert 'a' -> cdc/multi/a
        print("  Inserting 'a'...")
        cdc_publish(table, "a")
        # 2. Insert 'a/b' -> cdc/multi/a/b
        print("  Inserting 'a/b'...")
        cdc_publish(table, "a/b")
        
        received = []
        for _ in range(2):
            pkt = recv_packet(s, timeout=5)
            if pkt:
                topic, _, _, _, _, _, _ = validate_publish(pkt)
                received.append(topic)
                print(f"  ✓ Received {topic}")
        
        assert "cdc/multi/a" in received
        assert "cdc/multi/a/b" in received

    print("✓ test_cdc_wildcard_multi_level passed")


def test_cdc_topic_structure():
    """
    Test combined structural matching.
    Mapping: structure/{{data}}/event
    Subscribe: structure/+/event
    """
    print("\n[Test] CDC Topic Structure Matching")
    table = "test_cdc_struct"
    setup_table_and_mapping(table, "structure/{{columns.data}}/event")
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((MQTT_HOST, MQTT_PORT))
        s.settimeout(5)
        s.sendall(create_connect_packet("cdc_struct", clean_start=True))
        recv_packet(s)
        
        s.sendall(create_subscribe_packet(1, "structure/+/event", qos=0))
        recv_packet(s)
        
        print("  Inserting 'user1'...")
        cdc_publish(table, "user1") # -> structure/user1/event (Match)
        print("  Inserting 'user2'...")
        cdc_publish(table, "user2") # -> structure/user2/event (Match)
        
        for _ in range(2):
            pkt = recv_packet(s, timeout=5)
            if pkt is None:
                raise TimeoutError("Timed out waiting for message")
            topic, _, _, _, _, _, _ = validate_publish(pkt)
            print(f"  ✓ Received {topic}")

    print("✓ test_cdc_topic_structure passed")


if __name__ == "__main__":
    print("=== CDC Conformance Tests ===")
    try:
        test_cdc_wildcard_single_level()
        test_cdc_wildcard_multi_level()
        test_cdc_topic_structure()
        print("\nAll CDC Conformance Tests Passed")
    except Exception as e:
        print(f"\n✗ FAILED: {e}")
        sys.exit(1)
