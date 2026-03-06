#!/usr/bin/env python3
"""
Integration tests for MQTT 5.0 Session Durability.

Tests:
  1. A client connects with `clean_start=False` and `session_expiry_interval=300`.
  2. The client subscribes to a topic with QoS 1.
  3. The client disconnects.
  4. Messages are published to the database while the client is offline.
  5. The PostgreSQL broker is restarted (`docker compose restart postgres`).
  6. The client reconnects and receives the queued QoS 1 messages.
"""

import socket
import struct
import time
import sys
import os
import subprocess

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from proto_utils import (
    create_connect_packet,
    create_subscribe_packet,
    create_publish_packet,
    create_puback_packet,
    create_disconnect_packet,
    recv_packet,
    validate_connack,
    validate_suback,
    validate_publish,
    run_psql,
    MQTT_HOST,
    MQTT_PORT,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def connect(client_id, clean_start=True, session_expiry=0, keep_alive=60) -> socket.socket:
    props = {0x11: session_expiry} if session_expiry > 0 else {}
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    pkt = create_connect_packet(
        client_id,
        clean_start=clean_start,
        keep_alive=keep_alive,
        properties=props,
    )
    s.sendall(pkt)
    raw = recv_packet(s, timeout=5)
    assert raw is not None, "No CONNACK received"
    session_present, rc, _ = validate_connack(raw)
    assert rc == 0, f"CONNACK reason_code={rc:#04x}"
    return s, session_present


def setup_table(table="session_durability_test"):
    """Create/reset the CDC table used by session durability tests."""
    run_psql(f"DROP TABLE IF EXISTS {table};")
    run_psql(f"CREATE TABLE {table} (id serial primary key, msg text);")
    run_psql(f"ALTER TABLE {table} REPLICA IDENTITY FULL;")
    run_psql(
        f"SELECT pgmqtt_add_mapping('public', '{table}', "
        f"'test/session/durability', '{{{{ columns.msg }}}}', 1);"
    )


def wait_for_broker(timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((MQTT_HOST, MQTT_PORT), timeout=1):
                return True
        except OSError:
            time.sleep(1)
            continue
    raise RuntimeError("Broker did not start up in time")

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_session_durability_broker_restart():
    print("\n[Session Durability] Test 1: Broker Restart")
    client_id = "sd_restart_client"
    topic = "test/session/durability"
    
    # Needs to be able to connect to DB for setup
    wait_for_broker()
    setup_table()

    # 1. Connect with clean_start=True to fresh session
    print("  -> Connecting and subscribing")
    s, _ = connect(client_id, clean_start=True, session_expiry=300)
    s.sendall(create_subscribe_packet(1, topic, qos=1))
    validate_suback(recv_packet(s), 1)

    # 2. Cleanly disconnect
    print("  -> Client disconnecting")
    s.sendall(create_disconnect_packet())
    s.close()
    time.sleep(1)

    # 3. Publish messages while offline
    print("  -> Publishing messages while client is offline")
    run_psql("INSERT INTO session_durability_test (msg) VALUES ('durability-msg-1');")
    run_psql("INSERT INTO session_durability_test (msg) VALUES ('durability-msg-2');")
    time.sleep(2)  # Give broker time to process WAL

    # 4. Restart the broker
    print("  -> Restarting the broker... (docker compose restart postgres)")
    try:
        subprocess.run(["docker", "compose", "restart", "postgres"], check=True, capture_output=True)
    except FileNotFoundError:
        # Fallback to docker-compose if docker plugin is not available
        subprocess.run(["docker-compose", "restart", "postgres"], check=True, capture_output=True)

    print("  -> Waiting for broker to come back online...")
    time.sleep(15)  # Let docker spin up
    wait_for_broker()
    time.sleep(10)  # Give pgrx background worker time to initialize

    # 5. Reconnect and assert session is resumed
    print("  -> Reconnecting with clean_start=False")
    s2, session_present = connect(client_id, clean_start=False, session_expiry=300)
    assert session_present, "Expected session_present=True on reconnect"
    print(f"  ✓ session_present=True")

    # 6. Expect to receive the messages
    received_msgs = []
    for i in range(2):
        pkt = recv_packet(s2, timeout=10)
        assert pkt is not None, f"No PUBLISH received for msg {i+1}"
        topic_r, payload_r, qos_r, _, _, pid, _ = validate_publish(pkt)
        assert topic_r == topic, f"Wrong topic: {topic_r}"
        assert qos_r == 1
        s2.sendall(create_puback_packet(pid))
        received_msgs.append(payload_r.decode())

    print(f"  ✓ Received queued messages: {received_msgs}")
    assert received_msgs == ["durability-msg-1", "durability-msg-2"] or received_msgs == ["durability-msg-2", "durability-msg-1"]
    
    s2.sendall(create_disconnect_packet())
    s2.close()


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    failures = []
    tests = [
        test_session_durability_broker_restart,
    ]
    for t in tests:
        try:
            t()
            print(f"  PASS: {t.__name__}")
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"  FAIL: {t.__name__}: {e}")
            failures.append(t.__name__)

    if failures:
        print(f"\n{len(failures)} test(s) FAILED: {failures}")
        sys.exit(1)
    else:
        print("\nAll session durability tests PASSED")
