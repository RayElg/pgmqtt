#!/usr/bin/env python3
"""
Integration tests for MQTT 5.0 Session Persistence across Broker Restarts.

Tests:
  1. Session with expiry > 0 retains state and pending QoS 1 messages
     even if the broker (PostgreSQL) is restarted while the client is offline.
"""

import socket
import time
import sys
import os
import subprocess


from proto_utils import (
    create_connect_packet,
    create_subscribe_packet,
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
    """Open a socket and send CONNECT. Returns connected socket after CONNACK."""
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


def setup_table(table="session_restart_test"):
    """Create/reset the CDC table used by session restart tests."""
    run_psql(f"DROP TABLE IF EXISTS {table};")
    run_psql(
        f"CREATE TABLE {table} (id serial primary key, msg text);"
    )
    run_psql(f"ALTER TABLE {table} REPLICA IDENTITY FULL;")
    run_psql(
        f"SELECT pgmqtt_add_mapping('public', '{table}', "
        f"'test/session/restart', '{{{{ columns.msg }}}}', 1);"
    )


def restart_broker():
    """Restarts the postgres container using docker-compose."""
    print("  ! Restarting broker (postgres)...")
    subprocess.run(["docker-compose", "restart", "postgres"], check=True)
    
    # Wait for the broker to fully come back up
    max_retries = 30
    for i in range(max_retries):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            s.connect((MQTT_HOST, MQTT_PORT))
            s.close()
            print("  ! Broker is back up.")
            time.sleep(2) # Give extension a bit of time to start background workers
            return
        except (socket.timeout, ConnectionRefusedError):
            time.sleep(1)
    
    raise Exception("Broker failed to restart within 30 seconds.")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_session_persists_across_broker_restart():
    """
    A session with Session Expiry Interval > 0 should survive a broker restart.
    Messages queued while the client is offline should be delivered when the
    client reconnects after the broker has restarted.
    """
    print("\n[Session Restart] Test 1: session persists across broker restart")
    client_id = "sr_persist_client"
    topic = "test/session/restart"
    setup_table()

    # 1. Connect with a long expiry, subscribe
    s, _ = connect(client_id, clean_start=True, session_expiry=300)
    s.sendall(create_subscribe_packet(1, topic, qos=1))
    validate_suback(recv_packet(s), 1)
    print("  ✓ Subscribed")

    # 2. Cleanly disconnect
    s.sendall(create_disconnect_packet())
    s.close()
    time.sleep(0.5)

    # 3. Publish a message while client is offline
    run_psql("INSERT INTO session_restart_test (msg) VALUES ('restart-msg');")
    time.sleep(2)

    # 4. Restart the broker to simulate crash/restart while client is offline
    restart_broker()

    # 5. Reconnect with clean_start=False — should resume the session
    s2, session_present = connect(client_id, clean_start=False, session_expiry=300)
    assert session_present, "Expected session_present=True on reconnect after restart"
    print(f"  ✓ Reconnected after restart, session_present={session_present}")

    # 6. Expect to receive the message that was published while offline
    pkt = recv_packet(s2, timeout=10)
    assert pkt is not None, "No PUBLISH received after reconnect"
    topic_r, payload_r, qos_r, _, _, pid, _ = validate_publish(pkt)
    assert topic_r == topic, f"Wrong topic: {topic_r}"
    assert qos_r == 1
    s2.sendall(create_puback_packet(pid))
    print(f"  ✓ Received queued message from DB persistence: payload={payload_r!r}")
    s2.close()


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    failures = []
    tests = [
        test_session_persists_across_broker_restart,
    ]
    for t in tests:
        try:
            t()
            print(f"  PASS: {t.__name__}")
        except Exception as e:
            print(f"  FAIL: {t.__name__}: {e}")
            failures.append(t.__name__)

    if failures:
        print(f"\n{len(failures)} test(s) FAILED: {failures}")
        sys.exit(1)
    else:
        print("\nAll session restart tests PASSED")
