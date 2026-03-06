#!/usr/bin/env python3
"""
Integration tests for MQTT 5.0 Session Expiry Interval.

Tests:
  1. Session with expiry > 0 persists across a quick disconnect/reconnect.
  2. Session with expiry == 0 (or no expiry) does NOT persist across disconnect.
  3. Session that exceeds its expiry interval is cleaned up by the broker.
"""

import socket
import time
import sys
import os

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
    """Open a socket and send CONNECT. Returns connected socket after CONNACK."""
    # Property 0x11 = Session Expiry Interval (4-byte u32)
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


def setup_table(table="session_expiry_test"):
    """Create/reset the CDC table used by session expiry tests."""
    run_psql(f"DROP TABLE IF EXISTS {table};")
    run_psql(
        f"CREATE TABLE {table} (id serial primary key, msg text);"
    )
    run_psql(f"ALTER TABLE {table} REPLICA IDENTITY FULL;")
    run_psql(
        f"SELECT pgmqtt_add_mapping('public', '{table}', "
        f"'test/session/expiry', '{{{{ columns.msg }}}}', 1);"
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_session_persists_within_expiry():
    """
    A session with Session Expiry Interval > 0 should survive a clean disconnect
    and be resumed when the client reconnects with clean_start=False before the
    interval elapses.
    """
    print("\n[Session Expiry] Test 1: session persists within expiry interval")
    client_id = "se_persist_client"
    topic = "test/session/expiry"
    setup_table()

    # 1. Connect with a 30s expiry, subscribe
    s, _ = connect(client_id, clean_start=True, session_expiry=30)
    s.sendall(create_subscribe_packet(1, topic, qos=1))
    validate_suback(recv_packet(s), 1)
    print("  ✓ Subscribed")

    # 2. Cleanly disconnect
    s.sendall(create_disconnect_packet())
    s.close()
    time.sleep(0.5)

    # 3. Publish a message while client is offline
    run_psql("INSERT INTO session_expiry_test (msg) VALUES ('persisted-msg');")
    time.sleep(2)

    # 4. Reconnect with clean_start=False — should resume the session
    s2, session_present = connect(client_id, clean_start=False, session_expiry=30)
    assert session_present, "Expected session_present=True on reconnect"
    print(f"  ✓ Reconnected, session_present={session_present}")

    # 5. Expect to receive the message that was published while offline
    pkt = recv_packet(s2, timeout=10)
    assert pkt is not None, "No PUBLISH received after reconnect"
    topic_r, payload_r, qos_r, _, _, pid, _ = validate_publish(pkt)
    assert topic_r == topic, f"Wrong topic: {topic_r}"
    assert qos_r == 1
    s2.sendall(create_puback_packet(pid))
    print(f"  ✓ Received queued message: payload={payload_r!r}")
    s2.close()


def test_session_ends_at_disconnect_with_zero_expiry():
    """
    A session with Session Expiry Interval == 0 (default) should be discarded
    immediately on disconnect. Reconnecting should NOT show session_present=True
    and any buffered messages should be lost.
    """
    print("\n[Session Expiry] Test 2: session ends at disconnect (expiry=0)")
    client_id = "se_zero_expiry_client"
    topic = "test/session/expiry"
    setup_table()

    # 1. Connect with expiry=0 (default), subscribe
    s, _ = connect(client_id, clean_start=True, session_expiry=0)
    s.sendall(create_subscribe_packet(1, topic, qos=1))
    validate_suback(recv_packet(s), 1)
    print("  ✓ Subscribed")

    # 2. Cleanly disconnect (session should be destroyed immediately)
    s.sendall(create_disconnect_packet())
    s.close()
    time.sleep(0.5)

    # 3. Publish while client is offline
    run_psql("INSERT INTO session_expiry_test (msg) VALUES ('lost-msg');")
    time.sleep(2)

    # 4. Reconnect — session should NOT be present
    s2, session_present = connect(client_id, clean_start=False, session_expiry=0)
    assert not session_present, f"Expected session_present=False, got {session_present}"
    print(f"  ✓ session_present=False as expected")

    # 5. No message should arrive (no subscription alive)
    pkt = recv_packet(s2, timeout=3)
    assert pkt is None, f"Expected no buffered message, but received one: {pkt!r}"
    print("  ✓ No buffered message received (correct)")
    s2.close()


def test_session_expires_after_interval():
    """
    A session with a short Session Expiry Interval should be cleaned up by the
    broker after the interval elapses. Reconnecting afterwards should NOT show
    session_present=True.
    """
    print("\n[Session Expiry] Test 3: session expires after interval elapses")
    client_id = "se_short_expiry_client"
    topic = "test/session/expiry"
    setup_table()

    # MQTT 5 minimum meaningful interval is 1 second
    expiry_secs = 2

    # 1. Connect with a 2s expiry, subscribe
    s, _ = connect(client_id, clean_start=True, session_expiry=expiry_secs)
    s.sendall(create_subscribe_packet(1, topic, qos=1))
    validate_suback(recv_packet(s), 1)
    print(f"  ✓ Subscribed (expiry={expiry_secs}s)")

    # 2. Cleanly disconnect
    s.sendall(create_disconnect_packet())
    s.close()

    # 3. Wait longer than the expiry interval
    wait_for = expiry_secs + 2
    print(f"  ! Waiting {wait_for}s for session to expire...")
    time.sleep(wait_for)

    # 4. Reconnect — session should have been swept
    s2, session_present = connect(client_id, clean_start=False, session_expiry=5)
    assert not session_present, (
        f"Expected session_present=False after expiry, got {session_present}"
    )
    print(f"  ✓ session_present=False after expiry (correct)")
    s2.close()


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    failures = []
    tests = [
        test_session_persists_within_expiry,
        test_session_ends_at_disconnect_with_zero_expiry,
        test_session_expires_after_interval,
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
        print("\nAll session expiry tests PASSED")
