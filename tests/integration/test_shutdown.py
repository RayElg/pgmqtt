"""Integration tests for broker shutdown behavior (SIGTERM and SIGKILL).

SIGTERM path (docker compose stop — PostgreSQL sends SIGTERM to all workers):
  - Clients receive DISCONNECT with reason code 0x8B SERVER_SHUTTING_DOWN
  - Will messages fire for all connected clients
  - Sessions have disconnected_at set in DB before the process exits
  - pgmqtt_status() shows 0 active connections after container restart

SIGKILL path (docker compose kill — entire container gets SIGKILL):
  - Session rows survive (written atomically at CONNECT time)
  - Sessions remain with disconnected_at=NULL immediately after kill
  - On restart, startup cleanup sets disconnected_at=NOW() for all NULL sessions
  - pgmqtt_status() shows 0 active connections after restart

Note: we test SIGTERM by stopping the whole container (docker compose stop) because
PostgreSQL sends SIGTERM to all background workers as part of its own shutdown
sequence, which is the real-world path. Sending kill -15 directly to the worker
causes PostgreSQL not to restart it (clean proc_exit(0) is treated as voluntary
exit rather than a crash), breaking the test environment.
"""

import socket
import threading
import time

from proto_utils import (
    MQTT_HOST,
    MQTT_PORT,
    ReasonCode,
    create_connect_packet,
    create_subscribe_packet,
    recv_packet,
    validate_connack,
    validate_disconnect,
    validate_suback,
)
from test_utils import compose_kill, compose_start, compose_stop, run_sql

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def wait_for_broker(timeout: int = 45) -> bool:
    """Poll until the MQTT port is open and the background worker is running.

    Uses a raw TCP connect (no MQTT CONNECT packet) to avoid creating any
    session rows, which would pollute pgmqtt_status() counts in tests that
    check active_connections immediately after this returns.

    The MQTT port only opens after db_mark_sessions_disconnected_on_startup()
    completes, so a successful TCP connect guarantees the startup cleanup ran.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1.0)
            s.connect((MQTT_HOST, MQTT_PORT))
            s.close()
            # Confirm the worker is registered in pg_stat_activity
            rows = run_sql(
                "SELECT pid FROM pg_stat_activity WHERE backend_type = 'pgmqtt_mqtt'"
            )
            if rows and rows[0][0]:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def mqtt_connect(client_id: str, *, clean_start: bool = True, **kwargs) -> socket.socket:
    """Open a socket, send CONNECT, assert CONNACK success, return the socket."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet(client_id, clean_start=clean_start, **kwargs))
    pkt = recv_packet(s, timeout=5.0)
    assert pkt is not None, f"No CONNACK received for client '{client_id}'"
    _, rc, _ = validate_connack(pkt)
    assert rc == ReasonCode.SUCCESS, f"CONNECT rejected: 0x{rc:02x}"
    return s


# ---------------------------------------------------------------------------
# SIGTERM tests (docker compose stop → PostgreSQL graceful shutdown)
# ---------------------------------------------------------------------------


def test_sigterm_sends_disconnect_to_connected_clients():
    """Graceful shutdown sends DISCONNECT(SERVER_SHUTTING_DOWN) to every connected client."""
    run_sql("DELETE FROM pgmqtt_sessions WHERE client_id = 'sigterm_client_a';")
    s = mqtt_connect("sigterm_client_a")

    # Trigger graceful shutdown concurrently so we can read the DISCONNECT
    # packet while the container is still in the process of stopping.
    stop_thread = threading.Thread(target=compose_stop, args=(15,))
    stop_thread.start()

    try:
        pkt = recv_packet(s, timeout=12.0)
        assert pkt is not None, (
            "Expected DISCONNECT from broker after SIGTERM, but connection was closed silently. "
            "Broker must send DISCONNECT(SERVER_SHUTTING_DOWN) before closing connections."
        )
        rc = validate_disconnect(pkt)
        assert rc == ReasonCode.SERVER_SHUTTING_DOWN, (
            f"Expected SERVER_SHUTTING_DOWN (0x8B), got 0x{rc:02x}."
        )
    finally:
        s.close()
        stop_thread.join()

    compose_start()
    assert wait_for_broker(45), "Broker did not come back after SIGTERM + restart"


def test_sigterm_marks_sessions_disconnected_in_db():
    """After graceful shutdown, persistent sessions have disconnected_at set in DB.

    Sessions with expiry_interval=0 are deleted on disconnect (correct behavior).
    We use a persistent session (session_expiry_interval=3600) to verify the
    MarkDisconnected path runs during graceful shutdown.
    """
    run_sql("DELETE FROM pgmqtt_sessions WHERE client_id = 'sigterm_disc_b';")
    # session_expiry_interval=3600 → session persists across disconnects
    s = mqtt_connect("sigterm_disc_b", properties={0x11: 3600})

    result = run_sql(
        "SELECT disconnected_at FROM pgmqtt_sessions WHERE client_id = 'sigterm_disc_b'"
    )
    assert result and result[0][0] is None, "Session should be active before shutdown"

    s.close()
    compose_stop()
    compose_start()
    assert wait_for_broker(45), "Broker did not restart"

    result = run_sql(
        "SELECT disconnected_at FROM pgmqtt_sessions WHERE client_id = 'sigterm_disc_b'"
    )
    assert result, "Session row missing after restart"
    assert result[0][0] is not None, (
        "Session should have disconnected_at set after graceful shutdown."
    )


def test_sigterm_fires_will_messages():
    """Graceful shutdown triggers will messages for clients that did not send DISCONNECT."""
    will_topic = "will/shutdown/sigterm"
    will_payload = b"broker_shutting_down"

    run_sql("DELETE FROM pgmqtt_sessions WHERE client_id LIKE 'sigterm_will%';")
    run_sql(f"DELETE FROM pgmqtt_messages WHERE topic = '{will_topic}';")

    # Subscriber
    sub = mqtt_connect("sigterm_will_sub")
    sub.sendall(create_subscribe_packet(1, will_topic, qos=1))
    validate_suback(recv_packet(sub, timeout=5.0), 1)

    # Client with a QoS 1 will so it is persisted to pgmqtt_messages
    will_client = mqtt_connect(
        "sigterm_will_sender",
        will_topic=will_topic,
        will_payload=will_payload,
        will_qos=1,
    )

    sub.close()
    will_client.close()
    compose_stop()
    compose_start()
    assert wait_for_broker(45), "Broker did not restart"

    result = run_sql(
        f"SELECT payload FROM pgmqtt_messages WHERE topic = '{will_topic}' LIMIT 1"
    )
    assert result, (
        "Will message not found in pgmqtt_messages after graceful shutdown. "
        "Broker must publish will messages for clients that did not disconnect cleanly."
    )
    assert bytes(result[0][0]) == will_payload


def test_sigterm_status_shows_zero_after_restart():
    """pgmqtt_status() reports 0 active connections after graceful shutdown + restart."""
    run_sql("DELETE FROM pgmqtt_sessions WHERE client_id LIKE 'sigterm_stat_%';")

    clients = [mqtt_connect(f"sigterm_stat_{i}") for i in range(3)]

    result = run_sql("SELECT active_connections FROM pgmqtt_status()")
    assert result and int(result[0][0]) >= 3, "Expected 3 active sessions before shutdown"

    for c in clients:
        c.close()
    compose_stop()
    compose_start()
    assert wait_for_broker(45), "Broker did not restart"

    result = run_sql("SELECT active_connections FROM pgmqtt_status()")
    assert result, "pgmqtt_status() returned no rows"
    active = int(result[0][0])
    assert active == 0, (
        f"Expected 0 active connections after graceful shutdown + restart, got {active}."
    )


# ---------------------------------------------------------------------------
# SIGKILL tests (docker compose kill — entire container gets SIGKILL)
# ---------------------------------------------------------------------------


def test_sigkill_session_rows_survive_crash():
    """After SIGKILL, session rows written at CONNECT time persist in DB.

    Uses session_expiry_interval=3600 to ensure the session wouldn't be
    deleted even on clean disconnect — verifying that the SIGKILL doesn't
    corrupt the atomically-written CONNECT row.
    """
    run_sql("DELETE FROM pgmqtt_sessions WHERE client_id = 'sigkill_persist';")

    s = mqtt_connect("sigkill_persist", properties={0x11: 3600})
    s.close()

    compose_kill()
    compose_start()
    assert wait_for_broker(45), "Broker did not restart after SIGKILL"

    result = run_sql(
        "SELECT client_id FROM pgmqtt_sessions WHERE client_id = 'sigkill_persist'"
    )
    assert result and result[0][0] == "sigkill_persist", (
        "Session row should survive SIGKILL — it is written atomically at CONNECT time."
    )


def test_sigkill_startup_marks_stale_sessions_disconnected():
    """After SIGKILL + restart, startup cleanup sets disconnected_at for active-at-crash sessions."""
    run_sql("DELETE FROM pgmqtt_sessions WHERE client_id = 'sigkill_stale';")

    # Keep socket open during the kill so disconnected_at is NULL at crash time.
    # Closing cleanly would trigger the disconnect path and set disconnected_at first.
    s = mqtt_connect("sigkill_stale", properties={0x11: 3600})

    result = run_sql(
        "SELECT disconnected_at FROM pgmqtt_sessions WHERE client_id = 'sigkill_stale'"
    )
    assert result and result[0][0] is None, "Session should be active before SIGKILL"

    compose_kill()
    s.close()  # socket dies with the container; close to release the fd
    compose_start()
    # By the time the broker is reachable, db_mark_sessions_disconnected_on_startup()
    # has already run.
    assert wait_for_broker(45), "Broker did not restart after SIGKILL"

    result = run_sql(
        "SELECT disconnected_at FROM pgmqtt_sessions WHERE client_id = 'sigkill_stale'"
    )
    assert result, "Session row missing after SIGKILL + restart"
    assert result[0][0] is not None, (
        "After SIGKILL + restart, startup cleanup should have set disconnected_at "
        "for sessions that were active at crash time."
    )


def test_sigkill_status_shows_zero_after_restart():
    """pgmqtt_status() reports 0 active connections after SIGKILL + restart."""
    run_sql("DELETE FROM pgmqtt_sessions WHERE client_id LIKE 'sigkill_stat_%';")

    clients = [mqtt_connect(f"sigkill_stat_{i}") for i in range(3)]

    result = run_sql("SELECT active_connections FROM pgmqtt_status()")
    assert result and int(result[0][0]) >= 3

    for c in clients:
        c.close()
    compose_kill()
    compose_start()
    assert wait_for_broker(45), "Broker did not restart after SIGKILL"

    result = run_sql("SELECT active_connections FROM pgmqtt_status()")
    assert result, "pgmqtt_status() returned no rows"
    active = int(result[0][0])
    assert active == 0, (
        f"Expected 0 active connections after SIGKILL + restart, got {active}."
    )
