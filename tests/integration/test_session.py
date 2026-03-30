"""
MQTT 5.0 Session Persistence and Expiry Tests.

Covers session expiry intervals, persistence across broker restart,
and message/session expiry semantics.
"""

import socket
import subprocess
import time
import pytest

from proto_utils import (
    create_connect_packet,
    create_subscribe_packet,
    create_publish_packet,
    create_puback_packet,
    create_disconnect_packet,
    recv_packet,
    validate_connack,
    validate_disconnect,
    validate_suback,
    validate_publish,
    MQTTControlPacket,
    ReasonCode,
    MQTT_HOST,
    MQTT_PORT,
    run_psql,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _connect(client_id, clean_start=True, session_expiry=0, keep_alive=60):
    props = {0x11: session_expiry} if session_expiry > 0 else {}
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet(
        client_id, clean_start=clean_start, keep_alive=keep_alive, properties=props,
    ))
    raw = recv_packet(s, timeout=5)
    assert raw is not None, "No CONNACK received"
    session_present, rc, _ = validate_connack(raw)
    assert rc == 0, f"CONNACK reason_code={rc:#04x}"
    return s, session_present


def _setup_restart_table(table="session_restart_test"):
    run_psql(f"DROP TABLE IF EXISTS {table};")
    run_psql(f"CREATE TABLE {table} (id serial primary key, msg text);")
    run_psql(f"ALTER TABLE {table} REPLICA IDENTITY FULL;")
    run_psql(
        f"SELECT pgmqtt_add_outbound_mapping('public', '{table}', "
        f"'test/session/restart', '{{{{ columns.msg }}}}', 1);"
    )
    time.sleep(6)


def _restart_broker():
    subprocess.run(["docker", "compose", "restart", "postgres"], check=True)
    for _ in range(30):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            s.connect((MQTT_HOST, MQTT_PORT))
            s.close()
            time.sleep(2)
            return
        except (socket.timeout, ConnectionRefusedError):
            time.sleep(1)
    raise Exception("Broker failed to restart within 30 seconds.")


# ---------------------------------------------------------------------------
# Session expiry tests
# ---------------------------------------------------------------------------

def test_session_persists_within_expiry():
    """Session survives disconnect/reconnect within expiry interval."""
    client_id = "se_persist"
    topic = "test/session/persist"

    s, _ = _connect(client_id, clean_start=True, session_expiry=300)
    s.sendall(create_subscribe_packet(1, topic, qos=1))
    validate_suback(recv_packet(s), 1)
    s.sendall(create_disconnect_packet())
    s.close()

    # Publish while offline
    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("se_persist_pub"))
    recv_packet(s_pub)
    s_pub.sendall(create_publish_packet(topic, b"offline msg", qos=1, packet_id=1))
    recv_packet(s_pub)  # PUBACK
    s_pub.close()
    time.sleep(1)

    # Reconnect — session should be present
    s2, sp = _connect(client_id, clean_start=False, session_expiry=300)
    assert sp, "Session should be present"

    pkt = recv_packet(s2, timeout=10)
    assert pkt is not None, "Should receive queued message"
    _, _, qos, *_ = validate_publish(pkt)
    assert qos == 1
    s2.close()


def test_session_ends_at_disconnect_with_zero_expiry():
    """Session destroyed immediately when expiry=0."""
    client_id = "se_zero"

    s, _ = _connect(client_id, clean_start=True, session_expiry=0)
    s.sendall(create_subscribe_packet(1, "test/session/zero", qos=1))
    validate_suback(recv_packet(s), 1)
    s.sendall(create_disconnect_packet())
    s.close()
    time.sleep(1)

    s2, sp = _connect(client_id, clean_start=False)
    assert not sp, "Session should NOT be present with expiry=0"
    s2.close()


def test_session_expires_after_interval():
    """Session cleaned up after expiry interval elapses."""
    client_id = "se_interval"

    s, _ = _connect(client_id, clean_start=True, session_expiry=2)
    s.sendall(create_subscribe_packet(1, "test/session/expire", qos=1))
    validate_suback(recv_packet(s), 1)
    s.sendall(create_disconnect_packet())
    s.close()

    time.sleep(4)  # Wait past expiry

    s2, sp = _connect(client_id, clean_start=False)
    assert not sp, "Session should have expired"
    s2.close()


def test_session_expiry_loss():
    """Session expires, queued messages lost."""
    client_id = "se_loss"
    topic = "test/session/loss"

    s, _ = _connect(client_id, clean_start=True, session_expiry=2)
    s.sendall(create_subscribe_packet(1, topic, qos=1))
    validate_suback(recv_packet(s), 1)
    s.close()  # Ungraceful

    # Publish while offline
    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("se_loss_pub"))
    recv_packet(s_pub)
    s_pub.sendall(create_publish_packet(topic, b"session message", qos=1, packet_id=102))
    recv_packet(s_pub)
    s_pub.close()

    time.sleep(4)  # Wait past expiry

    s2, sp = _connect(client_id, clean_start=False)
    assert not sp, "Session should have expired"
    pkt = recv_packet(s2, timeout=1.0)
    assert pkt is None, "Should not receive message after session expired"
    s2.close()


def test_disconnect_expiry_override_rejected_when_connect_zero():
    """DISCONNECT with non-zero Session Expiry is Protocol Error if CONNECT had expiry=0.

    MQTT 5.0 §3.14.2.2.2: If the Session Expiry Interval in the CONNECT packet
    was zero, then it is a Protocol Error to set a non-zero Session Expiry
    Interval in the DISCONNECT packet sent by the Client.
    """
    client_id = "se_override_reject"

    # Connect with session_expiry=0 (default)
    s, _ = _connect(client_id, clean_start=True, session_expiry=0)

    # Send DISCONNECT with non-zero Session Expiry Interval (property 0x11)
    s.sendall(create_disconnect_packet(reason_code=0, properties={0x11: 300}))

    # Broker should respond with DISCONNECT + Protocol Error, or close connection
    resp = recv_packet(s, timeout=3)
    if resp is not None:
        ptype = (resp[0] >> 4)
        assert ptype == MQTTControlPacket.DISCONNECT, \
            f"Expected DISCONNECT, got packet type {ptype}"
        rc = validate_disconnect(resp)
        assert rc == ReasonCode.PROTOCOL_ERROR, \
            f"Expected Protocol Error (0x82), got 0x{rc:02x}"
        print("  Received DISCONNECT with Protocol Error")
    else:
        print("  Connection closed by broker (acceptable)")

    s.close()


def test_disconnect_expiry_override_accepted_when_connect_nonzero():
    """DISCONNECT with different Session Expiry is accepted if CONNECT had non-zero expiry.

    MQTT 5.0 §3.14.2.2.2: The client is allowed to change the expiry on
    DISCONNECT as long as the CONNECT session expiry was non-zero.
    """
    client_id = "se_override_accept"

    # Connect with session_expiry=300
    s, _ = _connect(client_id, clean_start=True, session_expiry=300)
    s.sendall(create_subscribe_packet(1, "test/session/override", qos=1))
    validate_suback(recv_packet(s), 1)

    # Send DISCONNECT with different Session Expiry — should be accepted
    s.sendall(create_disconnect_packet(reason_code=0, properties={0x11: 60}))
    s.close()

    time.sleep(1)

    # Reconnect — session should still be present (within 60s expiry)
    s2, sp = _connect(client_id, clean_start=False, session_expiry=300)
    assert sp, "Session should be present (override to 60s, reconnected within 1s)"
    s2.close()


# ---------------------------------------------------------------------------
# Broker restart
# ---------------------------------------------------------------------------

def test_session_persists_across_broker_restart():
    """Session + pending QoS 1 messages survive broker restart."""
    client_id = "sr_persist_client"
    topic = "test/session/restart"
    _setup_restart_table()

    s, _ = _connect(client_id, clean_start=True, session_expiry=300)
    s.sendall(create_subscribe_packet(1, topic, qos=1))
    validate_suback(recv_packet(s), 1)
    s.sendall(create_disconnect_packet())
    s.close()
    time.sleep(0.5)

    run_psql("INSERT INTO session_restart_test (msg) VALUES ('restart-msg');")
    time.sleep(2)

    _restart_broker()

    s2, sp = _connect(client_id, clean_start=False, session_expiry=300)
    assert sp, "Expected session_present=True after restart"

    pkt = recv_packet(s2, timeout=10)
    assert pkt is not None, "Should receive queued message after restart"
    topic_r, payload_r, qos_r, *_ = validate_publish(pkt)
    assert topic_r == topic
    assert qos_r == 1
    s2.close()


# ---------------------------------------------------------------------------
# Message expiry
# ---------------------------------------------------------------------------

@pytest.mark.xfail(reason="Message Expiry Interval not yet implemented")
def test_message_expiry():
    """Message with expiry=2s not delivered after 4s."""
    client_id = "msg_expiry"
    topic = "test/msg/expiry"

    s, _ = _connect(client_id, clean_start=True, session_expiry=3600)
    s.sendall(create_subscribe_packet(5, topic, qos=1))
    validate_suback(recv_packet(s), 5)
    s.close()

    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("msg_expiry_pub"))
    recv_packet(s_pub)
    s_pub.sendall(create_publish_packet(topic, b"expire me", qos=1, packet_id=101, properties={0x02: 2}))
    recv_packet(s_pub)
    s_pub.close()

    time.sleep(4)

    s2, _ = _connect(client_id, clean_start=False)
    pkt = recv_packet(s2, timeout=2.0)
    assert pkt is None, "Message should have expired"
    s2.close()
