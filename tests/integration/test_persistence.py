"""
Persistence and Broker Restart Regression Tests.

Tests that sessions, subscriptions, inflight messages, and LWT survive
broker restarts and write errors. Derived from m1.continued code review bugs.
"""

import socket
import subprocess
import time
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
    MQTTControlPacket,
    MQTT_HOST,
    MQTT_PORT,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _connect(client_id, clean_start=True, session_expiry=0, keep_alive=60,
             will_topic=None, will_payload=None, will_qos=0):
    props = {0x11: session_expiry} if session_expiry > 0 else {}
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet(
        client_id, clean_start=clean_start, keep_alive=keep_alive,
        properties=props if props else None,
        will_topic=will_topic, will_payload=will_payload, will_qos=will_qos,
    ))
    raw = recv_packet(s, timeout=5)
    assert raw is not None, f"No CONNACK received"
    sp, rc, _ = validate_connack(raw)
    assert rc == 0, f"CONNACK reason_code={rc:#04x}"
    return s, sp


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


def _setup_cdc_table(table, topic, qos=1):
    run_psql(f"DROP TABLE IF EXISTS {table};")
    run_psql(f"CREATE TABLE {table} (id serial primary key, msg text);")
    run_psql(f"ALTER TABLE {table} REPLICA IDENTITY FULL;")
    run_psql(
        f"SELECT pgmqtt_add_outbound_mapping('public', '{table}', "
        f"'{topic}', '{{{{ columns.msg }}}}', {qos});"
    )
    time.sleep(6)


def _drain_publishes(s, count, ack=False, timeout_per=3.0):
    received = []
    deadline = time.time() + timeout_per * max(count, 1) + 5
    while len(received) < count and time.time() < deadline:
        pkt = recv_packet(s, timeout=timeout_per)
        if pkt is None:
            continue
        if (pkt[0] & 0xF0) >> 4 == MQTTControlPacket.PUBLISH:
            topic, payload, qos_r, dup, _, pid, _ = validate_publish(pkt)
            received.append((topic, payload, qos_r, dup, pid))
            if ack and pid is not None:
                s.sendall(create_puback_packet(pid))
    return received


# ---------------------------------------------------------------------------
# Packet ID collision after restart
# ---------------------------------------------------------------------------

def test_no_packet_id_collision_after_restart():
    """After restart, new packet IDs must not collide with restored inflight IDs."""
    client_id = "pers_collision"
    table = "pers_collision_table"
    topic = "test/pers/collision"
    N = 3

    _setup_cdc_table(table, topic, qos=1)

    s, _ = _connect(client_id, clean_start=True, session_expiry=300)
    s.sendall(create_subscribe_packet(1, topic, qos=1))
    validate_suback(recv_packet(s), 1)

    for i in range(N):
        run_psql(f"INSERT INTO {table} (msg) VALUES ('pre-{i}');")

    initial = _drain_publishes(s, N, ack=False, timeout_per=5.0)
    assert len(initial) == N
    inflight_pids = {pid for _, _, _, _, pid in initial}

    s.sendall(create_disconnect_packet())
    s.close()
    time.sleep(0.5)

    _restart_broker()

    s2, sp = _connect(client_id, clean_start=False, session_expiry=300)
    assert sp, "Session should be present after restart"

    redelivered = _drain_publishes(s2, N, ack=False, timeout_per=8.0)
    assert len(redelivered) == N
    for _, _, _, dup, pid in redelivered:
        assert dup, f"Expected DUP=1 for redelivered pid {pid}"
        s2.sendall(create_puback_packet(pid))
    time.sleep(0.3)

    run_psql(f"INSERT INTO {table} (msg) VALUES ('post-restart');")
    new_msgs = _drain_publishes(s2, 1, ack=True, timeout_per=8.0)
    assert len(new_msgs) == 1
    _, _, _, dup_new, new_pid = new_msgs[0]
    assert not dup_new
    assert new_pid not in inflight_pids, (
        f"Collision: new pid {new_pid} reused inflight pid from {inflight_pids}"
    )
    s2.close()


# ---------------------------------------------------------------------------
# Client publish survives restart
# ---------------------------------------------------------------------------

def test_client_publish_survives_restart():
    """QoS 1 message persisted to DB before topic_buffer push survives restart."""
    sub_id = "pers_pub_sub"
    pub_id = "pers_pub_pub"
    topic = "test/pers/atomicity"
    payload = b"atomicity-test"

    s_sub, _ = _connect(sub_id, clean_start=True, session_expiry=300)
    s_sub.sendall(create_subscribe_packet(1, topic, qos=1))
    validate_suback(recv_packet(s_sub), 1)
    s_sub.sendall(create_disconnect_packet())
    s_sub.close()
    time.sleep(0.3)

    s_pub, _ = _connect(pub_id, clean_start=True)
    s_pub.sendall(create_publish_packet(topic, payload, qos=1, packet_id=42))
    puback = recv_packet(s_pub, timeout=5)
    assert puback is not None, "No PUBACK received"
    s_pub.close()
    time.sleep(2)

    _restart_broker()

    s2, sp = _connect(sub_id, clean_start=False, session_expiry=300)
    assert sp
    msgs = _drain_publishes(s2, 1, ack=True, timeout_per=10.0)
    assert len(msgs) == 1, "Message lost after restart"
    assert msgs[0][1] == payload
    s2.close()


# ---------------------------------------------------------------------------
# Subscriptions reloaded on restart
# ---------------------------------------------------------------------------

def test_subscriptions_reloaded_on_restart():
    """CDC events route to persistent sessions whose subscriptions were reloaded on restart."""
    client_id = "pers_sub_reload"
    table = "pers_sub_reload_table"
    topic = "test/pers/subreload"

    _setup_cdc_table(table, topic, qos=1)

    s, _ = _connect(client_id, clean_start=True, session_expiry=300)
    s.sendall(create_subscribe_packet(1, topic, qos=1))
    validate_suback(recv_packet(s), 1)
    s.sendall(create_disconnect_packet())
    s.close()
    time.sleep(0.3)

    _restart_broker()

    run_psql(f"INSERT INTO {table} (msg) VALUES ('after-restart');")
    time.sleep(3)

    s2, sp = _connect(client_id, clean_start=False, session_expiry=300)
    assert sp
    msgs = _drain_publishes(s2, 1, ack=True, timeout_per=10.0)
    assert len(msgs) == 1, "CDC message not received — subscriptions not reloaded"
    assert b"after-restart" in msgs[0][1]
    s2.close()


# ---------------------------------------------------------------------------
# Write error triggers LWT
# ---------------------------------------------------------------------------

def test_write_error_during_delivery_triggers_lwt():
    """Write error in publish_pending_messages triggers client's LWT."""
    lwt_topic = "test/pers/lwt"
    lwt_payload = b"victim-died"
    victim_id = "pers_lwt_victim"
    watcher_id = "pers_lwt_watcher"
    table = "pers_lwt_table"
    data_topic = "test/pers/data"

    _setup_cdc_table(table, data_topic, qos=1)

    s_watch, _ = _connect(watcher_id, clean_start=True)
    s_watch.sendall(create_subscribe_packet(1, lwt_topic, qos=0))
    validate_suback(recv_packet(s_watch), 1)

    s_victim, _ = _connect(
        victim_id, clean_start=True, keep_alive=60,
        will_topic=lwt_topic, will_payload=lwt_payload, will_qos=0,
    )
    s_victim.sendall(create_subscribe_packet(1, data_topic, qos=1))
    validate_suback(recv_packet(s_victim), 1)

    for i in range(30):
        run_psql(f"INSERT INTO {table} (msg) VALUES ('data-{i}');")
    time.sleep(1)

    s_victim.close()

    lwt_received = False
    deadline = time.time() + 10
    while time.time() < deadline:
        pkt = recv_packet(s_watch, timeout=1.0)
        if pkt is None:
            continue
        if (pkt[0] & 0xF0) >> 4 == MQTTControlPacket.PUBLISH:
            t, p, _, _, _, _, _ = validate_publish(pkt)
            if t == lwt_topic and p == lwt_payload:
                lwt_received = True
                break

    assert lwt_received, "LWT not received after write-error disconnect"
    s_watch.close()
