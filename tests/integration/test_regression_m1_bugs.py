#!/usr/bin/env python3
"""
Regression tests for bugs identified in the m1.continued code review.

Each test is designed to FAIL on the buggy code and PASS only after the
corresponding fix is applied.

Tests:
  1. [Critical #1] next_packet_id must advance past loaded inflight pids after restart.
     Repro: inflight pids {1,2,3} are loaded from DB; a new message must NOT be assigned
     pid 1, 2, or 3 — that would silently overwrite an existing in-flight entry.

  2. [Critical #2] Client-published QoS 1 messages must survive broker restart.
     Proves that publish_messages_batch commits to DB before calling topic_buffer::push.
     If push happens before commit, the message is lost on restart.

  3. [Critical #3] CDC events must reach persistent sessions after broker restart.
     Proves that db_load_sessions_on_startup reloads subscriptions so that
     match_topic() doesn't return empty for restored sessions.

  4. [Medium #4] Broker must honour the client's Receive Maximum (MQTT 5.0 §4.9).
     With Receive Maximum=2, no more than 2 QoS 1 PUBLISHes may be in-flight at once.

  5. [Medium #5] A write error during publish_pending_messages must trigger LWT and
     clean up the session, identical to a normal abrupt disconnect.
"""

import socket
import time
import sys
import os
import subprocess


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
# Shared helpers
# ---------------------------------------------------------------------------

def mqtt_connect(client_id, clean_start=True, session_expiry=0, keep_alive=60,
                 will_topic=None, will_payload=None, will_qos=0,
                 receive_maximum=None):
    """Open TCP socket, send CONNECT, return (socket, session_present)."""
    props = {}
    if session_expiry > 0:
        props[0x11] = session_expiry        # Session Expiry Interval
    if receive_maximum is not None:
        props[0x21] = receive_maximum       # Receive Maximum

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    pkt = create_connect_packet(
        client_id,
        clean_start=clean_start,
        keep_alive=keep_alive,
        properties=props if props else None,
        will_topic=will_topic,
        will_payload=will_payload,
        will_qos=will_qos,
    )
    s.sendall(pkt)
    raw = recv_packet(s, timeout=5)
    assert raw is not None, f"[{client_id}] No CONNACK received"
    sp, rc, _ = validate_connack(raw)
    assert rc == 0, f"[{client_id}] CONNACK reason_code={rc:#04x}"
    return s, sp


def restart_broker():
    """Restart the postgres/broker container and wait until MQTT port is reachable."""
    print("  ! Restarting broker...")
    subprocess.run(["docker", "compose", "restart", "postgres"], check=True)
    for _ in range(30):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            s.connect((MQTT_HOST, MQTT_PORT))
            s.close()
            time.sleep(2)  # let the background workers fully start
            print("  ! Broker is back up.")
            return
        except (socket.timeout, ConnectionRefusedError):
            time.sleep(1)
    raise RuntimeError("Broker did not come back within 30 s")


def setup_cdc_table(table, topic_pattern, payload_pattern="{{ columns.msg }}", qos=1):
    """Create (or recreate) a CDC table with a topic mapping."""
    run_psql(f"DROP TABLE IF EXISTS {table};")
    run_psql(f"CREATE TABLE {table} (id serial primary key, msg text);")
    run_psql(f"ALTER TABLE {table} REPLICA IDENTITY FULL;")
    run_psql(
        f"SELECT pgmqtt_add_mapping('public', '{table}', "
        f"'{topic_pattern}', '{payload_pattern}', {qos});"
    )


def drain_publishes(s, count, ack=False, timeout_per=3.0):
    """
    Read up to `count` PUBLISH packets from socket `s`.
    Returns list of (topic, payload, qos, dup, packet_id).
    Sends PUBACK for each QoS 1 packet when ack=True.
    Continues retrying until deadline even when recv_packet times out.
    """
    received = []
    deadline = time.time() + timeout_per * max(count, 1) + 5
    while len(received) < count and time.time() < deadline:
        pkt = recv_packet(s, timeout=timeout_per)
        if pkt is None:
            continue
        ptype = (pkt[0] & 0xF0) >> 4
        if ptype == MQTTControlPacket.PUBLISH:
            topic, payload, qos_r, dup, _, pid, _ = validate_publish(pkt)
            received.append((topic, payload, qos_r, dup, pid))
            if ack and pid is not None:
                s.sendall(create_puback_packet(pid))
    return received


# ---------------------------------------------------------------------------
# Bug #1 — next_packet_id collision after crash restart
# ---------------------------------------------------------------------------

def test_no_packet_id_collision_after_restart():
    """
    After restart the broker loads inflight pids {1..N} from the DB.
    next_packet_id must be advanced past max(inflight pids) so that the
    first new message does not reuse a pid that's still in-flight.
    """
    print("\n[Regression #1] next_packet_id must advance past loaded inflight pids after restart")

    client_id = "reg1_collision_client"
    table     = "reg1_table"
    topic     = "test/reg1/msgs"
    N         = 3   # messages to leave unacked

    setup_cdc_table(table, topic, qos=1)

    # 1. Connect with persistent session, subscribe
    s, _ = mqtt_connect(client_id, clean_start=True, session_expiry=300, keep_alive=60)
    s.sendall(create_subscribe_packet(1, topic, qos=1))
    validate_suback(recv_packet(s), 1)
    print(f"  ✓ Subscribed, session_expiry=300")

    # 2. Trigger N CDC messages
    for i in range(N):
        run_psql(f"INSERT INTO {table} (msg) VALUES ('pre-restart-{i}');")

    # 3. Receive all N without ACKing — leave them inflight in broker state
    initial = drain_publishes(s, N, ack=False, timeout_per=5.0)
    assert len(initial) == N, f"Expected {N} PUBLISHes, got {len(initial)}"
    inflight_pids = {pid for _, _, _, _, pid in initial}
    print(f"  ✓ Received {N} unacked PUBLISHes; inflight pids: {inflight_pids}")

    # 4. Cleanly disconnect — session + inflight committed to DB
    s.sendall(create_disconnect_packet())
    s.close()
    time.sleep(0.5)

    # 5. Restart broker — loads sessions and inflight from DB
    restart_broker()

    # 6. Reconnect; broker must restore next_packet_id > max(inflight_pids)
    s2, session_present = mqtt_connect(client_id, clean_start=False, session_expiry=300)
    assert session_present, "Expected session_present=True after restart"
    print(f"  ✓ Reconnected, session_present=True")

    # 7. Receive redeliveries of the N inflight messages (DUP=1, same pids)
    redelivered = drain_publishes(s2, N, ack=False, timeout_per=8.0)
    assert len(redelivered) == N, f"Expected {N} redeliveries, got {len(redelivered)}"
    for _, _, _, dup, pid in redelivered:
        assert dup, f"Expected DUP=1 on redelivery for pid {pid}"
        assert pid in inflight_pids, f"Redelivered pid {pid} not in original inflight set"
    for _, _, _, _, pid in redelivered:
        s2.sendall(create_puback_packet(pid))
    print(f"  ✓ Redeliveries DUP=1 for pids {inflight_pids}, PUBACKs sent")
    time.sleep(0.3)  # let PUBACKs be processed

    # 8. Insert one new CDC message; the broker must assign a pid NOT in inflight_pids
    run_psql(f"INSERT INTO {table} (msg) VALUES ('post-restart-new');")
    new_msgs = drain_publishes(s2, 1, ack=True, timeout_per=8.0)
    assert len(new_msgs) == 1, "No new message received after redelivery round"
    _, _, _, dup_new, new_pid = new_msgs[0]
    assert not dup_new, f"New message should not have DUP=1 (got pid {new_pid})"
    assert new_pid not in inflight_pids, (
        f"COLLISION: new pid {new_pid} reused an inflight pid from {inflight_pids}. "
        "Bug #1 (next_packet_id not advanced past loaded inflight) is not fixed."
    )
    print(f"  ✓ New message assigned pid {new_pid} — no collision with {inflight_pids}")
    s2.close()


# ---------------------------------------------------------------------------
# Bug #2 — publish_messages_batch atomicity
# ---------------------------------------------------------------------------

def test_client_publish_survives_restart():
    """
    A QoS 1 PUBLISH from client A to an offline QoS 1 subscriber (B) must be
    committed to pgmqtt_messages before topic_buffer::push is called.
    Proof: after a broker restart (which clears topic_buffer), subscriber B must
    still receive the message from the DB.
    """
    print("\n[Regression #2] Client-published QoS 1 messages must survive broker restart")

    sub_id  = "reg2_subscriber"
    pub_id  = "reg2_publisher"
    topic   = "test/reg2/atomicity"
    payload = b"atomicity-test-payload"

    # 1. Connect subscriber with persistent session, subscribe, then disconnect
    s_sub, _ = mqtt_connect(sub_id, clean_start=True, session_expiry=300)
    s_sub.sendall(create_subscribe_packet(1, topic, qos=1))
    validate_suback(recv_packet(s_sub), 1)
    s_sub.sendall(create_disconnect_packet())
    s_sub.close()
    time.sleep(0.3)
    print(f"  ✓ Subscriber disconnected with persistent session")

    # 2. Publisher sends a QoS 1 PUBLISH — goes through publish_messages_batch
    s_pub, _ = mqtt_connect(pub_id, clean_start=True)
    s_pub.sendall(create_publish_packet(topic, payload, qos=1, packet_id=42))
    puback = recv_packet(s_pub, timeout=5)
    assert puback is not None, "Publisher got no PUBACK — PUBLISH was not accepted"
    s_pub.close()
    print(f"  ✓ Publisher received PUBACK")

    # 3. Verify the message row exists in the DB (proves the transaction committed)
    time.sleep(0.5)
    rows = run_psql(
        "SELECT COUNT(*) FROM pgmqtt_messages m "
        "JOIN pgmqtt_session_messages sm ON sm.message_id = m.id "
        f"WHERE sm.client_id = '{sub_id}';"
    )
    count = rows[0][0] if rows else 0
    assert count >= 1, (
        f"Message not found in pgmqtt_messages for {sub_id}. "
        "Bug #2: topic_buffer::push may have happened before the DB commit."
    )
    print(f"  ✓ Message found in pgmqtt_messages (count={count})")

    # 4. Restart the broker — in-memory topic_buffer is wiped
    restart_broker()

    # 5. Subscriber reconnects; message must come from the DB, not topic_buffer
    s_sub2, session_present = mqtt_connect(sub_id, clean_start=False, session_expiry=300)
    assert session_present, "Expected session_present=True after restart"

    msgs = drain_publishes(s_sub2, 1, ack=True, timeout_per=10.0)
    assert len(msgs) == 1, (
        "Message not delivered after broker restart. "
        "Bug #2: message was pushed to topic_buffer before DB commit and is now lost."
    )
    _, recv_payload, _, _, _ = msgs[0]
    assert recv_payload == payload, f"Unexpected payload: {recv_payload!r}"
    print(f"  ✓ Message delivered after broker restart — atomicity confirmed")
    s_sub2.close()


# ---------------------------------------------------------------------------
# Bug #3 — subscriptions not reloaded on restart
# ---------------------------------------------------------------------------

def test_subscriptions_reloaded_on_restart():
    """
    After a broker restart, CDC events must be routed to persistent sessions
    that had active subscriptions before the restart.
    Without the fix, db_load_sessions_on_startup never calls subscriptions::subscribe,
    so match_topic() returns empty and CDC events are silently dropped.
    """
    print("\n[Regression #3] Subscriptions must survive broker restart")

    client_id = "reg3_sub_client"
    table     = "reg3_table"
    topic     = "test/reg3/cdc"

    setup_cdc_table(table, topic, qos=1)

    # 1. Connect with persistent session, subscribe, then cleanly disconnect
    s, _ = mqtt_connect(client_id, clean_start=True, session_expiry=300)
    s.sendall(create_subscribe_packet(1, topic, qos=1))
    validate_suback(recv_packet(s), 1)
    s.sendall(create_disconnect_packet())
    s.close()
    time.sleep(0.3)
    print(f"  ✓ Subscribed and disconnected with persistent session")

    # 2. Restart broker — clears all in-memory subscription state
    restart_broker()

    # 3. Insert a CDC event AFTER restart — subscription must have been reloaded
    run_psql(f"INSERT INTO {table} (msg) VALUES ('after-restart-msg');")
    time.sleep(3)  # allow CDC tick to drain the slot and queue the message
    print(f"  ! CDC event inserted post-restart, waiting for delivery...")

    # 4. Reconnect — the message must be in the session queue
    s2, session_present = mqtt_connect(client_id, clean_start=False, session_expiry=300)
    assert session_present, "Expected session_present=True after restart"
    print(f"  ✓ Reconnected, session_present=True")

    msgs = drain_publishes(s2, 1, ack=True, timeout_per=10.0)
    assert len(msgs) == 1, (
        "No CDC message received after restart. "
        "Bug #3: subscriptions were not reloaded — match_topic returned empty "
        "and the CDC event was silently dropped."
    )
    _, payload, qos_r, _, _ = msgs[0]
    assert qos_r == 1
    assert b"after-restart-msg" in payload, f"Unexpected payload: {payload!r}"
    print(f"  ✓ CDC message '{payload!r}' delivered after restart — subscriptions reloaded")
    s2.close()


# ---------------------------------------------------------------------------
# Bug #4 — Receive Maximum not enforced
# ---------------------------------------------------------------------------

def test_receive_maximum_enforced():
    """
    MQTT 5.0 §4.9: the broker MUST NOT send more unacknowledged QoS 1 PUBLISHes
    to a client than the client's Receive Maximum.
    With Receive Maximum=2 and 8 messages available, at most 2 should arrive
    before any PUBACK is sent.
    """
    print("\n[Regression #4] Broker must honour client Receive Maximum")

    client_id = "reg4_recv_max_client"
    table     = "reg4_table"
    topic     = "test/reg4/recvmax"
    RECV_MAX  = 2
    N         = 8   # intentionally > RECV_MAX

    setup_cdc_table(table, topic, qos=1)

    # Connect advertising Receive Maximum = RECV_MAX
    s, _ = mqtt_connect(client_id, clean_start=True, keep_alive=120,
                        receive_maximum=RECV_MAX)
    s.sendall(create_subscribe_packet(1, topic, qos=1))
    validate_suback(recv_packet(s), 1)
    print(f"  ✓ Connected with Receive Maximum={RECV_MAX}, subscribed")

    # Insert N CDC messages
    for i in range(N):
        run_psql(f"INSERT INTO {table} (msg) VALUES ('rm-msg-{i}');")
    print(f"  ! Inserted {N} CDC messages, waiting for initial burst (no PUBACKs sent)...")

    # Collect whatever arrives without sending any PUBACKs
    # Give enough time for all messages to arrive if the broker ignores Receive Maximum
    burst_pids = []
    deadline = time.time() + 5
    while time.time() < deadline:
        pkt = recv_packet(s, timeout=0.5)
        if pkt is None:
            continue
        ptype = (pkt[0] & 0xF0) >> 4
        if ptype == MQTTControlPacket.PUBLISH:
            _, _, qos_r, _, _, pid, _ = validate_publish(pkt)
            assert qos_r == 1
            burst_pids.append(pid)

    assert len(burst_pids) <= RECV_MAX, (
        f"Broker sent {len(burst_pids)} QoS 1 PUBLISHes before any PUBACK, "
        f"but Receive Maximum={RECV_MAX}. "
        "Bug #4: Receive Maximum is parsed but not enforced."
    )
    assert len(burst_pids) > 0, "No QoS 1 PUBLISHes received at all"
    print(f"  ✓ Broker sent {len(burst_pids)} (≤{RECV_MAX}) messages before first PUBACK")

    # ACK all received and drain the rest
    for pid in burst_pids:
        s.sendall(create_puback_packet(pid))
    drain_publishes(s, N - len(burst_pids), ack=True, timeout_per=3.0)
    s.close()


# ---------------------------------------------------------------------------
# Bug #5 — Incomplete disconnect in publish_pending_messages
# ---------------------------------------------------------------------------

def test_write_error_during_delivery_triggers_lwt():
    """
    When the broker detects a write error while sending QoS 1 messages to a client
    (via publish_pending_messages), it must trigger the client's LWT — exactly as if
    the client had abruptly disconnected on the read path.
    Without the fix, publish_pending_messages only calls clients.remove(), missing
    the LWT publication and session cleanup.
    """
    print("\n[Regression #5] Write error during delivery must trigger client LWT")

    lwt_topic   = "test/reg5/lwt"
    lwt_payload = b"reg5-victim-died"
    victim_id   = "reg5_victim_client"
    watcher_id  = "reg5_watcher_client"
    table       = "reg5_table"
    data_topic  = "test/reg5/data"

    setup_cdc_table(table, data_topic, qos=1)

    # 1. Watcher subscribes to the LWT topic
    s_watch, _ = mqtt_connect(watcher_id, clean_start=True)
    s_watch.sendall(create_subscribe_packet(1, lwt_topic, qos=0))
    validate_suback(recv_packet(s_watch), 1)
    print(f"  ✓ Watcher subscribed to LWT topic")

    # 2. Victim connects with LWT and subscribes to the data topic (will NOT ACK)
    s_victim, _ = mqtt_connect(
        victim_id,
        clean_start=True,
        keep_alive=60,
        will_topic=lwt_topic,
        will_payload=lwt_payload,
        will_qos=0,
    )
    s_victim.sendall(create_subscribe_packet(1, data_topic, qos=1))
    validate_suback(recv_packet(s_victim), 1)
    print(f"  ✓ Victim connected with LWT, subscribed to data topic")

    # 3. Flood the broker so messages are actively being delivered to victim
    for i in range(30):
        run_psql(f"INSERT INTO {table} (msg) VALUES ('data-{i}');")
    time.sleep(1)  # let CDC process events and start pushing to victim

    # 4. Abruptly close victim's socket mid-delivery — broker sees write error in
    #    publish_pending_messages before the next poll_mqtt_clients read detects it
    s_victim.close()
    print(f"  ! Victim socket closed — broker should detect write error and trigger LWT")

    # 5. Watcher must receive the LWT within a few seconds
    lwt_received = False
    deadline = time.time() + 10
    while time.time() < deadline:
        pkt = recv_packet(s_watch, timeout=1.0)
        if pkt is None:
            continue
        ptype = (pkt[0] & 0xF0) >> 4
        if ptype == MQTTControlPacket.PUBLISH:
            t, p, _, _, _, _, _ = validate_publish(pkt)
            if t == lwt_topic and p == lwt_payload:
                lwt_received = True
                print(f"  ✓ Watcher received LWT: topic={t!r} payload={p!r}")
                break

    assert lwt_received, (
        "Watcher did not receive the victim's LWT after write-error disconnect. "
        "Bug #5: publish_pending_messages removes the client but does not trigger LWT."
    )
    s_watch.close()


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    failures = []
    tests = [
        test_no_packet_id_collision_after_restart,
        test_client_publish_survives_restart,
        test_subscriptions_reloaded_on_restart,
        test_receive_maximum_enforced,
        test_write_error_during_delivery_triggers_lwt,
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
        print("\nAll regression tests PASSED")
