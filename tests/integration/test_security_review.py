"""Regression tests for the 2026-07 security/correctness review fixes.

Community-broker coverage (the multiprocess-only fixes are covered in
tests/enterprise/integration/test_multiprocess.py):

- Client IDs longer than the cross-worker command cap (128 bytes) are
  rejected at CONNECT instead of being admitted but unaddressable.
- Empty client IDs get broker-unique identifiers, returned to MQTT 5
  clients in the CONNACK Assigned Client Identifier property.
- Durable sessions are bound to the authenticated principal: the same
  client_id under a different identity gets a fresh session and never
  receives the previous identity's queued payloads.
- A failed CDC persist aborts the whole batch (slot unmoved) and the
  message is delivered once the failure clears — previously the slot
  advanced anyway and the message was silently lost.
- A burst of simultaneously-readable sockets larger than the old 256-event
  epoll buffer must not stall the tick loop (level-triggered re-reporting
  made the old drain loop a remotely triggerable livelock).
"""

import os
import socket
import struct
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from helpers.guc import reset_guc, set_guc  # noqa: E402
from proto_utils import (  # noqa: E402
    MQTTControlPacket,
    create_connect_packet,
    create_disconnect_packet,
    create_publish_packet,
    create_subscribe_packet,
    encode_properties,
    encode_utf8_string,
    encode_variable_byte_integer,
    recv_packet,
    validate_connack,
    validate_puback,
    validate_suback,
)
from test_utils import get_db_conn, run_sql  # noqa: E402

MQTT_HOST = os.environ.get("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))

CLIENT_ID_CAP = 128  # shmem_bridge::CMD_ARG_CAP
RC_SUCCESS = 0x00
RC_CLIENT_IDENTIFIER_NOT_VALID = 0x85
V3_IDENTIFIER_REJECTED = 0x02
PROP_ASSIGNED_CLIENT_ID = 0x12
PROP_SESSION_EXPIRY = 0x11

ROLE_A = "secrev_role_a"
ROLE_A_PW = "secrev-pw-a"
ROLE_B = "secrev_role_b"
ROLE_B_PW = "secrev-pw-b"


def _connect(client_id, *, protocol_version=5, clean_start=True,
             session_expiry=None, username=None, password=None,
             timeout=5.0):
    """CONNECT with optional credentials + session expiry; returns
    (socket, session_present, reason_code, props)."""
    v5 = protocol_version == 5
    flags = 0x02 if clean_start else 0x00
    if username is not None:
        flags |= 0x80
    if password is not None:
        flags |= 0x40
    props = b""
    if v5:
        props = encode_properties(
            {PROP_SESSION_EXPIRY: session_expiry} if session_expiry is not None else {}
        )
    payload = encode_utf8_string(client_id)
    if username is not None:
        payload += encode_utf8_string(username)
    if password is not None:
        payload += struct.pack("!H", len(password)) + password
    vh = (
        encode_utf8_string("MQTT")
        + (b"\x05" if v5 else b"\x04")
        + bytes([flags])
        + struct.pack("!H", 60)
        + props
    )
    pkt = (
        bytes([MQTTControlPacket.CONNECT << 4])
        + encode_variable_byte_integer(len(vh) + len(payload))
        + vh
        + payload
    )
    s = socket.create_connection((MQTT_HOST, MQTT_PORT), timeout=timeout)
    s.sendall(pkt)
    connack = recv_packet(s, timeout=timeout)
    assert connack is not None, "no CONNACK"
    session_present, rc, connack_props = validate_connack(connack, protocol_version)
    return s, session_present, rc, connack_props


def _subscribe(s, packet_id, topic_filter, qos):
    s.sendall(create_subscribe_packet(packet_id, topic_filter, qos=qos))
    suback = recv_packet(s)
    assert suback is not None, "no SUBACK"
    validate_suback(suback, packet_id)


def _publish_qos1_acked(s, topic, payload, packet_id):
    s.sendall(create_publish_packet(topic, payload, qos=1, packet_id=packet_id))
    puback = recv_packet(s, timeout=10.0)
    assert puback is not None, f"no PUBACK for packet {packet_id}"
    validate_puback(puback, packet_id)


def _poll_sql_value(query, want, timeout=15.0):
    deadline = time.time() + timeout
    val = None
    while time.time() < deadline:
        val = run_sql(query)[0][0]
        if val == want:
            return val
        time.sleep(0.5)
    return val


# ---------------------------------------------------------------------------
# Client-id admission bound
# ---------------------------------------------------------------------------


def test_client_id_at_cap_accepted():
    s, _present, rc, _props = _connect("c" * CLIENT_ID_CAP)
    s.close()
    assert rc == RC_SUCCESS


def test_client_id_over_cap_rejected_v5():
    s, _present, rc, _props = _connect("c" * (CLIENT_ID_CAP + 1))
    s.close()
    assert rc == RC_CLIENT_IDENTIFIER_NOT_VALID, hex(rc)


def test_client_id_over_cap_rejected_v311():
    s, _present, rc, _props = _connect("c" * (CLIENT_ID_CAP + 1), protocol_version=4)
    s.close()
    assert rc == V3_IDENTIFIER_REJECTED, hex(rc)


# ---------------------------------------------------------------------------
# Assigned client identifiers
# ---------------------------------------------------------------------------


def test_empty_client_id_gets_unique_assigned_id():
    """MQTT-3.1.3-7: a v5 client that sent an empty client ID is told the
    identifier the server assigned, and two such clients never share one
    (the old per-process counter handed out pgmqtt-auto-0 on every worker)."""
    s1, _p1, rc1, props1 = _connect("")
    s2, _p2, rc2, props2 = _connect("")
    try:
        assert rc1 == RC_SUCCESS and rc2 == RC_SUCCESS
        id1 = props1.get(PROP_ASSIGNED_CLIENT_ID)
        id2 = props2.get(PROP_ASSIGNED_CLIENT_ID)
        assert id1 and id2, f"missing Assigned Client Identifier: {props1} {props2}"
        assert id1 != id2, f"duplicate assigned client id: {id1}"
    finally:
        s1.close()
        s2.close()


# ---------------------------------------------------------------------------
# Session identity binding
# ---------------------------------------------------------------------------


@pytest.fixture()
def _password_roles():
    conn = get_db_conn()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SET password_encryption = 'scram-sha-256'")
            for role, pw in [(ROLE_A, ROLE_A_PW), (ROLE_B, ROLE_B_PW)]:
                cur.execute(f'DROP ROLE IF EXISTS "{role}"')
                cur.execute(f'CREATE ROLE "{role}" LOGIN PASSWORD %s', (pw,))
    finally:
        conn.close()
    set_guc("pgmqtt.password_auth_enabled", "on")
    yield
    reset_guc("pgmqtt.password_auth_enabled")
    run_sql(f'DROP ROLE IF EXISTS "{ROLE_A}"')
    run_sql(f'DROP ROLE IF EXISTS "{ROLE_B}"')


def test_session_not_resumed_across_identities(_password_roles):
    """A persistent session created by one authenticated role must not be
    resumed — queued payloads included — by a different role reusing the
    same client_id."""
    cid = "secrev-ident"
    topic = "secrev/ident/t"

    # Role A creates a durable session with a QoS 1 subscription, then goes
    # offline (normal DISCONNECT keeps the session alive).
    s, _present, rc, _props = _connect(
        cid, session_expiry=600, username=ROLE_A, password=ROLE_A_PW.encode()
    )
    assert rc == RC_SUCCESS
    _subscribe(s, 1, topic, qos=1)
    s.sendall(create_disconnect_packet())
    s.close()

    # A message published meanwhile is queued for the offline session.
    pub, _p, rc, _pr = _connect("secrev-ident-pub")
    assert rc == RC_SUCCESS
    _publish_qos1_acked(pub, topic, b"for-role-a-only", 7)
    pub.close()
    queued = _poll_sql_value(
        f"SELECT count(*) FROM pgmqtt_session_messages WHERE client_id = '{cid}'", 1
    )
    assert queued == 1, f"expected 1 queued message, found {queued}"

    # Role B reuses the client_id: fresh session, nothing handed over.
    s2, present, rc, _props = _connect(
        cid,
        clean_start=False,
        session_expiry=600,
        username=ROLE_B,
        password=ROLE_B_PW.encode(),
    )
    try:
        assert rc == RC_SUCCESS
        assert not present, "session_present=1 across different identities"
        leaked = recv_packet(s2, timeout=3.0)
        assert leaked is None or (leaked[0] & 0xF0) >> 4 != MQTTControlPacket.PUBLISH, (
            f"role B received role A's queued payload: {leaked!r}"
        )
    finally:
        s2.close()
    remaining = _poll_sql_value(
        f"SELECT count(*) FROM pgmqtt_session_messages WHERE client_id = '{cid}'", 0
    )
    assert remaining == 0, "previous identity's queued messages survived the identity change"
    run_sql(f"DELETE FROM pgmqtt_sessions WHERE client_id = '{cid}'")


def test_session_resumes_for_same_identity(_password_roles):
    """Positive control: the same role does resume its session."""
    cid = "secrev-ident-same"
    s, _present, rc, _props = _connect(
        cid, session_expiry=600, username=ROLE_A, password=ROLE_A_PW.encode()
    )
    assert rc == RC_SUCCESS
    _subscribe(s, 1, "secrev/same/t", qos=1)
    s.sendall(create_disconnect_packet())
    s.close()

    s2, present, rc, _props = _connect(
        cid,
        clean_start=False,
        session_expiry=600,
        username=ROLE_A,
        password=ROLE_A_PW.encode(),
    )
    s2.close()
    assert rc == RC_SUCCESS
    assert present, "same identity failed to resume its own session"
    run_sql(f"DELETE FROM pgmqtt_sessions WHERE client_id = '{cid}'")


# ---------------------------------------------------------------------------
# CDC batch failure atomicity
# ---------------------------------------------------------------------------


def test_cdc_persist_failure_aborts_batch_and_retries():
    """When persisting a CDC-rendered message fails, the whole batch must
    abort without consuming the slot so the event is retried; the message
    must reach a subscriber once the failure clears. Previously the slot
    position still advanced, permanently losing the message while the log
    claimed a retry.

    Asserted through a live subscriber: a delivered-but-unreceivable row is
    reclaimed by the no-subscriber cleanup, so row counts in
    pgmqtt_messages race that reclaim."""
    blocked_topic = "secrev/cdc/blocked"
    run_sql("DROP TABLE IF EXISTS secrev_cdc")
    run_sql("CREATE TABLE secrev_cdc (id serial PRIMARY KEY, name text, val text)")
    run_sql("ALTER TABLE secrev_cdc REPLICA IDENTITY FULL")
    run_sql(
        "SELECT pgmqtt_add_outbound_mapping('public', 'secrev_cdc', "
        "'secrev/cdc/{{ columns.name }}', '{{ columns.val }}', 1, 'secrev')"
    )
    time.sleep(6)  # mapping propagates to the slot cache through WAL

    # Leftover rows for this topic (earlier runs against the same data
    # directory) would make the ADD CONSTRAINT below fail on existing data.
    def _purge_topic():
        run_sql(
            "DELETE FROM pgmqtt_session_messages WHERE message_id IN "
            f"(SELECT id FROM pgmqtt_messages WHERE topic = '{blocked_topic}')"
        )
        run_sql(
            "DELETE FROM pgmqtt_cdc_outbox WHERE id IN "
            f"(SELECT id FROM pgmqtt_messages WHERE topic = '{blocked_topic}')"
        )
        run_sql(f"DELETE FROM pgmqtt_messages WHERE topic = '{blocked_topic}'")

    _purge_topic()
    sub, _present, rc, _props = _connect("secrev-cdc-sub")
    assert rc == RC_SUCCESS
    try:
        _subscribe(sub, 1, "secrev/cdc/#", qos=1)
        run_sql(
            "ALTER TABLE pgmqtt_messages ADD CONSTRAINT secrev_block "
            f"CHECK (topic <> '{blocked_topic}')"
        )
        try:
            run_sql("INSERT INTO secrev_cdc (name, val) VALUES ('blocked', 'survives')")
            # While blocked: every batch aborts, nothing is delivered.
            early = recv_packet(sub, timeout=3.0)
            assert early is None or (early[0] & 0xF0) >> 4 != MQTTControlPacket.PUBLISH, (
                f"message delivered while its persist was failing: {early!r}"
            )
        finally:
            run_sql("ALTER TABLE pgmqtt_messages DROP CONSTRAINT secrev_block")

        # The aborted batches never consumed the event, so it replays and
        # is delivered once the constraint is gone.
        got = None
        deadline = time.time() + 20.0
        while time.time() < deadline:
            pkt = recv_packet(sub, timeout=2.0)
            if pkt is not None and (pkt[0] & 0xF0) >> 4 == MQTTControlPacket.PUBLISH:
                got = pkt
                break
        assert got is not None, (
            "CDC event was consumed while its persist was failing — "
            "slot advanced without the message"
        )
        assert b"survives" in bytes(got), got
    finally:
        sub.close()
        run_sql("SELECT pgmqtt_remove_outbound_mapping('public', 'secrev_cdc', 'secrev')")
        run_sql("DROP TABLE IF EXISTS secrev_cdc")
        time.sleep(1)  # let the in-flight tick's bookkeeping commit
        _purge_topic()


# ---------------------------------------------------------------------------
# Readiness (epoll) under a wide simultaneous burst
# ---------------------------------------------------------------------------


def test_many_simultaneously_ready_sockets_do_not_stall_the_loop():
    """More than 256 sockets with unconsumed input at the same instant: the
    old ready-set drain re-polled level-triggered epoll until it returned a
    short batch, which never happens while >=256 fds stay readable — a
    remotely triggerable livelock of the whole worker. Every ping must be
    answered and the broker must still accept new connections afterwards."""
    n = 300
    conns = []
    try:
        for i in range(n):
            s = socket.create_connection((MQTT_HOST, MQTT_PORT), timeout=10.0)
            s.sendall(create_connect_packet(f"secrev-epoll-{i}"))
            connack = recv_packet(s, timeout=10.0)
            assert connack is not None, f"no CONNACK for connection {i}"
            conns.append(s)

        # Several rounds maximize the chance that one tick's epoll_wait sees
        # >= 256 ready fds (PINGRESP is 2 bytes; nothing is read back until
        # all sends are done, so readiness accumulates).
        pingreq = bytes([0xC0, 0x00])
        for _ in range(3):
            for s in conns:
                s.sendall(pingreq)

        for i, s in enumerate(conns):
            got_pingresp = False
            deadline = time.time() + 30.0
            while time.time() < deadline:
                pkt = recv_packet(s, timeout=5.0)
                assert pkt is not None, f"connection {i}: no PINGRESP (loop stalled?)"
                if (pkt[0] & 0xF0) >> 4 == MQTTControlPacket.PINGRESP:
                    got_pingresp = True
                    break
            assert got_pingresp, f"connection {i}: no PINGRESP within 30s"

        # The loop must still serve newcomers.
        probe, _p, rc, _pr = _connect("secrev-epoll-probe")
        probe.close()
        assert rc == RC_SUCCESS
    finally:
        for s in conns:
            try:
                s.close()
            except OSError:
                pass
