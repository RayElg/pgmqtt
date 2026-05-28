"""Integration tests for the enterprise per-topic ACL layer.

Covers:
- pgmqtt_acls allowlist enforced on SUBSCRIBE and PUBLISH when the role's
  rows exist.
- Empty allowlist = unrestricted (Community fallback when license has no ACL).
- pgmqtt_reload_acls refreshes a live connection's permissions without
  disconnecting it.
"""

import os
import socket
import struct
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from helpers.guc import reset_guc, set_guc  # noqa: E402
from helpers.license import generate_test_license  # noqa: E402
from helpers.mqtt import (  # noqa: E402
    build_connect_packet,
    parse_connack,
    read_packet,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "integration"))
from test_utils import get_db_conn, run_sql  # type: ignore[import-not-found]  # noqa: E402


MQTT_HOST = os.environ.get("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))

RC_SUCCESS = 0x00
RC_NOT_AUTHORIZED = 0x87
GRANTED_QOS_0 = 0x00

TEST_ROLE = "mqtt_acltest"
TEST_PASSWORD = "acl-test-pw"

pytestmark = pytest.mark.enterprise


# ---------------------------------------------------------------------------
# Local helpers
# ---------------------------------------------------------------------------


def _create_role() -> None:
    conn = get_db_conn()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SET password_encryption = 'scram-sha-256'")
            cur.execute(f'DROP ROLE IF EXISTS "{TEST_ROLE}"')
            cur.execute(
                f'CREATE ROLE "{TEST_ROLE}" LOGIN PASSWORD %s', (TEST_PASSWORD,)
            )
    finally:
        conn.close()


def _drop_role() -> None:
    run_sql(f'DROP ROLE IF EXISTS "{TEST_ROLE}"')


def _clear_acls() -> None:
    run_sql(f"DELETE FROM pgmqtt_acls WHERE role_name = '{TEST_ROLE}'")


def _insert_acl(topic: str, can_pub: bool, can_sub: bool) -> None:
    run_sql(
        f"INSERT INTO pgmqtt_acls (role_name, topic_filter, can_publish, can_subscribe) "
        f"VALUES ('{TEST_ROLE}', '{topic}', {str(can_pub).lower()}, {str(can_sub).lower()})"
    )


def _mqtt_connect_authed(client_id: str) -> socket.socket:
    sock = socket.create_connection((MQTT_HOST, MQTT_PORT), timeout=5.0)
    pkt = build_connect_packet(
        client_id=client_id,
        username=TEST_ROLE,
        password=TEST_PASSWORD.encode(),
        clean_start=True,
        keep_alive=60,
    )
    sock.sendall(pkt)
    connack = read_packet(sock, timeout=5.0)
    rc = parse_connack(connack)
    assert rc == RC_SUCCESS, f"unexpected CONNACK rc=0x{rc:02x}"
    return sock


def _build_subscribe(topic: str, packet_id: int = 1) -> bytes:
    tf = topic.encode("utf-8")
    var = struct.pack("!H", packet_id) + bytes([0x00])  # properties=empty
    payload = struct.pack("!H", len(tf)) + tf + bytes([0x00])  # QoS 0
    remaining = var + payload
    return bytes([0x82]) + bytes([len(remaining)]) + remaining


def _build_publish_qos1(topic: str, payload: bytes, packet_id: int) -> bytes:
    tb = topic.encode("utf-8")
    var = struct.pack("!H", len(tb)) + tb + struct.pack("!H", packet_id) + bytes([0x00])
    remaining = var + payload
    return bytes([0x32]) + bytes([len(remaining)]) + remaining  # QoS 1


def _parse_suback_reason(pkt: bytes) -> int:
    """SUBACK reason code (last byte after properties)."""
    assert (pkt[0] >> 4) == 9, f"expected SUBACK, got {pkt[0] >> 4}"
    return pkt[-1]


def _parse_puback_reason(pkt: bytes) -> int:
    """PUBACK reason code (byte 4 in v5 with non-zero rc; v5 omits it on rc=0)."""
    assert (pkt[0] >> 4) == 4, f"expected PUBACK, got {pkt[0] >> 4}"
    # Fixed header: 0x40 + remlen. Variable header starts at 2.
    # packet_id (2) + optional reason_code + properties.
    # If remlen == 2 the v5 spec says rc=0 implicitly.
    remlen = pkt[1]
    if remlen == 2:
        return 0
    return pkt[4]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def _verifier_is_scram():
    rows = run_sql("SHOW password_encryption")
    enc = rows[0][0] if rows else None
    if enc != "scram-sha-256":
        pytest.skip(
            f"password_encryption = '{enc}', need 'scram-sha-256' for ACL tests"
        )


@pytest.fixture(scope="module", autouse=True)
def _test_role():
    _create_role()
    yield
    _drop_role()


@pytest.fixture()
def acl_license():
    """Install an enterprise license that includes the 'acl' feature."""
    token = generate_test_license(
        customer="acl-test",
        days=1,
        features=["acl", "jwt", "tls", "metrics"],
        max_connections=100,
    )
    set_guc("pgmqtt.license_key", token)
    yield
    reset_guc("pgmqtt.license_key")


@pytest.fixture()
def no_acl_license():
    """Install an enterprise license WITHOUT the 'acl' feature."""
    token = generate_test_license(
        customer="no-acl-test",
        days=1,
        features=["jwt", "tls", "metrics"],
        max_connections=100,
    )
    set_guc("pgmqtt.license_key", token)
    yield
    reset_guc("pgmqtt.license_key")


@pytest.fixture(autouse=True)
def _auth_enabled_and_clean():
    set_guc("pgmqtt.password_auth_enabled", "on")
    _clear_acls()
    yield
    _clear_acls()
    reset_guc("pgmqtt.password_auth_enabled")
    reset_guc("pgmqtt.acl_default_deny")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_subscribe_allowed_topic_succeeds(acl_license):
    _insert_acl("allowed/+", can_pub=False, can_sub=True)

    sock = _mqtt_connect_authed("acl-sub-ok")
    try:
        sock.sendall(_build_subscribe("allowed/temp", packet_id=1))
        suback = read_packet(sock, timeout=5.0)
        rc = _parse_suback_reason(suback)
        assert rc == GRANTED_QOS_0, f"expected SUBACK granted, got 0x{rc:02x}"
    finally:
        sock.close()


def test_subscribe_disallowed_topic_returns_not_authorized(acl_license):
    _insert_acl("allowed/+", can_pub=False, can_sub=True)

    sock = _mqtt_connect_authed("acl-sub-blocked")
    try:
        sock.sendall(_build_subscribe("forbidden/data", packet_id=2))
        suback = read_packet(sock, timeout=5.0)
        rc = _parse_suback_reason(suback)
        assert rc == RC_NOT_AUTHORIZED, \
            f"expected NOT_AUTHORIZED, got 0x{rc:02x}"
    finally:
        sock.close()


def test_publish_qos1_disallowed_returns_not_authorized(acl_license):
    _insert_acl("telemetry/+", can_pub=True, can_sub=False)

    sock = _mqtt_connect_authed("acl-pub-blocked")
    try:
        sock.sendall(_build_publish_qos1("commands/forbidden", b"hi", packet_id=42))
        puback = read_packet(sock, timeout=5.0)
        rc = _parse_puback_reason(puback)
        assert rc == RC_NOT_AUTHORIZED, \
            f"expected NOT_AUTHORIZED on PUBACK, got 0x{rc:02x}"
    finally:
        sock.close()


def test_publish_qos1_allowed_succeeds(acl_license):
    _insert_acl("telemetry/+", can_pub=True, can_sub=False)

    sock = _mqtt_connect_authed("acl-pub-ok")
    try:
        sock.sendall(_build_publish_qos1("telemetry/temp", b"ok", packet_id=7))
        puback = read_packet(sock, timeout=5.0)
        rc = _parse_puback_reason(puback)
        assert rc == 0, f"expected PUBACK rc=0, got 0x{rc:02x}"
    finally:
        sock.close()


def test_pub_and_sub_are_independent(acl_license):
    """The publish and subscribe allowlists are built from independent
    columns of pgmqtt_acls.

    With one pub-only row and one sub-only row on different topics:
    - PUBLISH to the pub-only topic is allowed.
    - SUBSCRIBE to the pub-only topic is NOT_AUTHORIZED (not in sub list).
    - SUBSCRIBE to the sub-only topic is allowed.
    """
    _insert_acl("pub/only", can_pub=True, can_sub=False)
    _insert_acl("sub/only", can_pub=False, can_sub=True)

    sock = _mqtt_connect_authed("acl-pub-sub-independent")
    try:
        # Publish to pub/only allowed.
        sock.sendall(_build_publish_qos1("pub/only", b"x", packet_id=1))
        puback = read_packet(sock, timeout=5.0)
        assert _parse_puback_reason(puback) == 0

        # Subscribe to pub/only is denied — only sub/only is in the
        # subscribe allowlist.
        sock.sendall(_build_subscribe("pub/only", packet_id=2))
        suback = read_packet(sock, timeout=5.0)
        assert _parse_suback_reason(suback) == RC_NOT_AUTHORIZED

        # Subscribe to sub/only is granted.
        sock.sendall(_build_subscribe("sub/only", packet_id=3))
        suback = read_packet(sock, timeout=5.0)
        assert _parse_suback_reason(suback) == GRANTED_QOS_0
    finally:
        sock.close()


def test_default_deny_off_restores_unrestricted(acl_license):
    """With acl_default_deny explicitly off, a role with zero rows in
    pgmqtt_acls keeps full access — the pre-0.3.0 fail-open behavior, now
    opt-in."""
    set_guc("pgmqtt.acl_default_deny", "off")
    # No rows inserted.
    sock = _mqtt_connect_authed("acl-empty")
    try:
        sock.sendall(_build_subscribe("anything/here", packet_id=1))
        suback = read_packet(sock, timeout=5.0)
        assert _parse_suback_reason(suback) == GRANTED_QOS_0
    finally:
        sock.close()


def test_without_acl_license_rows_are_ignored(no_acl_license):
    """When the license lacks the 'acl' feature, pgmqtt_acls is not consulted —
    the client gets unrestricted access regardless of what's in the table."""
    _insert_acl("only/this", can_pub=False, can_sub=True)

    sock = _mqtt_connect_authed("acl-no-license")
    try:
        # Without the license, the allowlist is treated as empty
        # (unrestricted), so a topic NOT in the table is still allowed.
        sock.sendall(_build_subscribe("not/in/table", packet_id=1))
        suback = read_packet(sock, timeout=5.0)
        assert _parse_suback_reason(suback) == GRANTED_QOS_0
    finally:
        sock.close()


def test_default_deny_blocks_role_with_no_acls(acl_license):
    """By default (acl_default_deny on since 0.3.0), a password-authed role that
    has zero rows in pgmqtt_acls can neither subscribe nor publish (fail
    closed). No GUC is set here — this exercises the shipped default."""
    # No rows inserted for the role.
    sock = _mqtt_connect_authed("acl-deny-norows")
    try:
        sock.sendall(_build_subscribe("anything/here", packet_id=1))
        suback = read_packet(sock, timeout=5.0)
        assert _parse_suback_reason(suback) == RC_NOT_AUTHORIZED, \
            "default-deny should reject SUBSCRIBE for a role with no ACL rows"

        sock.sendall(_build_publish_qos1("anything/here", b"x", packet_id=2))
        puback = read_packet(sock, timeout=5.0)
        assert _parse_puback_reason(puback) == RC_NOT_AUTHORIZED, \
            "default-deny should reject PUBLISH for a role with no ACL rows"
    finally:
        sock.close()


def test_default_deny_is_per_side(acl_license):
    """default-deny is applied independently to the sub and pub allowlists: a
    role granted only a subscribe row keeps that subscribe access but is denied
    all publishing (its pub allowlist is empty → deny-all). Relies on the
    default-on behavior."""
    _insert_acl("allowed/#", can_pub=False, can_sub=True)

    sock = _mqtt_connect_authed("acl-deny-perside")
    try:
        # Granted subscribe topic still works.
        sock.sendall(_build_subscribe("allowed/here", packet_id=1))
        suback = read_packet(sock, timeout=5.0)
        assert _parse_suback_reason(suback) == GRANTED_QOS_0

        # Subscribe outside the grant is denied.
        sock.sendall(_build_subscribe("other/topic", packet_id=2))
        suback = read_packet(sock, timeout=5.0)
        assert _parse_suback_reason(suback) == RC_NOT_AUTHORIZED

        # Publishing is denied entirely — no can_publish row exists.
        sock.sendall(_build_publish_qos1("allowed/here", b"x", packet_id=3))
        puback = read_packet(sock, timeout=5.0)
        assert _parse_puback_reason(puback) == RC_NOT_AUTHORIZED
    finally:
        sock.close()


def test_default_deny_ignored_without_acl_license(no_acl_license):
    """acl_default_deny is an enterprise ('acl' feature) capability: without the
    license it must not lock out a Community password-authed client, which stays
    unrestricted."""
    set_guc("pgmqtt.acl_default_deny", "on")
    sock = _mqtt_connect_authed("acl-deny-no-license")
    try:
        sock.sendall(_build_subscribe("anything/here", packet_id=1))
        suback = read_packet(sock, timeout=5.0)
        assert _parse_suback_reason(suback) == GRANTED_QOS_0, \
            "default-deny should be a no-op without the 'acl' license feature"
    finally:
        sock.close()


def test_reload_acls_refreshes_without_disconnect(acl_license):
    """pgmqtt_reload_acls swaps the in-memory allowlist on a live client.

    Connect with no rules (unrestricted) → add a deny-implying rule
    (only 'allowed/#' permitted) → reload → previously-allowed topic is now
    rejected on the SAME socket without a reconnect.
    """
    # This scenario starts from the unrestricted (no-rows) state, so opt out of
    # the 0.3.0 default-deny; the point under test is the live in-memory swap.
    set_guc("pgmqtt.acl_default_deny", "off")
    sock = _mqtt_connect_authed("acl-reload")
    try:
        # Initially unrestricted: subscribe to a topic we'll later forbid.
        sock.sendall(_build_subscribe("once/allowed", packet_id=1))
        suback = read_packet(sock, timeout=5.0)
        assert _parse_suback_reason(suback) == GRANTED_QOS_0

        # Restrict the role to 'allowed/#' only, then reload the live conn.
        _insert_acl("allowed/#", can_pub=True, can_sub=True)
        run_sql("SELECT pgmqtt_reload_acls('acl-reload')")
        time.sleep(0.3)

        # 'once/allowed' is no longer in the allowlist → SUBSCRIBE denied
        # on the SAME connection.
        sock.sendall(_build_subscribe("once/allowed", packet_id=2))
        suback = read_packet(sock, timeout=5.0)
        assert _parse_suback_reason(suback) == RC_NOT_AUTHORIZED, \
            "reload_acls did not take effect on live connection"

        # Topic that IS in the new allowlist still works.
        sock.sendall(_build_subscribe("allowed/here", packet_id=3))
        suback = read_packet(sock, timeout=5.0)
        assert _parse_suback_reason(suback) == GRANTED_QOS_0
    finally:
        sock.close()


def test_reload_acls_wildcard_refreshes_all(acl_license):
    """pgmqtt_reload_acls('*') refreshes every password-authenticated client."""
    sock_a = _mqtt_connect_authed("acl-reload-all-a")
    sock_b = _mqtt_connect_authed("acl-reload-all-b")
    try:
        _insert_acl("only/this/one", can_pub=False, can_sub=True)
        run_sql("SELECT pgmqtt_reload_acls('*')")
        time.sleep(0.3)

        for sock, pid in ((sock_a, 1), (sock_b, 2)):
            sock.sendall(_build_subscribe("something/else", packet_id=pid))
            suback = read_packet(sock, timeout=5.0)
            assert _parse_suback_reason(suback) == RC_NOT_AUTHORIZED
    finally:
        for s in (sock_a, sock_b):
            try:
                s.close()
            except OSError:
                pass
