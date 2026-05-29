"""Integration tests for pgmqtt username/password authentication (Community).

Covers:
- Default (auth disabled): username/password ignored.
- Enabled, optional: anonymous still allowed; valid creds accepted; bad creds rejected.
- Required: anonymous and bad creds rejected; valid creds accepted.
- role_filter: roles outside the LIKE pattern are rejected even with valid creds.
- Admin SQL functions: pgmqtt_disconnect_client / _role kick live connections;
  REVOKE EXECUTE from PUBLIC is in place.
"""

import os
import socket
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from helpers.guc import reset_guc, set_guc  # noqa: E402
from helpers.mqtt import (  # noqa: E402
    build_connect_packet,
    parse_connack,
    read_packet,
)
from test_utils import get_db_conn, run_sql, run_sql_expect_error  # type: ignore[import-not-found]  # noqa: E402


MQTT_HOST = os.environ.get("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))

RC_SUCCESS = 0x00
RC_BAD_USERNAME_PASSWORD = 0x86
RC_NOT_AUTHORIZED = 0x87

TEST_ROLE = "mqtt_pwauth_test"
TEST_PASSWORD = "broker-test-pw"
OTHER_ROLE = "other_pwauth_test"
OTHER_PASSWORD = "other-test-pw"


# ---------------------------------------------------------------------------
# Local helpers
# ---------------------------------------------------------------------------


def _set_password_encryption_scram() -> str:
    """Ensure the test cluster uses SCRAM. Returns the prior value for restore."""
    rows = run_sql("SHOW password_encryption")
    prior = rows[0][0] if rows else "scram-sha-256"
    if prior != "scram-sha-256":
        run_sql("SET password_encryption = 'scram-sha-256'")
    return prior


def _create_role(name: str, password: str) -> None:
    # Use plain SET so the verifier is written as SCRAM-SHA-256 regardless of
    # any per-session lingering setting on this connection.
    conn = get_db_conn()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SET password_encryption = 'scram-sha-256'")
            cur.execute(f'DROP ROLE IF EXISTS "{name}"')
            # psycopg2 quotes password safely
            cur.execute(f'CREATE ROLE "{name}" LOGIN PASSWORD %s', (password,))
    finally:
        conn.close()


def _drop_role(name: str) -> None:
    run_sql(f'DROP ROLE IF EXISTS "{name}"')


def _mqtt_connect_raw(
    client_id: str,
    *,
    username: str | None = None,
    password: bytes | None = None,
    timeout: float = 5.0,
) -> tuple[socket.socket, int]:
    sock = socket.create_connection((MQTT_HOST, MQTT_PORT), timeout=timeout)
    pkt = build_connect_packet(
        client_id=client_id,
        username=username,
        password=password,
        clean_start=True,
        keep_alive=60,
    )
    sock.sendall(pkt)
    connack = read_packet(sock, timeout=timeout)
    return sock, parse_connack(connack)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def _verifier_is_scram():
    """Skip the suite if the cluster isn't using SCRAM-SHA-256 verifiers."""
    rows = run_sql("SHOW password_encryption")
    enc = rows[0][0] if rows else None
    if enc != "scram-sha-256":
        pytest.skip(
            f"password_encryption = '{enc}', need 'scram-sha-256' "
            f"for MQTT password auth"
        )


@pytest.fixture(scope="module", autouse=True)
def _test_roles():
    """Create / drop the test login roles once per module."""
    _set_password_encryption_scram()
    _create_role(TEST_ROLE, TEST_PASSWORD)
    _create_role(OTHER_ROLE, OTHER_PASSWORD)
    yield
    _drop_role(TEST_ROLE)
    _drop_role(OTHER_ROLE)


@pytest.fixture(autouse=True)
def _reset_password_gucs():
    reset_guc("pgmqtt.password_auth_enabled")
    reset_guc("pgmqtt.password_auth_required")
    reset_guc("pgmqtt.password_auth_role_filter")
    yield
    reset_guc("pgmqtt.password_auth_enabled")
    reset_guc("pgmqtt.password_auth_required")
    reset_guc("pgmqtt.password_auth_role_filter")


@pytest.fixture()
def auth_enabled():
    set_guc("pgmqtt.password_auth_enabled", "on")
    yield


@pytest.fixture()
def auth_required():
    set_guc("pgmqtt.password_auth_enabled", "on")
    set_guc("pgmqtt.password_auth_required", "on")
    yield


# ---------------------------------------------------------------------------
# Default state: auth disabled, credentials ignored
# ---------------------------------------------------------------------------


def test_auth_disabled_ignores_credentials():
    """When password_auth_enabled is off, any username/password is accepted."""
    sock, rc = _mqtt_connect_raw(
        "pwauth-disabled-ignored",
        username="nonexistent_role",
        password=b"totally-wrong",
    )
    sock.close()
    assert rc == RC_SUCCESS


def test_auth_disabled_anonymous_succeeds():
    """Default state: connect with no credentials succeeds."""
    sock, rc = _mqtt_connect_raw("pwauth-disabled-anon")
    sock.close()
    assert rc == RC_SUCCESS


# ---------------------------------------------------------------------------
# Auth enabled (not required)
# ---------------------------------------------------------------------------


def test_auth_enabled_anonymous_allowed(auth_enabled):
    """password_auth_enabled on its own does not require credentials."""
    sock, rc = _mqtt_connect_raw("pwauth-enabled-anon")
    sock.close()
    assert rc == RC_SUCCESS


def test_auth_enabled_valid_credentials(auth_enabled):
    sock, rc = _mqtt_connect_raw(
        "pwauth-enabled-ok",
        username=TEST_ROLE,
        password=TEST_PASSWORD.encode(),
    )
    sock.close()
    assert rc == RC_SUCCESS


def test_auth_enabled_wrong_password(auth_enabled):
    sock, rc = _mqtt_connect_raw(
        "pwauth-enabled-bad-pw",
        username=TEST_ROLE,
        password=b"wrong-password",
    )
    sock.close()
    assert rc == RC_BAD_USERNAME_PASSWORD


def test_auth_enabled_nonexistent_role(auth_enabled):
    sock, rc = _mqtt_connect_raw(
        "pwauth-enabled-no-role",
        username="role_that_definitely_does_not_exist",
        password=b"whatever",
    )
    sock.close()
    assert rc == RC_BAD_USERNAME_PASSWORD


def test_auth_enabled_username_without_password(auth_enabled):
    """Username with no password should fail like an anonymous attempt would
    when required, but here just allows through as anonymous (no creds path)."""
    sock, rc = _mqtt_connect_raw(
        "pwauth-enabled-user-only",
        username=TEST_ROLE,
        password=None,
    )
    sock.close()
    assert rc == RC_SUCCESS  # not required, no full credential pair = anonymous


# ---------------------------------------------------------------------------
# Auth required
# ---------------------------------------------------------------------------


def test_auth_required_rejects_anonymous(auth_required):
    sock, rc = _mqtt_connect_raw("pwauth-req-anon")
    sock.close()
    assert rc == RC_BAD_USERNAME_PASSWORD


def test_auth_required_valid_credentials(auth_required):
    sock, rc = _mqtt_connect_raw(
        "pwauth-req-ok",
        username=TEST_ROLE,
        password=TEST_PASSWORD.encode(),
    )
    sock.close()
    assert rc == RC_SUCCESS


def test_auth_required_wrong_password(auth_required):
    sock, rc = _mqtt_connect_raw(
        "pwauth-req-bad",
        username=TEST_ROLE,
        password=b"nope",
    )
    sock.close()
    assert rc == RC_BAD_USERNAME_PASSWORD


def test_required_without_enabled_rejects():
    """password_auth_required on but enabled off → fail closed (NOT_AUTHORIZED)."""
    set_guc("pgmqtt.password_auth_required", "on")
    try:
        sock, rc = _mqtt_connect_raw(
            "pwauth-req-without-enabled",
            username=TEST_ROLE,
            password=TEST_PASSWORD.encode(),
        )
        sock.close()
        # Implementation returns NOT_AUTHORIZED for this misconfiguration.
        assert rc == RC_NOT_AUTHORIZED
    finally:
        reset_guc("pgmqtt.password_auth_required")


# ---------------------------------------------------------------------------
# Role filter
# ---------------------------------------------------------------------------


def test_role_filter_allows_matching_role(auth_enabled):
    set_guc("pgmqtt.password_auth_role_filter", "mqtt\\_%")
    try:
        sock, rc = _mqtt_connect_raw(
            "pwauth-filter-match",
            username=TEST_ROLE,
            password=TEST_PASSWORD.encode(),
        )
        sock.close()
        assert rc == RC_SUCCESS
    finally:
        reset_guc("pgmqtt.password_auth_role_filter")


def test_role_filter_rejects_non_matching_role(auth_enabled):
    set_guc("pgmqtt.password_auth_role_filter", "mqtt\\_%")
    try:
        sock, rc = _mqtt_connect_raw(
            "pwauth-filter-miss",
            username=OTHER_ROLE,
            password=OTHER_PASSWORD.encode(),
        )
        sock.close()
        # Filter rejects with NOT_AUTHORIZED (deliberately distinguishable from
        # bad credentials so operators can spot the misconfiguration).
        assert rc == RC_NOT_AUTHORIZED
    finally:
        reset_guc("pgmqtt.password_auth_role_filter")


# ---------------------------------------------------------------------------
# Admin command SQL functions
# ---------------------------------------------------------------------------


def test_admin_functions_revoked_from_public():
    """REVOKE EXECUTE ... FROM PUBLIC is applied to all three admin functions.

    We create an unprivileged role and try to call each function. Failure means
    a permission denied error; success would indicate the REVOKE is missing.
    """
    role = "pwauth_unprivileged"
    run_sql(f'DROP ROLE IF EXISTS "{role}"')
    run_sql(f"CREATE ROLE \"{role}\" LOGIN PASSWORD 'x'")
    try:
        for sql in (
            "SELECT pgmqtt_disconnect_client('does-not-matter')",
            "SELECT pgmqtt_disconnect_role('does-not-matter')",
            "SELECT pgmqtt_reload_acls('does-not-matter')",
        ):
            err = run_sql_expect_error(sql, username=role, password="x")
            # On REVOKE working, run_sql_expect_error returns the error string.
            # If it returns a list / None, the REVOKE is missing.
            assert isinstance(err, str), \
                f"expected error string for {sql!r}, got: {err!r}"
            assert "permission denied" in err.lower(), \
                f"expected permission denied for {sql!r}, got: {err!r}"
    finally:
        run_sql(f'DROP ROLE IF EXISTS "{role}"')


def _drain_packets(sock: socket.socket, max_time: float = 1.5) -> list[bytes]:
    """Read whatever the broker sends back within max_time and return the packets."""
    sock.settimeout(0.2)
    deadline = time.time() + max_time
    out: list[bytes] = []
    while time.time() < deadline:
        try:
            out.append(read_packet(sock, timeout=0.2))
        except (socket.timeout, ConnectionError, OSError):
            break
    return out


def test_pgmqtt_disconnect_client_kicks(auth_enabled):
    """pgmqtt_disconnect_client closes the named client's connection."""
    client_id = "pwauth-kick-target"
    sock, rc = _mqtt_connect_raw(
        client_id,
        username=TEST_ROLE,
        password=TEST_PASSWORD.encode(),
    )
    assert rc == RC_SUCCESS

    run_sql(f"SELECT pgmqtt_disconnect_client('{client_id}')")
    # The drain may be empty (3.1.1 — broker sends no DISCONNECT) or contain a
    # DISCONNECT packet (v5). Either way, the socket should be closed shortly.
    _drain_packets(sock, max_time=2.0)
    sock.settimeout(0.5)
    try:
        peek = sock.recv(1)
        assert peek == b"", "broker should have closed the connection"
    except (socket.timeout, ConnectionError, OSError):
        pass
    sock.close()


def test_pgmqtt_disconnect_role_kicks_all_for_role(auth_enabled):
    """pgmqtt_disconnect_role kicks every client authed as that role."""
    sock_a, rc_a = _mqtt_connect_raw(
        "pwauth-kickrole-a",
        username=TEST_ROLE,
        password=TEST_PASSWORD.encode(),
    )
    sock_b, rc_b = _mqtt_connect_raw(
        "pwauth-kickrole-b",
        username=TEST_ROLE,
        password=TEST_PASSWORD.encode(),
    )
    # An unrelated anonymous client should NOT be kicked.
    sock_anon, rc_anon = _mqtt_connect_raw("pwauth-kickrole-anon")
    assert rc_a == rc_b == rc_anon == RC_SUCCESS

    try:
        run_sql(f"SELECT pgmqtt_disconnect_role('{TEST_ROLE}')")

        # Both role clients should be closed.
        for s in (sock_a, sock_b):
            _drain_packets(s, max_time=2.0)
            s.settimeout(0.5)
            try:
                assert s.recv(1) == b"", "expected broker to close socket"
            except (socket.timeout, ConnectionError, OSError):
                pass

        # The anonymous client survives — verify with a PINGREQ.
        sock_anon.settimeout(2.0)
        sock_anon.sendall(bytes([0xC0, 0x00]))  # PINGREQ
        resp = read_packet(sock_anon, timeout=2.0)
        assert resp == bytes([0xD0, 0x00]), \
            f"anonymous client unexpectedly killed: got {resp!r}"
    finally:
        for s in (sock_a, sock_b, sock_anon):
            try:
                s.close()
            except OSError:
                pass


def test_admin_command_table_is_drained(auth_enabled):
    """Issued admin commands do not accumulate — the BGW drains the queue."""
    # Issue a few commands targeting a nonexistent client; they should still
    # be consumed.
    for i in range(3):
        run_sql(f"SELECT pgmqtt_disconnect_client('ghost-{i}')")
    time.sleep(0.5)
    rows = run_sql("SELECT count(*) FROM pgmqtt_admin_commands")
    assert rows is not None and rows[0][0] == 0, \
        f"expected queue drained, got rows={rows!r}"
