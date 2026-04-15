"""End-to-end observability integration tests for pgmqtt enterprise.

Focuses on the full observability pipeline under real MQTT traffic:
  - msgs_received / msgs_sent counters after client-to-client publish
  - connections_current reflects live concurrent clients
  - Alert-hook pattern (mirrors the observability demo's pgmqtt_alert_hook),
    including both the non-alert (clean traffic) and alert (rejected connection) paths
  - NOTIFY snapshot JSON contains the complete metric field set
  - postgres_exporter counter values are positive after traffic
  - Snapshot rows are monotonically non-decreasing across flushes
"""

import json
import os
import select as _select
import socket
import sys
import time
import urllib.error
import urllib.request

import psycopg2
import pytest

# ── Path setup ────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from helpers.guc import set_guc, reset_guc  # noqa: E402
from helpers.license import generate_test_license  # noqa: E402
from helpers.jwt import public_key_b64url  # noqa: E402

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "integration"))
from test_utils import run_sql  # noqa: E402
from proto_utils import (  # noqa: E402
    create_connect_packet,
    create_subscribe_packet,
    create_publish_packet,
    recv_packet,
    validate_connack,
    validate_suback,
    MQTT_HOST,
    MQTT_PORT,
)

pytestmark = pytest.mark.enterprise

# ── Constants ─────────────────────────────────────────────────────────────────

EXPORTER_HOST = os.environ.get("EXPORTER_HOST", "127.0.0.1")
EXPORTER_PORT = int(os.environ.get("EXPORTER_PORT", "9187"))
EXPORTER_URL = f"http://{EXPORTER_HOST}:{EXPORTER_PORT}/metrics"

FLUSH_INTERVAL_SECS = 2
FLUSH_WAIT = FLUSH_INTERVAL_SECS + 1.5


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def metrics_license():
    """Activate a metrics-enabled enterprise license with fast flush intervals."""
    token = generate_test_license(
        customer="obs_test", days=1, features=["metrics"]
    )
    set_guc("pgmqtt.license_key", token)
    set_guc("pgmqtt.metrics_snapshot_interval", str(FLUSH_INTERVAL_SECS))
    set_guc("pgmqtt.metrics_connections_cache_interval", str(FLUSH_INTERVAL_SECS))
    yield
    for guc in (
        "pgmqtt.license_key",
        "pgmqtt.metrics_snapshot_interval",
        "pgmqtt.metrics_connections_cache_interval",
        "pgmqtt.metrics_hook_function",
        "pgmqtt.metrics_notify_channel",
    ):
        reset_guc(guc)


# ── Internal helpers ──────────────────────────────────────────────────────────


def _wait_for_flush():
    time.sleep(FLUSH_WAIT)


def _get_metric(name: str):
    rows = run_sql(f"SELECT value FROM pgmqtt_metrics() WHERE metric_name = '{name}'")
    return rows[0][0] if rows else None


def _mqtt_connect(client_id: str) -> socket.socket:
    """Connect to the broker and return the socket. Caller must close it."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet(client_id, clean_start=True))
    pkt = recv_packet(s, timeout=5)
    assert pkt is not None, f"No CONNACK from broker for client {client_id!r}"
    _, rc, _ = validate_connack(pkt)
    assert rc == 0, f"CONNACK reason code {rc:#04x} for {client_id!r}"
    return s


def _mqtt_subscribe(sock: socket.socket, topic: str, packet_id: int = 1) -> None:
    sock.sendall(create_subscribe_packet(packet_id, topic, qos=0))
    pkt = recv_packet(sock, timeout=5)
    assert pkt is not None, f"No SUBACK for topic {topic!r}"
    codes = validate_suback(pkt, packet_id)
    assert codes[0] == 0, f"SUBACK reason code {codes[0]:#04x} for {topic!r}"


def _pg_conn():
    conn = psycopg2.connect(
        host=os.environ.get("PG_HOST", "127.0.0.1"),
        port=int(os.environ.get("PG_PORT", "5432")),
        user=os.environ.get("PG_USER", "postgres"),
        password=os.environ.get("PG_PASSWORD", "postgres"),
        dbname=os.environ.get("PG_DATABASE", "postgres"),
    )
    conn.autocommit = True
    return conn


# ── Traffic-driven counter tests ──────────────────────────────────────────────


def test_msgs_received_increments_after_publish():
    """Publishing QoS 0 messages increments the global msgs_received counter."""
    _wait_for_flush()
    before = _get_metric("msgs_received") or 0

    pub = _mqtt_connect("obs_pub_recv")
    try:
        for i in range(5):
            pub.sendall(create_publish_packet(f"obs/recv/{i}", b"data", qos=0))
        time.sleep(0.5)
    finally:
        pub.close()

    _wait_for_flush()
    after = _get_metric("msgs_received") or 0
    assert after >= before + 5, (
        f"msgs_received should have risen by ≥5; before={before}, after={after}"
    )


def test_msgs_sent_increments_after_delivery():
    """Delivering messages to a subscriber increments the global msgs_sent counter."""
    _wait_for_flush()
    before = _get_metric("msgs_sent") or 0

    sub = _mqtt_connect("obs_sub_sent")
    try:
        _mqtt_subscribe(sub, "obs/sent/+")

        pub = _mqtt_connect("obs_pub_sent")
        try:
            for i in range(5):
                pub.sendall(create_publish_packet(f"obs/sent/{i}", b"payload", qos=0))
            time.sleep(0.5)
        finally:
            pub.close()

        _wait_for_flush()
        after = _get_metric("msgs_sent") or 0
        assert after >= before + 5, (
            f"msgs_sent should have risen by ≥5; before={before}, after={after}"
        )
    finally:
        sub.close()


def test_connections_current_reflects_concurrent_clients():
    """connections_current in the connections cache tracks live client count."""
    _wait_for_flush()
    before_rows = run_sql("SELECT COUNT(*) FROM pgmqtt_connections()")
    before = before_rows[0][0] if before_rows else 0

    clients = [_mqtt_connect(f"obs_concurrent_{i}") for i in range(3)]
    try:
        _wait_for_flush()
        during_rows = run_sql("SELECT COUNT(*) FROM pgmqtt_connections()")
        during = during_rows[0][0] if during_rows else 0
        assert during >= before + 3, (
            f"Expected ≥{before + 3} connections during; got {during}"
        )
    finally:
        for s in clients:
            s.close()

    _wait_for_flush()
    after_rows = run_sql("SELECT COUNT(*) FROM pgmqtt_connections()")
    after = after_rows[0][0] if after_rows else 0
    assert after <= before + 1, (
        f"Connections should drop back after close; before={before}, after={after}"
    )


def test_per_client_msgs_received_in_connection_cache():
    """The per-client msgs_received column in the connection cache reflects publishes."""
    sock = _mqtt_connect("obs_perclient_pub")
    try:
        _wait_for_flush()
        rows_before = run_sql(
            "SELECT msgs_received FROM pgmqtt_connections() "
            "WHERE client_id = 'obs_perclient_pub'"
        )
        before = rows_before[0][0] if rows_before else 0

        for i in range(4):
            sock.sendall(create_publish_packet(f"obs/perclient/{i}", b"x", qos=0))
        time.sleep(0.3)

        _wait_for_flush()
        rows_after = run_sql(
            "SELECT msgs_received FROM pgmqtt_connections() "
            "WHERE client_id = 'obs_perclient_pub'"
        )
        after = rows_after[0][0] if rows_after else 0
        assert after >= before + 4, (
            f"Per-client msgs_received should rise by ≥4; before={before}, after={after}"
        )
    finally:
        sock.close()


# ── Alert-hook pattern (mirrors the observability demo) ───────────────────────


def test_alert_hook_pattern_captures_snapshots():
    """The demo's alert-hook pattern: every flush is logged with an is_alert flag.

    Mirrors demos/observability/db/init.sql: creates a hook-log table and a
    plpgsql hook function that marks rows as alerts when any error counter is
    non-zero.  Verifies the hook is called on each flush and that normal traffic
    (no errors) produces is_alert = false rows.

    Error counters are cumulative in-memory values and are not reset between
    test runs unless the broker restarts.  To be run-order agnostic, we snapshot
    the baseline values before the test window and alert only on NEW increments
    that exceed the baseline.
    """
    # Snapshot baseline before the test so stale counters from earlier test
    # activity (e.g. connections_rejected from the JWT test) do not cause
    # false-positive alerts.
    baseline_rows = run_sql(
        "SELECT metric_name, value::bigint "
        "FROM pgmqtt_metrics() "
        "WHERE metric_name IN ("
        "  'connections_rejected',"
        "  'db_session_errors','db_message_errors','db_subscription_errors',"
        "  'cdc_render_errors','cdc_slot_errors','cdc_persist_errors')"
    )
    baseline = {r[0]: int(r[1]) for r in baseline_rows}
    base_rejected = baseline.get("connections_rejected", 0)
    base_db_sess = baseline.get("db_session_errors", 0)
    base_db_msg = baseline.get("db_message_errors", 0)
    base_db_sub = baseline.get("db_subscription_errors", 0)
    base_cdc_render = baseline.get("cdc_render_errors", 0)
    base_cdc_slot = baseline.get("cdc_slot_errors", 0)
    base_cdc_persist = baseline.get("cdc_persist_errors", 0)

    run_sql(
        "CREATE TABLE IF NOT EXISTS _obs_hook_log ("
        "  id BIGSERIAL PRIMARY KEY, "
        "  logged_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
        "  is_alert BOOLEAN NOT NULL DEFAULT false, "
        "  payload JSONB NOT NULL"
        ")"
    )
    run_sql("TRUNCATE _obs_hook_log")
    # Alert only when a counter EXCEEDS its baseline — i.e. a new error
    # occurred during this test window, not a leftover from a prior run.
    run_sql(
        "CREATE OR REPLACE FUNCTION _obs_alert_hook(snap JSONB) RETURNS void "
        "LANGUAGE plpgsql AS $$ "
        "BEGIN "
        "  INSERT INTO _obs_hook_log (is_alert, payload) VALUES ( "
        f"    COALESCE((snap->>'connections_rejected')::bigint, 0) > {base_rejected} OR "
        f"    COALESCE((snap->>'db_session_errors')::bigint, 0) > {base_db_sess} OR "
        f"    COALESCE((snap->>'db_message_errors')::bigint, 0) > {base_db_msg} OR "
        f"    COALESCE((snap->>'db_subscription_errors')::bigint, 0) > {base_db_sub} OR "
        f"    COALESCE((snap->>'cdc_render_errors')::bigint, 0) > {base_cdc_render} OR "
        f"    COALESCE((snap->>'cdc_slot_errors')::bigint, 0) > {base_cdc_slot} OR "
        f"    COALESCE((snap->>'cdc_persist_errors')::bigint, 0) > {base_cdc_persist}, "
        "    snap "
        "  ); "
        "END; $$"
    )
    try:
        set_guc("pgmqtt.metrics_hook_function", "_obs_alert_hook")

        # Two full flush intervals → at least two log rows
        _wait_for_flush()
        time.sleep(FLUSH_INTERVAL_SECS + 0.5)

        rows = run_sql("SELECT COUNT(*) FROM _obs_hook_log")
        assert rows[0][0] >= 2, (
            f"Expected ≥2 hook invocations, got {rows[0][0]}"
        )

        # Normal traffic should not trigger alerts
        alert_rows = run_sql(
            "SELECT COUNT(*) FROM _obs_hook_log WHERE is_alert = true"
        )
        assert alert_rows[0][0] == 0, (
            f"Unexpected alerts logged for normal traffic: {alert_rows[0][0]} rows"
        )

        # Verify payload carries expected fields
        latest = run_sql(
            "SELECT payload FROM _obs_hook_log ORDER BY logged_at DESC LIMIT 1"
        )
        assert latest, "Hook log is empty"
        payload = latest[0][0]
        if isinstance(payload, str):
            payload = json.loads(payload)
        for key in ("connections_accepted", "msgs_received", "captured_at_unix"):
            assert key in payload, f"Hook payload missing field: {key!r}"
    finally:
        reset_guc("pgmqtt.metrics_hook_function")
        run_sql("DROP FUNCTION IF EXISTS _obs_alert_hook(jsonb)")
        run_sql("DROP TABLE IF EXISTS _obs_hook_log")


def test_alert_hook_fires_on_rejected_connection():
    """Alert hook sets is_alert=true when connections_rejected is non-zero.

    Enables JWT-required auth then connects without a token.  The broker
    rejects the connection in finish_connect() and increments
    connections_rejected — the only reliable path that does so without a
    server-side exception that would crash the background worker.
    """
    run_sql(
        "CREATE TABLE IF NOT EXISTS _obs_hook_log ("
        "  id BIGSERIAL PRIMARY KEY, "
        "  logged_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
        "  is_alert BOOLEAN NOT NULL DEFAULT false, "
        "  payload JSONB NOT NULL"
        ")"
    )
    run_sql("TRUNCATE _obs_hook_log")
    run_sql(
        "CREATE OR REPLACE FUNCTION _obs_alert_hook(snap JSONB) RETURNS void "
        "LANGUAGE plpgsql AS $$ "
        "BEGIN "
        "  INSERT INTO _obs_hook_log (is_alert, payload) VALUES ( "
        "    COALESCE((snap->>'connections_rejected')::bigint, 0) > 0 OR "
        "    COALESCE((snap->>'db_session_errors')::bigint, 0) > 0 OR "
        "    COALESCE((snap->>'db_message_errors')::bigint, 0) > 0 OR "
        "    COALESCE((snap->>'db_subscription_errors')::bigint, 0) > 0 OR "
        "    COALESCE((snap->>'cdc_render_errors')::bigint, 0) > 0 OR "
        "    COALESCE((snap->>'cdc_slot_errors')::bigint, 0) > 0 OR "
        "    COALESCE((snap->>'cdc_persist_errors')::bigint, 0) > 0, "
        "    snap "
        "  ); "
        "END; $$"
    )
    try:
        set_guc("pgmqtt.metrics_hook_function", "_obs_alert_hook")
        _wait_for_flush()  # establish a clean baseline (is_alert=false)

        run_sql("TRUNCATE _obs_hook_log")

        # Enable JWT-required auth so an anonymous connect is rejected.
        set_guc("pgmqtt.jwt_public_key", public_key_b64url())
        set_guc("pgmqtt.jwt_required", "on")
        try:
            # Connect without a JWT — broker sends CONNACK 0x87 (NOT_AUTHORIZED)
            # and increments connections_rejected.
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((MQTT_HOST, MQTT_PORT))
            sock.sendall(create_connect_packet("obs_jwt_rejected", clean_start=True))
            sock.settimeout(3.0)
            sock.recv(256)
            sock.close()
        except OSError:
            pass
        time.sleep(0.3)

        _wait_for_flush()

        rows = run_sql("SELECT COUNT(*) FROM _obs_hook_log WHERE is_alert = true")
        assert rows[0][0] >= 1, (
            "Expected at least one is_alert=true row after a JWT-rejected connection.\n"
            "connections_rejected did not increment or alert hook was not called."
        )
    finally:
        reset_guc("pgmqtt.jwt_required")
        reset_guc("pgmqtt.jwt_public_key")
        reset_guc("pgmqtt.metrics_hook_function")
        run_sql("DROP FUNCTION IF EXISTS _obs_alert_hook(jsonb)")
        run_sql("DROP TABLE IF EXISTS _obs_hook_log")


# ── NOTIFY snapshot completeness ──────────────────────────────────────────────


# Minimum set of fields expected in every NOTIFY snapshot.
_EXPECTED_SNAPSHOT_KEYS = frozenset({
    "captured_at_unix",
    "connections_accepted",
    "connections_rejected",
    "connections_current",
    "msgs_received",
    "msgs_sent",
    "msgs_dropped_queue_full",
    "db_batches_committed",
    "db_session_errors",
    "db_message_errors",
    "db_subscription_errors",
    "cdc_events_processed",
    "cdc_render_errors",
    "cdc_slot_errors",
    "cdc_persist_errors",
})


def test_notify_snapshot_contains_complete_field_set():
    """NOTIFY payload includes all core metric fields, not just a subset."""
    channel = "obs_notify_complete"
    listen_conn = _pg_conn()
    with listen_conn.cursor() as cur:
        cur.execute(f"LISTEN {channel}")

    try:
        set_guc("pgmqtt.metrics_notify_channel", channel)
        _wait_for_flush()

        _select.select([listen_conn], [], [], 1.0)
        listen_conn.poll()

        notifs = listen_conn.notifies
        assert notifs, f"Expected NOTIFY on '{channel}' after flush interval"

        payload = json.loads(notifs[0].payload)
        missing = _EXPECTED_SNAPSHOT_KEYS - payload.keys()
        assert not missing, (
            f"NOTIFY snapshot missing expected fields: {sorted(missing)}\n"
            f"Keys present: {sorted(payload.keys())}"
        )
    finally:
        reset_guc("pgmqtt.metrics_notify_channel")
        with listen_conn.cursor() as cur:
            cur.execute(f"UNLISTEN {channel}")
        listen_conn.close()


# ── postgres_exporter counter values after traffic ────────────────────────────


def test_exporter_connections_accepted_positive_after_traffic():
    """postgres_exporter reports a positive connections_accepted value after connects.

    Extends the format-only check in test_metrics.py to assert that the counter
    actually carries a meaningful positive value after real broker activity.
    """
    # Generate some connections
    for i in range(3):
        s = _mqtt_connect(f"obs_exp_traffic_{i}")
        s.close()

    _wait_for_flush()

    try:
        with urllib.request.urlopen(EXPORTER_URL, timeout=10) as resp:
            body = resp.read().decode()
    except urllib.error.URLError as exc:
        pytest.fail(
            f"postgres_exporter not reachable at {EXPORTER_URL}: {exc}\n"
            "Ensure the postgres-exporter service is running (docker compose up)."
        )

    # Find the connections_accepted line from pg_pgmqtt output
    target_line = None
    for line in body.splitlines():
        if line.startswith("pg_pgmqtt") and 'metric_name="connections_accepted"' in line:
            target_line = line
            break

    assert target_line is not None, (
        f"pg_pgmqtt line for connections_accepted not found in exporter output.\n"
        f"First 2000 chars:\n{body[:2000]}"
    )

    # Extract and validate the numeric value
    value_str = target_line.rsplit(None, 1)[-1]
    value = float(value_str)
    assert value > 0, (
        f"connections_accepted reported as {value} — expected positive after connects"
    )


# ── Snapshot monotonicity ──────────────────────────────────────────────────────


def test_snapshot_counters_are_non_decreasing():
    """Cumulative counters must never decrease between consecutive snapshot rows.

    Takes two consecutive snapshots from pgmqtt_metrics_snapshots and asserts
    that connections_accepted and msgs_received are ≥ in the later row.
    """
    run_sql("DELETE FROM pgmqtt_metrics_snapshots")
    _wait_for_flush()                          # first snapshot

    # Connect a client and publish a message to drive counter activity
    pub = _mqtt_connect("obs_monotone_pub")
    try:
        pub.sendall(create_publish_packet("obs/monotone", b"tick", qos=0))
        time.sleep(0.2)
    finally:
        pub.close()

    time.sleep(FLUSH_INTERVAL_SECS + 0.5)     # second snapshot

    rows = run_sql(
        "SELECT connections_accepted, msgs_received "
        "FROM pgmqtt_metrics_snapshots "
        "ORDER BY snapshot_at ASC"
    )
    assert len(rows) >= 2, (
        f"Expected ≥2 snapshot rows for monotonicity check, got {len(rows)}"
    )

    first_conn, first_msgs = rows[0]
    last_conn, last_msgs = rows[-1]

    assert last_conn >= first_conn, (
        f"connections_accepted decreased: {first_conn} → {last_conn}"
    )
    assert last_msgs >= first_msgs, (
        f"msgs_received decreased: {first_msgs} → {last_msgs}"
    )


# ── CDC pipeline observability ────────────────────────────────────────────────


def test_cdc_pipeline_increments_metrics():
    """A successful CDC event increments cdc_events_processed and cdc_msgs_published.

    Sets up a QoS 0 outbound mapping (fire-and-forget, no pgmqtt_messages
    write), connects a subscriber, INSERTs a row, and asserts both CDC
    counters increase.  Also verifies the CDC error counters and cdc_lag_ms_last
    are present as observable metric names.
    """
    TABLE = "_obs_cdc_happy"
    TOPIC = "obs/cdc/happy"

    run_sql(f"DROP TABLE IF EXISTS {TABLE}")
    run_sql(f"CREATE TABLE {TABLE} (id serial PRIMARY KEY, v text)")
    run_sql(f"ALTER TABLE {TABLE} REPLICA IDENTITY FULL")
    run_sql(
        f"SELECT pgmqtt_add_outbound_mapping('public', '{TABLE}', "
        f"'{TOPIC}', '{{{{ columns | tojson }}}}', 0)"
    )
    # The broker picks up the new mapping from a WAL event on pgmqtt_topic_mappings.
    # Allow enough CDC ticks for the mapping to be loaded into the in-process cache.
    time.sleep(6)

    sub = _mqtt_connect("obs_cdc_happy_sub")
    try:
        _mqtt_subscribe(sub, TOPIC)

        _wait_for_flush()
        before_processed = _get_metric("cdc_events_processed") or 0
        before_published = _get_metric("cdc_msgs_published") or 0

        # Fire a CDC event.
        run_sql(f"INSERT INTO {TABLE} (v) VALUES ('hello')")
        time.sleep(FLUSH_WAIT)   # wait for the CDC tick + metrics flush

        after_processed = _get_metric("cdc_events_processed") or 0
        after_published = _get_metric("cdc_msgs_published") or 0

        assert after_processed > before_processed, (
            f"cdc_events_processed did not increment; "
            f"before={before_processed}, after={after_processed}"
        )
        assert after_published > before_published, (
            f"cdc_msgs_published did not increment; "
            f"before={before_published}, after={after_published}"
        )

        # Verify split CDC error counters and lag are accessible metric names
        # (value may be 0 in a healthy run, but the key must be present).
        names = {r[0] for r in run_sql("SELECT metric_name FROM pgmqtt_metrics()")}
        assert "cdc_render_errors" in names, "cdc_render_errors should be a named metric"
        assert "cdc_slot_errors" in names, "cdc_slot_errors should be a named metric"
        assert "cdc_persist_errors" in names, "cdc_persist_errors should be a named metric"
        assert "cdc_lag_ms_last" in names, "cdc_lag_ms_last should be a named metric"
    finally:
        run_sql(f"SELECT pgmqtt_remove_outbound_mapping('public', '{TABLE}', 'default')")
        time.sleep(2)  # allow the broker to process the mapping removal from WAL
        run_sql(f"DROP TABLE IF EXISTS {TABLE}")
        sub.close()


def test_cdc_error_increments_counter():
    """A CDC persist failure increments cdc_persist_errors without crashing the broker.

    Attaches a BEFORE INSERT trigger on pgmqtt_messages that raises an exception,
    then fires a QoS 1 CDC event.  The savepoint wrapper in the broker catches the
    PostgreSQL exception, rolls back the persist subtransaction, and increments
    cdc_persist_errors.  The background worker must survive — verified by a subsequent
    successful connection.
    """
    TABLE = "_obs_cdc_err"
    TOPIC = "obs/cdc/error"

    run_sql(f"DROP TABLE IF EXISTS {TABLE}")
    run_sql(f"CREATE TABLE {TABLE} (id serial PRIMARY KEY, v text)")
    run_sql(f"ALTER TABLE {TABLE} REPLICA IDENTITY FULL")
    run_sql(
        f"SELECT pgmqtt_add_outbound_mapping('public', '{TABLE}', "
        f"'{TOPIC}', '{{{{ columns | tojson }}}}', 1)"  # QoS 1 → triggers persist_message
    )
    # Trigger on pgmqtt_messages rejects every insert so persist_message always fails.
    run_sql(
        "CREATE OR REPLACE FUNCTION _obs_reject_persist() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'obs: persist rejected'; END; $$"
    )
    run_sql(
        "CREATE TRIGGER _obs_reject_persist_trg "
        "BEFORE INSERT ON pgmqtt_messages "
        "FOR EACH ROW EXECUTE FUNCTION _obs_reject_persist()"
    )
    # Allow the broker to pick up the new mapping from WAL before we insert.
    time.sleep(6)

    sub = _mqtt_connect("obs_cdc_err_sub")
    try:
        _mqtt_subscribe(sub, TOPIC)

        _wait_for_flush()
        before = _get_metric("cdc_persist_errors") or 0

        run_sql(f"INSERT INTO {TABLE} (v) VALUES ('trigger_me')")
        time.sleep(FLUSH_WAIT)

        after = _get_metric("cdc_persist_errors") or 0
        assert after > before, (
            f"cdc_persist_errors should have incremented after a persist failure; "
            f"before={before}, after={after}"
        )

        # Confirm the background worker is still alive.
        s = _mqtt_connect("obs_cdc_err_alive")
        s.close()
    finally:
        sub.close()
        run_sql("DROP TRIGGER IF EXISTS _obs_reject_persist_trg ON pgmqtt_messages")
        run_sql("DROP FUNCTION IF EXISTS _obs_reject_persist()")
        run_sql(f"SELECT pgmqtt_remove_outbound_mapping('public', '{TABLE}', 'default')")
        time.sleep(2)
        run_sql(f"DROP TABLE IF EXISTS {TABLE}")
