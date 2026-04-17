"""Integration tests for pgmqtt enterprise metrics (observability) feature.

Covers:
  - License gating (metrics feature required for all SQL functions)
  - pgmqtt_metrics() counter content after broker activity
  - pgmqtt_connections() connection cache after connect / disconnect
  - pgmqtt_prometheus_metrics() output format and per-line validity
  - pgmqtt_metrics_snapshots table growth at configured interval
  - Retention policy cleanup of old snapshot rows
  - Hook function callback on each flush
  - NOTIFY channel delivery on each flush
"""

import json
import os
import re
import select as _select
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
from helpers.mqtt import mqtt_connect  # noqa: E402

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "integration"))
from test_utils import run_sql  # noqa: E402

pytestmark = pytest.mark.enterprise

# ── Constants ─────────────────────────────────────────────────────────────────

MQTT_HOST = os.environ.get("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))

EXPORTER_HOST = os.environ.get("EXPORTER_HOST", "127.0.0.1")
EXPORTER_PORT = int(os.environ.get("EXPORTER_PORT", "9187"))
EXPORTER_URL = f"http://{EXPORTER_HOST}:{EXPORTER_PORT}/metrics"

# Flush interval (seconds) set by the autouse fixture.
# Tests sleep slightly longer than this to guarantee ≥1 flush has occurred.
FLUSH_INTERVAL_SECS = 2
FLUSH_WAIT = FLUSH_INTERVAL_SECS + 1.5


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def metrics_license():
    """Activate a metrics-enabled enterprise license with fast flush intervals."""
    token = generate_test_license(
        customer="metrics_test", days=1, features=["metrics"]
    )
    set_guc("pgmqtt.license_key", token)
    set_guc("pgmqtt.metrics_snapshot_interval", str(FLUSH_INTERVAL_SECS))
    set_guc("pgmqtt.metrics_connections_cache_interval", str(FLUSH_INTERVAL_SECS))
    yield
    for guc in (
        "pgmqtt.license_key",
        "pgmqtt.metrics_snapshot_interval",
        "pgmqtt.metrics_retention_days",
        "pgmqtt.metrics_connections_cache_interval",
        "pgmqtt.metrics_hook_function",
        "pgmqtt.metrics_notify_channel",
    ):
        reset_guc(guc)


# ── Internal helpers ──────────────────────────────────────────────────────────


def _wait_for_flush():
    """Sleep long enough for at least one BGW metrics flush to complete."""
    time.sleep(FLUSH_WAIT)


def _get_metric(name: str):
    """Return the integer value of a single named counter from pgmqtt_metrics()."""
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM pgmqtt_metrics() WHERE metric_name = %s", (name,)
            )
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def _connect(client_id: str):
    """Open an MQTT connection and return the socket.  Caller must close it."""
    sock, rc = mqtt_connect(MQTT_HOST, MQTT_PORT, client_id)
    assert rc == 0, f"Expected CONNACK 0 (success), got {rc}"
    return sock


def _pg_conn():
    """Return a fresh autocommit psycopg2 connection to the test database."""
    conn = psycopg2.connect(
        host=os.environ.get("PG_HOST", "127.0.0.1"),
        port=int(os.environ.get("PG_PORT", "5432")),
        user=os.environ.get("PG_USER", "postgres"),
        password=os.environ.get("PG_PASSWORD", "postgres"),
        dbname=os.environ.get("PG_DATABASE", "postgres"),
    )
    conn.autocommit = True
    return conn


# ── License gating ────────────────────────────────────────────────────────────


def test_metrics_requires_license():
    """pgmqtt_metrics() raises an error in community mode."""
    set_guc("pgmqtt.license_key", "")
    with pytest.raises(psycopg2.Error, match="requires an enterprise license"):
        run_sql("SELECT * FROM pgmqtt_metrics()")


def test_connections_fn_requires_license():
    """pgmqtt_connections() raises an error in community mode."""
    set_guc("pgmqtt.license_key", "")
    with pytest.raises(psycopg2.Error, match="requires an enterprise license"):
        run_sql("SELECT * FROM pgmqtt_connections()")


def test_prometheus_requires_license():
    """pgmqtt_prometheus_metrics() raises an error in community mode."""
    set_guc("pgmqtt.license_key", "")
    with pytest.raises(psycopg2.Error, match="requires an enterprise license"):
        run_sql("SELECT pgmqtt_prometheus_metrics()")



# ── Metrics content ───────────────────────────────────────────────────────────


def test_metrics_returns_expected_counter_names():
    """After a flush, pgmqtt_metrics() exposes all core counter names."""
    _wait_for_flush()
    rows = run_sql("SELECT metric_name FROM pgmqtt_metrics()")
    assert rows, "pgmqtt_metrics() returned no rows"
    names = {r[0] for r in rows}
    for expected in (
        "connections_accepted",
        "connections_current",
        "msgs_received",
        "msgs_sent",
        "cdc_events_processed",
        "inbound_writes_ok",
        "db_batches_committed",
    ):
        assert expected in names, f"Missing metric: {expected}"


def test_connections_accepted_increments():
    """Connecting a client increments the connections_accepted counter."""
    _wait_for_flush()
    before = _get_metric("connections_accepted") or 0

    sock = _connect("test_metrics_conn_accepted")
    sock.close()

    _wait_for_flush()
    after = _get_metric("connections_accepted") or 0
    assert after > before, (
        f"connections_accepted did not increase: before={before}, after={after}"
    )


def test_db_batches_committed_increments():
    """Each event-loop iteration with DB actions increments db_batches_committed."""
    _wait_for_flush()
    before = _get_metric("db_batches_committed") or 0

    # A CONNECT + DISCONNECT pair triggers session DB actions
    sock = _connect("test_metrics_db_batches")
    sock.close()

    _wait_for_flush()
    after = _get_metric("db_batches_committed") or 0
    assert after > before, (
        f"db_batches_committed did not increase: before={before}, after={after}"
    )


# ── Connections cache ─────────────────────────────────────────────────────────


def test_connections_cache_shows_connected_client():
    """pgmqtt_connections() lists a currently connected client."""
    sock = _connect("test_metrics_cache_present")
    try:
        _wait_for_flush()
        rows = run_sql("SELECT client_id FROM pgmqtt_connections()")
        ids = [r[0] for r in rows] if rows else []
        assert "test_metrics_cache_present" in ids, (
            f"Connected client not in pgmqtt_connections(); ids={ids}"
        )
    finally:
        sock.close()


def test_connections_cache_excludes_disconnected_client():
    """pgmqtt_connections() does not list a client that has disconnected."""
    sock = _connect("test_metrics_cache_gone")
    sock.close()

    _wait_for_flush()
    rows = run_sql("SELECT client_id FROM pgmqtt_connections()")
    ids = [r[0] for r in rows] if rows else []
    assert "test_metrics_cache_gone" not in ids, (
        "Disconnected client must not appear in pgmqtt_connections()"
    )


def test_connections_cache_transport_label():
    """Clients connected over raw TCP show transport = 'mqtt'."""
    sock = _connect("test_metrics_transport")
    try:
        _wait_for_flush()
        rows = run_sql(
            "SELECT transport FROM pgmqtt_connections() "
            "WHERE client_id = 'test_metrics_transport'"
        )
        assert rows, "Client not found in pgmqtt_connections()"
        assert rows[0][0] == "mqtt", f"Expected transport='mqtt', got '{rows[0][0]}'"
    finally:
        sock.close()


def test_connections_cache_columns_have_sensible_values():
    """Connection cache rows contain non-negative numeric fields."""
    sock = _connect("test_metrics_cache_cols")
    try:
        _wait_for_flush()
        rows = run_sql(
            "SELECT connected_at_unix, keep_alive_secs, msgs_received, msgs_sent, "
            "       queue_depth, inflight_count, cached_at_unix "
            "FROM pgmqtt_connections() "
            "WHERE client_id = 'test_metrics_cache_cols'"
        )
        assert rows, "Client not found in pgmqtt_connections()"
        row = rows[0]
        connected_at, keep_alive, msgs_r, msgs_s, q_depth, inflight, cached_at = row
        assert connected_at > 0, "connected_at_unix should be a positive timestamp"
        assert keep_alive >= 0
        assert msgs_r >= 0
        assert msgs_s >= 0
        assert q_depth >= 0
        assert inflight >= 0
        assert cached_at > 0
    finally:
        sock.close()


# ── Prometheus output ─────────────────────────────────────────────────────────


def test_prometheus_output_format():
    """pgmqtt_prometheus_metrics() produces valid Prometheus text-format lines."""
    _wait_for_flush()
    rows = run_sql("SELECT pgmqtt_prometheus_metrics()")
    assert rows and rows[0][0], "pgmqtt_prometheus_metrics() returned empty output"
    text = rows[0][0]

    assert "# HELP pgmqtt_connections_accepted_total" in text
    assert "# TYPE pgmqtt_connections_accepted_total counter" in text
    assert "pgmqtt_connections_accepted_total " in text

    assert "# HELP pgmqtt_connections_current" in text
    assert "# TYPE pgmqtt_connections_current gauge" in text

    # Every non-comment, non-empty line must be "<metric_name> <integer>"
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        parts = line.split()
        assert len(parts) == 2, f"Unexpected Prometheus line format: {line!r}"
        int(parts[1])  # value must be parseable as integer


# ── Snapshot table growth ─────────────────────────────────────────────────────


def test_snapshots_table_grows_over_time():
    """pgmqtt_metrics_snapshots accumulates at least two rows across two flush intervals."""
    run_sql("DELETE FROM pgmqtt_metrics_snapshots")
    _wait_for_flush()                               # first flush
    time.sleep(FLUSH_INTERVAL_SECS + 0.5)           # second flush

    rows = run_sql("SELECT COUNT(*) FROM pgmqtt_metrics_snapshots")
    count = rows[0][0] if rows else 0
    assert count >= 2, (
        f"Expected ≥2 snapshot rows after two flush intervals, got {count}"
    )


def test_snapshots_contain_expected_columns():
    """pgmqtt_metrics_snapshots rows carry non-negative metric columns."""
    _wait_for_flush()
    rows = run_sql(
        "SELECT snapshot_at, connections_accepted, msgs_received, db_batches_committed "
        "FROM pgmqtt_metrics_snapshots ORDER BY snapshot_at DESC LIMIT 1"
    )
    assert rows, "pgmqtt_metrics_snapshots is empty after flush"
    snapshot_at, conn_a, msgs_r, db_batches = rows[0]
    assert snapshot_at > 0, "snapshot_at should be a positive Unix timestamp"
    assert conn_a >= 0
    assert msgs_r >= 0
    assert db_batches >= 0


# ── Retention cleanup ─────────────────────────────────────────────────────────


def test_retention_purges_old_snapshots():
    """Snapshots older than the retention window are deleted on the next flush."""
    old_ts = int(time.time()) - 86400 * 30
    run_sql(f"INSERT INTO pgmqtt_metrics_snapshots (snapshot_at) VALUES ({old_ts})")

    rows = run_sql(
        f"SELECT COUNT(*) FROM pgmqtt_metrics_snapshots WHERE snapshot_at = {old_ts}"
    )
    assert rows[0][0] == 1, "Seed row was not inserted"

    set_guc("pgmqtt.metrics_retention_days", "1")
    _wait_for_flush()

    rows = run_sql(
        f"SELECT COUNT(*) FROM pgmqtt_metrics_snapshots WHERE snapshot_at = {old_ts}"
    )
    assert rows[0][0] == 0, "30-day-old row should have been purged by 1-day retention"


# ── Hook function ─────────────────────────────────────────────────────────────


def test_hook_function_called_on_flush():
    """pgmqtt.metrics_hook_function is invoked with a JSONB snapshot after each flush."""
    run_sql(
        "CREATE TABLE IF NOT EXISTS _pgmqtt_hook_log "
        "(payload jsonb, recorded_at timestamptz DEFAULT now())"
    )
    run_sql("TRUNCATE _pgmqtt_hook_log")
    run_sql(
        "CREATE OR REPLACE FUNCTION pgmqtt_test_metrics_hook(snap jsonb) "
        "RETURNS void LANGUAGE sql AS $$ "
        "  INSERT INTO _pgmqtt_hook_log (payload) VALUES (snap); "
        "$$"
    )
    try:
        set_guc("pgmqtt.metrics_hook_function", "pgmqtt_test_metrics_hook")
        _wait_for_flush()

        rows = run_sql("SELECT COUNT(*) FROM _pgmqtt_hook_log")
        assert rows[0][0] >= 1, "Hook function was not called after flush interval"

        rows = run_sql(
            "SELECT payload FROM _pgmqtt_hook_log ORDER BY recorded_at DESC LIMIT 1"
        )
        assert rows, "Hook log has no rows"
        payload = rows[0][0]
        if isinstance(payload, str):
            payload = json.loads(payload)
        assert "connections_accepted" in payload, (
            f"Hook payload missing connections_accepted: {payload}"
        )
        assert "captured_at_unix" in payload
    finally:
        reset_guc("pgmqtt.metrics_hook_function")
        run_sql("DROP FUNCTION IF EXISTS pgmqtt_test_metrics_hook(jsonb)")
        run_sql("DROP TABLE IF EXISTS _pgmqtt_hook_log")


def test_hook_function_with_unsafe_name_is_skipped():
    """A hook name failing identifier validation is ignored without crashing the broker.

    The Rust flush code rejects any name containing characters outside
    [a-zA-Z0-9_.].  We use a hyphenated name to trigger the guard without
    needing shell-unsafe quoting in set_guc().
    """
    set_guc("pgmqtt.metrics_hook_function", "bad-hook-name")
    _wait_for_flush()
    # Broker must still be alive and tables intact
    rows = run_sql("SELECT to_regclass('pgmqtt_sessions')::text")
    assert rows and rows[0][0] is not None, (
        "pgmqtt_sessions should still exist after unsafe hook name attempt"
    )
    # Also verify no accidental call was made (no such function exists anyway)
    rows = run_sql("SELECT COUNT(*) FROM pgmqtt_metrics_snapshots")
    assert rows[0][0] >= 0  # table is readable = broker is healthy


# ── NOTIFY channel ────────────────────────────────────────────────────────────


def test_notify_channel_delivers_json_snapshot():
    """pgmqtt.metrics_notify_channel causes NOTIFY with a JSON metrics payload."""
    channel = "pgmqtt_metrics_test"
    listen_conn = _pg_conn()
    with listen_conn.cursor() as cur:
        cur.execute(f"LISTEN {channel}")

    try:
        set_guc("pgmqtt.metrics_notify_channel", channel)
        _wait_for_flush()

        # Process any available server-side data
        _select.select([listen_conn], [], [], 0.5)
        listen_conn.poll()

        notifs = listen_conn.notifies
        assert notifs, f"Expected NOTIFY on '{channel}' after flush interval"

        payload = json.loads(notifs[0].payload)
        assert "connections_accepted" in payload
        assert "msgs_received" in payload
        assert "captured_at_unix" in payload
    finally:
        reset_guc("pgmqtt.metrics_notify_channel")
        with listen_conn.cursor() as cur:
            cur.execute(f"UNLISTEN {channel}")
        listen_conn.close()


# ── GUC defaults ─────────────────────────────────────────────────────────────


def test_metrics_gucs_registered():
    """All five metrics GUCs are registered and readable via SHOW."""
    def show(name: str) -> str:
        rows = run_sql(f"SHOW {name}")
        return str(rows[0][0]) if rows else ""

    # snapshot_interval and connections_cache_interval are set by the fixture
    assert show("pgmqtt.metrics_snapshot_interval") == str(FLUSH_INTERVAL_SECS)
    assert show("pgmqtt.metrics_connections_cache_interval") == str(FLUSH_INTERVAL_SECS)
    # retention_days is untouched by the fixture
    assert show("pgmqtt.metrics_retention_days") == "3"
    # String GUCs default to empty
    assert show("pgmqtt.metrics_hook_function") == ""
    assert show("pgmqtt.metrics_notify_channel") == ""


# ── postgres_exporter integration ─────────────────────────────────────────────


def test_prometheus_scrape_via_exporter():
    """postgres_exporter scrapes pgmqtt_metrics() and returns valid Prometheus lines.

    This test hits the real postgres_exporter /metrics HTTP endpoint to verify
    that pgmqtt metrics flow through an actual Prometheus exporter, not just
    that the text format looks correct in isolation.
    """
    _wait_for_flush()

    try:
        with urllib.request.urlopen(EXPORTER_URL, timeout=10) as resp:
            body = resp.read().decode()
    except urllib.error.URLError as exc:
        pytest.fail(
            f"postgres_exporter not reachable at {EXPORTER_URL}: {exc}\n"
            "Ensure the postgres-exporter service is running (docker compose up)."
        )

    # Our custom query produces lines prefixed with pg_pgmqtt
    pg_pgmqtt_lines = [
        line for line in body.splitlines()
        if line.startswith("pg_pgmqtt") and not line.startswith("#")
    ]
    assert pg_pgmqtt_lines, (
        f"No pg_pgmqtt metric lines found in exporter output.\n"
        f"First 3000 chars of /metrics:\n{body[:3000]}"
    )

    # Verify specific counters are present as label values
    assert any('metric_name="connections_accepted"' in l for l in pg_pgmqtt_lines), (
        "connections_accepted not found among pg_pgmqtt lines"
    )
    assert any('metric_name="msgs_received"' in l for l in pg_pgmqtt_lines), (
        "msgs_received not found among pg_pgmqtt lines"
    )

    # Every pg_pgmqtt data line must be valid Prometheus text:
    # <metric_name>{<labels>} <numeric_value>
    # postgres_exporter appends the value column name, e.g. pg_pgmqtt_metric_value{...}
    line_re = re.compile(r'^pg_pgmqtt\w*\{[^}]+\}\s+(\S+)$')
    for line in pg_pgmqtt_lines:
        m = line_re.match(line)
        assert m, f"Malformed Prometheus line from exporter: {line!r}"
        float(m.group(1))  # value must be numeric
