"""
UNLOGGED table reassembly tests.

pgmqtt_connections_cache and pgmqtt_metrics_current are UNLOGGED, meaning
their contents are wiped on a PostgreSQL crash.  These tests verify that both
tables are correctly rebuilt from in-process BGW state on the next flush cycle
after the tables are emptied — equivalent to the post-crash recovery path.

These tests require the enterprise `metrics` feature.  They auto-activate a
license using PGMQTT_TEST_SIGNING_KEY (loaded from .env at the repo root if
not already in the environment) and skip if the key is unavailable.
"""

import os
import socket
import sys
import time
import pytest

from proto_utils import (
    create_connect_packet,
    recv_packet,
    validate_connack,
    MQTT_HOST,
    MQTT_PORT,
)
from test_utils import run_sql

# ---------------------------------------------------------------------------
# Enterprise license setup
# ---------------------------------------------------------------------------

def _load_env_file():
    """Load .env from the repo root into os.environ if PGMQTT_TEST_SIGNING_KEY is absent."""
    if os.environ.get("PGMQTT_TEST_SIGNING_KEY"):
        return
    repo_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..")
    )
    env_path = os.path.join(repo_root, ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())


_load_env_file()

# Add tests/helpers to path so we can import the license/guc helpers.
_helpers_dir = os.path.join(os.path.dirname(__file__), "..", "helpers")
if _helpers_dir not in sys.path:
    sys.path.insert(0, _helpers_dir)

from license import signing_key_available, generate_test_license  # noqa: E402
from guc import set_guc, reset_guc  # noqa: E402


@pytest.fixture()
def metrics_license():
    """
    Activate a metrics-enabled enterprise license for the duration of the test,
    then restore the original license key.  Skips if the signing key is absent.
    """
    if not signing_key_available():
        pytest.skip("PGMQTT_TEST_SIGNING_KEY not set — metrics license unavailable")

    token = generate_test_license(
        customer="test-unlogged",
        days=1,
        features=["tls", "jwt", "metrics"],
        max_connections=1000,
    )
    set_guc("pgmqtt.license_key", token)

    rows = run_sql("SELECT status FROM pgmqtt_license_status()")
    status = rows[0][0] if rows else None
    if status not in ("active", "grace"):
        reset_guc("pgmqtt.license_key")
        pytest.skip(f"License probe returned '{status}' — broker may not have the matching public key")

    yield

    reset_guc("pgmqtt.license_key")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _connect(client_id, clean_start=True, session_expiry=0, keep_alive=60):
    props = {0x11: session_expiry} if session_expiry > 0 else {}
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet(
        client_id, clean_start=clean_start, keep_alive=keep_alive,
        properties=props if props else None,
    ))
    raw = recv_packet(s, timeout=5)
    assert raw is not None, "No CONNACK received"
    sp, rc, _ = validate_connack(raw)
    assert rc == 0, f"CONNACK reason_code={rc:#04x}"
    return s, sp


def _connections_cache_interval() -> int:
    """Return the configured cache flush interval in seconds (default 10)."""
    rows = run_sql(
        "SELECT current_setting('pgmqtt.metrics_connections_cache_interval', true)"
    )
    if rows and rows[0][0]:
        try:
            return int(rows[0][0])
        except (ValueError, TypeError):
            pass
    return 10


# ---------------------------------------------------------------------------
# pgmqtt_connections_cache reassembly
# ---------------------------------------------------------------------------

def test_connections_cache_rebuilds_after_truncate(metrics_license):
    """
    Verify pgmqtt_connections_cache reassembles after being emptied.

    Simulates the UNLOGGED post-crash state by directly truncating the table
    while a client is connected.  After waiting one flush cycle, the table
    must contain a row for the connected client.
    """
    client_id = "ur_cache_rebuild"
    flush_interval = _connections_cache_interval()

    s, _ = _connect(client_id)

    # Wait for the initial cache write so we know the flush path is working.
    deadline = time.time() + flush_interval + 5
    populated = False
    while time.time() < deadline:
        rows = run_sql(
            f"SELECT client_id FROM pgmqtt_connections_cache "
            f"WHERE client_id = '{client_id}'"
        )
        if rows:
            populated = True
            break
        time.sleep(1)
    assert populated, (
        f"pgmqtt_connections_cache not populated within {flush_interval + 5}s"
    )

    # Simulate crash: wipe the table.
    run_sql("TRUNCATE pgmqtt_connections_cache")
    rows = run_sql(
        f"SELECT client_id FROM pgmqtt_connections_cache "
        f"WHERE client_id = '{client_id}'"
    )
    assert not rows, "TRUNCATE did not clear the table"

    # Wait for the next flush cycle to rebuild it.
    deadline = time.time() + flush_interval + 5
    rebuilt = False
    while time.time() < deadline:
        rows = run_sql(
            f"SELECT client_id FROM pgmqtt_connections_cache "
            f"WHERE client_id = '{client_id}'"
        )
        if rows:
            rebuilt = True
            break
        time.sleep(1)

    s.close()
    assert rebuilt, (
        f"pgmqtt_connections_cache not rebuilt within {flush_interval + 5}s after truncate"
    )


def test_connections_cache_does_not_retain_stale_rows(metrics_license):
    """
    After a client disconnects, its row must not persist in the cache beyond
    the next flush cycle.

    Also validates that the cache content is a fresh write-through (not an
    accumulating append), so UNLOGGED truncation is always safe to rely on.
    """
    client_id = "ur_cache_stale"
    flush_interval = _connections_cache_interval()

    s, _ = _connect(client_id)

    # Wait for the cache entry to appear.
    deadline = time.time() + flush_interval + 5
    while time.time() < deadline:
        rows = run_sql(
            f"SELECT client_id FROM pgmqtt_connections_cache "
            f"WHERE client_id = '{client_id}'"
        )
        if rows:
            break
        time.sleep(1)

    s.close()  # Disconnect

    # After disconnect, the row should be removed on the next flush.
    deadline = time.time() + flush_interval + 5
    gone = False
    while time.time() < deadline:
        rows = run_sql(
            f"SELECT client_id FROM pgmqtt_connections_cache "
            f"WHERE client_id = '{client_id}'"
        )
        if not rows:
            gone = True
            break
        time.sleep(1)

    assert gone, (
        f"Disconnected client '{client_id}' still present in connections_cache "
        f"after {flush_interval + 5}s"
    )


# ---------------------------------------------------------------------------
# pgmqtt_metrics_current reassembly
# ---------------------------------------------------------------------------

def test_metrics_current_rebuilds_after_delete(metrics_license):
    """
    Verify pgmqtt_metrics_current is restored after its single row is deleted.

    The BGW maintains all counters in memory and writes them to this table on
    each metrics flush.  Deleting the row simulates the UNLOGGED post-crash
    state; the next flush must re-insert it with correct values.
    """
    snapshot_interval_rows = run_sql(
        "SELECT current_setting('pgmqtt.metrics_snapshot_interval', true)"
    )
    snap_interval = 60
    if snapshot_interval_rows and snapshot_interval_rows[0][0]:
        try:
            snap_interval = int(snapshot_interval_rows[0][0])
        except (ValueError, TypeError):
            pass

    if snap_interval == 0:
        pytest.skip("pgmqtt.metrics_snapshot_interval=0 (metrics disabled)")

    # Ensure there is a current-metrics row to start with.
    deadline = time.time() + snap_interval + 5
    present = False
    while time.time() < deadline:
        rows = run_sql("SELECT id FROM pgmqtt_metrics_current WHERE id = 1")
        if rows:
            present = True
            break
        time.sleep(1)
    assert present, "pgmqtt_metrics_current row never appeared — is metrics feature licensed?"

    # Record the connections_accepted counter before we do anything.
    rows = run_sql("SELECT connections_accepted FROM pgmqtt_metrics_current WHERE id = 1")
    before_accepted = rows[0][0] if rows else 0

    # Wipe the row to simulate crash loss.
    run_sql("DELETE FROM pgmqtt_metrics_current")
    rows = run_sql("SELECT id FROM pgmqtt_metrics_current")
    assert not rows, "DELETE did not remove the metrics row"

    # Connect a client so there is fresh activity to capture.
    s, _ = _connect("ur_metrics_rebuild")
    s.close()

    # Wait for the next flush to re-insert the row.
    deadline = time.time() + snap_interval + 5
    rebuilt = False
    while time.time() < deadline:
        rows = run_sql(
            "SELECT connections_accepted FROM pgmqtt_metrics_current WHERE id = 1"
        )
        if rows:
            after_accepted = rows[0][0]
            # In-memory counter should be at least as high as before the delete.
            assert after_accepted >= before_accepted, (
                f"connections_accepted regressed after rebuild: "
                f"{after_accepted} < {before_accepted}"
            )
            rebuilt = True
            break
        time.sleep(1)

    assert rebuilt, (
        f"pgmqtt_metrics_current not rebuilt within {snap_interval + 5}s after delete"
    )
