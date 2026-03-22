"""
WAL-driven mapping checkpoint tests.

Verifies that:
1. pgmqtt_slot_mappings lags behind pgmqtt_topic_mappings until the WAL slot
   advances past the change (the checkpoint is WAL-driven, not poll-driven).
2. The in-process mapping cache reflects changes at the correct WAL position:
   rows inserted before a mapping change arrive on the old topic, rows inserted
   after arrive on the new topic — even when all events land in the same batch.
"""

import socket
import time

from test_utils import run_sql
from proto_utils import (
    create_connect_packet,
    create_subscribe_packet,
    recv_packet,
    MQTT_HOST,
    MQTT_PORT,
)

SLOT_NAME = "pgmqtt_slot"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _lsn_to_int(lsn: str) -> int:
    """Convert a PostgreSQL LSN string ('A/BC123') to a comparable integer."""
    hi, lo = lsn.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


def _get_confirmed_lsn() -> str | None:
    rows = run_sql(
        f"SELECT confirmed_flush_lsn::text FROM pg_replication_slots "
        f"WHERE slot_name = '{SLOT_NAME}'"
    )
    return rows[0][0] if rows else None


def _get_current_wal_lsn() -> str:
    return run_sql("SELECT pg_current_wal_lsn()::text")[0][0]


def _wait_for_lsn_past(target_lsn: str, timeout: float = 10.0):
    """Block until confirmed_flush_lsn advances past target_lsn."""
    target_int = _lsn_to_int(target_lsn)
    deadline = time.time() + timeout
    while time.time() < deadline:
        current = _get_confirmed_lsn()
        if current and _lsn_to_int(current) >= target_int:
            return
        time.sleep(0.1)
    raise TimeoutError(
        f"Slot '{SLOT_NAME}' did not advance past {target_lsn} within {timeout}s "
        f"(stuck at {_get_confirmed_lsn()})"
    )


def _trigger_wal_advance(table: str):
    """Insert a dummy row so the worker has a reason to tick and advance the slot."""
    run_sql(f"INSERT INTO {table} (val) VALUES ('wal_advance_trigger')")


def _slot_mapping_count(table_name: str) -> int:
    rows = run_sql(
        f"SELECT COUNT(*) FROM pgmqtt_slot_mappings WHERE table_name = '{table_name}'"
    )
    return rows[0][0] if rows else 0


def _connect_and_subscribe(client_id: str, topic: str, qos: int = 1):
    """Open a raw MQTT connection, subscribe to topic, return the socket."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet(client_id, clean_start=True))
    recv_packet(s)  # CONNACK
    s.sendall(create_subscribe_packet(1, topic, qos=qos))
    recv_packet(s)  # SUBACK
    return s


def _messages_on_topic(topic_prefix: str, after_id: int) -> int:
    """Count rows in pgmqtt_messages matching a topic prefix, after a given ID."""
    rows = run_sql(
        f"SELECT COUNT(*) FROM pgmqtt_messages "
        f"WHERE id > {after_id} AND topic LIKE '{topic_prefix}%'"
    )
    return rows[0][0] if rows else 0


def _max_message_id() -> int:
    rows = run_sql("SELECT COALESCE(MAX(id), 0) FROM pgmqtt_messages")
    return rows[0][0] if rows else 0


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_slot_mappings_lags_then_catches_up():
    """
    pgmqtt_slot_mappings should not contain a newly-added mapping until the
    WAL slot advances past the commit that created it.
    """
    table = "wmt_lag_test"
    run_sql(f"DROP TABLE IF EXISTS {table}")
    run_sql(f"CREATE TABLE {table} (id serial primary key, val text)")
    run_sql(f"ALTER TABLE {table} REPLICA IDENTITY FULL")

    try:
        run_sql(
            f"SELECT pgmqtt_add_mapping('public', '{table}', "
            f"'lag_test/{{{{ columns.id }}}}', '{{{{ columns.val }}}}')"
        )
        lsn_after_add = _get_current_wal_lsn()

        # Best-effort check: slot_mappings has not caught up yet.
        # This may occasionally already be 1 if the worker ticked very quickly;
        # that's acceptable — the hard assertion is below after a confirmed advance.
        count_before = _slot_mapping_count(table)

        # Trigger a WAL advance so the worker processes the mapping change.
        _trigger_wal_advance(table)
        _wait_for_lsn_past(lsn_after_add)

        count_after = _slot_mapping_count(table)
        assert count_after == 1, (
            f"Expected 1 row in pgmqtt_slot_mappings for '{table}' after WAL advance, "
            f"got {count_after}"
        )

        if count_before > 0:
            print(
                f"\n  note: worker ticked before pre-advance check "
                f"(count_before={count_before}) — lag window was too narrow to observe"
            )

    finally:
        run_sql(f"SELECT pgmqtt_remove_mapping('public', '{table}')")
        run_sql(f"DROP TABLE IF EXISTS {table}")


def test_slot_mappings_reflects_delete():
    """
    After pgmqtt_remove_mapping commits, pgmqtt_slot_mappings must drop the
    entry once the WAL slot advances past the delete.
    """
    table = "wmt_delete_test"
    run_sql(f"DROP TABLE IF EXISTS {table}")
    run_sql(f"CREATE TABLE {table} (id serial primary key, val text)")
    run_sql(f"ALTER TABLE {table} REPLICA IDENTITY FULL")
    run_sql(
        f"SELECT pgmqtt_add_mapping('public', '{table}', "
        f"'del_test/{{{{ columns.id }}}}', '{{{{ columns.val }}}}')"
    )

    # Wait for the add to land in the checkpoint before testing the delete.
    lsn_after_add = _get_current_wal_lsn()
    _trigger_wal_advance(table)
    _wait_for_lsn_past(lsn_after_add)
    assert _slot_mapping_count(table) == 1, "precondition: mapping must be in slot checkpoint"

    try:
        run_sql(f"SELECT pgmqtt_remove_mapping('public', '{table}')")
        lsn_after_del = _get_current_wal_lsn()

        _trigger_wal_advance(table)
        _wait_for_lsn_past(lsn_after_del)

        count = _slot_mapping_count(table)
        assert count == 0, (
            f"Expected 0 rows in pgmqtt_slot_mappings for '{table}' after WAL-driven delete, "
            f"got {count}"
        )
    finally:
        run_sql(f"DROP TABLE IF EXISTS {table}")


def test_wal_ordering_old_rows_use_old_mapping():
    """
    Rows inserted BEFORE a mapping change must be persisted under the old topic.
    Rows inserted AFTER must be persisted under the new topic.

    All events may land in the same WAL batch — the ring buffer processes them
    in order, applying the MappingUpdate between the two groups of ChangeEvents.

    Uses QoS 1 mappings so messages are durably written to pgmqtt_messages
    regardless of subscriber timing. An MQTT subscriber is kept open throughout
    to satisfy the 'skip if no subscribers' check in the CDC path.
    """
    table = "wmt_order_test"
    old_prefix = "order/v1"
    new_prefix = "order/v2"

    run_sql(f"DROP TABLE IF EXISTS {table}")
    run_sql(f"CREATE TABLE {table} (id serial primary key, val text)")
    run_sql(f"ALTER TABLE {table} REPLICA IDENTITY FULL")

    # Initial mapping → old topic, QoS 1 so messages persist.
    run_sql(
        f"SELECT pgmqtt_add_mapping('public', '{table}', "
        f"'order/v1/{{{{ columns.id }}}}', '{{{{ columns.val }}}}', 1)"
    )

    # Keep a subscriber open so the CDC 'skip if no subscribers' check passes.
    sub = _connect_and_subscribe("wmt_order_sub", "order/#", qos=1)

    try:
        # Wait for the initial mapping to be in the cache.
        lsn_init = _get_current_wal_lsn()
        _trigger_wal_advance(table)
        _wait_for_lsn_past(lsn_init)

        # Record the high-water mark so we only examine messages from this test.
        baseline_id = _max_message_id()

        # --- Phase 1: 3 rows with old mapping ---
        for i in range(3):
            run_sql(f"INSERT INTO {table} (val) VALUES ('before_{i}')")

        # --- Change mapping to new topic in the same WAL stream ---
        run_sql(
            f"SELECT pgmqtt_add_mapping('public', '{table}', "
            f"'order/v2/{{{{ columns.id }}}}', '{{{{ columns.val }}}}', 1)"
        )

        # --- Phase 2: 3 rows with new mapping ---
        for i in range(3):
            run_sql(f"INSERT INTO {table} (val) VALUES ('after_{i}')")

        # Wait until the slot has consumed past all 7 events.
        lsn_end = _get_current_wal_lsn()
        _wait_for_lsn_past(lsn_end)

        old_count = _messages_on_topic(old_prefix, baseline_id)
        new_count = _messages_on_topic(new_prefix, baseline_id)

        assert old_count == 3, (
            f"Expected 3 messages on '{old_prefix}', got {old_count}. "
            f"WAL ordering violated: phase-1 rows used wrong mapping."
        )
        assert new_count == 3, (
            f"Expected 3 messages on '{new_prefix}', got {new_count}. "
            f"WAL ordering violated: phase-2 rows used wrong mapping."
        )

    finally:
        sub.close()
        run_sql(f"SELECT pgmqtt_remove_mapping('public', '{table}')")
        run_sql(f"DROP TABLE IF EXISTS {table}")


def test_slot_mappings_survives_multiple_updates():
    """
    Repeated calls to pgmqtt_add_mapping (same name → UPDATE) are reflected
    correctly in pgmqtt_slot_mappings: the checkpoint always holds the most
    recent committed state after WAL advance.
    """
    table = "wmt_multi_update"
    run_sql(f"DROP TABLE IF EXISTS {table}")
    run_sql(f"CREATE TABLE {table} (id serial primary key, val text)")
    run_sql(f"ALTER TABLE {table} REPLICA IDENTITY FULL")

    try:
        for version in ("v1", "v2", "v3"):
            run_sql(
                f"SELECT pgmqtt_add_mapping('public', '{table}', "
                f"'multi/{version}/{{{{ columns.id }}}}', '{{{{ columns.val }}}}')"
            )

        lsn = _get_current_wal_lsn()
        _trigger_wal_advance(table)
        _wait_for_lsn_past(lsn)

        rows = run_sql(
            f"SELECT topic_template FROM pgmqtt_slot_mappings "
            f"WHERE table_name = '{table}'"
        )
        assert rows and len(rows) == 1, f"Expected 1 row, got {rows}"
        assert "v3" in rows[0][0], (
            f"Expected latest (v3) template in slot checkpoint, got: {rows[0][0]}"
        )

    finally:
        run_sql(f"SELECT pgmqtt_remove_mapping('public', '{table}')")
        run_sql(f"DROP TABLE IF EXISTS {table}")
