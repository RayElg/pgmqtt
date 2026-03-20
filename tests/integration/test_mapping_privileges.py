"""Integration tests for pgmqtt topic mapping privileges.

Topic mapping commands (pgmqtt_add_mapping, pgmqtt_remove_mapping, pgmqtt_list_mappings)
are restricted via PostgreSQL's native privilege system:
  REVOKE EXECUTE ... FROM PUBLIC  (applied at extension install time via extension_sql!)

DBAs can open access to specific roles with GRANT EXECUTE without needing superuser.
"""

from test_utils import run_sql, run_sql_expect_error

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------



def test_superuser_can_add_mapping():
    """Superuser (postgres) can successfully add a topic mapping."""
    result = run_sql(
        "SELECT pgmqtt_add_mapping('public', 'events', 'events/{{ op }}', '{{ columns | tojson }}');"
    )
    assert result
    assert result[0][0] == "ok"

    # Verify it was stored
    rows = run_sql("SELECT schema_name, table_name FROM pgmqtt_topic_mappings")
    assert any(r[0] == "public" and r[1] == "events" for r in rows)


def test_superuser_can_list_mappings():
    """Superuser can list all mappings."""
    # Add a mapping first
    run_sql("SELECT pgmqtt_add_mapping('public', 'test_list', 'test/topic', 'payload');")

    # List mappings
    result = run_sql("SELECT COUNT(*) FROM pgmqtt_list_mappings();")
    assert result
    count = int(result[0][0])
    assert count >= 1


def test_superuser_can_remove_mapping():
    """Superuser can remove a topic mapping."""
    # Add a mapping first
    run_sql("SELECT pgmqtt_add_mapping('public', 'remove_test', 'test/topic', 'payload');")

    # Remove it
    result = run_sql("SELECT pgmqtt_remove_mapping('public', 'remove_test');")
    assert result
    assert result[0][0]  # Should return true

    # Verify it's gone
    rows = run_sql("SELECT COUNT(*) FROM pgmqtt_topic_mappings WHERE table_name = 'remove_test'")
    assert int(rows[0][0]) == 0


def test_mapping_operations_with_default_qos():
    """Mapping operations use default QoS value when not specified."""
    # Add mapping without QoS (should default to 0)
    run_sql("SELECT pgmqtt_add_mapping('public', 'qos_default', 'test/topic', 'payload');")

    rows = run_sql(
        "SELECT qos FROM pgmqtt_topic_mappings WHERE table_name = 'qos_default';"
    )
    assert rows
    assert rows[0][0] == 0


def test_mapping_operations_with_explicit_qos():
    """Mapping operations store explicit QoS value."""
    # Add mapping with QoS = 1
    run_sql("SELECT pgmqtt_add_mapping('public', 'qos_one', 'test/topic', 'payload', 1);")

    rows = run_sql("SELECT qos FROM pgmqtt_topic_mappings WHERE table_name = 'qos_one';")
    assert rows
    assert rows[0][0] == 1


def test_mapping_overwrite():
    """Adding a mapping twice overwrites the first (upsert behavior)."""
    # Add initial mapping
    run_sql(
        "SELECT pgmqtt_add_mapping('public', 'overwrite', 'topic/v1', 'payload_v1');"
    )

    # Verify initial
    rows = run_sql(
        "SELECT topic_template FROM pgmqtt_topic_mappings WHERE table_name = 'overwrite';"
    )
    assert rows[0][0] == "topic/v1"

    # Overwrite with new topic
    run_sql(
        "SELECT pgmqtt_add_mapping('public', 'overwrite', 'topic/v2', 'payload_v2');"
    )

    # Verify updated
    rows = run_sql(
        "SELECT topic_template FROM pgmqtt_topic_mappings WHERE table_name = 'overwrite';"
    )
    assert rows[0][0] == "topic/v2"


def _drop_user(username: str) -> None:
    """Drop a user and all their privileges cleanly."""
    run_sql(f"DROP OWNED BY {username};")
    run_sql(f"DROP USER IF EXISTS {username};")


def test_unprivileged_user_denied():
    """Non-privileged user cannot call mapping functions (REVOKE FROM PUBLIC enforced)."""
    run_sql_expect_error("DROP OWNED BY pgmqtt_test_unpriv;")
    run_sql("DROP USER IF EXISTS pgmqtt_test_unpriv;")
    run_sql("CREATE USER pgmqtt_test_unpriv WITH PASSWORD 'testpass';")
    run_sql("GRANT CONNECT ON DATABASE postgres TO pgmqtt_test_unpriv;")

    try:
        error = run_sql_expect_error(
            "SELECT pgmqtt_add_mapping('public', 'x', 'x', 'x');",
            username="pgmqtt_test_unpriv",
            password="testpass",
        )
        assert error is not None, "Expected permission denied error"
        assert "permission denied" in error.lower(), f"Unexpected error: {error}"
    finally:
        _drop_user("pgmqtt_test_unpriv")


def test_granted_user_allowed():
    """User explicitly granted EXECUTE can call mapping functions."""
    run_sql_expect_error("DROP OWNED BY pgmqtt_test_priv;")
    run_sql("DROP USER IF EXISTS pgmqtt_test_priv;")
    run_sql("CREATE USER pgmqtt_test_priv WITH PASSWORD 'testpass';")
    run_sql("GRANT CONNECT ON DATABASE postgres TO pgmqtt_test_priv;")
    run_sql(
        "GRANT EXECUTE ON FUNCTION pgmqtt_add_mapping(text, text, text, text, int) "
        "TO pgmqtt_test_priv;"
    )
    run_sql("GRANT SELECT, INSERT, UPDATE ON TABLE pgmqtt_topic_mappings TO pgmqtt_test_priv;")

    try:
        result = run_sql_expect_error(
            "SELECT pgmqtt_add_mapping('public', 'priv_test', 'priv/topic', 'payload');",
            username="pgmqtt_test_priv",
            password="testpass",
        )
        # run_sql_expect_error returns the error string on failure, or None/rows on success
        assert not isinstance(result, str), f"Expected success but got error: {result}"
    finally:
        run_sql("DELETE FROM pgmqtt_topic_mappings WHERE table_name = 'priv_test';")
        _drop_user("pgmqtt_test_priv")
