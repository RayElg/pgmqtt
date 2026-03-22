"""Integration tests for pgmqtt_status() function."""

from test_utils import run_sql

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------



def test_status_function_exists_and_returns_row():
    """pgmqtt_status() function exists and returns a single row."""
    result = run_sql("SELECT * FROM pgmqtt_status();")
    assert result
    assert len(result) == 1  # Single row
    row = result[0]
    assert len(row) == 7  # 7 columns (including inbound_mappings)


def test_status_reports_zero_connections_on_empty_database():
    """Initial status shows zero active connections."""
    # Clear any existing sessions
    run_sql("DELETE FROM pgmqtt_sessions;")

    result = run_sql("SELECT active_connections FROM pgmqtt_status();")
    assert result
    assert result[0][0] == 0


def test_status_counts_active_connections():
    """pgmqtt_status() counts active (non-disconnected) sessions."""
    # Clear sessions and insert test data
    run_sql("DELETE FROM pgmqtt_sessions;")

    # Insert 3 active sessions (no disconnected_at)
    run_sql(
        "INSERT INTO pgmqtt_sessions (client_id, next_packet_id, expiry_interval) "
        "VALUES ('client1', 1, 0), ('client2', 1, 0), ('client3', 1, 0);"
    )

    result = run_sql("SELECT active_connections FROM pgmqtt_status();")
    assert result
    assert result[0][0] == 3

    # Disconnect one client
    run_sql("UPDATE pgmqtt_sessions SET disconnected_at = NOW() WHERE client_id = 'client1';")

    result = run_sql("SELECT active_connections FROM pgmqtt_status();")
    assert result
    assert result[0][0] == 2


def test_status_counts_subscriptions():
    """pgmqtt_status() counts total subscriptions."""
    # Clear data
    run_sql("DELETE FROM pgmqtt_subscriptions; DELETE FROM pgmqtt_sessions;")

    # Create sessions and subscriptions
    run_sql(
        "INSERT INTO pgmqtt_sessions (client_id, next_packet_id, expiry_interval) "
        "VALUES ('sub1', 1, 0), ('sub2', 1, 0);"
    )

    # Add subscriptions
    run_sql(
        "INSERT INTO pgmqtt_subscriptions (client_id, topic_filter, qos) "
        "VALUES ('sub1', 'sensors/temp', 0), ('sub1', 'sensors/humidity', 1), ('sub2', 'alerts/#', 0);"
    )

    result = run_sql("SELECT total_subscriptions FROM pgmqtt_status();")
    assert result
    assert result[0][0] == 3


def test_status_counts_retained_messages():
    """pgmqtt_status() counts retained messages."""
    # Clear data
    run_sql("DELETE FROM pgmqtt_retained; DELETE FROM pgmqtt_messages;")

    # Insert test messages and retained mappings
    run_sql(
        "INSERT INTO pgmqtt_messages (topic, payload, retain) "
        "VALUES ('topic/1', 'payload1', true), ('topic/2', 'payload2', true);"
    )

    # Get message IDs
    msg_ids = run_sql("SELECT id FROM pgmqtt_messages ORDER BY id;")

    # Create retained mappings
    if len(msg_ids) >= 2:
        msg_id_1 = msg_ids[0][0]
        msg_id_2 = msg_ids[1][0]
        run_sql(
            f"INSERT INTO pgmqtt_retained (topic, message_id) "
            f"VALUES ('topic/1', {msg_id_1}), ('topic/2', {msg_id_2});"
        )

        result = run_sql("SELECT total_retained_messages FROM pgmqtt_status();")
        assert result
        assert result[0][0] == 2


def test_status_counts_pending_messages():
    """pgmqtt_status() counts pending session messages."""
    # Clear data
    run_sql(
        "DELETE FROM pgmqtt_session_messages; DELETE FROM pgmqtt_sessions; DELETE FROM pgmqtt_messages;"
    )

    # Create a session and messages
    run_sql(
        "INSERT INTO pgmqtt_sessions (client_id, next_packet_id, expiry_interval) "
        "VALUES ('pending1', 1, 0);"
    )

    run_sql(
        "INSERT INTO pgmqtt_messages (topic, payload) "
        "VALUES ('test/1', 'msg1'), ('test/2', 'msg2'), ('test/3', 'msg3');"
    )

    # Get message IDs
    msg_ids = run_sql("SELECT id FROM pgmqtt_messages ORDER BY id;")

    # Add pending messages for the session
    if len(msg_ids) >= 3:
        msg_id_1 = msg_ids[0][0]
        msg_id_2 = msg_ids[1][0]
        msg_id_3 = msg_ids[2][0]
        run_sql(
            f"INSERT INTO pgmqtt_session_messages (client_id, message_id) "
            f"VALUES ('pending1', {msg_id_1}), ('pending1', {msg_id_2}), ('pending1', {msg_id_3});"
        )

        result = run_sql("SELECT pending_session_messages FROM pgmqtt_status();")
        assert result
        assert result[0][0] == 3


def test_status_counts_cdc_mappings():
    """pgmqtt_status() counts CDC topic mappings."""
    # Clear mappings
    run_sql("DELETE FROM pgmqtt_topic_mappings;")

    # Add mappings
    run_sql(
        "SELECT pgmqtt_add_outbound_mapping('public', 'users', 'users/{{ op }}', '{{ columns | tojson }}');"
    )
    run_sql(
        "SELECT pgmqtt_add_outbound_mapping('public', 'orders', 'orders/{{ op }}', '{{ columns | tojson }}', 1);"
    )

    result = run_sql("SELECT cdc_mappings FROM pgmqtt_status();")
    assert result
    assert result[0][0] == 2


def test_status_returns_complete_row():
    """pgmqtt_status() returns all 6 metrics in correct order."""
    # Ensure clean state
    run_sql("DELETE FROM pgmqtt_sessions; DELETE FROM pgmqtt_subscriptions;")
    run_sql("DELETE FROM pgmqtt_messages; DELETE FROM pgmqtt_retained;")
    run_sql("DELETE FROM pgmqtt_session_messages; DELETE FROM pgmqtt_topic_mappings;")

    # Add test data
    run_sql(
        "INSERT INTO pgmqtt_sessions (client_id, next_packet_id, expiry_interval) "
        "VALUES ('test_client', 1, 0);"
    )
    run_sql(
        "INSERT INTO pgmqtt_subscriptions (client_id, topic_filter, qos) "
        "VALUES ('test_client', 'test/topic', 0);"
    )
    run_sql("SELECT pgmqtt_add_outbound_mapping('public', 'test_table', 'test/cdc', 'payload');")

    result = run_sql(
        "SELECT active_connections, total_subscriptions, total_retained_messages, "
        "pending_session_messages, cdc_mappings, cdc_slot_active FROM pgmqtt_status();"
    )
    assert result
    row = result[0]
    assert row[0] >= 1  # active_connections
    assert row[1] >= 1  # total_subscriptions
    assert row[2] >= 0  # total_retained_messages
    assert row[3] >= 0  # pending_session_messages
    assert row[4] >= 1  # cdc_mappings
    assert row[5] >= 0  # cdc_slot_active
