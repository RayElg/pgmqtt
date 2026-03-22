"""
Inbound MQTT → PostgreSQL Table Mapping Tests.

Tests that MQTT publishes land as rows in PostgreSQL tables via inbound mappings.
"""

import json
import socket
import time

from test_utils import run_sql
from proto_utils import (
    create_connect_packet,
    create_publish_packet,
    create_subscribe_packet,
    recv_packet,
    validate_connack,
    validate_suback,
    validate_publish,
    MQTTControlPacket,
    MQTT_HOST,
    MQTT_PORT,
    run_psql,
)

TICK_WAIT = 2.0


def _connect(client_id, clean_start=True):
    """Connect to the MQTT broker and validate CONNACK."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet(client_id, clean_start=clean_start))
    pkt = recv_packet(s)
    validate_connack(pkt)
    return s


def _publish_qos0(sock, topic, payload_dict):
    """Publish a QoS 0 message with JSON payload."""
    payload = json.dumps(payload_dict).encode()
    sock.sendall(create_publish_packet(topic, payload, qos=0))


def _publish_qos1(sock, topic, payload_dict, packet_id):
    """Publish a QoS 1 message with JSON payload. Returns PUBACK packet."""
    payload = json.dumps(payload_dict).encode()
    sock.sendall(create_publish_packet(topic, payload, qos=1, packet_id=packet_id))


def _recv_puback(sock, timeout=5.0):
    """Receive and validate a PUBACK packet."""
    sock.settimeout(timeout)
    pkt = recv_packet(sock)
    assert pkt is not None, "Expected PUBACK but got nothing"
    ptype = (pkt[0] & 0xF0) >> 4
    assert ptype == MQTTControlPacket.PUBACK, f"Expected PUBACK (4), got {ptype}"
    return pkt


# ── Setup / Teardown ──────────────────────────────────────────────────────────

def _cleanup():
    """Remove test tables and inbound mappings."""
    run_sql("SELECT pgmqtt_remove_inbound_mapping('test_insert')")
    run_sql("SELECT pgmqtt_remove_inbound_mapping('test_upsert')")
    run_sql("SELECT pgmqtt_remove_inbound_mapping('test_delete')")
    run_sql("SELECT pgmqtt_remove_inbound_mapping('test_nested')")
    run_sql("SELECT pgmqtt_remove_inbound_mapping('test_multi')")
    run_sql("SELECT pgmqtt_remove_inbound_mapping('default')")
    run_sql("DROP TABLE IF EXISTS test_sensors CASCADE")
    run_sql("DROP TABLE IF EXISTS test_nested_data CASCADE")
    run_sql("DROP TABLE IF EXISTS test_multi_target CASCADE")
    # Wait for inbound mapping reload
    time.sleep(TICK_WAIT * 5)


def _setup_test_table():
    """Create the test_sensors table."""
    run_sql("""
        CREATE TABLE IF NOT EXISTS test_sensors (
            site_id text NOT NULL,
            sensor_id text NOT NULL,
            temperature numeric,
            PRIMARY KEY (site_id, sensor_id)
        )
    """)
    run_sql("TRUNCATE test_sensors")


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_add_inbound_mapping():
    """Test creating an inbound mapping via SQL function."""
    _cleanup()
    _setup_test_table()

    result = run_sql("""
        SELECT pgmqtt_add_inbound_mapping(
            'sensor/{site_id}/temperature/{sensor_id}',
            'test_sensors',
            '{"site_id": "{site_id}", "sensor_id": "{sensor_id}", "temperature": "$.temperature"}'::jsonb,
            'insert',
            NULL,
            'public',
            'test_insert'
        )
    """)
    assert result is not None
    assert result[0][0] == "ok"


def test_list_inbound_mappings():
    """Test listing inbound mappings."""
    _cleanup()
    _setup_test_table()

    run_sql("""
        SELECT pgmqtt_add_inbound_mapping(
            'sensor/{site_id}/temperature/{sensor_id}',
            'test_sensors',
            '{"site_id": "{site_id}", "sensor_id": "{sensor_id}", "temperature": "$.temperature"}'::jsonb,
            'insert',
            NULL,
            'public',
            'test_insert'
        )
    """)

    result = run_sql("SELECT * FROM pgmqtt_list_inbound_mappings()")
    assert result is not None
    assert len(result) >= 1
    # Find our mapping
    found = False
    for row in result:
        if row[0] == "test_insert":
            found = True
            assert row[1] == "sensor/{site_id}/temperature/{sensor_id}"
            assert row[2] == "public"
            assert row[3] == "test_sensors"
            assert row[5] == "insert"
    assert found, "Mapping 'test_insert' not found in list"


def test_remove_inbound_mapping():
    """Test removing an inbound mapping."""
    _cleanup()
    _setup_test_table()

    run_sql("""
        SELECT pgmqtt_add_inbound_mapping(
            'sensor/{site_id}/temperature/{sensor_id}',
            'test_sensors',
            '{"site_id": "{site_id}", "sensor_id": "{sensor_id}", "temperature": "$.temperature"}'::jsonb,
            'insert',
            NULL,
            'public',
            'test_insert'
        )
    """)

    result = run_sql("SELECT pgmqtt_remove_inbound_mapping('test_insert')")
    assert result is not None
    assert result[0][0] is True

    # Verify it's gone
    mappings = run_sql("SELECT * FROM pgmqtt_list_inbound_mappings()")
    if mappings:
        for row in mappings:
            assert row[0] != "test_insert"


def test_insert_via_mqtt_publish():
    """Test: MQTT publish → INSERT into PostgreSQL table."""
    _cleanup()
    _setup_test_table()

    run_sql("""
        SELECT pgmqtt_add_inbound_mapping(
            'sensor/{site_id}/temperature/{sensor_id}',
            'test_sensors',
            '{"site_id": "{site_id}", "sensor_id": "{sensor_id}", "temperature": "$.temperature"}'::jsonb,
            'insert',
            NULL,
            'public',
            'test_insert'
        )
    """)

    # Wait for mapping to be picked up by background worker
    time.sleep(TICK_WAIT * 5)

    sock = _connect("inbound_test_insert")
    _publish_qos0(sock, "sensor/site-A/temperature/s1", {"temperature": 22.5})
    time.sleep(TICK_WAIT)

    rows = run_sql("SELECT site_id, sensor_id, temperature FROM test_sensors ORDER BY site_id, sensor_id")
    assert rows is not None, "No rows returned from test_sensors"
    assert len(rows) == 1, f"Expected 1 row, got {len(rows)}: {rows}"
    assert rows[0][0] == "site-A"
    assert rows[0][1] == "s1"
    assert float(rows[0][2]) == 22.5

    sock.close()


def test_upsert_via_mqtt_publish():
    """Test: MQTT publish twice with same key → UPSERT updates existing row."""
    _cleanup()
    _setup_test_table()

    run_sql("""
        SELECT pgmqtt_add_inbound_mapping(
            'sensor/{site_id}/temperature/{sensor_id}',
            'test_sensors',
            '{"site_id": "{site_id}", "sensor_id": "{sensor_id}", "temperature": "$.temperature"}'::jsonb,
            'upsert',
            ARRAY['site_id', 'sensor_id'],
            'public',
            'test_upsert'
        )
    """)

    time.sleep(TICK_WAIT * 5)

    sock = _connect("inbound_test_upsert")
    _publish_qos0(sock, "sensor/site-B/temperature/s2", {"temperature": 18.0})
    time.sleep(TICK_WAIT)

    _publish_qos0(sock, "sensor/site-B/temperature/s2", {"temperature": 25.0})
    time.sleep(TICK_WAIT)

    rows = run_sql("SELECT site_id, sensor_id, temperature FROM test_sensors WHERE site_id = 'site-B' AND sensor_id = 's2'")
    assert rows is not None
    assert len(rows) == 1, f"Expected 1 row after upsert, got {len(rows)}"
    assert float(rows[0][2]) == 25.0, f"Expected temperature 25.0, got {rows[0][2]}"

    sock.close()


def test_delete_via_mqtt_publish():
    """Test: MQTT publish → DELETE from PostgreSQL table."""
    _cleanup()
    _setup_test_table()

    # Insert a row first
    run_sql("INSERT INTO test_sensors (site_id, sensor_id, temperature) VALUES ('site-C', 's3', 30.0)")

    run_sql("""
        SELECT pgmqtt_add_inbound_mapping(
            'sensor/{site_id}/delete/{sensor_id}',
            'test_sensors',
            '{"site_id": "{site_id}", "sensor_id": "{sensor_id}"}'::jsonb,
            'delete',
            ARRAY['site_id', 'sensor_id'],
            'public',
            'test_delete'
        )
    """)

    time.sleep(TICK_WAIT * 5)

    sock = _connect("inbound_test_delete")
    # Send empty JSON object as payload
    sock.sendall(create_publish_packet("sensor/site-C/delete/s3", b"{}", qos=0))
    time.sleep(TICK_WAIT)

    rows = run_sql("SELECT * FROM test_sensors WHERE site_id = 'site-C' AND sensor_id = 's3'")
    assert rows is None or len(rows) == 0, f"Expected row to be deleted, but found: {rows}"

    sock.close()


def test_qos1_puback_after_db_write():
    """Test: QoS 1 PUBACK is deferred until DB write commits."""
    _cleanup()
    _setup_test_table()

    run_sql("""
        SELECT pgmqtt_add_inbound_mapping(
            'sensor/{site_id}/temperature/{sensor_id}',
            'test_sensors',
            '{"site_id": "{site_id}", "sensor_id": "{sensor_id}", "temperature": "$.temperature"}'::jsonb,
            'insert',
            NULL,
            'public',
            'test_insert'
        )
    """)

    time.sleep(TICK_WAIT * 5)

    sock = _connect("inbound_test_qos1")
    _publish_qos1(sock, "sensor/site-D/temperature/s4", {"temperature": 42.0}, packet_id=1)

    # Should receive PUBACK after DB write
    puback = _recv_puback(sock, timeout=10.0)
    assert puback is not None

    # Verify row was written
    rows = run_sql("SELECT temperature FROM test_sensors WHERE site_id = 'site-D' AND sensor_id = 's4'")
    assert rows is not None
    assert len(rows) == 1
    assert float(rows[0][0]) == 42.0

    sock.close()


def test_nested_json_path():
    """Test: Nested JSON dot-path extraction."""
    _cleanup()

    run_sql("""
        CREATE TABLE IF NOT EXISTS test_nested_data (
            device_id text PRIMARY KEY,
            temp numeric,
            humidity numeric
        )
    """)
    run_sql("TRUNCATE test_nested_data")

    run_sql("""
        SELECT pgmqtt_add_inbound_mapping(
            'device/{device_id}/data',
            'test_nested_data',
            '{"device_id": "{device_id}", "temp": "$.readings.temp", "humidity": "$.readings.humidity"}'::jsonb,
            'insert',
            NULL,
            'public',
            'test_nested'
        )
    """)

    time.sleep(TICK_WAIT * 5)

    sock = _connect("inbound_test_nested")
    payload = {"readings": {"temp": 21.5, "humidity": 65.2}}
    _publish_qos0(sock, "device/dev-001/data", payload)
    time.sleep(TICK_WAIT)

    rows = run_sql("SELECT device_id, temp, humidity FROM test_nested_data")
    assert rows is not None
    assert len(rows) == 1
    assert rows[0][0] == "dev-001"
    assert float(rows[0][1]) == 21.5
    assert float(rows[0][2]) == 65.2

    sock.close()


def test_missing_json_field_inserts_null():
    """Test: Missing JSON field results in NULL column value."""
    _cleanup()
    _setup_test_table()

    run_sql("""
        SELECT pgmqtt_add_inbound_mapping(
            'sensor/{site_id}/temperature/{sensor_id}',
            'test_sensors',
            '{"site_id": "{site_id}", "sensor_id": "{sensor_id}", "temperature": "$.temperature"}'::jsonb,
            'insert',
            NULL,
            'public',
            'test_insert'
        )
    """)

    time.sleep(TICK_WAIT * 5)

    sock = _connect("inbound_test_null")
    # Publish with no "temperature" field
    _publish_qos0(sock, "sensor/site-E/temperature/s5", {"other_field": 99})
    time.sleep(TICK_WAIT)

    rows = run_sql("SELECT site_id, sensor_id, temperature FROM test_sensors WHERE site_id = 'site-E'")
    assert rows is not None
    assert len(rows) == 1
    assert rows[0][0] == "site-E"
    assert rows[0][1] == "s5"
    assert rows[0][2] is None, f"Expected NULL temperature, got {rows[0][2]}"

    sock.close()


def test_non_json_payload_skipped():
    """Test: Non-JSON payload is skipped gracefully (no crash)."""
    _cleanup()
    _setup_test_table()

    run_sql("""
        SELECT pgmqtt_add_inbound_mapping(
            'sensor/{site_id}/temperature/{sensor_id}',
            'test_sensors',
            '{"site_id": "{site_id}", "sensor_id": "{sensor_id}", "temperature": "$.temperature"}'::jsonb,
            'insert',
            NULL,
            'public',
            'test_insert'
        )
    """)

    time.sleep(TICK_WAIT * 5)

    sock = _connect("inbound_test_nonjson")
    # Publish non-JSON payload
    raw_payload = b"not json at all"
    sock.sendall(create_publish_packet("sensor/site-F/temperature/s6", raw_payload, qos=0))
    time.sleep(TICK_WAIT)

    # The row should still be inserted (topic vars work, JSON fields become NULL)
    rows = run_sql("SELECT site_id, sensor_id, temperature FROM test_sensors WHERE site_id = 'site-F'")
    assert rows is not None
    assert len(rows) == 1
    assert rows[0][0] == "site-F"
    assert rows[0][1] == "s6"
    assert rows[0][2] is None  # temperature is NULL since JSON parse failed

    sock.close()


def test_validation_bad_table():
    """Test: Creating mapping with non-existent table fails."""
    _cleanup()

    from test_utils import run_sql_expect_error
    result = run_sql_expect_error("""
        SELECT pgmqtt_add_inbound_mapping(
            'foo/{id}',
            'nonexistent_table_xyz',
            '{"id": "{id}"}'::jsonb
        )
    """)
    assert result is not None and "nonexistent_table_xyz" in str(result), \
        f"Expected error about nonexistent table, got: {result}"


def test_validation_bad_column():
    """Test: Creating mapping with non-existent column fails."""
    _cleanup()
    _setup_test_table()

    from test_utils import run_sql_expect_error
    result = run_sql_expect_error("""
        SELECT pgmqtt_add_inbound_mapping(
            'sensor/{site_id}/temperature/{sensor_id}',
            'test_sensors',
            '{"site_id": "{site_id}", "nonexistent_col": "{sensor_id}"}'::jsonb,
            'insert',
            NULL,
            'public',
            'test_insert'
        )
    """)
    assert result is not None and "nonexistent_col" in str(result), f"Expected error about nonexistent_col, got: {result}"


def test_validation_bad_pattern_wildcards():
    """Test: Creating mapping with MQTT wildcards fails."""
    _cleanup()
    _setup_test_table()

    from test_utils import run_sql_expect_error
    result = run_sql_expect_error("""
        SELECT pgmqtt_add_inbound_mapping(
            'sensor/+/temperature/#',
            'test_sensors',
            '{"site_id": "$topic"}'::jsonb,
            'insert',
            NULL,
            'public',
            'test_insert'
        )
    """)
    assert result is not None and "wildcard" in str(result).lower(), f"Expected wildcard error, got: {result}"


def test_validation_missing_var_reference():
    """Test: Column map referencing non-existent topic variable fails."""
    _cleanup()
    _setup_test_table()

    from test_utils import run_sql_expect_error
    result = run_sql_expect_error("""
        SELECT pgmqtt_add_inbound_mapping(
            'sensor/{site_id}/temperature',
            'test_sensors',
            '{"site_id": "{site_id}", "sensor_id": "{missing_var}", "temperature": "$.temperature"}'::jsonb,
            'insert',
            NULL,
            'public',
            'test_insert'
        )
    """)
    assert result is not None and "missing_var" in str(result), f"Expected error about missing_var, got: {result}"


def test_coexistence_inbound_and_subscriber():
    """Test: Inbound mapping + MQTT subscriber both receive the message."""
    _cleanup()
    _setup_test_table()

    run_sql("""
        SELECT pgmqtt_add_inbound_mapping(
            'sensor/{site_id}/temperature/{sensor_id}',
            'test_sensors',
            '{"site_id": "{site_id}", "sensor_id": "{sensor_id}", "temperature": "$.temperature"}'::jsonb,
            'insert',
            NULL,
            'public',
            'test_insert'
        )
    """)

    time.sleep(TICK_WAIT * 5)

    # Connect a subscriber
    sub_sock = _connect("inbound_test_sub")
    sub_sock.sendall(create_subscribe_packet(1, "sensor/+/temperature/+", qos=0))
    suback = recv_packet(sub_sock)
    validate_suback(suback, 1)

    # Connect a publisher
    pub_sock = _connect("inbound_test_pub")
    _publish_qos0(pub_sock, "sensor/site-G/temperature/s7", {"temperature": 33.3})
    time.sleep(TICK_WAIT)

    # Subscriber should receive the message
    sub_sock.settimeout(5.0)
    pkt = recv_packet(sub_sock)
    assert pkt is not None, "Subscriber should receive the published message"
    topic, payload, _qos, _dup, _retain, _pid, _props = validate_publish(pkt)
    assert topic == "sensor/site-G/temperature/s7"

    # Row should also be in the database
    rows = run_sql("SELECT temperature FROM test_sensors WHERE site_id = 'site-G' AND sensor_id = 's7'")
    assert rows is not None
    assert len(rows) == 1
    assert float(rows[0][0]) == 33.3

    pub_sock.close()
    sub_sock.close()


def test_topic_pattern_no_match():
    """Test: Publish to non-matching topic doesn't trigger inbound write."""
    _cleanup()
    _setup_test_table()

    run_sql("""
        SELECT pgmqtt_add_inbound_mapping(
            'sensor/{site_id}/temperature/{sensor_id}',
            'test_sensors',
            '{"site_id": "{site_id}", "sensor_id": "{sensor_id}", "temperature": "$.temperature"}'::jsonb,
            'insert',
            NULL,
            'public',
            'test_insert'
        )
    """)

    time.sleep(TICK_WAIT * 5)

    sock = _connect("inbound_test_nomatch")
    # Publish to a topic that doesn't match the pattern
    _publish_qos0(sock, "other/topic/completely", {"temperature": 99})
    time.sleep(TICK_WAIT)

    rows = run_sql("SELECT * FROM test_sensors")
    assert rows is None or len(rows) == 0, f"Expected no rows, but found: {rows}"

    sock.close()


def test_payload_and_topic_sources():
    """Test: $payload and $topic column sources work."""
    _cleanup()

    run_sql("""
        CREATE TABLE IF NOT EXISTS test_multi_target (
            id serial PRIMARY KEY,
            raw_topic text,
            raw_payload text,
            device_id text
        )
    """)
    run_sql("TRUNCATE test_multi_target")

    run_sql("""
        SELECT pgmqtt_add_inbound_mapping(
            'log/{device_id}',
            'test_multi_target',
            '{"raw_topic": "$topic", "raw_payload": "$payload", "device_id": "{device_id}"}'::jsonb,
            'insert',
            NULL,
            'public',
            'test_multi'
        )
    """)

    time.sleep(TICK_WAIT * 5)

    sock = _connect("inbound_test_special")
    payload = {"msg": "hello"}
    _publish_qos0(sock, "log/dev-X", payload)
    time.sleep(TICK_WAIT)

    rows = run_sql("SELECT raw_topic, raw_payload, device_id FROM test_multi_target")
    assert rows is not None
    assert len(rows) == 1
    assert rows[0][0] == "log/dev-X"
    assert rows[0][2] == "dev-X"
    # raw_payload should be the JSON string
    parsed = json.loads(rows[0][1])
    assert parsed["msg"] == "hello"

    sock.close()


def test_status_includes_inbound_mappings():
    """Test: pgmqtt_status() includes inbound_mappings count."""
    _cleanup()
    _setup_test_table()

    run_sql("""
        SELECT pgmqtt_add_inbound_mapping(
            'sensor/{site_id}/temperature/{sensor_id}',
            'test_sensors',
            '{"site_id": "{site_id}", "sensor_id": "{sensor_id}", "temperature": "$.temperature"}'::jsonb,
            'insert',
            NULL,
            'public',
            'test_insert'
        )
    """)

    result = run_sql("SELECT * FROM pgmqtt_status()")
    assert result is not None
    # The new column should be the 7th (index 6)
    assert len(result[0]) == 7, f"Expected 7 columns in pgmqtt_status(), got {len(result[0])}"
    inbound_count = result[0][6]
    assert inbound_count >= 1, f"Expected at least 1 inbound mapping, got {inbound_count}"


# ── Cleanup at end ────────────────────────────────────────────────────────────

def teardown_module():
    """Clean up after all tests."""
    _cleanup()
