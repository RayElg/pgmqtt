"""
MQTT 5.0 Subscribe, Unsubscribe, and Wildcard Topic Matching Tests.

Covers both client-to-client pub/sub wildcards and CDC wildcard matching.
"""

import socket
import time

from proto_utils import (
    create_connect_packet,
    create_subscribe_packet,
    create_unsubscribe_packet,
    create_publish_packet,
    create_puback_packet,
    recv_packet,
    validate_connack,
    validate_suback,
    validate_unsuback,
    validate_publish,
    ReasonCode,
    MQTTControlPacket,
    MQTT_HOST,
    MQTT_PORT,
    run_psql,
)


# ---------------------------------------------------------------------------
# Client-to-client wildcard tests
# ---------------------------------------------------------------------------

def test_single_level_wildcard():
    """test/+/data matches test/a/data but not test/a/b/data."""
    s_cons = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_cons.connect((MQTT_HOST, MQTT_PORT))
    s_cons.sendall(create_connect_packet("wc_single_cons", clean_start=True))
    validate_connack(recv_packet(s_cons))
    s_cons.sendall(create_subscribe_packet(20, "test/+/data", qos=0))
    validate_suback(recv_packet(s_cons), 20)

    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("wc_single_pub"))
    recv_packet(s_pub)

    s_pub.sendall(create_publish_packet("test/a/data", b"match1", qos=0))
    s_pub.sendall(create_publish_packet("test/sensor1/data", b"match2", qos=0))
    s_pub.sendall(create_publish_packet("test/a/b/data", b"nomatch", qos=0))
    s_pub.close()

    topics = []
    for _ in range(2):
        pkt = recv_packet(s_cons, timeout=2.0)
        assert pkt is not None
        t, *_ = validate_publish(pkt)
        topics.append(t)

    assert "test/a/data" in topics
    assert "test/sensor1/data" in topics

    extra = recv_packet(s_cons, timeout=1.0)
    assert extra is None, "Should not match test/a/b/data"
    s_cons.close()


def test_multi_level_wildcard():
    """test/multi/# matches test/multi, test/multi/a, test/multi/a/b/c."""
    s_cons = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_cons.connect((MQTT_HOST, MQTT_PORT))
    s_cons.sendall(create_connect_packet("wc_multi_cons", clean_start=True))
    recv_packet(s_cons)
    s_cons.sendall(create_subscribe_packet(21, "test/multi/#", qos=0))
    recv_packet(s_cons)

    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("wc_multi_pub"))
    recv_packet(s_pub)

    s_pub.sendall(create_publish_packet("test/multi", b"m1", qos=0))
    s_pub.sendall(create_publish_packet("test/multi/a", b"m2", qos=0))
    s_pub.sendall(create_publish_packet("test/multi/a/b/c", b"m3", qos=0))
    s_pub.close()

    topics = []
    for _ in range(3):
        pkt = recv_packet(s_cons, timeout=2.0)
        assert pkt is not None
        t, *_ = validate_publish(pkt)
        topics.append(t)

    assert "test/multi" in topics
    assert "test/multi/a" in topics
    assert "test/multi/a/b/c" in topics
    s_cons.close()


def test_combined_wildcards():
    """+/sensor/# matches home/sensor, office/sensor/temp, etc."""
    s_cons = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_cons.connect((MQTT_HOST, MQTT_PORT))
    s_cons.sendall(create_connect_packet("wc_combo_cons", clean_start=True))
    recv_packet(s_cons)
    s_cons.sendall(create_subscribe_packet(22, "+/sensor/#", qos=0))
    recv_packet(s_cons)

    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("wc_combo_pub"))
    recv_packet(s_pub)

    s_pub.sendall(create_publish_packet("home/sensor", b"c1", qos=0))
    s_pub.sendall(create_publish_packet("office/sensor/temp", b"c2", qos=0))
    s_pub.sendall(create_publish_packet("garage/sensor/humidity/1", b"c3", qos=0))
    s_pub.sendall(create_publish_packet("home/device/temp", b"nomatch", qos=0))
    s_pub.close()

    topics = []
    for _ in range(3):
        pkt = recv_packet(s_cons, timeout=2.0)
        assert pkt is not None
        t, *_ = validate_publish(pkt)
        topics.append(t)

    assert "home/sensor" in topics
    assert "office/sensor/temp" in topics
    assert "garage/sensor/humidity/1" in topics

    extra = recv_packet(s_cons, timeout=1.0)
    assert extra is None, "Should not receive non-matching topic"
    s_cons.close()


# ---------------------------------------------------------------------------
# Unsubscribe tests
# ---------------------------------------------------------------------------

def test_unsubscribe_stops_messages():
    """After UNSUBSCRIBE, broker stops delivering messages."""
    topic = "test/unsub/stop"

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("unsub_cons", clean_start=True))
    validate_connack(recv_packet(s))
    s.sendall(create_subscribe_packet(30, topic, qos=0))
    validate_suback(recv_packet(s), 30)

    # Verify subscription works
    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("unsub_pub1"))
    recv_packet(s_pub)
    s_pub.sendall(create_publish_packet(topic, b"before unsub", qos=0))
    s_pub.close()

    pkt = recv_packet(s)
    assert pkt is not None, "Should receive message before unsubscribe"

    # Unsubscribe
    s.sendall(create_unsubscribe_packet(31, topic))
    reason_codes = validate_unsuback(recv_packet(s), 31)
    assert reason_codes[0] in (ReasonCode.SUCCESS, ReasonCode.NO_SUBSCRIPTION_EXISTED)

    # Publish again — should NOT be delivered
    s_pub2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub2.connect((MQTT_HOST, MQTT_PORT))
    s_pub2.sendall(create_connect_packet("unsub_pub2"))
    recv_packet(s_pub2)
    s_pub2.sendall(create_publish_packet(topic, b"after unsub", qos=0))
    s_pub2.close()

    pkt2 = recv_packet(s, timeout=1.0)
    assert pkt2 is None, "Should NOT receive after unsubscribe"
    s.close()


def test_unsuback_header_and_reason():
    """UNSUBACK has correct flags and valid reason codes."""
    topic = "test/unsub/header"

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("unsuback_test", clean_start=True))
    recv_packet(s)

    s.sendall(create_subscribe_packet(32, topic, qos=0))
    recv_packet(s)

    # Unsubscribe from existing
    s.sendall(create_unsubscribe_packet(33, topic))
    rc = validate_unsuback(recv_packet(s), 33)
    assert rc[0] == ReasonCode.SUCCESS

    # Unsubscribe from non-existent
    s.sendall(create_unsubscribe_packet(34, "test/unsub/nonexistent"))
    rc2 = validate_unsuback(recv_packet(s), 34)
    assert rc2[0] in (ReasonCode.SUCCESS, ReasonCode.NO_SUBSCRIPTION_EXISTED)
    s.close()


# ---------------------------------------------------------------------------
# CDC wildcard tests
# ---------------------------------------------------------------------------

def _setup_cdc_table(table, topic_template, payload_template="{{ columns.val }}", qos=0):
    run_psql(f"DROP TABLE IF EXISTS {table};")
    run_psql(f"CREATE TABLE {table} (id serial primary key, name text, val text);")
    run_psql(f"ALTER TABLE {table} REPLICA IDENTITY FULL;")
    run_psql(
        f"SELECT pgmqtt_add_outbound_mapping('public', '{table}', "
        f"'{topic_template}', '{payload_template}', {qos});"
    )
    time.sleep(6)


def test_cdc_wildcard_single_level():
    """CDC topic cdc/{name}/data matched by subscription cdc/+/data."""
    table = "cdc_wc_single"
    _setup_cdc_table(table, "cdc/{{ columns.name }}/data")

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("cdc_wc_single_sub", clean_start=True))
    validate_connack(recv_packet(s))
    s.sendall(create_subscribe_packet(1, "cdc/+/data", qos=0))
    validate_suback(recv_packet(s), 1)

    run_psql(f"INSERT INTO {table} (name, val) VALUES ('sensor1', 'hello');")

    pkt = recv_packet(s, timeout=5.0)
    assert pkt is not None, "Should receive CDC message via single-level wildcard"
    topic, *_ = validate_publish(pkt)
    assert topic == "cdc/sensor1/data"
    s.close()


def test_cdc_wildcard_multi_level():
    """CDC topic cdc/multi/{name} matched by subscription cdc/multi/#."""
    table = "cdc_wc_multi"
    _setup_cdc_table(table, "cdc/multi/{{ columns.name }}")

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("cdc_wc_multi_sub", clean_start=True))
    validate_connack(recv_packet(s))
    s.sendall(create_subscribe_packet(1, "cdc/multi/#", qos=0))
    validate_suback(recv_packet(s), 1)

    run_psql(f"INSERT INTO {table} (name, val) VALUES ('deep', 'world');")

    pkt = recv_packet(s, timeout=5.0)
    assert pkt is not None, "Should receive CDC message via multi-level wildcard"
    topic, *_ = validate_publish(pkt)
    assert topic == "cdc/multi/deep"
    s.close()


def test_cdc_unsubscribe_stops_messages():
    """UNSUBSCRIBE stops CDC message delivery."""
    table = "cdc_unsub_test"
    _setup_cdc_table(table, "cdc/unsub/{{ columns.name }}")

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("cdc_unsub_sub", clean_start=True))
    validate_connack(recv_packet(s))
    s.sendall(create_subscribe_packet(1, "cdc/unsub/#", qos=0))
    validate_suback(recv_packet(s), 1)

    # Verify delivery works
    run_psql(f"INSERT INTO {table} (name, val) VALUES ('first', 'v1');")
    pkt = recv_packet(s, timeout=5.0)
    assert pkt is not None, "Should receive first CDC message"

    # Unsubscribe
    s.sendall(create_unsubscribe_packet(2, "cdc/unsub/#"))
    validate_unsuback(recv_packet(s), 2)

    # Insert again — should NOT be delivered
    run_psql(f"INSERT INTO {table} (name, val) VALUES ('second', 'v2');")
    pkt2 = recv_packet(s, timeout=3.0)
    assert pkt2 is None, "Should NOT receive after unsubscribe"
    s.close()
