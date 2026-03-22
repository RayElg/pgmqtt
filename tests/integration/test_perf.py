"""
Performance and Load Tests for pgmqtt.

All tests are marked @pytest.mark.slow — skip with: pytest -m "not slow"
Results are printed in a table format for GHA step summary output.
"""

import json
import socket
import threading
import time
import pytest

from proto_utils import (
    create_connect_packet,
    create_subscribe_packet,
    create_publish_packet,
    create_puback_packet,
    recv_packet,
    validate_publish,
    MQTTControlPacket,
    MQTT_HOST,
    MQTT_PORT,
    run_psql,
)
from test_utils import run_sql

NUM_MESSAGES = 200
TOPIC = "perf/test"


def _print_result(name, count, duration):
    rate = count / duration if duration > 0 else 0
    print(f"  PERF | {name:<30} | {count:>5} msgs | {duration:>6.2f}s | {rate:>8.1f} msg/s")


# ---------------------------------------------------------------------------
# Client pub/sub throughput
# ---------------------------------------------------------------------------

@pytest.mark.slow
def test_published_qos0_throughput():
    """Throughput: 200 QoS 0 client-published messages."""
    payload = b"perf qos0"

    s_sub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_sub.connect((MQTT_HOST, MQTT_PORT))
    s_sub.sendall(create_connect_packet("perf_q0_sub", clean_start=True))
    recv_packet(s_sub)
    s_sub.sendall(create_subscribe_packet(1, TOPIC, qos=0))
    recv_packet(s_sub)

    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("perf_q0_pub", clean_start=True))
    recv_packet(s_pub)

    start = time.time()
    for _ in range(NUM_MESSAGES):
        s_pub.sendall(create_publish_packet(TOPIC, payload, qos=0))
    s_pub.close()

    received = 0
    while received < NUM_MESSAGES:
        pkt = recv_packet(s_sub, timeout=2.0)
        if not pkt:
            break
        received += 1

    elapsed = time.time() - start
    _print_result("Published QoS 0", received, elapsed)
    assert received >= NUM_MESSAGES * 0.95, f"Too many dropped: {received}/{NUM_MESSAGES}"
    s_sub.close()


@pytest.mark.slow
def test_published_qos1_pipelined_throughput():
    """Throughput: 200 QoS 1 pipelined (send all, then collect ACKs)."""
    payload = b"perf qos1 pipe"

    s_sub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_sub.connect((MQTT_HOST, MQTT_PORT))
    s_sub.sendall(create_connect_packet("perf_q1p_sub", clean_start=True))
    recv_packet(s_sub)
    s_sub.sendall(create_subscribe_packet(1, TOPIC, qos=1))
    recv_packet(s_sub)

    results = {"count": 0}

    def sub_thread():
        count = 0
        while count < NUM_MESSAGES:
            pkt = recv_packet(s_sub, timeout=10.0)
            if not pkt:
                break
            try:
                _, _, _, _, _, pid, _ = validate_publish(pkt)
                if pid:
                    s_sub.sendall(create_puback_packet(pid))
                count += 1
            except Exception:
                break
        results["count"] = count

    t = threading.Thread(target=sub_thread)
    t.start()

    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("perf_q1p_pub", clean_start=True))
    recv_packet(s_pub)

    start = time.time()
    for i in range(NUM_MESSAGES):
        s_pub.sendall(create_publish_packet(TOPIC, payload, qos=1, packet_id=i + 1))

    acks = 0
    while acks < NUM_MESSAGES:
        ack = recv_packet(s_pub, timeout=5.0)
        if not ack:
            break
        acks += 1
    s_pub.close()

    t.join()
    elapsed = time.time() - start
    _print_result("Published QoS 1 (pipelined)", results["count"], elapsed)
    assert results["count"] == NUM_MESSAGES
    s_sub.close()


# ---------------------------------------------------------------------------
# CDC throughput
# ---------------------------------------------------------------------------

def _setup_perf_cdc(qos):
    run_psql("DROP TABLE IF EXISTS perf_table CASCADE;")
    run_psql("CREATE TABLE perf_table (id serial PRIMARY KEY, data text);")
    run_psql("ALTER TABLE perf_table REPLICA IDENTITY FULL;")
    run_psql(
        f"SELECT pgmqtt_add_outbound_mapping('public', 'perf_table', "
        f"'{TOPIC}', '{{{{columns.data}}}}', {qos});"
    )
    time.sleep(6)


@pytest.mark.slow
def test_cdc_qos0_throughput():
    """Throughput: 200 CDC QoS 0 messages."""
    _setup_perf_cdc(0)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("perf_cdc_q0", clean_start=True))
    recv_packet(s)
    s.sendall(create_subscribe_packet(1, TOPIC, qos=0))
    recv_packet(s)

    start = time.time()
    run_psql(f"INSERT INTO perf_table (data) SELECT 'msg-' || generate_series(1, {NUM_MESSAGES});")

    received = 0
    while received < NUM_MESSAGES:
        pkt = recv_packet(s, timeout=10.0)
        if not pkt:
            break
        received += 1

    elapsed = time.time() - start
    _print_result("CDC QoS 0", received, elapsed)
    assert received >= NUM_MESSAGES * 0.95
    s.close()


@pytest.mark.slow
def test_cdc_qos1_throughput():
    """Throughput: 200 CDC QoS 1 messages."""
    _setup_perf_cdc(1)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("perf_cdc_q1", clean_start=True))
    recv_packet(s)
    s.sendall(create_subscribe_packet(1, TOPIC, qos=1))
    recv_packet(s)

    start = time.time()
    run_psql(f"INSERT INTO perf_table (data) SELECT 'msg-' || generate_series(1, {NUM_MESSAGES});")

    received = 0
    while received < NUM_MESSAGES:
        pkt = recv_packet(s, timeout=10.0)
        if not pkt:
            break
        try:
            _, _, _, _, _, pid, _ = validate_publish(pkt)
            if pid:
                s.sendall(create_puback_packet(pid))
            received += 1
        except Exception:
            break

    elapsed = time.time() - start
    _print_result("CDC QoS 1", received, elapsed)
    assert received == NUM_MESSAGES
    s.close()


# ---------------------------------------------------------------------------
# Long-lived connection
# ---------------------------------------------------------------------------

@pytest.mark.slow
def test_long_lived_connection():
    """40-second connection with periodic heartbeat messages."""
    run_psql("DROP TABLE IF EXISTS heartbeat;")
    run_psql("CREATE TABLE heartbeat (id serial PRIMARY KEY, ts timestamp DEFAULT now());")
    run_psql("ALTER TABLE heartbeat REPLICA IDENTITY FULL;")
    run_psql("SELECT pgmqtt_add_outbound_mapping('public', 'heartbeat', 'system/heartbeat', '{{ columns.ts }}');")
    time.sleep(6)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("perf_longlived"))
    recv_packet(s)
    s.sendall(create_subscribe_packet(1, "system/heartbeat"))
    recv_packet(s)

    duration = 40
    interval = 10
    count = 0
    received = 0
    start = time.time()
    last_send = 0

    while time.time() - start < duration:
        now = time.time()
        if now - last_send >= interval:
            run_psql("INSERT INTO heartbeat DEFAULT VALUES;")
            count += 1
            last_send = now

        pkt = recv_packet(s, timeout=1.0)
        if pkt:
            ptype = pkt[0] >> 4
            if ptype == MQTTControlPacket.PUBLISH:
                topic, *_ = validate_publish(pkt)
                if topic == "system/heartbeat":
                    received += 1

        if count % 3 == 0 and count > 0:
            s.sendall(bytes([0xC0, 0x00]))  # PINGREQ

        time.sleep(0.1)

    _print_result("Long-lived (40s)", received, time.time() - start)
    assert received >= count, f"Only received {received}/{count} heartbeats"
    s.close()


# ---------------------------------------------------------------------------
# High concurrency CDC
# ---------------------------------------------------------------------------

@pytest.mark.slow
def test_high_concurrency_cdc():
    """50 concurrent subscribers, 1000 CDC messages each."""
    NUM_CLIENTS = 50
    NUM_MSGS = 1000
    BATCH_SIZE = 100

    run_sql("DROP TABLE IF EXISTS load_test;")
    run_sql("CREATE TABLE load_test (id serial PRIMARY KEY, val int);")
    run_sql("ALTER TABLE load_test REPLICA IDENTITY FULL;")
    run_sql(
        "SELECT pgmqtt_add_outbound_mapping('public', 'load_test', "
        "'load/test/{{columns.val}}', '{\"val\": {{columns.val}}}');"
    )
    time.sleep(6)

    results = [0] * NUM_CLIENTS

    def client_task(idx):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((MQTT_HOST, MQTT_PORT))
            s.settimeout(60)
            s.sendall(create_connect_packet(f"load_client_{idx}", clean_start=True))
            recv_packet(s)
            s.sendall(create_subscribe_packet(1, "load/test/#"))
            recv_packet(s)

            count = 0
            deadline = time.time() + 60
            while count < NUM_MSGS and time.time() < deadline:
                pkt = recv_packet(s, timeout=5.0)
                if not pkt:
                    continue
                if pkt[0] >> 4 == MQTTControlPacket.PUBLISH:
                    count += 1
            s.close()
            results[idx] = count
        except Exception as e:
            print(f"  Client {idx} error: {e}")

    threads = []
    for i in range(NUM_CLIENTS):
        t = threading.Thread(target=client_task, args=(i,))
        t.start()
        threads.append(t)

    time.sleep(5)  # Let all clients connect

    start = time.time()
    sent = 0
    while sent < NUM_MSGS:
        batch = min(BATCH_SIZE, NUM_MSGS - sent)
        run_sql(f"INSERT INTO load_test (val) SELECT generate_series({sent + 1}, {sent + batch});")
        sent += batch
        time.sleep(0.5)

    for t in threads:
        t.join()

    elapsed = time.time() - start
    total = sum(results)
    _print_result(f"Concurrency ({NUM_CLIENTS}×{NUM_MSGS})", total, elapsed)

    failures = sum(1 for r in results if r != NUM_MSGS)
    assert failures == 0, f"{failures} clients missed messages"


# ---------------------------------------------------------------------------
# Inbound mapping throughput (MQTT → PostgreSQL table writes)
# ---------------------------------------------------------------------------

def _setup_inbound_perf():
    """Create target table and inbound mapping for perf tests."""
    run_sql("DROP TABLE IF EXISTS perf_inbound CASCADE;")
    run_sql(
        "CREATE TABLE perf_inbound ("
        "  site_id text NOT NULL, sensor_id text NOT NULL, value numeric,"
        "  PRIMARY KEY (site_id, sensor_id)"
        ");"
    )
    run_sql(
        "SELECT pgmqtt_add_inbound_mapping("
        "  'perf/{site_id}/data/{sensor_id}',"
        "  'perf_inbound',"
        "  '{\"site_id\": \"{site_id}\", \"sensor_id\": \"{sensor_id}\", \"value\": \"$.value\"}'::jsonb,"
        "  'upsert',"
        "  ARRAY['site_id', 'sensor_id'],"
        "  'public',"
        "  'perf_inbound'"
        ");"
    )
    time.sleep(2)  # Wait for mapping reload (per-tick, ~80ms)


@pytest.mark.slow
def test_inbound_qos0_throughput():
    """Throughput: 5000 QoS 0 inbound writes (MQTT → DB)."""
    NUM_INBOUND = 5000
    _setup_inbound_perf()
    run_sql("TRUNCATE perf_inbound;")

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("perf_inbound_q0", clean_start=True))
    recv_packet(s)

    start = time.time()
    for i in range(NUM_INBOUND):
        payload = json.dumps({"value": i * 0.1}).encode()
        s.sendall(create_publish_packet(
            f"perf/site-A/data/s{i}", payload, qos=0,
        ))
        # Brief yield every 200 msgs to let BGW drain the socket buffer
        if (i + 1) % 200 == 0:
            time.sleep(0.1)
    s.close()

    # Drain — wait for final batches to flush
    time.sleep(3)
    elapsed = time.time() - start

    rows = run_sql("SELECT COUNT(*) FROM perf_inbound;")
    count = rows[0][0] if rows else 0
    _print_result("Inbound QoS 0 (5k)", count, elapsed)
    assert count >= NUM_INBOUND * 0.95, f"Only {count}/{NUM_INBOUND} rows written"


@pytest.mark.slow
def test_inbound_qos1_throughput():
    """Throughput: 200 QoS 1 inbound writes with PUBACK."""
    _setup_inbound_perf()
    run_sql("TRUNCATE perf_inbound;")

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("perf_inbound_q1", clean_start=True))
    recv_packet(s)

    start = time.time()
    for i in range(NUM_MESSAGES):
        payload = json.dumps({"value": i * 0.1}).encode()
        s.sendall(create_publish_packet(
            f"perf/site-B/data/s{i}", payload, qos=1, packet_id=i + 1,
        ))

    # Collect all PUBACKs
    acks = 0
    while acks < NUM_MESSAGES:
        pkt = recv_packet(s, timeout=10.0)
        if not pkt:
            break
        if pkt[0] >> 4 == MQTTControlPacket.PUBACK:
            acks += 1

    elapsed = time.time() - start
    s.close()

    _print_result("Inbound QoS 1 (PUBACK)", acks, elapsed)
    assert acks == NUM_MESSAGES, f"Only {acks}/{NUM_MESSAGES} PUBACKs received"

    # Virtual subscriber processes pending writes asynchronously (batch of 50/tick).
    # Wait for the queue to drain.
    count = 0
    for _ in range(60):
        rows = run_sql("SELECT COUNT(*) FROM perf_inbound;")
        count = rows[0][0] if rows else 0
        if count >= NUM_MESSAGES:
            break
        time.sleep(0.5)
    assert count == NUM_MESSAGES, f"Only {count}/{NUM_MESSAGES} rows written"


@pytest.mark.slow
def test_inbound_no_match_overhead():
    """Overhead: 200 publishes to non-matching topics with inbound mappings active."""
    _setup_inbound_perf()

    # Subscriber to receive the messages
    s_sub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_sub.connect((MQTT_HOST, MQTT_PORT))
    s_sub.sendall(create_connect_packet("perf_nomatch_sub", clean_start=True))
    recv_packet(s_sub)
    s_sub.sendall(create_subscribe_packet(1, "other/topic", qos=0))
    recv_packet(s_sub)

    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("perf_nomatch_pub", clean_start=True))
    recv_packet(s_pub)

    start = time.time()
    for _ in range(NUM_MESSAGES):
        s_pub.sendall(create_publish_packet("other/topic", b"payload", qos=0))
    s_pub.close()

    received = 0
    while received < NUM_MESSAGES:
        pkt = recv_packet(s_sub, timeout=2.0)
        if not pkt:
            break
        received += 1

    elapsed = time.time() - start
    _print_result("No-match overhead", received, elapsed)
    assert received >= NUM_MESSAGES * 0.95
    s_sub.close()


@pytest.mark.slow
def test_inbound_coexistence_throughput():
    """Throughput: 200 messages that match inbound mapping + have MQTT subscriber."""
    _setup_inbound_perf()
    run_sql("TRUNCATE perf_inbound;")

    # Subscriber on matching topic
    s_sub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_sub.connect((MQTT_HOST, MQTT_PORT))
    s_sub.sendall(create_connect_packet("perf_coex_sub", clean_start=True))
    recv_packet(s_sub)
    s_sub.sendall(create_subscribe_packet(1, "perf/+/data/+", qos=0))
    recv_packet(s_sub)

    results = {"count": 0}

    def sub_thread():
        count = 0
        while count < NUM_MESSAGES:
            pkt = recv_packet(s_sub, timeout=10.0)
            if not pkt:
                break
            if pkt[0] >> 4 == MQTTControlPacket.PUBLISH:
                count += 1
        results["count"] = count

    t = threading.Thread(target=sub_thread)
    t.start()

    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("perf_coex_pub", clean_start=True))
    recv_packet(s_pub)

    start = time.time()
    for i in range(NUM_MESSAGES):
        payload = json.dumps({"value": i}).encode()
        s_pub.sendall(create_publish_packet(
            f"perf/site-C/data/s{i}", payload, qos=0,
        ))
    s_pub.close()

    t.join()
    elapsed = time.time() - start

    time.sleep(3)
    rows = run_sql("SELECT COUNT(*) FROM perf_inbound;")
    db_count = rows[0][0] if rows else 0

    _print_result("Coexistence (sub+DB)", results["count"], elapsed)
    assert results["count"] >= NUM_MESSAGES * 0.95, \
        f"Subscriber got {results['count']}/{NUM_MESSAGES}"
    assert db_count >= NUM_MESSAGES * 0.95, \
        f"Only {db_count}/{NUM_MESSAGES} rows written"
    s_sub.close()
