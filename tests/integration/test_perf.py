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

TOPIC = "perf/test"

# Saturation-test sizes. Chosen so each run exceeds the broker's per-tick batch
# latency (~10-50ms) by >10x — otherwise elapsed time is dominated by connect
# setup and the measured rate is noise rather than a real ceiling.
#
# Ceilings observed on the profiling sweep (2026-04, 4p4s @ 128B):
#   QoS0 sub fan-out : ~11.7k msg/s  (CPU-bound in flush_outbox write_all)
#   QoS1 pipelined   : ~2.8k msg/s   (WAL-bound: fdatasync per tick commit)
#   CDC fan-out      : ~15.4k msg/s  (matches INSERT rate × n_subs)
QOS0_SAT_N = 5000      # ~450ms @ 11k/s
QOS1_SAT_N = 2000      # ~700ms @ 2.8k/s
QOS0_FANOUT_SUBS = 4   # stresses flush_outbox serial write loop
QOS1_MULTI_PUB = 4     # exercises WAL contention on the inline INSERT
CDC_SAT_N = 5000       # CDC fan-out: ~330ms @ 15k/s
INBOUND_SAT_N = 2000   # inbound QoS 1 / coexistence
NOMATCH_SAT_N = 5000   # pure no-match-lookup overhead


def _print_result(name, count, duration):
    rate = count / duration if duration > 0 else 0
    print(f"  PERF | {name:<30} | {count:>6} msgs | {duration:>6.2f}s | {rate:>8.1f} msg/s")


# ---------------------------------------------------------------------------
# Client pub/sub throughput
# ---------------------------------------------------------------------------

@pytest.mark.slow
def test_published_qos0_throughput():
    """Saturation: 1 pub → 1 sub, QoS 0, QOS0_SAT_N messages back-to-back.

    Measures the single-subscriber fan-out ceiling. The test is sized so
    elapsed time is dominated by broker work, not socket setup.
    """
    payload = b"x" * 128  # match profiling sweep payload size

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

    # Concurrent drain: subscriber must be consuming while publisher spams, or
    # the broker's flush_outbox write_all hits WouldBlock on a full sub kernel
    # recv buffer and disconnects the sub, silently truncating the test.
    result = {"count": 0}

    def drain():
        count = 0
        while count < QOS0_SAT_N:
            pkt = recv_packet(s_sub, timeout=5.0)
            if not pkt:
                break
            count += 1
        result["count"] = count

    t = threading.Thread(target=drain)
    t.start()

    start = time.time()
    for _ in range(QOS0_SAT_N):
        s_pub.sendall(create_publish_packet(TOPIC, payload, qos=0))

    t.join(timeout=30.0)
    elapsed = time.time() - start
    received = result["count"]
    _print_result("Published QoS 0 (saturation)", received, elapsed)
    s_pub.close()
    s_sub.close()
    # QoS 0 is at-most-once; minor drops under saturation are spec-compliant.
    assert received >= QOS0_SAT_N * 0.90, f"Excessive drops: {received}/{QOS0_SAT_N}"


@pytest.mark.slow
def test_published_qos0_fanout_throughput():
    """Saturation fan-out: 1 pub → N subs, QoS 0.

    Exercises flush_outbox's per-subscriber write loop. The bottleneck is the
    serial write_all over each subscriber's TCP send buffer, so aggregate
    sub throughput should scale ~N× until the broker event loop saturates.
    """
    payload = b"x" * 128
    n_subs = QOS0_FANOUT_SUBS

    subs = []
    for i in range(n_subs):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((MQTT_HOST, MQTT_PORT))
        s.sendall(create_connect_packet(f"perf_q0_fanout_sub_{i}", clean_start=True))
        recv_packet(s)
        s.sendall(create_subscribe_packet(1, TOPIC, qos=0))
        recv_packet(s)
        subs.append(s)

    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("perf_q0_fanout_pub", clean_start=True))
    recv_packet(s_pub)

    results = [0] * n_subs

    def sub_drain(idx, sock):
        count = 0
        while count < QOS0_SAT_N:
            pkt = recv_packet(sock, timeout=15.0)
            if not pkt:
                break
            count += 1
        results[idx] = count

    threads = [threading.Thread(target=sub_drain, args=(i, s)) for i, s in enumerate(subs)]
    for t in threads:
        t.start()

    start = time.time()
    for _ in range(QOS0_SAT_N):
        s_pub.sendall(create_publish_packet(TOPIC, payload, qos=0))

    for t in threads:
        t.join(timeout=30.0)
    elapsed = time.time() - start

    total = sum(results)
    expected = QOS0_SAT_N * n_subs
    _print_result(f"QoS 0 fan-out ({n_subs} subs)", total, elapsed)
    s_pub.close()
    for s in subs:
        s.close()
    assert total >= expected * 0.90, f"Excessive drops: {total}/{expected}"


@pytest.mark.slow
def test_published_qos1_pipelined_throughput():
    """Saturation: 1 pub (pipelined) → 1 sub, QoS 1.

    Pipelines all publishes before collecting PUBACKs so throughput is
    bounded by broker commit rate, not client round-trip.
    """
    payload = b"x" * 128
    n = QOS1_SAT_N

    s_sub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_sub.connect((MQTT_HOST, MQTT_PORT))
    s_sub.sendall(create_connect_packet("perf_q1p_sub", clean_start=True))
    recv_packet(s_sub)
    s_sub.sendall(create_subscribe_packet(1, TOPIC, qos=1))
    recv_packet(s_sub)

    results = {"count": 0}

    def sub_thread():
        count = 0
        while count < n:
            pkt = recv_packet(s_sub, timeout=15.0)
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

    # Concurrent PUBACK drain: without this, the publisher's kernel recv buffer
    # fills while pipelining, backpressuring the broker and capping throughput.
    ack_result = {"count": 0}

    def ack_drain():
        acks = 0
        while acks < n:
            pkt = recv_packet(s_pub, timeout=15.0)
            if not pkt:
                break
            acks += 1
        ack_result["count"] = acks

    t_ack = threading.Thread(target=ack_drain)
    t_ack.start()

    start = time.time()
    for i in range(n):
        s_pub.sendall(create_publish_packet(TOPIC, payload, qos=1, packet_id=(i % 65535) + 1))

    t_ack.join(timeout=30.0)
    acks = ack_result["count"]
    t.join(timeout=30.0)
    elapsed = time.time() - start
    _print_result("Published QoS 1 (pipelined)", results["count"], elapsed)
    s_pub.close()
    s_sub.close()
    assert acks == n, f"Only {acks}/{n} PUBACKs"
    assert results["count"] == n, f"Sub got {results['count']}/{n}"


@pytest.mark.slow
def test_published_qos1_multi_pub_throughput():
    """Saturation: N parallel pubs → 1 sub, QoS 1.

    Multiple publishers exercise WAL-commit contention on the inline
    persist_message INSERT. Each publisher pipelines a share of QOS1_SAT_N
    messages; aggregate ACK rate is the broker's sustained commit ceiling.
    """
    payload = b"x" * 128
    n_pubs = QOS1_MULTI_PUB
    per_pub = QOS1_SAT_N // n_pubs
    total = per_pub * n_pubs

    s_sub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_sub.connect((MQTT_HOST, MQTT_PORT))
    s_sub.sendall(create_connect_packet("perf_q1m_sub", clean_start=True))
    recv_packet(s_sub)
    s_sub.sendall(create_subscribe_packet(1, TOPIC, qos=1))
    recv_packet(s_sub)

    sub_result = {"count": 0}

    def sub_thread():
        count = 0
        while count < total:
            pkt = recv_packet(s_sub, timeout=20.0)
            if not pkt:
                break
            try:
                _, _, _, _, _, pid, _ = validate_publish(pkt)
                if pid:
                    s_sub.sendall(create_puback_packet(pid))
                count += 1
            except Exception:
                break
        sub_result["count"] = count

    ack_counts = [0] * n_pubs

    def pub_thread(idx):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((MQTT_HOST, MQTT_PORT))
        s.sendall(create_connect_packet(f"perf_q1m_pub_{idx}", clean_start=True))
        recv_packet(s)

        acks = [0]

        def ack_drain():
            count = 0
            while count < per_pub:
                pkt = recv_packet(s, timeout=20.0)
                if not pkt:
                    break
                count += 1
            acks[0] = count

        t_ack = threading.Thread(target=ack_drain)
        t_ack.start()

        for i in range(per_pub):
            s.sendall(create_publish_packet(TOPIC, payload, qos=1, packet_id=(i % 65535) + 1))

        t_ack.join(timeout=30.0)
        ack_counts[idx] = acks[0]
        s.close()

    t_sub = threading.Thread(target=sub_thread)
    t_sub.start()

    start = time.time()
    pubs = [threading.Thread(target=pub_thread, args=(i,)) for i in range(n_pubs)]
    for t in pubs:
        t.start()
    for t in pubs:
        t.join(timeout=60.0)
    t_sub.join(timeout=60.0)
    elapsed = time.time() - start

    acks_total = sum(ack_counts)
    _print_result(f"QoS 1 multi-pub ({n_pubs} pubs)", acks_total, elapsed)
    s_sub.close()
    assert acks_total == total, f"Only {acks_total}/{total} PUBACKs across pubs"
    assert sub_result["count"] == total, f"Sub got {sub_result['count']}/{total}"


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
    """Saturation: CDC_SAT_N CDC QoS 0 messages."""
    _setup_perf_cdc(0)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("perf_cdc_q0", clean_start=True))
    recv_packet(s)
    s.sendall(create_subscribe_packet(1, TOPIC, qos=0))
    recv_packet(s)

    start = time.time()
    run_psql(f"INSERT INTO perf_table (data) SELECT 'msg-' || generate_series(1, {CDC_SAT_N});")

    received = 0
    while received < CDC_SAT_N:
        pkt = recv_packet(s, timeout=10.0)
        if not pkt:
            break
        received += 1

    elapsed = time.time() - start
    _print_result("CDC QoS 0", received, elapsed)
    assert received >= CDC_SAT_N * 0.95
    s.close()


@pytest.mark.slow
def test_cdc_qos1_throughput():
    """Saturation: CDC_SAT_N CDC QoS 1 messages."""
    _setup_perf_cdc(1)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("perf_cdc_q1", clean_start=True))
    recv_packet(s)
    s.sendall(create_subscribe_packet(1, TOPIC, qos=1))
    recv_packet(s)

    start = time.time()
    run_psql(f"INSERT INTO perf_table (data) SELECT 'msg-' || generate_series(1, {CDC_SAT_N});")

    received = 0
    while received < CDC_SAT_N:
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
    assert received == CDC_SAT_N
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
    last_ping = 0

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

        # Send a PINGREQ every 15 seconds to keep the connection alive.
        if now - last_ping >= 15:
            s.sendall(bytes([0xC0, 0x00]))  # PINGREQ
            last_ping = now

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
    """Create target table and inbound mapping for perf tests.

    Avoids DROP TABLE between test cases: the broker's process_inbound_pending
    reads a batch of rows into memory without row-level locking, so a DROP
    that races a batch read causes the broker's in-memory INSERT to fail on
    a missing relation and exits the BGW. TRUNCATE keeps the OID stable.
    """
    # Wait for any broker-queued rows to drain (previous test's residue).
    for _ in range(120):
        rows = run_sql("SELECT COUNT(*) FROM pgmqtt_inbound_pending;")
        if rows and rows[0][0] == 0:
            break
        time.sleep(0.5)
    run_sql("DELETE FROM pgmqtt_inbound_pending;")

    # Use CREATE IF NOT EXISTS + TRUNCATE so schema/OID persists across tests.
    run_sql(
        "CREATE TABLE IF NOT EXISTS perf_inbound ("
        "  site_id text NOT NULL, sensor_id text NOT NULL, value numeric,"
        "  PRIMARY KEY (site_id, sensor_id)"
        ");"
    )
    run_sql("TRUNCATE perf_inbound;")

    # The mapping is upserted by name, safe to re-add.
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
    s.close()

    # Drain — wait for final batches to flush
    time.sleep(3)
    elapsed = time.time() - start

    rows = run_sql("SELECT COUNT(*) FROM perf_inbound;")
    count = rows[0][0] if rows else 0
    _print_result("Inbound QoS 0 (5k)", count, elapsed)
    # QoS 0 is at-most-once; drops under saturation are spec-compliant.
    assert count >= NUM_INBOUND * 0.85, f"Only {count}/{NUM_INBOUND} rows written"


@pytest.mark.slow
def test_inbound_qos1_throughput():
    """Saturation: INBOUND_SAT_N QoS 1 inbound writes with PUBACK."""
    _setup_inbound_perf()
    run_sql("TRUNCATE perf_inbound;")

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("perf_inbound_q1", clean_start=True))
    recv_packet(s)

    # Concurrent PUBACK drain: without this, the publisher's kernel recv buffer
    # backpressures the broker partway through pipelining.
    ack_result = {"count": 0}

    def ack_drain():
        acks = 0
        while acks < INBOUND_SAT_N:
            pkt = recv_packet(s, timeout=15.0)
            if not pkt:
                break
            if pkt[0] >> 4 == MQTTControlPacket.PUBACK:
                acks += 1
        ack_result["count"] = acks

    t_ack = threading.Thread(target=ack_drain)
    t_ack.start()

    start = time.time()
    for i in range(INBOUND_SAT_N):
        payload = json.dumps({"value": i * 0.1}).encode()
        s.sendall(create_publish_packet(
            f"perf/site-B/data/s{i}", payload, qos=1, packet_id=(i % 65535) + 1,
        ))

    t_ack.join(timeout=30.0)
    acks = ack_result["count"]
    elapsed = time.time() - start
    s.close()

    _print_result("Inbound QoS 1 (PUBACK)", acks, elapsed)
    assert acks == INBOUND_SAT_N, f"Only {acks}/{INBOUND_SAT_N} PUBACKs received"

    # Virtual subscriber processes pending writes asynchronously (batch of 50/tick).
    # Wait for the queue to drain.
    count = 0
    for _ in range(120):
        rows = run_sql("SELECT COUNT(*) FROM perf_inbound;")
        count = rows[0][0] if rows else 0
        if count >= INBOUND_SAT_N:
            break
        time.sleep(0.5)
    assert count == INBOUND_SAT_N, f"Only {count}/{INBOUND_SAT_N} rows written"


@pytest.mark.slow
def test_inbound_no_match_overhead():
    """Saturation: NOMATCH_SAT_N publishes that lookup (but don't match) inbound mappings."""
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

    # Concurrent drain to keep the sub's recv buffer from filling and triggering
    # a broker-side WouldBlock disconnect mid-test.
    result = {"count": 0}

    def drain():
        count = 0
        while count < NOMATCH_SAT_N:
            pkt = recv_packet(s_sub, timeout=5.0)
            if not pkt:
                break
            count += 1
        result["count"] = count

    t = threading.Thread(target=drain)
    t.start()

    start = time.time()
    for _ in range(NOMATCH_SAT_N):
        s_pub.sendall(create_publish_packet("other/topic", b"payload", qos=0))

    t.join(timeout=30.0)
    elapsed = time.time() - start
    received = result["count"]
    _print_result("No-match overhead", received, elapsed)
    s_pub.close()
    s_sub.close()
    assert received >= NOMATCH_SAT_N * 0.90


@pytest.mark.slow
def test_inbound_coexistence_throughput():
    """Saturation: INBOUND_SAT_N messages matching inbound mapping + MQTT subscriber."""
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
        while count < INBOUND_SAT_N:
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
    for i in range(INBOUND_SAT_N):
        payload = json.dumps({"value": i}).encode()
        s_pub.sendall(create_publish_packet(
            f"perf/site-C/data/s{i}", payload, qos=0,
        ))
    s_pub.close()

    t.join(timeout=30.0)
    elapsed = time.time() - start

    # Wait for async inbound row writes to drain.
    db_count = 0
    for _ in range(60):
        rows = run_sql("SELECT COUNT(*) FROM perf_inbound;")
        db_count = rows[0][0] if rows else 0
        if db_count >= INBOUND_SAT_N * 0.95:
            break
        time.sleep(0.5)

    _print_result("Coexistence (sub+DB)", results["count"], elapsed)
    assert results["count"] >= INBOUND_SAT_N * 0.90, \
        f"Subscriber got {results['count']}/{INBOUND_SAT_N}"
    assert db_count >= INBOUND_SAT_N * 0.90, \
        f"Only {db_count}/{INBOUND_SAT_N} rows written"
    s_sub.close()
