"""
Enterprise multiprocess topology tests (`multiprocess` license feature).

Unlike the rest of the enterprise suite, these tests manage their own
containers: process topology is decided once at _PG_init from the license
key present at postmaster start, so it cannot be toggled with ALTER SYSTEM
on the already-running broker the other tests target. Each fixture boots
the built image (pgmqtt-enterprise-postgres) with the license passed on the
postgres command line, on ports chosen to avoid the default test broker.
"""

import os
import socket
import subprocess
import sys
import time

import psycopg2
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from helpers.license import generate_test_license, signing_key_available  # noqa: E402

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "integration"))
from proto_utils import (  # noqa: E402
    MQTTControlPacket,
    create_connect_packet,
    create_disconnect_packet,
    create_puback_packet,
    create_publish_packet,
    create_subscribe_packet,
    recv_packet,
    validate_connack,
    validate_puback,
    validate_publish,
    validate_suback,
)

IMAGE = "pgmqtt-enterprise-postgres"

pytestmark = pytest.mark.skipif(
    not signing_key_available(),
    reason="PGMQTT_TEST_SIGNING_KEY not set",
)


def _image_available() -> bool:
    try:
        subprocess.check_output(
            ["docker", "image", "inspect", IMAGE], stderr=subprocess.STDOUT
        )
        return True
    except Exception:
        return False


class Broker:
    """A dedicated pgmqtt container with known host ports."""

    def __init__(
        self,
        name: str,
        pg_port: int,
        mqtt_port: int,
        license_token: str | None,
        extra_conf: list | None = None,
        auto_remove: bool = True,
    ):
        self.name = name
        self.pg_port = pg_port
        self.mqtt_port = mqtt_port
        self.license_token = license_token
        self.extra_conf = extra_conf or []
        self.auto_remove = auto_remove

    def start(self):
        subprocess.call(
            ["docker", "rm", "-f", self.name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        cmd = ["docker", "run", "-d"]
        if self.auto_remove:
            cmd.append("--rm")
        cmd += [
            "--name", self.name,
            "-e", "POSTGRES_PASSWORD=postgres",
            "-p", f"127.0.0.1:{self.pg_port}:5432",
            "-p", f"127.0.0.1:{self.mqtt_port}:1883",
            IMAGE,
            "postgres",
        ]
        if self.license_token is not None:
            # Must be on the command line: _PG_init reads the license before
            # any SQL session exists, so ALTER SYSTEM after boot is too late
            # to affect worker registration.
            cmd += ["-c", f"pgmqtt.license_key={self.license_token}"]
        for conf in self.extra_conf:
            cmd += ["-c", conf]
        subprocess.check_call(cmd)
        self._wait_pg()
        self._wait_mqtt()

    def stop(self):
        subprocess.call(
            ["docker", "rm", "-f", self.name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def restart(self):
        """Full postmaster restart, keeping the data directory.
        Requires auto_remove=False (a --rm container may be reaped on stop)."""
        subprocess.check_call(
            ["docker", "restart", self.name],
            stdout=subprocess.DEVNULL,
        )
        self._wait_pg()
        self._wait_mqtt()

    def logs(self) -> str:
        try:
            return subprocess.check_output(
                ["docker", "logs", self.name], stderr=subprocess.STDOUT
            ).decode(errors="replace")
        except Exception as e:
            return f"<no logs: {e}>"

    def sql(self, query):
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=self.pg_port,
            user="postgres",
            password="postgres",
            dbname="postgres",
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                if cur.description:
                    return cur.fetchall()
                return None
        finally:
            conn.close()

    def _wait_pg(self, timeout=90):
        deadline = time.time() + timeout
        last_err = None
        while time.time() < deadline:
            try:
                self.sql("SELECT 1")
                return
            except Exception as e:
                last_err = e
                time.sleep(1)
        raise AssertionError(
            f"{self.name}: postgres never became ready: {last_err}\n{self.logs()}"
        )

    def _wait_mqtt(self, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                s = self.connect_mqtt(f"probe-{int(time.time())}")
                s.close()
                return
            except Exception:
                time.sleep(1)
        raise AssertionError(
            f"{self.name}: MQTT listener never became ready\n{self.logs()}"
        )

    def connect_mqtt(
        self, client_id: str, clean_start: bool = True, properties: dict | None = None
    ) -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        s.connect(("127.0.0.1", self.mqtt_port))
        s.sendall(
            create_connect_packet(client_id, clean_start=clean_start, properties=properties)
        )
        connack = recv_packet(s)
        assert connack is not None, "no CONNACK"
        validate_connack(connack)
        return s

    def worker_types(self) -> set:
        rows = self.sql(
            "SELECT backend_type FROM pg_stat_activity WHERE backend_type LIKE 'pgmqtt%'"
        )
        return {r[0] for r in rows}


def _subscribe(s: socket.socket, packet_id: int, topic_filter: str, qos: int):
    s.sendall(create_subscribe_packet(packet_id, topic_filter, qos=qos))
    suback = recv_packet(s)
    assert suback is not None, "no SUBACK"
    validate_suback(suback, packet_id)


def _collect_publishes(s: socket.socket, expect: int, timeout: float = 30.0):
    """Receive PUBLISH packets (PUBACKing QoS 1) until `expect` arrive or timeout."""
    got = []
    deadline = time.time() + timeout
    while len(got) < expect and time.time() < deadline:
        packet = recv_packet(s, timeout=2.0)
        if packet is None:
            continue
        ptype = (packet[0] & 0xF0) >> 4
        if ptype != MQTTControlPacket.PUBLISH:
            continue
        topic, payload, qos, _dup, _retain, packet_id, _props = validate_publish(packet)
        if qos == 1:
            s.sendall(create_puback_packet(packet_id))
        got.append((topic, bytes(payload), qos))
    return got


def _setup_cdc_fixture_table(broker: Broker):
    """Table with one QoS 1 and one QoS 0 mapping on distinct topic roots."""
    broker.sql("DROP TABLE IF EXISTS mp_events")
    broker.sql("CREATE TABLE mp_events (id serial PRIMARY KEY, name text, val text)")
    broker.sql("ALTER TABLE mp_events REPLICA IDENTITY FULL")
    broker.sql(
        "SELECT pgmqtt_add_outbound_mapping('public', 'mp_events', "
        "'mp/q1/{{ columns.name }}', '{{ columns.val }}', 1, 'q1')"
    )
    broker.sql(
        "SELECT pgmqtt_add_outbound_mapping('public', 'mp_events', "
        "'mp/q0/{{ columns.name }}', '{{ columns.val }}', 0, 'q0')"
    )
    # Mapping changes propagate to the CDC worker's cache through WAL.
    time.sleep(6)


@pytest.fixture(scope="module")
def enterprise_broker():
    if not _image_available():
        pytest.skip(f"docker image {IMAGE} not available")
    token = generate_test_license(
        customer="mp-test", days=1, features=["multiprocess", "metrics"]
    )
    broker = Broker("pgmqtt-test-multiprocess", 15499, 11899, token)
    broker.start()
    yield broker
    broker.stop()


@pytest.fixture(scope="module")
def community_broker():
    if not _image_available():
        pytest.skip(f"docker image {IMAGE} not available")
    broker = Broker("pgmqtt-test-community-boot", 15498, 11898, None)
    broker.start()
    yield broker
    broker.stop()


def test_multiprocess_registers_two_workers(enterprise_broker):
    workers = enterprise_broker.worker_types()
    assert "pgmqtt_mqtt" in workers, f"pgmqtt_mqtt worker missing: {workers}"
    assert "pgmqtt_cdc" in workers, f"pgmqtt_cdc worker missing: {workers}"

    # Each worker session tags its WAL with a replication origin so the
    # decoder can skip it (filter_by_origin_cb).
    origins = {r[0] for r in enterprise_broker.sql("SELECT roname FROM pg_replication_origin")}
    assert {"pgmqtt_mqtt", "pgmqtt_cdc"} <= origins, origins


def test_multiprocess_license_active(enterprise_broker):
    rows = enterprise_broker.sql("SELECT status, features FROM pgmqtt_license_status()")
    status, features = rows[0]
    assert status == "active", rows
    assert "multiprocess" in features, rows


def test_multiprocess_cdc_delivery_both_qos(enterprise_broker):
    """End-to-end across the process boundary: QoS 1 via the outbox, QoS 0
    via the shared-memory inline ring."""
    _setup_cdc_fixture_table(enterprise_broker)

    sub = enterprise_broker.connect_mqtt("mp-sub")
    try:
        _subscribe(sub, 1, "mp/#", qos=1)
        enterprise_broker.sql(
            "INSERT INTO mp_events (name, val) VALUES ('alpha', 'small-payload')"
        )
        got = _collect_publishes(sub, expect=2)
    finally:
        sub.close()

    topics = {t for (t, _p, _q) in got}
    assert topics == {"mp/q1/alpha", "mp/q0/alpha"}, got
    for topic, payload, qos in got:
        assert payload == b"small-payload", got
        if topic.startswith("mp/q1/"):
            assert qos == 1, got

    # The delivered QoS 1 id must be dequeued from the outbox (same-tick
    # transaction); poll briefly for the commit.
    deadline = time.time() + 10
    remaining = None
    while time.time() < deadline:
        remaining = enterprise_broker.sql("SELECT count(*) FROM pgmqtt_cdc_outbox")[0][0]
        if remaining == 0:
            break
        time.sleep(0.5)
    assert remaining == 0, f"pgmqtt_cdc_outbox not drained: {remaining} rows left"


def test_multiprocess_oversize_qos0_spills_to_outbox(enterprise_broker):
    """A QoS 0 payload larger than the inline ring's 1 KB slot cap must be
    delivered through the durable outbox path, not dropped."""
    _setup_cdc_fixture_table(enterprise_broker)
    big = "x" * 3000

    sub = enterprise_broker.connect_mqtt("mp-sub-oversize")
    try:
        _subscribe(sub, 1, "mp/q0/#", qos=0)
        enterprise_broker.sql(
            f"INSERT INTO mp_events (name, val) VALUES ('big', '{big}')"
        )
        got = _collect_publishes(sub, expect=1)
    finally:
        sub.close()

    assert len(got) == 1, f"oversize QoS 0 message was not delivered: {got}"
    topic, payload, qos = got[0]
    assert topic == "mp/q0/big"
    assert qos == 0
    assert payload == big.encode()

    # The spilled row is reclaimed after its single delivery attempt: both
    # the outbox id and the message row itself.
    deadline = time.time() + 10
    counts = None
    while time.time() < deadline:
        counts = (
            enterprise_broker.sql("SELECT count(*) FROM pgmqtt_cdc_outbox")[0][0],
            enterprise_broker.sql(
                f"SELECT count(*) FROM pgmqtt_messages WHERE octet_length(payload) = {len(big)}"
            )[0][0],
        )
        if counts == (0, 0):
            break
        time.sleep(0.5)
    assert counts == (0, 0), f"spilled QoS 0 row not reclaimed: {counts}"


def test_multiprocess_single_transaction_burst_lossless(enterprise_broker):
    """One bulk transaction bigger than any batch/buffer: logical decoding
    replays it as a single atomic unit, so the handoff sees the whole burst
    at once. Every QoS 1 message must arrive — the outbox has no capacity
    to overflow, unlike a fixed-size ring."""
    _setup_cdc_fixture_table(enterprise_broker)
    n = 5000

    sub = enterprise_broker.connect_mqtt("mp-sub-burst")
    try:
        _subscribe(sub, 1, "mp/q1/#", qos=1)
        enterprise_broker.sql(
            f"INSERT INTO mp_events (name, val) "
            f"SELECT 'burst', i::text FROM generate_series(1, {n}) i"
        )
        got = _collect_publishes(sub, expect=n, timeout=120.0)
    finally:
        sub.close()

    assert len(got) == n, f"lost {n - len(got)} of {n} QoS 1 messages"
    payloads = {p for (_t, p, _q) in got}
    assert payloads == {str(i).encode() for i in range(1, n + 1)}

    deadline = time.time() + 15
    remaining = None
    while time.time() < deadline:
        remaining = enterprise_broker.sql("SELECT count(*) FROM pgmqtt_cdc_outbox")[0][0]
        if remaining == 0:
            break
        time.sleep(0.5)
    assert remaining == 0, f"pgmqtt_cdc_outbox not drained: {remaining} rows left"


def test_multiprocess_qos1_client_publish_deferred_puback(enterprise_broker):
    """Client QoS 1 publishes commit asynchronously; the PUBACK is deferred
    until the WAL flush covers the commit (forced by the CDC worker's
    beacon when idle). The publisher must still get its PUBACK promptly and
    the subscriber must receive the message."""
    sub = enterprise_broker.connect_mqtt("mp-c2c-sub")
    pub = enterprise_broker.connect_mqtt("mp-c2c-pub")
    try:
        _subscribe(sub, 1, "mpc2c/#", qos=1)
        pub.sendall(
            create_publish_packet("mpc2c/data", b"deferred-ack", qos=1, packet_id=77)
        )
        puback = recv_packet(pub, timeout=10.0)
        assert puback is not None, "no PUBACK for deferred QoS 1 publish"
        validate_puback(puback, 77)

        got = _collect_publishes(sub, expect=1)
    finally:
        sub.close()
        pub.close()

    assert len(got) == 1, f"subscriber did not receive the publish: {got}"
    topic, payload, qos = got[0]
    assert (topic, payload, qos) == ("mpc2c/data", b"deferred-ack", 1)


def test_multiprocess_inbound_pump_runs_in_cdc_worker(enterprise_broker):
    """QoS 1 inbound-mapped publishes: the PUBACK reflects the durably
    committed pending row (deferred on the flush watermark), and the
    pgmqtt_cdc worker — not the socket worker — pumps the row into the
    target table."""
    enterprise_broker.sql("DROP TABLE IF EXISTS mp_inbound")
    enterprise_broker.sql(
        "CREATE TABLE mp_inbound (id serial PRIMARY KEY, device text, temperature numeric)"
    )
    enterprise_broker.sql(
        """
        SELECT pgmqtt_add_inbound_mapping(
            'mpin/{device}/temp',
            'mp_inbound',
            '{"device": "{device}", "temperature": "$.temperature"}'::jsonb,
            'insert',
            NULL,
            'public',
            'mp_inbound_test'
        )
        """
    )
    # Both workers refresh their inbound mapping caches on a ~500 ms cadence.
    time.sleep(2)

    pub = enterprise_broker.connect_mqtt("mp-inbound-pub")
    try:
        pub.sendall(
            create_publish_packet(
                "mpin/dev42/temp", b'{"temperature": 21.5}', qos=1, packet_id=88
            )
        )
        puback = recv_packet(pub, timeout=10.0)
        assert puback is not None, "no PUBACK for inbound-mapped QoS 1 publish"
        validate_puback(puback, 88)
    finally:
        pub.close()

    deadline = time.time() + 15
    rows = []
    while time.time() < deadline:
        rows = enterprise_broker.sql(
            "SELECT device, temperature::text FROM mp_inbound"
        )
        if rows:
            break
        time.sleep(0.5)
    assert rows == [("dev42", "21.5")], f"inbound row not pumped: {rows}"

    # The pending row is consumed and the backing message reclaimed.
    deadline = time.time() + 10
    pending = None
    while time.time() < deadline:
        pending = enterprise_broker.sql("SELECT count(*) FROM pgmqtt_inbound_pending")[0][0]
        if pending == 0:
            break
        time.sleep(0.5)
    assert pending == 0, f"pgmqtt_inbound_pending not drained: {pending}"


@pytest.fixture(scope="module")
def multi_broker():
    """Enterprise broker with two socket workers (SO_REUSEPORT sharding)."""
    if not _image_available():
        pytest.skip(f"docker image {IMAGE} not available")
    token = generate_test_license(
        customer="mw-test", days=1, features=["multiprocess", "metrics"]
    )
    broker = Broker(
        "pgmqtt-test-multiworker",
        15495,
        11895,
        token,
        extra_conf=["pgmqtt.socket_workers=2"],
    )
    broker.start()
    yield broker
    broker.stop()


def test_multiworker_topology(multi_broker):
    workers = multi_broker.worker_types()
    assert {"pgmqtt_mqtt", "pgmqtt_mqtt_1", "pgmqtt_cdc"} <= workers, workers

    origins = {r[0] for r in multi_broker.sql("SELECT roname FROM pg_replication_origin")}
    assert {"pgmqtt_mqtt", "pgmqtt_mqtt_1", "pgmqtt_cdc"} <= origins, origins

    cursors = multi_broker.sql(
        "SELECT worker_slot FROM pgmqtt_outbox_cursors ORDER BY worker_slot"
    )
    assert [r[0] for r in cursors] == [0, 1], cursors


def test_multiworker_cross_worker_pubsub(multi_broker):
    """Publishes must reach subscribers regardless of which worker the
    kernel assigned each connection to: everything routes through the
    shared outbox. Several connections make it overwhelmingly likely both
    workers hold some of them."""
    subs = []
    pubs = []
    try:
        for i in range(3):
            s = multi_broker.connect_mqtt(f"xw-sub-{i}")
            _subscribe(s, 1, "xw/#", qos=1)
            subs.append(s)
        for i in range(4):
            pubs.append(multi_broker.connect_mqtt(f"xw-pub-{i}"))

        expected = set()
        for i, pub in enumerate(pubs):
            pub.sendall(
                create_publish_packet(f"xw/q1/{i}", f"m{i}".encode(), qos=1, packet_id=10 + i)
            )
            expected.add((f"xw/q1/{i}", f"m{i}".encode(), 1))
            pub.sendall(create_publish_packet(f"xw/q0/{i}", f"z{i}".encode(), qos=0))
            expected.add((f"xw/q0/{i}", f"z{i}".encode(), 0))

        for i, pub in enumerate(pubs):
            puback = recv_packet(pub, timeout=15.0)
            assert puback is not None, f"publisher {i}: no PUBACK"
            validate_puback(puback, 10 + i)

        for i, sub in enumerate(subs):
            got = set(_collect_publishes(sub, expect=len(expected), timeout=30.0))
            assert got == expected, f"subscriber {i} missing: {expected - got}"
    finally:
        for s in subs + pubs:
            s.close()


def test_multiworker_isolated_pair_delivery(multi_broker):
    """One subscriber, one publisher, a topic nobody else subscribes to —
    repeated so kernel placement splits the pair across workers in some
    rounds. Regression for the 'no local subscribers' fast path silently
    dropping cross-worker publishes (PUBACK with no delivery)."""
    for i in range(6):
        sub = multi_broker.connect_mqtt(f"iso-sub-{i}")
        pub = multi_broker.connect_mqtt(f"iso-pub-{i}")
        try:
            _subscribe(sub, 1, f"iso/{i}", qos=1)
            pub.sendall(
                create_publish_packet(f"iso/{i}", f"p{i}".encode(), qos=1, packet_id=40 + i)
            )
            puback = recv_packet(pub, timeout=15.0)
            assert puback is not None, f"pair {i}: no PUBACK"
            validate_puback(puback, 40 + i)
            got = _collect_publishes(sub, expect=1, timeout=15.0)
            assert len(got) == 1 and got[0] == (f"iso/{i}", f"p{i}".encode(), 1), (
                f"pair {i}: publish not delivered (cross-worker drop): {got}"
            )
        finally:
            sub.close()
            pub.close()


def test_multiworker_cdc_delivery(multi_broker):
    _setup_cdc_fixture_table(multi_broker)
    sub = multi_broker.connect_mqtt("xw-cdc-sub")
    try:
        _subscribe(sub, 1, "mp/#", qos=1)
        multi_broker.sql("INSERT INTO mp_events (name, val) VALUES ('xw', 'multi-worker')")
        got = _collect_publishes(sub, expect=2)
    finally:
        sub.close()
    topics = {t for (t, _p, _q) in got}
    assert topics == {"mp/q1/xw", "mp/q0/xw"}, got


def test_multiworker_session_takeover(multi_broker):
    """A second CONNECT with the same client_id must disconnect the first
    connection even when the two land on different workers (broadcast
    kick). Placement is kernel-chosen, so this exercises the cross-worker
    path probabilistically and the in-process path otherwise — both must
    behave identically."""
    first = multi_broker.connect_mqtt("xw-takeover")
    second = multi_broker.connect_mqtt("xw-takeover")
    try:
        # The first connection should observe a DISCONNECT or EOF shortly.
        deadline = time.time() + 10
        closed = False
        while time.time() < deadline and not closed:
            p = recv_packet(first, timeout=2.0)
            if p is None:
                continue
            ptype = (p[0] & 0xF0) >> 4
            if ptype == MQTTControlPacket.DISCONNECT or len(p) == 0:
                closed = True
        if not closed:
            # EOF manifests as recv_packet returning None forever; probe by
            # checking the socket is actually closed.
            first.settimeout(2.0)
            try:
                closed = first.recv(1) == b""
            except (TimeoutError, OSError):
                closed = False
        assert closed, "old connection was not kicked on takeover"

        # The new connection must be fully functional.
        _subscribe(second, 1, "xwt/#", qos=1)
    finally:
        first.close()
        second.close()


def test_multiworker_outbox_gc(multi_broker):
    """After traffic quiesces, every worker's cursor passes the last row
    and the slot-0 GC empties the outbox."""
    deadline = time.time() + 30
    remaining = None
    while time.time() < deadline:
        remaining = multi_broker.sql("SELECT count(*) FROM pgmqtt_cdc_outbox")[0][0]
        if remaining == 0:
            break
        time.sleep(1)
    assert remaining == 0, f"outbox not garbage-collected: {remaining} rows left"


def _publish_qos1_acked(pub: socket.socket, topic: str, payload: bytes, packet_id: int):
    pub.sendall(create_publish_packet(topic, payload, qos=1, packet_id=packet_id))
    puback = recv_packet(pub, timeout=10.0)
    assert puback is not None, f"no PUBACK for packet {packet_id}"
    validate_puback(puback, packet_id)


def _drain_payload_set(s: socket.socket, want: set, timeout: float = 40.0) -> set:
    got = set()
    deadline = time.time() + timeout
    while got != want and time.time() < deadline:
        packet = recv_packet(s, timeout=2.0)
        if packet is None:
            continue
        if (packet[0] & 0xF0) >> 4 != MQTTControlPacket.PUBLISH:
            continue
        _t, payload, qos, _d, _r, pid, _p = validate_publish(packet)
        if qos == 1 and pid is not None:
            s.sendall(create_puback_packet(pid))
        got.add(bytes(payload))
    return got


def test_multiworker_offline_persistent_delivery(multi_broker):
    """A QoS 1 publish while a persistent subscriber is offline is queued and
    delivered when the subscriber reconnects, wherever either connection lands."""
    sub = multi_broker.connect_mqtt("mp-off-sub", properties={0x11: 3600})
    _subscribe(sub, 1, "mpoff/#", qos=1)
    sub.sendall(create_disconnect_packet())
    sub.close()
    time.sleep(0.5)

    want = {f"off-{i}".encode() for i in range(5)}
    pub = multi_broker.connect_mqtt("mp-off-pub")
    try:
        for i in range(5):
            _publish_qos1_acked(pub, "mpoff/data", f"off-{i}".encode(), i + 1)
    finally:
        pub.close()
    time.sleep(1)

    sub2 = multi_broker.connect_mqtt(
        "mp-off-sub", clean_start=False, properties={0x11: 3600}
    )
    try:
        got = _drain_payload_set(sub2, want)
    finally:
        sub2.close()
    assert got == want, f"missing queued messages after reconnect: {want - got}"


def test_multiworker_shared_subscription_exactly_once(multi_broker):
    """Each message published to a shared subscription reaches exactly one
    group member cluster-wide, even with members spread across workers."""
    members = []
    try:
        for i in range(12):
            m = multi_broker.connect_mqtt(f"mp-share-{i}")
            _subscribe(m, 1, "$share/g1/mpshare/t", qos=1)
            members.append(m)

        deadline = time.time() + 30
        slots = 0
        while time.time() < deadline:
            slots = multi_broker.sql(
                "SELECT count(DISTINCT worker_slot) FROM pgmqtt_connections_cache "
                "WHERE client_id LIKE 'mp-share-%'"
            )[0][0]
            if slots == 2:
                break
            time.sleep(1)
        if slots < 2:
            pytest.skip("kernel placed all group members on one worker")

        n = 20
        pub = multi_broker.connect_mqtt("mp-share-pub")
        try:
            for i in range(n):
                _publish_qos1_acked(pub, "mpshare/t", f"s-{i}".encode(), i + 1)
        finally:
            pub.close()

        # Map payload -> set of member indexes that received it. QoS 1
        # redelivery to the same member (slow PUBACK) is legal; the same
        # payload reaching two different members is the group violation.
        recipients: dict = {}
        quiet_passes = 0
        deadline = time.time() + 30
        while quiet_passes < 5 and time.time() < deadline:
            saw_any = False
            for idx, m in enumerate(members):
                packet = recv_packet(m, timeout=0.05)
                if packet is None:
                    continue
                if (packet[0] & 0xF0) >> 4 != MQTTControlPacket.PUBLISH:
                    continue
                saw_any = True
                _t, payload, qos, _d, _r, pid, _p = validate_publish(packet)
                if qos == 1 and pid is not None:
                    m.sendall(create_puback_packet(pid))
                recipients.setdefault(bytes(payload), set()).add(idx)
            quiet_passes = 0 if saw_any else quiet_passes + 1
    finally:
        for m in members:
            m.close()

    expected = {f"s-{i}".encode() for i in range(n)}
    split = {p: sorted(idxs) for p, idxs in recipients.items() if len(idxs) > 1}
    assert set(recipients) == expected and not split, (
        f"shared group delivery violated exactly-one-member: "
        f"delivered_to_multiple={split}, "
        f"missing={sorted(expected - set(recipients))}"
    )


def test_multiworker_puback_durable_across_restart():
    """Every QoS 1 publish that was PUBACKed before a graceful restart is
    delivered to a persistent subscriber afterwards."""
    if not _image_available():
        pytest.skip(f"docker image {IMAGE} not available")
    token = generate_test_license(
        customer="mw-durable", days=1, features=["multiprocess", "metrics"]
    )
    broker = Broker(
        "pgmqtt-test-durable",
        15493,
        11893,
        token,
        extra_conf=["pgmqtt.socket_workers=2"],
        auto_remove=False,
    )
    broker.start()
    try:
        sub = broker.connect_mqtt("mp-dur-sub", properties={0x11: 3600})
        _subscribe(sub, 1, "mpdur/#", qos=1)
        sub.sendall(create_disconnect_packet())
        sub.close()
        time.sleep(0.5)

        want = {f"dur-{i}".encode() for i in range(50)}
        pub = broker.connect_mqtt("mp-dur-pub")
        for i in range(50):
            _publish_qos1_acked(pub, "mpdur/data", f"dur-{i}".encode(), i + 1)

        broker.restart()
        pub.close()

        sub2 = broker.connect_mqtt(
            "mp-dur-sub", clean_start=False, properties={0x11: 3600}
        )
        try:
            got = _drain_payload_set(sub2, want)
        finally:
            sub2.close()
        assert got == want, (
            f"{len(want - got)} PUBACKed messages lost across graceful restart: "
            f"{sorted(want - got)[:5]}..."
        )
    finally:
        broker.stop()


def test_defunct_slot_rows_swept_on_startup():
    """Rows left behind by a socket_workers decrease are swept at startup.

    A defunct slot's outbox cursor would pin the slot-0 GC watermark forever
    (nothing else ever advances it); its connections_cache rows would linger
    as phantom connections. Simulate the leftovers of a larger previous
    topology, then restart the postmaster: slot 0's startup sweep must
    remove every row for slots >= the boot-time worker count while the live
    slots' rows survive.
    """
    if not _image_available():
        pytest.skip(f"docker image {IMAGE} not available")
    token = generate_test_license(
        customer="mw-sweep", days=1, features=["multiprocess", "metrics"]
    )
    broker = Broker(
        "pgmqtt-test-defunct-sweep",
        15494,
        11894,
        token,
        extra_conf=["pgmqtt.socket_workers=2"],
        auto_remove=False,
    )
    broker.start()
    try:
        broker.sql(
            "INSERT INTO pgmqtt_outbox_cursors (worker_slot, last_id) VALUES (7, 0) "
            "ON CONFLICT (worker_slot) DO UPDATE SET last_id = 0"
        )
        broker.sql(
            "INSERT INTO pgmqtt_connections_cache (client_id, worker_slot) "
            "VALUES ('ghost-from-slot-7', 7) "
            "ON CONFLICT (client_id) DO UPDATE SET worker_slot = 7"
        )

        broker.restart()

        deadline = time.time() + 30
        cursors = cache = None
        while time.time() < deadline:
            cursors = broker.sql(
                "SELECT count(*) FROM pgmqtt_outbox_cursors WHERE worker_slot >= 2"
            )[0][0]
            cache = broker.sql(
                "SELECT count(*) FROM pgmqtt_connections_cache WHERE worker_slot >= 2"
            )[0][0]
            if cursors == 0 and cache == 0:
                break
            time.sleep(1)
        assert cursors == 0, (
            "defunct outbox cursor row not swept (would pin the GC watermark)"
        )
        assert cache == 0, "defunct connections_cache rows not swept"

        # The live slots' cursor rows survive the sweep (intact or reseeded).
        deadline = time.time() + 30
        live = 0
        while time.time() < deadline:
            live = broker.sql(
                "SELECT count(*) FROM pgmqtt_outbox_cursors WHERE worker_slot IN (0, 1)"
            )[0][0]
            if live == 2:
                break
            time.sleep(1)
        assert live == 2, f"expected live cursor rows for slots 0 and 1, found {live}"
    finally:
        broker.stop()


def test_community_boot_single_worker(community_broker):
    """Without the license at boot, only the combined worker runs and CDC
    still delivers inline — the outbox stays untouched."""
    workers = community_broker.worker_types()
    assert "pgmqtt_mqtt" in workers, f"pgmqtt_mqtt worker missing: {workers}"
    assert "pgmqtt_cdc" not in workers, f"unexpected pgmqtt_cdc worker: {workers}"

    _setup_cdc_fixture_table(community_broker)
    sub = community_broker.connect_mqtt("mp-sub-community")
    try:
        _subscribe(sub, 1, "mp/#", qos=1)
        community_broker.sql(
            "INSERT INTO mp_events (name, val) VALUES ('beta', 'community-payload')"
        )
        got = _collect_publishes(sub, expect=2)
    finally:
        sub.close()

    topics = {t for (t, _p, _q) in got}
    assert topics == {"mp/q1/beta", "mp/q0/beta"}, got
    assert community_broker.sql("SELECT count(*) FROM pgmqtt_cdc_outbox")[0][0] == 0


# ---------------------------------------------------------------------------
# 2026-07 security-review regressions (multi-worker)
# ---------------------------------------------------------------------------


def test_multiworker_retained_clear_reaches_live_subscribers(multi_broker):
    """MQTT-3.3.1-6/7/10: an empty retained publish clears the retained
    value AND is forwarded to current subscribers like any publish. In the
    multi-worker topology delivery only travels through the outbox, so a
    QoS 0 clear (no message id) used to reach no live subscriber on any
    worker — they kept believing the retained value existed."""
    topic = "mpclear/t"
    pub = multi_broker.connect_mqtt("mp-clear-pub")
    sub = multi_broker.connect_mqtt("mp-clear-sub")
    try:
        pub.sendall(create_publish_packet(topic, b"v1", qos=0, retain=True))
        time.sleep(1)
        _subscribe(sub, 1, topic, qos=0)
        got = _collect_publishes(sub, expect=1, timeout=15.0)
        assert [(t, p) for (t, p, _q) in got] == [(topic, b"v1")], got

        pub.sendall(create_publish_packet(topic, b"", qos=0, retain=True))
        got = _collect_publishes(sub, expect=1, timeout=15.0)
        assert [(t, p) for (t, p, _q) in got] == [(topic, b"")], (
            f"live subscriber never saw the retained clear: {got}"
        )

        sub2 = multi_broker.connect_mqtt("mp-clear-sub2")
        try:
            _subscribe(sub2, 1, topic, qos=0)
            got2 = _collect_publishes(sub2, expect=1, timeout=3.0)
            assert got2 == [], f"retained message survived the clear: {got2}"
        finally:
            sub2.close()
    finally:
        pub.close()
        sub.close()


def test_multiworker_disconnected_sessions_expire_on_any_worker(multi_broker):
    """Session expiry must be database-authoritative: a session disconnected
    on a non-primary worker used to live only in that worker's process-local
    map, which the slot-0 sweeper never scanned — it never expired. Several
    clients make it overwhelmingly likely both workers own some of them."""
    ids = [f"mpexp-{i}" for i in range(6)]
    for cid in ids:
        s = multi_broker.connect_mqtt(cid, properties={0x11: 3})
        s.sendall(create_disconnect_packet())
        s.close()

    # Session rows commit with the end-of-tick action batch; wait for all 6.
    deadline = time.time() + 15
    rows = 0
    while time.time() < deadline:
        rows = multi_broker.sql(
            "SELECT count(*) FROM pgmqtt_sessions WHERE client_id LIKE 'mpexp-%'"
        )[0][0]
        if rows == len(ids):
            break
        time.sleep(0.5)
    assert rows == len(ids), f"expected {len(ids)} session rows, found {rows}"

    # 3 s expiry + 500 ms sweep cadence + commit slack.
    deadline = time.time() + 25
    remaining = None
    while time.time() < deadline:
        remaining = multi_broker.sql(
            "SELECT count(*) FROM pgmqtt_sessions WHERE client_id LIKE 'mpexp-%'"
        )[0][0]
        if remaining == 0:
            break
        time.sleep(1)
    assert remaining == 0, (
        f"{remaining} expired sessions never reaped — sessions disconnected on "
        f"non-primary workers are invisible to a process-local sweep"
    )


def test_multiworker_concurrent_qos1_publishers_no_loss(multi_broker):
    """Concurrent publishers commit their outbox batches in arbitrary order
    while ids are assigned in allocation order; without the enqueue-floor
    barrier a delivery cursor can pass an id whose transaction commits a
    moment later, and that message is never delivered despite its PUBACK.
    Every payload must reach the subscriber (duplicates allowed — QoS 1)."""
    import threading

    n_pubs, n_msgs = 4, 50
    sub = multi_broker.connect_mqtt("mpfloor-sub")
    errors = []

    def publisher(t):
        try:
            pub = multi_broker.connect_mqtt(f"mpfloor-pub-{t}")
            try:
                for i in range(n_msgs):
                    _publish_qos1_acked(pub, "mpfloor/t", f"fl-{t}-{i}".encode(), i + 1)
            finally:
                pub.close()
        except Exception as e:  # noqa: BLE001 — surface in the main thread
            errors.append(f"publisher {t}: {e}")

    want = {f"fl-{t}-{i}".encode() for t in range(n_pubs) for i in range(n_msgs)}
    try:
        _subscribe(sub, 1, "mpfloor/#", qos=1)
        threads = [threading.Thread(target=publisher, args=(t,)) for t in range(n_pubs)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=120)
        assert not errors, errors
        got = _drain_payload_set(sub, want, timeout=60.0)
    finally:
        sub.close()
    missing = want - got
    assert not missing, (
        f"lost {len(missing)} PUBACKed QoS 1 messages "
        f"(cursor passed an uncommitted id?): {sorted(missing)[:10]}"
    )
