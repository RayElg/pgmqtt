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
