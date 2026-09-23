"""
Worker writes appear in pg_stat_user_tables while the workers run, which autovacuum
depends on.
"""
import socket
import time

from proto_utils import (
    create_connect_packet,
    create_subscribe_packet,
    create_publish_packet,
    create_puback_packet,
    recv_packet,
    validate_publish,
    validate_connack,
    validate_suback,
    run_psql,
    MQTT_HOST,
    MQTT_PORT,
)

MESSAGES = 20
TOPIC = "test/worker_stats/q1"

# report_stats forces a flush at least every 10 s.
FLUSH_DEADLINE_S = 30


def inserts(table):
    """n_tup_ins for table. A new connection per call avoids a cached stats snapshot."""
    rows = run_psql(
        f"SELECT coalesce((SELECT n_tup_ins FROM pg_stat_user_tables"
        f" WHERE relname = '{table}'), 0)"
    )
    return int(rows[0][0]) if rows else 0


def test_worker_table_changes_reach_the_stats_system_while_running():
    before = {t: inserts(t) for t in ("pgmqtt_messages", "pgmqtt_session_messages")}

    sub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sub.connect((MQTT_HOST, MQTT_PORT))
    sub.sendall(create_connect_packet("worker_stats_sub", clean_start=True))
    validate_connack(recv_packet(sub))
    sub.sendall(create_subscribe_packet(1, TOPIC, qos=1))
    validate_suback(recv_packet(sub), 1)

    pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    pub.connect((MQTT_HOST, MQTT_PORT))
    pub.sendall(create_connect_packet("worker_stats_pub", clean_start=True))
    validate_connack(recv_packet(pub))

    for i in range(MESSAGES):
        pub.sendall(create_publish_packet(TOPIC, f"m{i}".encode(), qos=1, packet_id=100 + i))
        assert recv_packet(pub) is not None, f"no PUBACK for publish {i}"
        _, _, qos, _, _, pid, _ = validate_publish(recv_packet(sub))
        assert qos == 1
        sub.sendall(create_puback_packet(pid))
    pub.close()
    sub.close()

    deadline = time.time() + FLUSH_DEADLINE_S
    seen = {t: inserts(t) - n for t, n in before.items()}
    while min(seen.values()) < MESSAGES and time.time() < deadline:
        time.sleep(0.5)
        seen = {t: inserts(t) - n for t, n in before.items()}

    for table, n in seen.items():
        assert n >= MESSAGES, (
            f"{MESSAGES} messages delivered, {n} inserts into {table} visible "
            f"after {FLUSH_DEADLINE_S}s"
        )
