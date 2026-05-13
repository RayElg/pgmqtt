"""
MQTT 5.0 Retained Message Tests.
"""

import socket
import subprocess
import time

from proto_utils import (
    create_connect_packet,
    create_subscribe_packet,
    create_publish_packet,
    create_puback_packet,
    create_pingreq_packet,
    create_disconnect_packet,
    recv_packet,
    validate_connack,
    validate_suback,
    validate_publish,
    validate_pingresp,
    ReasonCode,
    MQTT_HOST,
    MQTT_PORT,
)


def test_receive_retained_on_subscribe():
    """Subscriber receives retained message on subscribe with RETAIN=1."""
    topic = "test/retain/receive"
    payload = b"retained message"

    # Publish retained message
    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("retain_pub"))
    validate_connack(recv_packet(s_pub))
    s_pub.sendall(create_publish_packet(topic, payload, qos=1, packet_id=1, retain=True))
    recv_packet(s_pub)  # PUBACK
    s_pub.sendall(create_disconnect_packet())
    s_pub.close()

    # Subscribe — should receive retained
    s_cons = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_cons.connect((MQTT_HOST, MQTT_PORT))
    s_cons.sendall(create_connect_packet("retain_cons", clean_start=True))
    validate_connack(recv_packet(s_cons))
    s_cons.sendall(create_subscribe_packet(10, topic, qos=1))
    validate_suback(recv_packet(s_cons), 10)

    pub = recv_packet(s_cons)
    assert pub is not None, "Should receive retained message"
    t, p, qos, dup, retain, pid, props = validate_publish(pub)
    assert t == topic
    assert p == payload
    assert retain, "RETAIN flag should be 1"
    s_cons.close()

    # Cleanup
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("retain_cleaner"))
    recv_packet(s)
    s.sendall(create_publish_packet(topic, b"", qos=0, retain=True))
    s.close()


def test_clear_retained_with_empty_payload():
    """Empty payload with RETAIN=1 clears retained message."""
    topic = "test/retain/clear"

    # Set retained — QoS 1 so we can wait for PUBACK before proceeding
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("retain_setter"))
    recv_packet(s)
    s.sendall(create_publish_packet(topic, b"to be cleared", qos=1, packet_id=1, retain=True))
    recv_packet(s)  # PUBACK — retained message is now committed
    s.close()

    # Clear with empty payload — QoS 1 so we can wait for PUBACK before subscribing
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("retain_clearer"))
    recv_packet(s)
    s.sendall(create_publish_packet(topic, b"", qos=1, packet_id=2, retain=True))
    recv_packet(s)  # PUBACK — delete from pgmqtt_retained is now committed
    s.close()

    # Subscriber should NOT receive retained
    s_cons = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_cons.connect((MQTT_HOST, MQTT_PORT))
    s_cons.sendall(create_connect_packet("retain_checker", clean_start=True))
    recv_packet(s_cons)
    s_cons.sendall(create_subscribe_packet(11, topic, qos=0))
    recv_packet(s_cons)

    pub = recv_packet(s_cons, timeout=1.0)
    assert pub is None, "Should not receive after clearing retained"
    s_cons.close()


def test_retained_delivered_at_subscription_qos():
    """Retained message delivered at min(msg_qos, sub_qos) per MQTT 5.0 §3.3.1.2."""
    topic = "test/retain/qos1delivery"
    payload = b"retained qos1"

    # Publish retained at QoS 1
    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("retain_qos1_pub"))
    validate_connack(recv_packet(s_pub))
    s_pub.sendall(create_publish_packet(topic, payload, qos=1, packet_id=1, retain=True))
    recv_packet(s_pub)  # PUBACK
    s_pub.sendall(create_disconnect_packet())
    s_pub.close()

    # Subscribe at QoS 1 — retained should arrive at QoS 1 with packet ID
    s_cons = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_cons.connect((MQTT_HOST, MQTT_PORT))
    s_cons.sendall(create_connect_packet("retain_qos1_cons", clean_start=True))
    validate_connack(recv_packet(s_cons))
    s_cons.sendall(create_subscribe_packet(20, topic, qos=1))
    validate_suback(recv_packet(s_cons), 20)

    pub = recv_packet(s_cons)
    assert pub is not None, "Should receive retained message"
    t, p, qos, dup, retain, packet_id, props = validate_publish(pub)
    assert t == topic
    assert p == payload
    assert retain, "RETAIN flag should be 1 for retained delivery"
    assert qos == 1, f"Expected QoS 1, got QoS {qos}"
    assert packet_id is not None, "QoS 1 retained must have a packet ID"
    s_cons.close()

    # Subscribe at QoS 0 — retained should be downgraded to QoS 0
    s_cons2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_cons2.connect((MQTT_HOST, MQTT_PORT))
    s_cons2.sendall(create_connect_packet("retain_qos0_cons", clean_start=True))
    validate_connack(recv_packet(s_cons2))
    s_cons2.sendall(create_subscribe_packet(21, topic, qos=0))
    validate_suback(recv_packet(s_cons2), 21)

    pub2 = recv_packet(s_cons2)
    assert pub2 is not None, "Should receive retained message"
    t2, p2, qos2, dup2, retain2, pid2, props2 = validate_publish(pub2)
    assert t2 == topic
    assert p2 == payload
    assert retain2, "RETAIN flag should be 1"
    assert qos2 == 0, f"Expected QoS 0 (downgraded), got QoS {qos2}"
    s_cons2.close()

    # Cleanup retained
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("retain_qos1_cleaner"))
    recv_packet(s)
    s.sendall(create_publish_packet(topic, b"", qos=0, retain=True))
    s.close()


def test_retain_flag_zero_on_live_publish():
    """Live (non-retained) publish forwarded with RETAIN=0."""
    topic = "test/retain/live"
    payload = b"live message"

    s_cons = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_cons.connect((MQTT_HOST, MQTT_PORT))
    s_cons.sendall(create_connect_packet("retain_live_cons", clean_start=True))
    recv_packet(s_cons)
    s_cons.sendall(create_subscribe_packet(12, topic, qos=0))
    recv_packet(s_cons)

    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("retain_live_pub"))
    recv_packet(s_pub)
    s_pub.sendall(create_publish_packet(topic, payload, qos=0, retain=False))
    s_pub.close()

    pub = recv_packet(s_cons)
    assert pub is not None
    _, p, _, _, retain, _, _ = validate_publish(pub)
    assert p == payload
    assert not retain, "RETAIN should be 0 for live message"
    s_cons.close()


def test_retained_qos1_puback_accepted():
    """PUBACK for QoS 1 retained message is accepted and connection stays healthy.

    Regression test: retained QoS 1 messages must be tracked in the session
    inflight map so the server recognises the PUBACK. Previously the PUBACK
    would hit 'unknown packet_id' and be silently discarded.
    """
    topic = "test/retain/qos1puback"
    payload = b"retained qos1 puback test"

    # Publish retained message at QoS 1
    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("ret_q1pa_pub"))
    validate_connack(recv_packet(s_pub))
    s_pub.sendall(create_publish_packet(topic, payload, qos=1, packet_id=1, retain=True))
    recv_packet(s_pub)  # PUBACK
    s_pub.sendall(create_disconnect_packet())
    s_pub.close()

    # Subscribe at QoS 1 and receive the retained message
    s_cons = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_cons.connect((MQTT_HOST, MQTT_PORT))
    s_cons.sendall(create_connect_packet("ret_q1pa_cons", clean_start=True))
    validate_connack(recv_packet(s_cons))
    s_cons.sendall(create_subscribe_packet(10, topic, qos=1))
    validate_suback(recv_packet(s_cons), 10)

    pub = recv_packet(s_cons)
    assert pub is not None, "Should receive retained message"
    t, p, qos, dup, retain, packet_id, props = validate_publish(pub)
    assert qos == 1, f"Expected QoS 1, got {qos}"
    assert packet_id is not None, "QoS 1 retained must have packet ID"

    # Send PUBACK — this is the core of the test
    s_cons.sendall(create_puback_packet(packet_id))

    # Verify connection is still healthy via PINGREQ/PINGRESP
    s_cons.sendall(create_pingreq_packet())
    resp = recv_packet(s_cons, timeout=3.0)
    assert resp is not None, "Connection should still be alive after PUBACK"
    validate_pingresp(resp)

    s_cons.sendall(create_disconnect_packet())
    s_cons.close()

    # Cleanup retained
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("ret_q1pa_cleaner"))
    recv_packet(s)
    s.sendall(create_publish_packet(topic, b"", qos=0, retain=True))
    s.close()


def _restart_broker():
    subprocess.run(["docker", "compose", "restart", "postgres"], check=True)
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            s.connect((MQTT_HOST, MQTT_PORT))
            s.close()
            time.sleep(2)
            return
        except (socket.timeout, ConnectionRefusedError):
            time.sleep(1)
    raise Exception("Broker failed to restart within 30 seconds.")


def test_retained_clear_durable_across_restart():
    """QoS 1 retained-clear is persisted to subscriber sessions, surviving a broker restart.

    Regression test for the empty-payload retained-clear durability fix.
    Per MQTT-3.3.1-6 the clear PUBLISH is forwarded to current subscribers as
    a normal QoS 1 PUBLISH (RETAIN=0).  A persistent-session subscriber that
    is offline when the clear fires must receive the empty PUBLISH on resume,
    even after a broker restart — otherwise the clear notification is lost
    and the client never learns the topic is no longer retained.
    """
    topic = "test/retain/clear_durable"
    sub_id = "retain_clear_durable_sub"

    # Wipe any leftover session state from prior runs.  Note: in this codebase
    # the pgmqtt_session_messages → pgmqtt_sessions FK is missing on schemas
    # created before the FK was added to init010.rs, so a clean_start reconnect
    # leaves orphaned session_messages rows.  Delete both tables directly to
    # guarantee a clean slate regardless of FK presence.
    from proto_utils import run_psql
    run_psql(f"DELETE FROM pgmqtt_session_messages WHERE client_id = '{sub_id}';")
    run_psql(f"DELETE FROM pgmqtt_sessions WHERE client_id = '{sub_id}';")
    # Also clear any leftover retained on this topic.
    s_wipe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_wipe.connect((MQTT_HOST, MQTT_PORT))
    s_wipe.sendall(create_connect_packet("retain_clear_durable_wipe"))
    validate_connack(recv_packet(s_wipe))
    s_wipe.sendall(create_publish_packet(topic, b"", qos=1, packet_id=1, retain=True))
    recv_packet(s_wipe)
    s_wipe.sendall(create_disconnect_packet())
    s_wipe.close()
    time.sleep(0.5)

    # Pre-set the retained value BEFORE the subscriber subscribes, so the
    # retained-on-subscribe path delivers it inline (and the subscriber ACKs
    # it).  This isolates the test to the QoS 1 forward of the LATER clear.
    s_setter = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_setter.connect((MQTT_HOST, MQTT_PORT))
    s_setter.sendall(create_connect_packet("retain_clear_durable_setter"))
    validate_connack(recv_packet(s_setter))
    s_setter.sendall(create_publish_packet(topic, b"initial", qos=1, packet_id=1, retain=True))
    recv_packet(s_setter)  # PUBACK
    s_setter.sendall(create_disconnect_packet())
    s_setter.close()

    # Subscriber: persistent session, subscribe QoS 1, ACK the retained-on-subscribe
    # delivery, then close TCP abruptly.
    s_sub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_sub.connect((MQTT_HOST, MQTT_PORT))
    s_sub.sendall(create_connect_packet(
        sub_id, clean_start=True, properties={0x11: 300},
    ))
    validate_connack(recv_packet(s_sub))
    s_sub.sendall(create_subscribe_packet(1, topic, qos=1))
    validate_suback(recv_packet(s_sub), 1)
    pkt = recv_packet(s_sub, timeout=5)
    assert pkt is not None, "Should receive retained-on-subscribe"
    _, p_init, _, _, _, pid_init, _ = validate_publish(pkt)
    assert p_init == b"initial"
    s_sub.sendall(create_puback_packet(pid_init))
    s_sub.close()  # abrupt close — session preserved by expiry

    # Publisher: clear retained.  The clear is a QoS 1 forward to the offline
    # subscriber and must be persisted to pgmqtt_session_messages.
    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("retain_clear_durable_pub"))
    validate_connack(recv_packet(s_pub))
    s_pub.sendall(create_publish_packet(topic, b"", qos=1, packet_id=2, retain=True))
    recv_packet(s_pub)  # PUBACK — clear committed
    s_pub.sendall(create_disconnect_packet())
    s_pub.close()

    # Allow the broker's next poll tick to flush session_db_actions (the
    # InsertMessageBatch row for the offline subscriber) before we restart.
    # PUBACK is sent before that batch is flushed, so we can't rely on its
    # arrival as a synchronization point.
    time.sleep(2)

    # Broker restart wipes in-memory session.queue but persisted rows survive.
    _restart_broker()

    # Subscriber reconnects: must receive the empty-payload PUBLISH.
    s_sub2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_sub2.connect((MQTT_HOST, MQTT_PORT))
    s_sub2.sendall(create_connect_packet(
        sub_id, clean_start=False, properties={0x11: 300},
    ))
    sp, rc, _ = validate_connack(recv_packet(s_sub2))
    assert rc == 0, f"CONNACK reason_code={rc:#04x}"
    assert sp, "session_present should be true after restart"

    pkt = recv_packet(s_sub2, timeout=10)
    assert pkt is not None, "Should receive durable retained-clear notification after restart"
    t, p, qos, _dup, retain, _pid, _props = validate_publish(pkt)
    assert t == topic, f"Expected topic {topic}, got {t}"
    assert p == b"", f"Expected empty payload (clear), got {p!r}"
    assert qos == 1, f"Expected QoS 1, got {qos}"
    assert not retain, "Forwarded clear must have RETAIN=0 (MQTT-3.3.1-9)"
    s_sub2.close()
