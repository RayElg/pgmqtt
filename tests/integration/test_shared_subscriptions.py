"""
MQTT 5.0 Shared Subscription Tests ($share/{group}/{filter}).

Covers round-robin delivery, mixed shared/regular subscriptions,
QoS semantics, retained message exclusion, unsubscribe, session
persistence, malformed filter rejection, group+filter identity (§4.8.2),
duplicate-free delivery, topic name preservation, re-subscribe QoS update,
mixed QoS members, and member rejoin after disconnect.
"""

import socket
import time

from proto_utils import (
    create_connect_packet,
    create_disconnect_packet,
    create_publish_packet,
    create_puback_packet,
    create_subscribe_packet,
    create_unsubscribe_packet,
    recv_packet,
    validate_connack,
    validate_publish,
    validate_suback,
    validate_unsuback,
    MQTTControlPacket,
    ReasonCode,
    MQTT_HOST,
    MQTT_PORT,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def mqtt_connect(client_id, clean_start=True, session_expiry=0):
    """Open TCP socket, send CONNECT, return (socket, session_present)."""
    props = {}
    if session_expiry > 0:
        props[0x11] = session_expiry
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet(
        client_id,
        clean_start=clean_start,
        properties=props if props else None,
    ))
    raw = recv_packet(s, timeout=5)
    assert raw is not None, f"[{client_id}] No CONNACK received"
    sp, rc, _ = validate_connack(raw)
    assert rc == 0, f"[{client_id}] CONNACK reason_code={rc:#04x}"
    return s, sp


def mqtt_subscribe(sock, packet_id, topic_filter, qos=0):
    """Subscribe and validate SUBACK. Returns the granted reason code."""
    sock.sendall(create_subscribe_packet(packet_id, topic_filter, qos=qos))
    raw = recv_packet(sock, timeout=5)
    assert raw is not None, "No SUBACK received"
    reason_codes = validate_suback(raw, packet_id)
    return reason_codes[0]


def collect_publishes(sock, count, timeout=5.0):
    """Collect up to `count` PUBLISH packets within `timeout`."""
    results = []
    deadline = time.time() + timeout
    while len(results) < count and time.time() < deadline:
        remaining = max(0.1, deadline - time.time())
        pkt = recv_packet(sock, timeout=remaining)
        if pkt is None:
            continue
        ptype = (pkt[0] & 0xF0) >> 4
        if ptype == MQTTControlPacket.PUBLISH:
            results.append(validate_publish(pkt))
    return results


# ---------------------------------------------------------------------------
# Basic round-robin delivery
# ---------------------------------------------------------------------------

def test_shared_sub_round_robin_qos0():
    """Two members of a shared group receive QoS 0 messages in round-robin."""
    topic = "test/shared/rr0"
    shared_filter = "$share/grp_rr0/" + topic

    s_a, _ = mqtt_connect("shrr0_a", clean_start=True)
    s_b, _ = mqtt_connect("shrr0_b", clean_start=True)
    s_pub, _ = mqtt_connect("shrr0_pub", clean_start=True)

    rc_a = mqtt_subscribe(s_a, 1, shared_filter, qos=0)
    rc_b = mqtt_subscribe(s_b, 1, shared_filter, qos=0)
    assert rc_a <= 0x02, f"Subscribe A failed: {rc_a:#04x}"
    assert rc_b <= 0x02, f"Subscribe B failed: {rc_b:#04x}"

    num_messages = 10
    for i in range(num_messages):
        s_pub.sendall(create_publish_packet(topic, f"msg-{i}".encode(), qos=0))

    msgs_a = collect_publishes(s_a, num_messages, timeout=5.0)
    msgs_b = collect_publishes(s_b, num_messages, timeout=2.0)

    total = len(msgs_a) + len(msgs_b)
    assert total == num_messages, (
        f"Expected {num_messages} total deliveries, got {total} "
        f"(A={len(msgs_a)}, B={len(msgs_b)})"
    )
    # Each member should receive at least 1 (round-robin distributes).
    assert len(msgs_a) >= 1, "Member A received no messages"
    assert len(msgs_b) >= 1, "Member B received no messages"

    s_a.close()
    s_b.close()
    s_pub.close()


def test_shared_sub_round_robin_qos1():
    """Two members of a shared group receive QoS 1 messages in round-robin."""
    topic = "test/shared/rr1"
    shared_filter = "$share/grp_rr1/" + topic

    s_a, _ = mqtt_connect("shrr1_a", clean_start=True)
    s_b, _ = mqtt_connect("shrr1_b", clean_start=True)
    s_pub, _ = mqtt_connect("shrr1_pub", clean_start=True)

    rc_a = mqtt_subscribe(s_a, 1, shared_filter, qos=1)
    rc_b = mqtt_subscribe(s_b, 1, shared_filter, qos=1)
    assert rc_a <= 0x02
    assert rc_b <= 0x02

    num_messages = 10
    for i in range(num_messages):
        pid = i + 1
        s_pub.sendall(create_publish_packet(topic, f"msg-{i}".encode(), qos=1, packet_id=pid))
        # Collect PUBACK from broker for each publish.
        recv_packet(s_pub, timeout=5)

    # Collect from both members, ACKing each one.
    msgs_a = []
    msgs_b = []
    deadline = time.time() + 10
    while len(msgs_a) + len(msgs_b) < num_messages and time.time() < deadline:
        for sock, msgs in [(s_a, msgs_a), (s_b, msgs_b)]:
            pkt = recv_packet(sock, timeout=0.3)
            if pkt is None:
                continue
            ptype = (pkt[0] & 0xF0) >> 4
            if ptype == MQTTControlPacket.PUBLISH:
                t, p, qos, dup, retain, pid, props = validate_publish(pkt)
                msgs.append((t, p))
                if qos == 1 and pid is not None:
                    sock.sendall(create_puback_packet(pid))

    total = len(msgs_a) + len(msgs_b)
    assert total == num_messages, (
        f"Expected {num_messages} total, got {total} (A={len(msgs_a)}, B={len(msgs_b)})"
    )
    assert len(msgs_a) >= 1
    assert len(msgs_b) >= 1

    s_a.close()
    s_b.close()
    s_pub.close()


def test_shared_sub_three_members():
    """Three members share messages; all messages accounted for."""
    topic = "test/shared/tri"
    shared_filter = "$share/grp_tri/" + topic
    num_messages = 12  # divisible by 3 for even distribution

    subs = []
    for i in range(3):
        s, _ = mqtt_connect(f"shtri_{i}", clean_start=True)
        rc = mqtt_subscribe(s, 1, shared_filter, qos=0)
        assert rc <= 0x02
        subs.append(s)

    s_pub, _ = mqtt_connect("shtri_pub", clean_start=True)
    for i in range(num_messages):
        s_pub.sendall(create_publish_packet(topic, f"m{i}".encode(), qos=0))

    all_msgs = []
    for s in subs:
        msgs = collect_publishes(s, num_messages, timeout=3.0)
        assert len(msgs) >= 1, "Each member should receive at least one message"
        all_msgs.extend(msgs)

    assert len(all_msgs) == num_messages, (
        f"Expected {num_messages} total, got {len(all_msgs)}"
    )

    for s in subs:
        s.close()
    s_pub.close()


# ---------------------------------------------------------------------------
# Shared + regular subscriptions coexist
# ---------------------------------------------------------------------------

def test_shared_and_regular_coexist():
    """A shared group member AND a regular subscriber both get the message.

    The regular subscriber gets every message; the shared group delivers
    each message to exactly one of its members.
    """
    topic = "test/shared/coexist"
    shared_filter = "$share/grp_coex/" + topic

    # Two shared-group members.
    s_a, _ = mqtt_connect("shcoex_a", clean_start=True)
    s_b, _ = mqtt_connect("shcoex_b", clean_start=True)
    # One regular subscriber.
    s_reg, _ = mqtt_connect("shcoex_reg", clean_start=True)

    mqtt_subscribe(s_a, 1, shared_filter, qos=0)
    mqtt_subscribe(s_b, 1, shared_filter, qos=0)
    mqtt_subscribe(s_reg, 1, topic, qos=0)

    s_pub, _ = mqtt_connect("shcoex_pub", clean_start=True)
    num_messages = 6
    for i in range(num_messages):
        s_pub.sendall(create_publish_packet(topic, f"c{i}".encode(), qos=0))

    # Regular subscriber gets ALL messages.
    msgs_reg = collect_publishes(s_reg, num_messages, timeout=5.0)
    assert len(msgs_reg) == num_messages, (
        f"Regular subscriber expected {num_messages}, got {len(msgs_reg)}"
    )

    # Shared group members collectively get all messages.
    msgs_a = collect_publishes(s_a, num_messages, timeout=3.0)
    msgs_b = collect_publishes(s_b, num_messages, timeout=2.0)
    total_shared = len(msgs_a) + len(msgs_b)
    assert total_shared == num_messages, (
        f"Shared group expected {num_messages} total, got {total_shared}"
    )

    s_a.close()
    s_b.close()
    s_reg.close()
    s_pub.close()


# ---------------------------------------------------------------------------
# Wildcard filters inside shared subscriptions
# ---------------------------------------------------------------------------

def test_shared_sub_with_single_level_wildcard():
    """$share/group/sensor/+/data matches sensor/X/data topics."""
    shared_filter = "$share/grp_wc/sensor/+/data"

    s_a, _ = mqtt_connect("shwc_a", clean_start=True)
    s_b, _ = mqtt_connect("shwc_b", clean_start=True)
    mqtt_subscribe(s_a, 1, shared_filter, qos=0)
    mqtt_subscribe(s_b, 1, shared_filter, qos=0)

    s_pub, _ = mqtt_connect("shwc_pub", clean_start=True)
    s_pub.sendall(create_publish_packet("sensor/temp/data", b"t1", qos=0))
    s_pub.sendall(create_publish_packet("sensor/hum/data", b"t2", qos=0))
    # Non-matching: extra level.
    s_pub.sendall(create_publish_packet("sensor/a/b/data", b"no", qos=0))

    msgs_a = collect_publishes(s_a, 2, timeout=3.0)
    msgs_b = collect_publishes(s_b, 2, timeout=2.0)
    total = len(msgs_a) + len(msgs_b)
    assert total == 2, f"Expected 2 matching publishes, got {total}"

    topics = [m[0] for m in msgs_a + msgs_b]
    assert "sensor/temp/data" in topics
    assert "sensor/hum/data" in topics

    s_a.close()
    s_b.close()
    s_pub.close()


def test_shared_sub_with_multi_level_wildcard():
    """$share/group/devices/# matches devices, devices/a, devices/a/b."""
    shared_filter = "$share/grp_mlwc/devices/#"

    s_a, _ = mqtt_connect("shmlwc_a", clean_start=True)
    s_b, _ = mqtt_connect("shmlwc_b", clean_start=True)
    mqtt_subscribe(s_a, 1, shared_filter, qos=0)
    mqtt_subscribe(s_b, 1, shared_filter, qos=0)

    s_pub, _ = mqtt_connect("shmlwc_pub", clean_start=True)
    s_pub.sendall(create_publish_packet("devices", b"d0", qos=0))
    s_pub.sendall(create_publish_packet("devices/a", b"d1", qos=0))
    s_pub.sendall(create_publish_packet("devices/a/b/c", b"d2", qos=0))

    msgs_a = collect_publishes(s_a, 3, timeout=3.0)
    msgs_b = collect_publishes(s_b, 3, timeout=2.0)
    total = len(msgs_a) + len(msgs_b)
    assert total == 3, f"Expected 3, got {total}"

    topics = [m[0] for m in msgs_a + msgs_b]
    assert "devices" in topics
    assert "devices/a" in topics
    assert "devices/a/b/c" in topics

    s_a.close()
    s_b.close()
    s_pub.close()


# ---------------------------------------------------------------------------
# Retained messages NOT delivered for shared subscriptions
# ---------------------------------------------------------------------------

def test_shared_sub_no_retained_delivery():
    """Retained messages must NOT be delivered on shared subscription."""
    topic = "test/shared/retain"
    shared_filter = "$share/grp_ret/" + topic

    # Publish a retained message.
    s_pub, _ = mqtt_connect("shret_pub", clean_start=True)
    s_pub.sendall(create_publish_packet(topic, b"retained!", qos=1, packet_id=1, retain=True))
    recv_packet(s_pub, timeout=5)  # PUBACK
    s_pub.sendall(create_disconnect_packet())
    s_pub.close()

    # Subscribe via shared subscription — should NOT receive retained.
    s_sub, _ = mqtt_connect("shret_sub", clean_start=True)
    rc = mqtt_subscribe(s_sub, 10, shared_filter, qos=1)
    assert rc <= 0x02

    pkt = recv_packet(s_sub, timeout=2.0)
    assert pkt is None, "Shared subscription should NOT receive retained messages"
    s_sub.close()

    # Verify a regular subscription DOES receive the retained message.
    s_reg, _ = mqtt_connect("shret_reg", clean_start=True)
    mqtt_subscribe(s_reg, 11, topic, qos=1)

    pkt = recv_packet(s_reg, timeout=5.0)
    assert pkt is not None, "Regular subscription should receive retained message"
    t, p, *_ = validate_publish(pkt)
    assert t == topic
    assert p == b"retained!"
    s_reg.close()

    # Clean up retained.
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("shret_cleaner"))
    recv_packet(s)
    s.sendall(create_publish_packet(topic, b"", qos=0, retain=True))
    s.close()


# ---------------------------------------------------------------------------
# Unsubscribe from shared subscription
# ---------------------------------------------------------------------------

def test_shared_sub_unsubscribe():
    """Unsubscribing from a shared group stops delivery to that member."""
    topic = "test/shared/unsub"
    shared_filter = "$share/grp_unsub/" + topic

    s_a, _ = mqtt_connect("shunsub_a", clean_start=True)
    s_b, _ = mqtt_connect("shunsub_b", clean_start=True)
    mqtt_subscribe(s_a, 1, shared_filter, qos=0)
    mqtt_subscribe(s_b, 1, shared_filter, qos=0)

    # Verify both receive messages.
    s_pub, _ = mqtt_connect("shunsub_pub", clean_start=True)
    for i in range(6):
        s_pub.sendall(create_publish_packet(topic, f"pre-{i}".encode(), qos=0))

    msgs_a = collect_publishes(s_a, 6, timeout=3.0)
    msgs_b = collect_publishes(s_b, 6, timeout=2.0)
    assert len(msgs_a) + len(msgs_b) == 6

    # Unsubscribe member A.
    s_a.sendall(create_unsubscribe_packet(2, shared_filter))
    raw = recv_packet(s_a, timeout=5)
    assert raw is not None, "No UNSUBACK received"
    unsub_rcs = validate_unsuback(raw, 2)
    assert unsub_rcs[0] == ReasonCode.SUCCESS

    # Publish more — only B should receive.
    for i in range(4):
        s_pub.sendall(create_publish_packet(topic, f"post-{i}".encode(), qos=0))

    msgs_b_post = collect_publishes(s_b, 4, timeout=3.0)
    msgs_a_post = collect_publishes(s_a, 4, timeout=1.0)
    assert len(msgs_a_post) == 0, "A should not receive after unsubscribe"
    assert len(msgs_b_post) == 4, f"B should get all 4, got {len(msgs_b_post)}"

    s_a.close()
    s_b.close()
    s_pub.close()


def test_shared_sub_last_member_unsubscribes():
    """When the only member unsubscribes, messages have no recipient."""
    topic = "test/shared/lastun"
    shared_filter = "$share/grp_lastun/" + topic

    s_a, _ = mqtt_connect("shlastun_a", clean_start=True)
    mqtt_subscribe(s_a, 1, shared_filter, qos=0)

    # Unsubscribe.
    s_a.sendall(create_unsubscribe_packet(2, shared_filter))
    validate_unsuback(recv_packet(s_a, timeout=5), 2)

    # Publish — no one to receive.
    s_pub, _ = mqtt_connect("shlastun_pub", clean_start=True)
    s_pub.sendall(create_publish_packet(topic, b"orphan", qos=0))

    pkt = recv_packet(s_a, timeout=1.0)
    assert pkt is None, "No delivery after last member unsubscribes"

    s_a.close()
    s_pub.close()


# ---------------------------------------------------------------------------
# Multiple groups on the same underlying topic
# ---------------------------------------------------------------------------

def test_shared_sub_multiple_groups():
    """Two different share groups on the same topic each deliver independently."""
    topic = "test/shared/multigrp"
    filter_g1 = "$share/group1/" + topic
    filter_g2 = "$share/group2/" + topic

    s_g1a, _ = mqtt_connect("shmg_g1a", clean_start=True)
    s_g1b, _ = mqtt_connect("shmg_g1b", clean_start=True)
    s_g2a, _ = mqtt_connect("shmg_g2a", clean_start=True)
    s_g2b, _ = mqtt_connect("shmg_g2b", clean_start=True)

    mqtt_subscribe(s_g1a, 1, filter_g1, qos=0)
    mqtt_subscribe(s_g1b, 1, filter_g1, qos=0)
    mqtt_subscribe(s_g2a, 1, filter_g2, qos=0)
    mqtt_subscribe(s_g2b, 1, filter_g2, qos=0)

    s_pub, _ = mqtt_connect("shmg_pub", clean_start=True)
    num_messages = 6
    for i in range(num_messages):
        s_pub.sendall(create_publish_packet(topic, f"mg{i}".encode(), qos=0))

    # Group 1: collectively receives all messages.
    g1a = collect_publishes(s_g1a, num_messages, timeout=3.0)
    g1b = collect_publishes(s_g1b, num_messages, timeout=2.0)
    assert len(g1a) + len(g1b) == num_messages, (
        f"Group1 expected {num_messages}, got {len(g1a)+len(g1b)}"
    )

    # Group 2: collectively receives all messages (independently of group 1).
    g2a = collect_publishes(s_g2a, num_messages, timeout=3.0)
    g2b = collect_publishes(s_g2b, num_messages, timeout=2.0)
    assert len(g2a) + len(g2b) == num_messages, (
        f"Group2 expected {num_messages}, got {len(g2a)+len(g2b)}"
    )

    for s in [s_g1a, s_g1b, s_g2a, s_g2b, s_pub]:
        s.close()


# ---------------------------------------------------------------------------
# Session persistence with shared subscriptions
# ---------------------------------------------------------------------------

def test_shared_sub_persists_across_reconnect():
    """Shared subscription survives disconnect + reconnect with persistent session."""
    topic = "test/shared/persist"
    shared_filter = "$share/grp_persist/" + topic

    # Member A connects with persistent session, subscribes, disconnects.
    s_a, _ = mqtt_connect("shpersist_a", clean_start=True, session_expiry=300)
    mqtt_subscribe(s_a, 1, shared_filter, qos=1)
    s_a.sendall(create_disconnect_packet())
    s_a.close()

    # Member B stays connected.
    s_b, _ = mqtt_connect("shpersist_b", clean_start=True, session_expiry=300)
    mqtt_subscribe(s_b, 1, shared_filter, qos=1)

    # Publish while A is offline — B (connected) should be preferred.
    s_pub, _ = mqtt_connect("shpersist_pub", clean_start=True)
    for i in range(4):
        pid = i + 1
        s_pub.sendall(create_publish_packet(topic, f"p{i}".encode(), qos=1, packet_id=pid))
        recv_packet(s_pub, timeout=5)  # PUBACK

    msgs_b = []
    deadline = time.time() + 5
    while len(msgs_b) < 4 and time.time() < deadline:
        pkt = recv_packet(s_b, timeout=1.0)
        if pkt is None:
            continue
        ptype = (pkt[0] & 0xF0) >> 4
        if ptype == MQTTControlPacket.PUBLISH:
            t, p, qos, dup, retain, pid, props = validate_publish(pkt)
            msgs_b.append(p)
            if qos == 1 and pid is not None:
                s_b.sendall(create_puback_packet(pid))

    # B should have received all messages (connected member preferred).
    assert len(msgs_b) == 4, (
        f"Connected member B should receive all 4, got {len(msgs_b)}"
    )

    # Reconnect A — session present.
    s_a2, sp = mqtt_connect("shpersist_a", clean_start=False, session_expiry=300)
    assert sp, "Session should be present for A"

    # Now both A and B are connected. Publish more and verify round-robin resumes.
    for i in range(6):
        pid = i + 10
        s_pub.sendall(create_publish_packet(topic, f"q{i}".encode(), qos=1, packet_id=pid))
        recv_packet(s_pub, timeout=5)

    msgs_a2 = []
    msgs_b2 = []
    deadline = time.time() + 5
    while len(msgs_a2) + len(msgs_b2) < 6 and time.time() < deadline:
        for sock, msgs in [(s_a2, msgs_a2), (s_b, msgs_b2)]:
            pkt = recv_packet(sock, timeout=0.3)
            if pkt is None:
                continue
            ptype = (pkt[0] & 0xF0) >> 4
            if ptype == MQTTControlPacket.PUBLISH:
                t, p, qos, dup, retain, pid, props = validate_publish(pkt)
                msgs.append(p)
                if qos == 1 and pid is not None:
                    sock.sendall(create_puback_packet(pid))

    total = len(msgs_a2) + len(msgs_b2)
    assert total == 6, f"Expected 6, got {total}"
    assert len(msgs_a2) >= 1, "Reconnected A should participate in round-robin"
    assert len(msgs_b2) >= 1, "B should still participate in round-robin"

    s_a2.close()
    s_b.close()
    s_pub.close()


# ---------------------------------------------------------------------------
# QoS 1 delivery semantics within shared subscription
# ---------------------------------------------------------------------------

def test_shared_sub_qos1_puback():
    """QoS 1 shared subscription: message has packet ID, PUBACK completes it."""
    topic = "test/shared/qos1ack"
    shared_filter = "$share/grp_q1ack/" + topic

    s_sub, _ = mqtt_connect("shq1ack_sub", clean_start=True)
    rc = mqtt_subscribe(s_sub, 1, shared_filter, qos=1)
    assert rc == ReasonCode.GRANTED_QOS_1

    s_pub, _ = mqtt_connect("shq1ack_pub", clean_start=True)
    s_pub.sendall(create_publish_packet(topic, b"qos1-data", qos=1, packet_id=42))
    recv_packet(s_pub, timeout=5)  # PUBACK from broker

    pkt = recv_packet(s_sub, timeout=5)
    assert pkt is not None, "Should receive QoS 1 PUBLISH"
    t, p, qos, dup, retain, pid, props = validate_publish(pkt)
    assert t == topic
    assert p == b"qos1-data"
    assert qos == 1
    assert pid is not None, "QoS 1 must have packet ID"

    # ACK it.
    s_sub.sendall(create_puback_packet(pid))

    # No extra messages.
    extra = recv_packet(s_sub, timeout=1.0)
    assert extra is None

    s_sub.close()
    s_pub.close()


# ---------------------------------------------------------------------------
# Malformed shared subscription filters
# ---------------------------------------------------------------------------

def test_shared_sub_malformed_empty_group():
    """$share//topic — empty group name should be rejected."""
    s, _ = mqtt_connect("shmal_empty", clean_start=True)
    s.sendall(create_subscribe_packet(1, "$share//topic", qos=0))
    raw = recv_packet(s, timeout=5)
    assert raw is not None, "No SUBACK received"
    rcs = validate_suback(raw, 1)
    assert rcs[0] == ReasonCode.TOPIC_FILTER_INVALID, (
        f"Expected TOPIC_FILTER_INVALID (0x8F), got {rcs[0]:#04x}"
    )
    s.close()


def test_shared_sub_malformed_empty_filter():
    """$share/group/ — empty filter should be rejected."""
    s, _ = mqtt_connect("shmal_efilt", clean_start=True)
    s.sendall(create_subscribe_packet(1, "$share/group/", qos=0))
    raw = recv_packet(s, timeout=5)
    assert raw is not None
    rcs = validate_suback(raw, 1)
    assert rcs[0] == ReasonCode.TOPIC_FILTER_INVALID, (
        f"Expected TOPIC_FILTER_INVALID (0x8F), got {rcs[0]:#04x}"
    )
    s.close()


def test_shared_sub_malformed_no_filter():
    """$share/group — missing filter entirely should be rejected."""
    s, _ = mqtt_connect("shmal_noflt", clean_start=True)
    s.sendall(create_subscribe_packet(1, "$share/group", qos=0))
    raw = recv_packet(s, timeout=5)
    assert raw is not None
    rcs = validate_suback(raw, 1)
    assert rcs[0] == ReasonCode.TOPIC_FILTER_INVALID, (
        f"Expected TOPIC_FILTER_INVALID (0x8F), got {rcs[0]:#04x}"
    )
    s.close()


def test_shared_sub_malformed_wildcard_in_group():
    """$share/gr+up/topic — wildcard in group name should be rejected."""
    s, _ = mqtt_connect("shmal_wcgrp", clean_start=True)
    s.sendall(create_subscribe_packet(1, "$share/gr+up/topic", qos=0))
    raw = recv_packet(s, timeout=5)
    assert raw is not None
    rcs = validate_suback(raw, 1)
    assert rcs[0] == ReasonCode.TOPIC_FILTER_INVALID, (
        f"Expected TOPIC_FILTER_INVALID (0x8F), got {rcs[0]:#04x}"
    )
    s.close()


# ---------------------------------------------------------------------------
# Edge case: single member gets all messages
# ---------------------------------------------------------------------------

def test_shared_sub_single_member():
    """A group with one member delivers all messages to that member."""
    topic = "test/shared/single"
    shared_filter = "$share/grp_single/" + topic

    s_sub, _ = mqtt_connect("shsingle_sub", clean_start=True)
    mqtt_subscribe(s_sub, 1, shared_filter, qos=0)

    s_pub, _ = mqtt_connect("shsingle_pub", clean_start=True)
    num_messages = 5
    for i in range(num_messages):
        s_pub.sendall(create_publish_packet(topic, f"s{i}".encode(), qos=0))

    msgs = collect_publishes(s_sub, num_messages, timeout=5.0)
    assert len(msgs) == num_messages, f"Expected {num_messages}, got {len(msgs)}"

    s_sub.close()
    s_pub.close()


# ---------------------------------------------------------------------------
# Disconnect removes member from group
# ---------------------------------------------------------------------------

def test_shared_sub_disconnect_removes_member():
    """After disconnect (session_expiry=0), the member is removed from the group."""
    topic = "test/shared/dcremove"
    shared_filter = "$share/grp_dcrem/" + topic

    s_a, _ = mqtt_connect("shdcrem_a", clean_start=True)
    s_b, _ = mqtt_connect("shdcrem_b", clean_start=True)
    mqtt_subscribe(s_a, 1, shared_filter, qos=0)
    mqtt_subscribe(s_b, 1, shared_filter, qos=0)

    # Disconnect A (session_expiry defaults to 0 → immediate cleanup).
    s_a.sendall(create_disconnect_packet())
    s_a.close()
    time.sleep(0.5)  # Let the broker process the disconnect.

    # All messages should go to B now.
    s_pub, _ = mqtt_connect("shdcrem_pub", clean_start=True)
    num_messages = 4
    for i in range(num_messages):
        s_pub.sendall(create_publish_packet(topic, f"dc{i}".encode(), qos=0))

    msgs_b = collect_publishes(s_b, num_messages, timeout=5.0)
    assert len(msgs_b) == num_messages, (
        f"B should receive all {num_messages}, got {len(msgs_b)}"
    )

    s_b.close()
    s_pub.close()


# ---------------------------------------------------------------------------
# QoS downgrade in shared subscription
# ---------------------------------------------------------------------------

def test_shared_sub_qos_downgrade():
    """Subscriber at QoS 0 receives QoS 1 publish as QoS 0."""
    topic = "test/shared/qdown"
    shared_filter = "$share/grp_qdown/" + topic

    s_sub, _ = mqtt_connect("shqdown_sub", clean_start=True)
    rc = mqtt_subscribe(s_sub, 1, shared_filter, qos=0)
    assert rc == ReasonCode.GRANTED_QOS_0

    s_pub, _ = mqtt_connect("shqdown_pub", clean_start=True)
    s_pub.sendall(create_publish_packet(topic, b"downgraded", qos=1, packet_id=1))
    recv_packet(s_pub, timeout=5)  # PUBACK

    pkt = recv_packet(s_sub, timeout=5)
    assert pkt is not None
    t, p, qos, dup, retain, pid, props = validate_publish(pkt)
    assert qos == 0, f"Expected QoS 0 (downgraded), got {qos}"
    assert pid is None, "QoS 0 should not have packet ID"
    assert p == b"downgraded"

    s_sub.close()
    s_pub.close()


# ---------------------------------------------------------------------------
# Same group name, different topic filters (§4.8.2: identified by group+filter)
# ---------------------------------------------------------------------------

def test_shared_sub_same_group_different_filters():
    """$share/g/topicA and $share/g/topicB are independent subscriptions.

    MQTT 5.0 §4.8.2: a shared subscription is identified by the
    (ShareName, TopicFilter) pair, not by the ShareName alone.
    """
    topic_a = "test/shared/sgdf/alpha"
    topic_b = "test/shared/sgdf/beta"
    filter_a = "$share/sgdf_grp/" + topic_a
    filter_b = "$share/sgdf_grp/" + topic_b

    s_a, _ = mqtt_connect("sgdf_a", clean_start=True)
    s_b, _ = mqtt_connect("sgdf_b", clean_start=True)
    mqtt_subscribe(s_a, 1, filter_a, qos=0)
    mqtt_subscribe(s_b, 1, filter_b, qos=0)

    s_pub, _ = mqtt_connect("sgdf_pub", clean_start=True)
    s_pub.sendall(create_publish_packet(topic_a, b"for-a", qos=0))
    s_pub.sendall(create_publish_packet(topic_b, b"for-b", qos=0))

    msgs_a = collect_publishes(s_a, 2, timeout=3.0)
    msgs_b = collect_publishes(s_b, 2, timeout=2.0)

    # A should only get topic_a, B should only get topic_b.
    assert len(msgs_a) == 1, f"A expected 1, got {len(msgs_a)}"
    assert len(msgs_b) == 1, f"B expected 1, got {len(msgs_b)}"
    assert msgs_a[0][0] == topic_a, f"A got wrong topic: {msgs_a[0][0]}"
    assert msgs_b[0][0] == topic_b, f"B got wrong topic: {msgs_b[0][0]}"

    s_a.close()
    s_b.close()
    s_pub.close()


# ---------------------------------------------------------------------------
# No duplicate delivery within a shared group
# ---------------------------------------------------------------------------

def test_shared_sub_no_duplicate_payloads():
    """Each message is delivered to exactly one group member — no duplicates.

    Tracks payloads explicitly to ensure no message is delivered twice.
    """
    topic = "test/shared/nodup"
    shared_filter = "$share/grp_nodup/" + topic
    num_messages = 20

    subs = []
    for i in range(3):
        s, _ = mqtt_connect(f"shnodup_{i}", clean_start=True)
        mqtt_subscribe(s, 1, shared_filter, qos=0)
        subs.append(s)

    s_pub, _ = mqtt_connect("shnodup_pub", clean_start=True)
    for i in range(num_messages):
        s_pub.sendall(create_publish_packet(topic, f"u-{i}".encode(), qos=0))

    all_payloads = []
    for s in subs:
        msgs = collect_publishes(s, num_messages, timeout=3.0)
        all_payloads.extend(m[1] for m in msgs)  # m[1] = payload bytes

    assert len(all_payloads) == num_messages, (
        f"Expected {num_messages} total, got {len(all_payloads)}"
    )
    # Convert to set to check uniqueness.
    unique = set(all_payloads)
    assert len(unique) == num_messages, (
        f"Duplicate payloads detected: {len(all_payloads)} delivered, {len(unique)} unique"
    )

    for s in subs:
        s.close()
    s_pub.close()


# ---------------------------------------------------------------------------
# Topic name in PUBLISH is the original topic, not the $share/ filter
# ---------------------------------------------------------------------------

def test_shared_sub_topic_name_preserved():
    """PUBLISH delivered to shared subscriber must carry the original topic.

    Per §4.8.2, the topic in the forwarded PUBLISH is the concrete
    publish topic, not the $share/{group}/{filter} string.
    """
    topic = "test/shared/topicname"
    shared_filter = "$share/grp_tn/" + topic

    s_sub, _ = mqtt_connect("shtn_sub", clean_start=True)
    mqtt_subscribe(s_sub, 1, shared_filter, qos=0)

    s_pub, _ = mqtt_connect("shtn_pub", clean_start=True)
    s_pub.sendall(create_publish_packet(topic, b"check-topic", qos=0))

    msgs = collect_publishes(s_sub, 1, timeout=5.0)
    assert len(msgs) == 1
    received_topic = msgs[0][0]
    assert received_topic == topic, (
        f"Expected original topic '{topic}', got '{received_topic}'"
    )
    assert not received_topic.startswith("$share"), (
        "Topic must not include the $share/ prefix"
    )

    s_sub.close()
    s_pub.close()


# ---------------------------------------------------------------------------
# Re-subscribe updates QoS without creating duplicate member
# ---------------------------------------------------------------------------

def test_shared_sub_resubscribe_updates_qos():
    """Re-subscribing to a shared group with a different QoS updates the grant.

    The member must not be duplicated — message count must remain correct.
    """
    topic = "test/shared/resub"
    shared_filter = "$share/grp_resub/" + topic

    s_sub, _ = mqtt_connect("shresub_sub", clean_start=True)

    # Subscribe at QoS 0, then re-subscribe at QoS 1.
    rc0 = mqtt_subscribe(s_sub, 1, shared_filter, qos=0)
    assert rc0 == ReasonCode.GRANTED_QOS_0
    rc1 = mqtt_subscribe(s_sub, 2, shared_filter, qos=1)
    assert rc1 == ReasonCode.GRANTED_QOS_1

    # Publish QoS 1 — subscriber should receive at QoS 1 (upgraded grant).
    s_pub, _ = mqtt_connect("shresub_pub", clean_start=True)
    s_pub.sendall(create_publish_packet(topic, b"after-resub", qos=1, packet_id=1))
    recv_packet(s_pub, timeout=5)  # PUBACK

    pkt = recv_packet(s_sub, timeout=5)
    assert pkt is not None
    t, p, qos, dup, retain, pid, props = validate_publish(pkt)
    assert qos == 1, f"Expected QoS 1 after re-subscribe, got {qos}"

    # ACK it.
    if pid is not None:
        s_sub.sendall(create_puback_packet(pid))

    # Verify no duplicate delivery (re-subscribe must not create a second member).
    num_messages = 6
    for i in range(num_messages):
        pid = i + 10
        s_pub.sendall(create_publish_packet(topic, f"rd-{i}".encode(), qos=1, packet_id=pid))
        recv_packet(s_pub, timeout=5)

    msgs = []
    deadline = time.time() + 5
    while len(msgs) < num_messages and time.time() < deadline:
        pkt = recv_packet(s_sub, timeout=1.0)
        if pkt is None:
            continue
        ptype = (pkt[0] & 0xF0) >> 4
        if ptype == MQTTControlPacket.PUBLISH:
            t, p, qos, dup, retain, pid, props = validate_publish(pkt)
            msgs.append(p)
            if qos == 1 and pid is not None:
                s_sub.sendall(create_puback_packet(pid))

    assert len(msgs) == num_messages, (
        f"Single member should receive exactly {num_messages}, got {len(msgs)} "
        "(duplicate member from re-subscribe?)"
    )

    s_sub.close()
    s_pub.close()


# ---------------------------------------------------------------------------
# Mixed QoS levels within a shared group
# ---------------------------------------------------------------------------

def test_shared_sub_mixed_qos_members():
    """Members at different QoS levels each receive at their granted QoS."""
    topic = "test/shared/mixqos"
    shared_filter = "$share/grp_mixqos/" + topic

    s_q0, _ = mqtt_connect("shmixq_q0", clean_start=True)
    s_q1, _ = mqtt_connect("shmixq_q1", clean_start=True)
    rc0 = mqtt_subscribe(s_q0, 1, shared_filter, qos=0)
    rc1 = mqtt_subscribe(s_q1, 1, shared_filter, qos=1)
    assert rc0 == ReasonCode.GRANTED_QOS_0
    assert rc1 == ReasonCode.GRANTED_QOS_1

    # Publish several QoS 1 messages.
    s_pub, _ = mqtt_connect("shmixq_pub", clean_start=True)
    num_messages = 10
    for i in range(num_messages):
        pid = i + 1
        s_pub.sendall(create_publish_packet(topic, f"mq-{i}".encode(), qos=1, packet_id=pid))
        recv_packet(s_pub, timeout=5)

    # Collect from both, ACKing QoS 1 messages.
    msgs_q0 = []
    msgs_q1 = []
    deadline = time.time() + 10
    while len(msgs_q0) + len(msgs_q1) < num_messages and time.time() < deadline:
        for sock, msgs, expected_qos in [(s_q0, msgs_q0, 0), (s_q1, msgs_q1, 1)]:
            pkt = recv_packet(sock, timeout=0.3)
            if pkt is None:
                continue
            ptype = (pkt[0] & 0xF0) >> 4
            if ptype == MQTTControlPacket.PUBLISH:
                t, p, qos, dup, retain, pid, props = validate_publish(pkt)
                msgs.append(qos)
                if qos == 1 and pid is not None:
                    sock.sendall(create_puback_packet(pid))

    total = len(msgs_q0) + len(msgs_q1)
    assert total == num_messages, (
        f"Expected {num_messages} total, got {total}"
    )

    # Verify QoS levels: Q0 member should get QoS 0, Q1 member should get QoS 1.
    for qos in msgs_q0:
        assert qos == 0, f"QoS 0 member received QoS {qos}"
    for qos in msgs_q1:
        assert qos == 1, f"QoS 1 member received QoS {qos}"

    # Both should have received at least 1 (round-robin).
    assert len(msgs_q0) >= 1, "QoS 0 member got nothing"
    assert len(msgs_q1) >= 1, "QoS 1 member got nothing"

    s_q0.close()
    s_q1.close()
    s_pub.close()


# ---------------------------------------------------------------------------
# Malformed: # wildcard in group name (§4.8.2)
# ---------------------------------------------------------------------------

def test_shared_sub_malformed_hash_in_group():
    """$share/gr#up/topic — multi-level wildcard in group name should be rejected."""
    s, _ = mqtt_connect("shmal_hashgrp", clean_start=True)
    s.sendall(create_subscribe_packet(1, "$share/gr#up/topic", qos=0))
    raw = recv_packet(s, timeout=5)
    assert raw is not None
    rcs = validate_suback(raw, 1)
    assert rcs[0] == ReasonCode.TOPIC_FILTER_INVALID, (
        f"Expected TOPIC_FILTER_INVALID (0x8F), got {rcs[0]:#04x}"
    )
    s.close()


# ---------------------------------------------------------------------------
# Client with both shared and regular subscription on same topic
# ---------------------------------------------------------------------------

def test_shared_sub_client_also_regular():
    """A client subscribed to both $share/g/topic and topic receives the message."""
    topic = "test/shared/dual"
    shared_filter = "$share/grp_dual/" + topic

    s_sub, _ = mqtt_connect("shdual_sub", clean_start=True)
    mqtt_subscribe(s_sub, 1, shared_filter, qos=0)
    mqtt_subscribe(s_sub, 2, topic, qos=0)

    s_pub, _ = mqtt_connect("shdual_pub", clean_start=True)
    s_pub.sendall(create_publish_packet(topic, b"dual-msg", qos=0))

    # Client must receive at least one copy (the regular subscription guarantees it).
    msgs = collect_publishes(s_sub, 2, timeout=3.0)
    assert len(msgs) >= 1, "Client with regular sub must receive the message"
    for m in msgs:
        assert m[0] == topic

    s_sub.close()
    s_pub.close()


# ---------------------------------------------------------------------------
# Member rejoins after clean disconnect + clean_start reconnect
# ---------------------------------------------------------------------------

def test_shared_sub_rejoin_after_clean_disconnect():
    """A member disconnects (session_expiry=0), reconnects, re-subscribes, and
    participates in round-robin again."""
    topic = "test/shared/rejoin"
    shared_filter = "$share/grp_rejoin/" + topic

    s_a, _ = mqtt_connect("shrejoin_a", clean_start=True)
    s_b, _ = mqtt_connect("shrejoin_b", clean_start=True)
    mqtt_subscribe(s_a, 1, shared_filter, qos=0)
    mqtt_subscribe(s_b, 1, shared_filter, qos=0)

    # Disconnect A cleanly.
    s_a.sendall(create_disconnect_packet())
    s_a.close()
    time.sleep(0.5)

    # Reconnect A with clean_start, re-subscribe.
    s_a2, _ = mqtt_connect("shrejoin_a", clean_start=True)
    mqtt_subscribe(s_a2, 1, shared_filter, qos=0)

    # Publish and verify both participate.
    s_pub, _ = mqtt_connect("shrejoin_pub", clean_start=True)
    num_messages = 10
    for i in range(num_messages):
        s_pub.sendall(create_publish_packet(topic, f"rj-{i}".encode(), qos=0))

    msgs_a = collect_publishes(s_a2, num_messages, timeout=5.0)
    msgs_b = collect_publishes(s_b, num_messages, timeout=2.0)
    total = len(msgs_a) + len(msgs_b)
    assert total == num_messages, f"Expected {num_messages}, got {total}"
    assert len(msgs_a) >= 1, "Rejoined A should receive messages"
    assert len(msgs_b) >= 1, "B should still receive messages"

    s_a2.close()
    s_b.close()
    s_pub.close()
