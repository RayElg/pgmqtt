"""
MQTT 3.1.1 protocol integration tests for pgmqtt.

Covers: connect/connack, QoS 0 pub/sub, QoS 1 pub/sub, wildcard subscriptions,
retained messages, will messages (LWT), unsubscribe, clean disconnect,
cross-version interoperability (v5 <-> v3.1.1), packet format compliance,
protocol state machine, and edge cases.
"""

import socket
import struct
import time

from proto_utils import (
    create_connect_packet,
    create_subscribe_packet,
    create_unsubscribe_packet,
    create_publish_packet,
    create_puback_packet,
    create_disconnect_packet,
    create_pingreq_packet,
    recv_packet,
    validate_connack,
    validate_suback,
    validate_unsuback,
    validate_publish,
    validate_puback,
    validate_pingresp,
    decode_variable_byte_integer,
    encode_variable_byte_integer,
    encode_utf8_string,
    MQTTControlPacket,
    ReasonCode,
    MQTT_HOST,
    MQTT_PORT,
)

V4 = 4  # MQTT 3.1.1 protocol version


# -- Helpers ------------------------------------------------------------------

def v4_connect(client_id, clean_start=True, keep_alive=60,
               will_topic=None, will_payload=None, will_qos=0, will_retain=False):
    """Open TCP socket, send v3.1.1 CONNECT, validate CONNACK, return socket."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet(
        client_id,
        clean_start=clean_start,
        keep_alive=keep_alive,
        will_topic=will_topic,
        will_payload=will_payload,
        will_qos=will_qos,
        will_retain=will_retain,
        protocol_version=V4,
    ))
    resp = recv_packet(s, timeout=5)
    assert resp is not None, f"[{client_id}] No CONNACK received"
    sp, rc, _ = validate_connack(resp, protocol_version=V4)
    assert rc == 0x00, f"[{client_id}] CONNACK return code={rc:#04x}"
    return s


def v4_subscribe(sock, packet_id, topic, qos=0):
    """Subscribe on a v3.1.1 connection and validate SUBACK."""
    sock.sendall(create_subscribe_packet(packet_id, topic, qos=qos, protocol_version=V4))
    suback = recv_packet(sock, timeout=5)
    assert suback is not None, "No SUBACK received"
    reason_codes = validate_suback(suback, packet_id, protocol_version=V4)
    assert len(reason_codes) >= 1, "SUBACK has no return codes"
    assert reason_codes[0] != 0x80, f"Subscription failed: 0x{reason_codes[0]:02x}"
    return reason_codes


# -- Connection Tests ---------------------------------------------------------

def test_v311_connect_connack():
    """MQTT 3.1.1 CONNECT -> CONNACK handshake."""
    s = v4_connect("v311_connect_test")
    s.close()


def test_v311_connect_empty_client_id():
    """MQTT 3.1.1 with empty client ID and clean_session=1 gets auto-assigned ID."""
    s = v4_connect("", clean_start=True)
    # Verify the connection is live
    s.sendall(create_pingreq_packet())
    resp = recv_packet(s, timeout=5)
    assert resp is not None, "No PINGRESP after empty client ID connect"
    validate_pingresp(resp)
    s.close()


def test_v311_empty_client_id_persistent_rejected():
    """MQTT 3.1.1 §3.1.3.1: empty client ID + clean_session=0 MUST be rejected (rc=0x02)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("", clean_start=False, protocol_version=V4))
    resp = recv_packet(s, timeout=5)
    assert resp is not None, "No CONNACK received"
    sp, rc, _ = validate_connack(resp, protocol_version=V4)
    assert rc == 0x02, f"Expected Identifier Rejected (0x02), got {rc:#04x}"
    s.close()


def test_v311_ping():
    """PINGREQ/PINGRESP on a v3.1.1 connection."""
    s = v4_connect("v311_ping_test")
    for i in range(3):
        s.sendall(create_pingreq_packet())
        resp = recv_packet(s, timeout=5)
        assert resp is not None, f"No PINGRESP for ping {i+1}"
        validate_pingresp(resp)
    s.close()


def test_v311_clean_disconnect():
    """Clean v3.1.1 DISCONNECT (empty packet)."""
    s = v4_connect("v311_disconnect_test")
    s.sendall(create_disconnect_packet(protocol_version=V4))
    s.close()


# -- QoS 0 Tests -------------------------------------------------------------

def test_v311_qos0_pub_sub():
    """QoS 0 publish and subscribe on v3.1.1."""
    topic = "test/v311/qos0"
    payload = b"hello_v311_qos0"

    s_sub = v4_connect("v311_qos0_sub")
    v4_subscribe(s_sub, 1, topic, qos=0)

    s_pub = v4_connect("v311_qos0_pub")
    s_pub.sendall(create_publish_packet(topic, payload, qos=0, protocol_version=V4))

    pub_pkt = recv_packet(s_sub, timeout=5)
    assert pub_pkt is not None, "Subscriber did not receive PUBLISH"
    t, p, qos, dup, retain, pid, _ = validate_publish(pub_pkt, protocol_version=V4)
    assert t == topic
    assert p == payload
    assert qos == 0
    assert pid is None

    s_pub.close()
    s_sub.close()


def test_v311_qos0_multiple_messages():
    """Multiple QoS 0 messages delivered in order on v3.1.1."""
    topic = "test/v311/qos0/multi"
    count = 10

    s_sub = v4_connect("v311_qos0_multi_sub")
    v4_subscribe(s_sub, 1, topic, qos=0)

    s_pub = v4_connect("v311_qos0_multi_pub")
    for i in range(count):
        s_pub.sendall(create_publish_packet(topic, f"msg_{i}".encode(), qos=0, protocol_version=V4))

    received = []
    for _ in range(count):
        pkt = recv_packet(s_sub, timeout=5)
        if pkt is None:
            break
        t, p, *_ = validate_publish(pkt, protocol_version=V4)
        received.append(p.decode())

    assert len(received) == count, f"Expected {count} messages, got {len(received)}"

    s_pub.close()
    s_sub.close()


# -- QoS 1 Tests -------------------------------------------------------------

def test_v311_qos1_pub_sub():
    """QoS 1 publish and subscribe on v3.1.1 with PUBACK."""
    topic = "test/v311/qos1"
    payload = b"hello_v311_qos1"

    s_sub = v4_connect("v311_qos1_sub")
    codes = v4_subscribe(s_sub, 1, topic, qos=1)
    assert codes[0] == 0x01, f"Expected granted QoS 1 (0x01), got {codes[0]:#04x}"

    s_pub = v4_connect("v311_qos1_pub")
    s_pub.sendall(create_publish_packet(topic, payload, qos=1, packet_id=100, protocol_version=V4))

    # Publisher gets PUBACK
    puback = recv_packet(s_pub, timeout=5)
    assert puback is not None, "Publisher did not receive PUBACK"
    rc = validate_puback(puback, 100)
    assert rc == 0x00, f"PUBACK reason_code={rc:#04x}"

    # Subscriber receives
    pub_pkt = recv_packet(s_sub, timeout=5)
    assert pub_pkt is not None, "Subscriber did not receive PUBLISH"
    t, p, qos, dup, retain, pid, _ = validate_publish(pub_pkt, protocol_version=V4)
    assert t == topic
    assert p == payload
    assert qos == 1
    assert pid is not None

    # Subscriber ACKs
    s_sub.sendall(create_puback_packet(pid))

    s_pub.close()
    s_sub.close()


def test_v311_qos1_multiple():
    """Multiple QoS 1 messages with unique packet IDs on v3.1.1."""
    topic = "test/v311/qos1/multi"
    count = 5

    s_sub = v4_connect("v311_qos1m_sub")
    v4_subscribe(s_sub, 1, topic, qos=1)

    s_pub = v4_connect("v311_qos1m_pub")
    for i in range(count):
        pid = 200 + i
        s_pub.sendall(create_publish_packet(topic, f"q1msg_{i}".encode(), qos=1, packet_id=pid, protocol_version=V4))
        puback = recv_packet(s_pub, timeout=5)
        assert puback is not None, f"No PUBACK for packet_id={pid}"
        validate_puback(puback, pid)

    received = 0
    for _ in range(count):
        pkt = recv_packet(s_sub, timeout=5)
        if pkt is None:
            break
        t, p, qos, dup, retain, pid, _ = validate_publish(pkt, protocol_version=V4)
        assert qos == 1
        s_sub.sendall(create_puback_packet(pid))
        received += 1

    assert received == count, f"Expected {count} QoS 1 messages, got {received}"

    s_pub.close()
    s_sub.close()


# -- Wildcard Subscription Tests ----------------------------------------------

def test_v311_wildcard_hash():
    """Multi-level wildcard (#) subscription on v3.1.1."""
    s_sub = v4_connect("v311_wild_hash_sub")
    v4_subscribe(s_sub, 1, "test/v311/wild/#", qos=0)

    s_pub = v4_connect("v311_wild_hash_pub")
    s_pub.sendall(create_publish_packet("test/v311/wild/a/b", b"deep", qos=0, protocol_version=V4))

    pkt = recv_packet(s_sub, timeout=5)
    assert pkt is not None, "No PUBLISH for # wildcard"
    t, p, *_ = validate_publish(pkt, protocol_version=V4)
    assert t == "test/v311/wild/a/b"
    assert p == b"deep"

    s_pub.close()
    s_sub.close()


def test_v311_wildcard_plus():
    """Single-level wildcard (+) subscription on v3.1.1."""
    s_sub = v4_connect("v311_wild_plus_sub")
    v4_subscribe(s_sub, 1, "test/v311/plus/+/data", qos=0)

    s_pub = v4_connect("v311_wild_plus_pub")
    s_pub.sendall(create_publish_packet("test/v311/plus/sensor1/data", b"val1", qos=0, protocol_version=V4))

    pkt = recv_packet(s_sub, timeout=5)
    assert pkt is not None, "No PUBLISH for + wildcard"
    t, p, *_ = validate_publish(pkt, protocol_version=V4)
    assert t == "test/v311/plus/sensor1/data"
    assert p == b"val1"

    # Should NOT match deeper topic
    s_pub.sendall(create_publish_packet("test/v311/plus/sensor1/extra/data", b"nope", qos=0, protocol_version=V4))
    pkt2 = recv_packet(s_sub, timeout=1)
    assert pkt2 is None, "Should not match deeper topic with single-level wildcard"

    s_pub.close()
    s_sub.close()


# -- Retained Message Tests ---------------------------------------------------

def test_v311_retained_message():
    """Retained message delivered to new v3.1.1 subscriber."""
    topic = "test/v311/retain"
    payload = b"retained_v311"

    # Publish retained
    s_pub = v4_connect("v311_retain_pub")
    s_pub.sendall(create_publish_packet(topic, payload, qos=0, retain=True, protocol_version=V4))
    time.sleep(0.5)
    s_pub.close()

    # New subscriber should get the retained message
    s_sub = v4_connect("v311_retain_sub")
    v4_subscribe(s_sub, 1, topic, qos=0)

    pkt = recv_packet(s_sub, timeout=5)
    assert pkt is not None, "Retained message not received"
    t, p, qos, dup, retain, pid, _ = validate_publish(pkt, protocol_version=V4)
    assert t == topic
    assert p == payload
    assert retain, "Retained flag should be set"

    # Clear retained
    s_clear = v4_connect("v311_retain_clear")
    s_clear.sendall(create_publish_packet(topic, b"", qos=0, retain=True, protocol_version=V4))
    s_clear.close()

    s_sub.close()


# -- Will Message (LWT) Tests ------------------------------------------------

def test_v311_lwt_abrupt_disconnect():
    """Last Will delivered on ungraceful v3.1.1 disconnect."""
    will_topic = "test/v311/will/abrupt"
    will_payload = b"v311_client_died"

    s_sub = v4_connect("v311_lwt_sub")
    v4_subscribe(s_sub, 1, will_topic, qos=0)

    s_will = v4_connect(
        "v311_lwt_client",
        will_topic=will_topic,
        will_payload=will_payload,
        will_qos=0,
    )

    # Ungraceful disconnect
    s_will.close()

    pkt = recv_packet(s_sub, timeout=5)
    assert pkt is not None, "LWT not received after ungraceful disconnect"
    t, p, *_ = validate_publish(pkt, protocol_version=V4)
    assert t == will_topic
    assert p == will_payload

    s_sub.close()


def test_v311_lwt_clean_disconnect_suppresses():
    """Clean v3.1.1 DISCONNECT suppresses LWT."""
    will_topic = "test/v311/will/clean"
    will_payload = b"should_not_see"

    s_sub = v4_connect("v311_lwt_clean_sub")
    v4_subscribe(s_sub, 1, will_topic, qos=0)

    s_will = v4_connect(
        "v311_lwt_clean_client",
        will_topic=will_topic,
        will_payload=will_payload,
        will_qos=0,
    )

    # Clean disconnect
    s_will.sendall(create_disconnect_packet(protocol_version=V4))
    s_will.close()

    pkt = recv_packet(s_sub, timeout=2)
    assert pkt is None, "LWT should NOT be delivered on clean disconnect"

    s_sub.close()


# -- Unsubscribe Tests --------------------------------------------------------

def test_v311_unsubscribe():
    """UNSUBSCRIBE stops message delivery on v3.1.1."""
    topic = "test/v311/unsub"

    s = v4_connect("v311_unsub_client")
    v4_subscribe(s, 1, topic, qos=0)

    # Publish and verify we receive
    s_pub = v4_connect("v311_unsub_pub")
    s_pub.sendall(create_publish_packet(topic, b"before_unsub", qos=0, protocol_version=V4))
    pkt = recv_packet(s, timeout=5)
    assert pkt is not None, "Should receive before unsubscribe"

    # Unsubscribe
    s.sendall(create_unsubscribe_packet(2, topic, protocol_version=V4))
    unsuback = recv_packet(s, timeout=5)
    assert unsuback is not None, "No UNSUBACK received"
    validate_unsuback(unsuback, 2, protocol_version=V4)

    # Publish again -- should NOT receive
    s_pub.sendall(create_publish_packet(topic, b"after_unsub", qos=0, protocol_version=V4))
    pkt2 = recv_packet(s, timeout=2)
    assert pkt2 is None, "Should NOT receive after unsubscribe"

    s_pub.close()
    s.close()


# -- Cross-Version Interoperability Tests -------------------------------------

def test_cross_version_v5_pub_v311_sub():
    """v5 publisher -> v3.1.1 subscriber."""
    topic = "test/cross/v5_to_v311"
    payload = b"from_v5"

    # v3.1.1 subscriber
    s_sub = v4_connect("cross_v311_sub")
    v4_subscribe(s_sub, 1, topic, qos=0)

    # v5 publisher
    s_pub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_pub.settimeout(5)
    s_pub.connect((MQTT_HOST, MQTT_PORT))
    s_pub.sendall(create_connect_packet("cross_v5_pub", protocol_version=5))
    resp = recv_packet(s_pub, timeout=5)
    assert resp is not None
    validate_connack(resp, protocol_version=5)

    s_pub.sendall(create_publish_packet(topic, payload, qos=0, protocol_version=5))

    # v3.1.1 subscriber receives (broker strips v5 properties)
    pkt = recv_packet(s_sub, timeout=5)
    assert pkt is not None, "v3.1.1 subscriber did not receive from v5 publisher"
    t, p, *_ = validate_publish(pkt, protocol_version=V4)
    assert t == topic
    assert p == payload

    s_pub.close()
    s_sub.close()


def test_cross_version_v311_pub_v5_sub():
    """v3.1.1 publisher -> v5 subscriber."""
    topic = "test/cross/v311_to_v5"
    payload = b"from_v311"

    # v5 subscriber
    s_sub = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_sub.settimeout(5)
    s_sub.connect((MQTT_HOST, MQTT_PORT))
    s_sub.sendall(create_connect_packet("cross_v5_sub", protocol_version=5))
    resp = recv_packet(s_sub, timeout=5)
    assert resp is not None
    validate_connack(resp, protocol_version=5)
    s_sub.sendall(create_subscribe_packet(1, topic, qos=0, protocol_version=5))
    suback = recv_packet(s_sub, timeout=5)
    validate_suback(suback, 1, protocol_version=5)

    # v3.1.1 publisher
    s_pub = v4_connect("cross_v311_pub")
    s_pub.sendall(create_publish_packet(topic, payload, qos=0, protocol_version=V4))

    # v5 subscriber receives
    pkt = recv_packet(s_sub, timeout=5)
    assert pkt is not None, "v5 subscriber did not receive from v3.1.1 publisher"
    t, p, *_ = validate_publish(pkt, protocol_version=5)
    assert t == topic
    assert p == payload

    s_pub.close()
    s_sub.close()


def test_cross_version_qos1():
    """QoS 1 cross-version: v3.1.1 pub -> v5 sub and v5 pub -> v3.1.1 sub."""
    topic_a = "test/cross/qos1/v311_to_v5"
    topic_b = "test/cross/qos1/v5_to_v311"

    # v5 client
    s_v5 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_v5.settimeout(5)
    s_v5.connect((MQTT_HOST, MQTT_PORT))
    s_v5.sendall(create_connect_packet("cross_qos1_v5", protocol_version=5))
    validate_connack(recv_packet(s_v5, timeout=5), protocol_version=5)
    s_v5.sendall(create_subscribe_packet(1, topic_a, qos=1, protocol_version=5))
    validate_suback(recv_packet(s_v5, timeout=5), 1, protocol_version=5)

    # v3.1.1 client
    s_v4 = v4_connect("cross_qos1_v311")
    v4_subscribe(s_v4, 1, topic_b, qos=1)

    # v3.1.1 -> v5 (QoS 1)
    s_v4.sendall(create_publish_packet(topic_a, b"v311_qos1", qos=1, packet_id=300, protocol_version=V4))
    puback = recv_packet(s_v4, timeout=5)
    assert puback is not None, "v3.1.1 publisher did not get PUBACK"
    validate_puback(puback, 300)

    pkt = recv_packet(s_v5, timeout=5)
    assert pkt is not None, "v5 subscriber did not receive QoS 1 from v3.1.1"
    t, p, qos, _, _, pid, _ = validate_publish(pkt, protocol_version=5)
    assert t == topic_a and p == b"v311_qos1" and qos == 1
    s_v5.sendall(create_puback_packet(pid))

    # v5 -> v3.1.1 (QoS 1)
    s_v5.sendall(create_publish_packet(topic_b, b"v5_qos1", qos=1, packet_id=301, protocol_version=5))
    puback2 = recv_packet(s_v5, timeout=5)
    assert puback2 is not None
    validate_puback(puback2, 301)

    pkt2 = recv_packet(s_v4, timeout=5)
    assert pkt2 is not None, "v3.1.1 subscriber did not receive QoS 1 from v5"
    t2, p2, qos2, _, _, pid2, _ = validate_publish(pkt2, protocol_version=V4)
    assert t2 == topic_b and p2 == b"v5_qos1" and qos2 == 1
    s_v4.sendall(create_puback_packet(pid2))

    s_v5.close()
    s_v4.close()


# -- Session Takeover ---------------------------------------------------------

def test_v311_session_takeover():
    """Second v3.1.1 connection with same client_id disconnects the first."""
    client_id = "v311_takeover"

    s1 = v4_connect(client_id)
    s2 = v4_connect(client_id)

    # First connection should be closed
    resp = recv_packet(s1, timeout=3)
    if resp is not None:
        ptype = (resp[0] >> 4)
        assert ptype == MQTTControlPacket.DISCONNECT, \
            f"Expected DISCONNECT, got packet type {ptype}"
    else:
        # Connection was simply closed -- also valid
        pass

    # Second connection is alive
    s2.sendall(create_pingreq_packet())
    pingresp = recv_packet(s2, timeout=5)
    assert pingresp is not None, "Second connection should be alive"
    validate_pingresp(pingresp)

    s1.close()
    s2.close()


# -- Additional Helpers -------------------------------------------------------

def _v5_connect(client_id, clean_start=True, keep_alive=60):
    """Open TCP socket, send v5 CONNECT, validate CONNACK, return socket."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet(client_id, clean_start=clean_start,
                                     keep_alive=keep_alive, protocol_version=5))
    resp = recv_packet(s, timeout=5)
    assert resp is not None, f"[{client_id}] No CONNACK received"
    sp, rc, _ = validate_connack(resp, protocol_version=5)
    assert rc == 0x00, f"[{client_id}] CONNACK reason={rc:#04x}"
    return s


def _expect_disconnect_or_close(s, timeout=2.0):
    """Expect either connection close or DISCONNECT packet."""
    data = recv_packet(s, timeout=timeout)
    if not data:
        return True  # Connection closed
    if data[0] >> 4 == MQTTControlPacket.DISCONNECT:
        return True
    return False


# -- CONNACK Return Code Verification ----------------------------------------

def test_v311_connack_return_code_success():
    """Verify v3.1.1 CONNACK uses return code 0x00 (ACCEPTED), not v5 reason code."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("v311_adv_connack", protocol_version=V4))
    resp = recv_packet(s, timeout=5)
    assert resp is not None, "No CONNACK received"

    ptype = (resp[0] >> 4)
    assert ptype == MQTTControlPacket.CONNACK
    return_code = resp[3]
    assert return_code == 0x00, f"Expected ACCEPTED (0x00), got {return_code:#04x}"
    s.close()


def test_v311_connack_unsupported_version():
    """Send protocol version 3 (old MQIsdp era) -- expect CONNACK with return code 0x01."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect((MQTT_HOST, MQTT_PORT))

    protocol_name = encode_utf8_string("MQTT")
    protocol_version = b'\x03'
    connect_flags = b'\x02'
    keep_alive = struct.pack('!H', 60)
    client_id = encode_utf8_string("v311_adv_badver")

    vh = protocol_name + protocol_version + connect_flags + keep_alive
    payload = client_id
    remaining = len(vh) + len(payload)
    fixed = bytes([(MQTTControlPacket.CONNECT << 4)]) + encode_variable_byte_integer(remaining)
    s.sendall(fixed + vh + payload)

    resp = recv_packet(s, timeout=5)
    if resp is not None:
        ptype = (resp[0] >> 4)
        if ptype == MQTTControlPacket.CONNACK:
            return_code = resp[3]
            assert return_code == 0x01, \
                f"Expected UNACCEPTABLE_PROTOCOL (0x01), got {return_code:#04x}"
    s.close()


def test_v311_connack_packet_format():
    """Verify v3.1.1 CONNACK is exactly 4 bytes (no properties section)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("v311_adv_fmt", protocol_version=V4))
    resp = recv_packet(s, timeout=5)
    assert resp is not None, "No CONNACK received"

    ptype = (resp[0] >> 4)
    assert ptype == MQTTControlPacket.CONNACK
    assert len(resp) == 4, \
        f"v3.1.1 CONNACK should be 4 bytes (no properties), got {len(resp)} bytes"
    assert resp[1] == 0x02, f"CONNACK remaining length should be 2, got {resp[1]}"
    s.close()


# -- Packet Format Compliance ------------------------------------------------

def test_v311_unsuback_no_reason_codes():
    """v3.1.1 UNSUBACK should be exactly 4 bytes: fixed header + packet_id, no reason codes."""
    topic = "test/v311/adv/unsuback"

    s = v4_connect("v311_adv_unsuback")
    s.sendall(create_subscribe_packet(1, topic, qos=0, protocol_version=V4))
    suback = recv_packet(s, timeout=5)
    assert suback is not None
    validate_suback(suback, 1, protocol_version=V4)

    s.sendall(create_unsubscribe_packet(2, topic, protocol_version=V4))
    unsuback = recv_packet(s, timeout=5)
    assert unsuback is not None, "No UNSUBACK received"

    ptype = (unsuback[0] >> 4)
    assert ptype == MQTTControlPacket.UNSUBACK
    assert len(unsuback) == 4, \
        f"v3.1.1 UNSUBACK should be 4 bytes, got {len(unsuback)}"
    assert unsuback[1] == 0x02, f"UNSUBACK remaining length should be 2, got {unsuback[1]}"
    pid = struct.unpack_from('!H', unsuback, 2)[0]
    assert pid == 2, f"UNSUBACK packet ID should be 2, got {pid}"

    s.close()


def test_v311_publish_no_properties():
    """PUBLISH delivered to v3.1.1 subscriber should have no properties section."""
    topic = "test/v311/adv/noprops"
    payload = b"noprops_test"

    s_sub = v4_connect("v311_adv_noprops_sub")
    s_sub.sendall(create_subscribe_packet(1, topic, qos=0, protocol_version=V4))
    suback = recv_packet(s_sub, timeout=5)
    validate_suback(suback, 1, protocol_version=V4)

    s_pub = v4_connect("v311_adv_noprops_pub")
    s_pub.sendall(create_publish_packet(topic, payload, qos=0, protocol_version=V4))

    pkt = recv_packet(s_sub, timeout=5)
    assert pkt is not None, "No PUBLISH received"

    remain_len, offset = decode_variable_byte_integer(pkt, 1)
    header_len = offset
    topic_len = struct.unpack_from('!H', pkt, offset)[0]
    offset += 2 + topic_len
    extracted_payload = pkt[offset:header_len + remain_len]
    assert extracted_payload == payload, \
        f"Payload mismatch -- properties section may be present: got {extracted_payload!r}"

    s_pub.close()
    s_sub.close()


def test_v311_suback_return_codes():
    """v3.1.1 SUBACK return codes must be 0x00, 0x01, 0x02 (granted QoS) or 0x80 (failure)."""
    s = v4_connect("v311_adv_suback_rc")

    s.sendall(create_subscribe_packet(1, "test/v311/adv/subrc0", qos=0, protocol_version=V4))
    suback = recv_packet(s, timeout=5)
    assert suback is not None
    codes = validate_suback(suback, 1, protocol_version=V4)
    for code in codes:
        assert code in (0x00, 0x01, 0x02, 0x80), \
            f"v3.1.1 SUBACK code must be 0x00-0x02 or 0x80, got {code:#04x}"
    assert codes[0] == 0x00, f"Expected granted QoS 0 (0x00), got {codes[0]:#04x}"

    s.sendall(create_subscribe_packet(2, "test/v311/adv/subrc1", qos=1, protocol_version=V4))
    suback = recv_packet(s, timeout=5)
    assert suback is not None
    codes = validate_suback(suback, 2, protocol_version=V4)
    assert codes[0] == 0x01, f"Expected granted QoS 1 (0x01), got {codes[0]:#04x}"

    s.close()


# -- Protocol State Machine ---------------------------------------------------

def test_v311_subscribe_before_connect():
    """Sending SUBSCRIBE before CONNECT should close the connection."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect((MQTT_HOST, MQTT_PORT))

    s.sendall(create_subscribe_packet(1, "test/nope", qos=0, protocol_version=V4))

    assert _expect_disconnect_or_close(s), "Connection should be closed after SUBSCRIBE before CONNECT"
    s.close()


def test_v311_publish_before_connect():
    """Sending PUBLISH before CONNECT should close the connection."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect((MQTT_HOST, MQTT_PORT))

    s.sendall(create_publish_packet("test/nope", b"data", qos=0, protocol_version=V4))

    assert _expect_disconnect_or_close(s), "Connection should be closed after PUBLISH before CONNECT"
    s.close()


def test_v311_duplicate_connect():
    """Sending a second CONNECT on the same connection should close it."""
    s = v4_connect("v311_adv_dup_connect")

    s.sendall(create_connect_packet("v311_adv_dup_connect2", protocol_version=V4))

    assert _expect_disconnect_or_close(s), "Connection should be closed after duplicate CONNECT"
    s.close()


def test_v311_unexpected_puback():
    """PUBACK for a non-existent packet ID should be handled gracefully (no crash)."""
    s = v4_connect("v311_adv_unexp_puback")

    s.sendall(create_puback_packet(9999))

    s.sendall(create_pingreq_packet())
    resp = recv_packet(s, timeout=3)
    if resp is not None:
        ptype = (resp[0] >> 4)
        assert ptype in (MQTTControlPacket.PINGRESP, MQTTControlPacket.DISCONNECT), \
            f"Unexpected packet type {ptype} after bogus PUBACK"
    s.close()


# -- Topic & Payload Validation -----------------------------------------------

def test_v311_publish_to_wildcard_topic():
    """PUBLISH to a topic containing wildcards should cause disconnect."""
    s = v4_connect("v311_adv_wild_pub")

    s.sendall(create_publish_packet("test/v311/#", b"bad", qos=0, protocol_version=V4))

    assert _expect_disconnect_or_close(s, timeout=3), \
        "Should disconnect after PUBLISH to wildcard topic"
    s.close()


def test_v311_publish_to_plus_wildcard():
    """PUBLISH to a topic containing + wildcard should cause disconnect."""
    s = v4_connect("v311_adv_plus_pub")

    s.sendall(create_publish_packet("test/+/data", b"bad", qos=0, protocol_version=V4))

    assert _expect_disconnect_or_close(s, timeout=3), \
        "Should disconnect after PUBLISH to + wildcard topic"
    s.close()


def test_v311_topic_with_null_char():
    """Topic containing null character should cause disconnect."""
    s = v4_connect("v311_adv_null_topic")

    s.sendall(create_publish_packet("test/null\x00char", b"bad", qos=0, protocol_version=V4))

    assert _expect_disconnect_or_close(s, timeout=3), \
        "Should disconnect after PUBLISH with null char in topic"
    s.close()


def test_v311_empty_topic_publish():
    """PUBLISH with empty topic string should be rejected."""
    s = v4_connect("v311_adv_empty_topic")

    s.sendall(create_publish_packet("", b"bad", qos=0, protocol_version=V4))

    assert _expect_disconnect_or_close(s, timeout=3), \
        "Should disconnect after PUBLISH with empty topic"
    s.close()


# -- v3.1.1-Specific Feature Boundaries ---------------------------------------

def test_v311_shared_subscription_rejected():
    """v3.1.1 client subscribing to $share/group/topic should get failure (0x80)."""
    s = v4_connect("v311_adv_shared_sub")

    s.sendall(create_subscribe_packet(1, "$share/grp/test/v311/shared", qos=0, protocol_version=V4))
    suback = recv_packet(s, timeout=5)
    assert suback is not None, "No SUBACK received"
    codes = validate_suback(suback, 1, protocol_version=V4)
    assert len(codes) >= 1
    assert codes[0] == 0x80, \
        f"Shared subscription for v3.1.1 should fail with 0x80, got {codes[0]:#04x}"

    s.close()


def test_v311_session_takeover_disconnects_first():
    """Second v3.1.1 connection with same client_id disconnects the first."""
    client_id = "v311_adv_takeover"

    s1 = v4_connect(client_id)
    s2 = v4_connect(client_id)

    resp = recv_packet(s1, timeout=3)
    if resp is not None:
        ptype = (resp[0] >> 4)
        assert ptype == MQTTControlPacket.DISCONNECT, \
            f"Expected DISCONNECT on old connection, got packet type {ptype}"

    s2.sendall(create_pingreq_packet())
    pingresp = recv_packet(s2, timeout=5)
    assert pingresp is not None, "Second connection should be alive"
    validate_pingresp(pingresp)

    s1.close()
    s2.close()


def test_v311_clean_session_false_resumption():
    """Connect with clean_session=0, disconnect, reconnect -- session_present should be 1."""
    client_id = "v311_adv_session_resume"
    topic = "test/v311/adv/session_resume"

    s0 = v4_connect(client_id, clean_start=True)
    s0.sendall(create_disconnect_packet(protocol_version=V4))
    s0.close()
    time.sleep(0.3)

    s1 = v4_connect(client_id, clean_start=False)
    s1.sendall(create_subscribe_packet(1, topic, qos=1, protocol_version=V4))
    suback = recv_packet(s1, timeout=5)
    assert suback is not None
    validate_suback(suback, 1, protocol_version=V4)
    s1.sendall(create_disconnect_packet(protocol_version=V4))
    s1.close()
    time.sleep(0.3)

    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.settimeout(5)
    s2.connect((MQTT_HOST, MQTT_PORT))
    s2.sendall(create_connect_packet(
        client_id, clean_start=False, protocol_version=V4,
    ))
    resp = recv_packet(s2, timeout=5)
    assert resp is not None, "No CONNACK on reconnect"
    sp, rc, _ = validate_connack(resp, protocol_version=V4)
    assert rc == 0x00, f"CONNACK return code={rc:#04x}"
    assert sp is True, "session_present should be True on reconnect with clean_session=0"

    s2.sendall(create_disconnect_packet(protocol_version=V4))
    s2.close()


def test_v311_clean_session_true_clears():
    """Connect with clean_session=1 should clear previous session state."""
    client_id = "v311_adv_session_clear"
    topic = "test/v311/adv/session_clear"

    s1 = v4_connect(client_id, clean_start=False)
    s1.sendall(create_subscribe_packet(1, topic, qos=1, protocol_version=V4))
    suback = recv_packet(s1, timeout=5)
    validate_suback(suback, 1, protocol_version=V4)
    s1.sendall(create_disconnect_packet(protocol_version=V4))
    s1.close()
    time.sleep(0.3)

    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.settimeout(5)
    s2.connect((MQTT_HOST, MQTT_PORT))
    s2.sendall(create_connect_packet(client_id, clean_start=True, protocol_version=V4))
    resp = recv_packet(s2, timeout=5)
    assert resp is not None
    sp, rc, _ = validate_connack(resp, protocol_version=V4)
    assert rc == 0x00
    assert sp is False, "session_present should be False with clean_session=1"

    s_pub = v4_connect("v311_adv_session_clear_pub")
    s_pub.sendall(create_publish_packet(topic, b"should_not_arrive", qos=0, protocol_version=V4))
    pkt = recv_packet(s2, timeout=2)
    assert pkt is None, "Should NOT receive messages after clean_session=1 cleared subscriptions"

    s_pub.close()
    s2.close()


# -- Cross-Version Session Edge Cases ----------------------------------------

def test_v311_session_then_v5_reconnect():
    """v3.1.1 client creates session, v5 client reconnects with same client_id."""
    client_id = "v311_adv_cross_v4_to_v5"
    topic = "test/v311/adv/cross_session_a"

    s1 = v4_connect(client_id, clean_start=False)
    s1.sendall(create_subscribe_packet(1, topic, qos=1, protocol_version=V4))
    suback = recv_packet(s1, timeout=5)
    validate_suback(suback, 1, protocol_version=V4)
    s1.sendall(create_disconnect_packet(protocol_version=V4))
    s1.close()
    time.sleep(0.3)

    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.settimeout(5)
    s2.connect((MQTT_HOST, MQTT_PORT))
    s2.sendall(create_connect_packet(client_id, clean_start=False, protocol_version=5))
    resp = recv_packet(s2, timeout=5)
    assert resp is not None
    sp, rc, _ = validate_connack(resp, protocol_version=5)
    assert rc == 0x00, f"v5 reconnect CONNACK reason={rc:#04x}"
    assert sp is True, "session_present should be True -- session was created by v3.1.1 client"

    s_pub = v4_connect("v311_adv_cross_pub_a")
    s_pub.sendall(create_publish_packet(topic, b"cross_v4_v5", qos=0, protocol_version=V4))

    pkt = recv_packet(s2, timeout=5)
    assert pkt is not None, "v5 client should receive message from inherited v3.1.1 subscription"
    t, p, *_ = validate_publish(pkt, protocol_version=5)
    assert t == topic
    assert p == b"cross_v4_v5"

    s_pub.close()
    s2.close()


def test_v5_session_then_v311_reconnect():
    """v5 client creates session, v3.1.1 client reconnects with same client_id."""
    client_id = "v311_adv_cross_v5_to_v4"
    topic = "test/v311/adv/cross_session_b"

    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s1.settimeout(5)
    s1.connect((MQTT_HOST, MQTT_PORT))
    s1.sendall(create_connect_packet(
        client_id, clean_start=False, protocol_version=5,
        properties={0x11: 300},  # Session Expiry Interval = 300s
    ))
    resp = recv_packet(s1, timeout=5)
    assert resp is not None
    validate_connack(resp, protocol_version=5)
    s1.sendall(create_subscribe_packet(1, topic, qos=1, protocol_version=5))
    suback = recv_packet(s1, timeout=5)
    validate_suback(suback, 1, protocol_version=5)
    s1.sendall(create_disconnect_packet(protocol_version=5))
    s1.close()
    time.sleep(0.3)

    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.settimeout(5)
    s2.connect((MQTT_HOST, MQTT_PORT))
    s2.sendall(create_connect_packet(client_id, clean_start=False, protocol_version=V4))
    resp = recv_packet(s2, timeout=5)
    assert resp is not None
    sp, rc, _ = validate_connack(resp, protocol_version=V4)
    assert rc == 0x00, f"v3.1.1 reconnect return code={rc:#04x}"
    assert sp is True, "session_present should be True -- session was created by v5 client"

    s_pub = _v5_connect("v311_adv_cross_pub_b")
    s_pub.sendall(create_publish_packet(topic, b"cross_v5_v4", qos=0, protocol_version=5))

    pkt = recv_packet(s2, timeout=5)
    assert pkt is not None, "v3.1.1 client should receive message from inherited v5 subscription"
    t, p, *_ = validate_publish(pkt, protocol_version=V4)
    assert t == topic
    assert p == b"cross_v5_v4"

    s_pub.close()
    s2.close()


def test_v311_pub_qos1_v5_sub_cross_ack():
    """v3.1.1 QoS 1 publisher, v5 subscriber -- verify packet formats correct per version."""
    topic = "test/v311/adv/cross_qos1_fmt"

    s_v5 = _v5_connect("v311_adv_cross_qos1_v5sub")
    s_v5.sendall(create_subscribe_packet(1, topic, qos=1, protocol_version=5))
    suback = recv_packet(s_v5, timeout=5)
    validate_suback(suback, 1, protocol_version=5)

    s_v4 = v4_connect("v311_adv_cross_qos1_v4pub")

    s_v4.sendall(create_publish_packet(topic, b"cross_qos1", qos=1, packet_id=500, protocol_version=V4))

    puback = recv_packet(s_v4, timeout=5)
    assert puback is not None, "v3.1.1 publisher did not get PUBACK"
    rc = validate_puback(puback, 500)
    assert rc == 0x00

    pkt = recv_packet(s_v5, timeout=5)
    assert pkt is not None, "v5 subscriber did not receive PUBLISH"
    t, p, qos, _, _, pid, _ = validate_publish(pkt, protocol_version=5)
    assert t == topic
    assert p == b"cross_qos1"
    assert qos == 1
    s_v5.sendall(create_puback_packet(pid))

    s_v4.close()
    s_v5.close()


# -- Malformed Packet Handling ------------------------------------------------

def test_v311_truncated_connect():
    """Send an incomplete CONNECT packet -- should timeout or close connection."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect((MQTT_HOST, MQTT_PORT))

    s.sendall(bytes([MQTTControlPacket.CONNECT << 4, 0x10]))
    s.sendall(b'\x00\x04MQTT')

    resp = recv_packet(s, timeout=3)
    if resp is not None:
        ptype = (resp[0] >> 4)
        assert ptype == MQTTControlPacket.CONNACK, \
            f"Unexpected packet type {ptype} for truncated CONNECT"
    s.close()


def test_v311_garbage_after_connect():
    """Send random garbage bytes after a valid CONNECT -- should disconnect."""
    s = v4_connect("v311_adv_garbage")

    s.sendall(b'\xff\xfe\xfd\xfc\xfb\xfa\xf9\xf8')

    assert _expect_disconnect_or_close(s, timeout=3), \
        "Should disconnect after receiving garbage bytes"
    s.close()


def test_v311_oversized_packet():
    """Send a packet claiming an excessively large remaining length."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect((MQTT_HOST, MQTT_PORT))

    fixed_header = bytes([MQTTControlPacket.CONNECT << 4, 0xFF, 0xFF, 0xFF, 0x7F])
    s.sendall(fixed_header)
    s.sendall(b'\x00' * 1024)

    recv_packet(s, timeout=5)
    s.close()
