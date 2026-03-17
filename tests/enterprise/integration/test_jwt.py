"""Integration tests for pgmqtt enterprise JWT authentication."""

import base64
import os
import socket
import struct
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from helpers.guc import set_guc, reset_guc  # noqa: E402
from helpers.jwt import make_jwt, public_key_b64url  # noqa: E402
from helpers.mqtt import (  # noqa: E402
    build_connect_packet,
    mqtt_connect,
    parse_connack,
    read_packet,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MQTT_HOST = os.environ.get("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
WS_PORT = int(os.environ.get("WS_PORT", "9001"))

# MQTT reason codes
RC_SUCCESS = 0x00
RC_NOT_AUTHORIZED = 0x87

pytestmark = pytest.mark.enterprise


# ---------------------------------------------------------------------------
# GUC helpers
# ---------------------------------------------------------------------------




# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_jwt_gucs():
    """Reset JWT GUCs before and after each test."""
    reset_guc("pgmqtt.jwt_public_key")
    reset_guc("pgmqtt.jwt_required")
    reset_guc("pgmqtt.jwt_required_ws")
    yield
    reset_guc("pgmqtt.jwt_public_key")
    reset_guc("pgmqtt.jwt_required")
    reset_guc("pgmqtt.jwt_required_ws")


@pytest.fixture()
def jwt_enabled():
    """Enable JWT with the test public key."""
    set_guc("pgmqtt.jwt_public_key", public_key_b64url())
    yield


@pytest.fixture()
def jwt_required(jwt_enabled):
    """Enable JWT and require it for all connections."""
    set_guc("pgmqtt.jwt_required", "on")
    yield


@pytest.fixture()
def jwt_required_ws_only(jwt_enabled):
    """Enable JWT and require it for WebSocket connections only."""
    set_guc("pgmqtt.jwt_required_ws", "on")
    yield


# ---------------------------------------------------------------------------
# WebSocket helpers
# ---------------------------------------------------------------------------


def _ws_handshake(
    sock: socket.socket,
    path: str = "/",
    extra_headers: str = "",
) -> None:
    """Perform a WebSocket Upgrade handshake."""
    key_b64 = base64.b64encode(b"pgmqtt-test-key-1234").decode()
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {MQTT_HOST}:{WS_PORT}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key_b64}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "Sec-WebSocket-Protocol: mqtt\r\n"
        f"{extra_headers}"
        "\r\n"
    )
    sock.sendall(request.encode())

    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(1024)
        if not chunk:
            raise ConnectionError("WS handshake: connection closed")
        buf += chunk
    assert b"101" in buf, f"WS handshake failed: {buf[:200]}"


def _ws_send_binary(sock: socket.socket, data: bytes) -> None:
    """Send a masked WebSocket binary frame."""
    import os as _os

    mask = _os.urandom(4)
    payload = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
    length = len(data)
    if length < 126:
        header = bytes([0x82, 0x80 | length]) + mask
    elif length < 65536:
        header = bytes([0x82, 0xFE]) + struct.pack("!H", length) + mask
    else:
        header = bytes([0x82, 0xFF]) + struct.pack("!Q", length) + mask
    sock.sendall(header + payload)


def _ws_recv_frame(sock: socket.socket) -> bytes:
    """Receive one unmasked WebSocket data frame payload."""
    sock.settimeout(5.0)

    def recv_exact(n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("WS recv: connection closed")
            buf += chunk
        return buf

    hdr = recv_exact(2)
    opcode = hdr[0] & 0x0F
    masked = bool(hdr[1] & 0x80)
    length = hdr[1] & 0x7F
    if length == 126:
        length = struct.unpack("!H", recv_exact(2))[0]
    elif length == 127:
        length = struct.unpack("!Q", recv_exact(8))[0]

    mask_key = recv_exact(4) if masked else b""
    payload = recv_exact(length)
    if masked:
        payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))

    if opcode == 0x8:  # Close
        raise ConnectionError("WS close frame received")
    return payload


def _ws_mqtt_connect(
    path: str = "/",
    jwt: str | None = None,
    client_id: str = "ws-test",
) -> tuple[socket.socket, int]:
    """Open a WebSocket connection and perform an MQTT CONNECT, return (sock, rc)."""
    sock = socket.create_connection((MQTT_HOST, WS_PORT), timeout=5.0)
    _ws_handshake(sock, path)

    password: bytes | None = jwt.encode("utf-8") if jwt else None
    username: str | None = "" if jwt else None

    pkt = build_connect_packet(
        client_id=client_id,
        username=username,
        password=password,
        clean_start=True,
        keep_alive=60,
    )
    _ws_send_binary(sock, pkt)

    frame = _ws_recv_frame(sock)
    rc = parse_connack(frame)
    return sock, rc


# ---------------------------------------------------------------------------
# Helper: SUBSCRIBE and PUBLISH packets
# ---------------------------------------------------------------------------


def _build_subscribe(topic_filter: str, packet_id: int = 1) -> bytes:
    """Build an MQTT 5.0 SUBSCRIBE packet."""
    tf_encoded = topic_filter.encode("utf-8")
    # Variable header: packet_id (2 bytes) + properties length (0)
    variable_header = struct.pack("!H", packet_id) + bytes([0x00])
    # Payload: topic filter (2-byte len prefix) + subscription options (QoS 0)
    payload = struct.pack("!H", len(tf_encoded)) + tf_encoded + bytes([0x00])
    remaining = variable_header + payload
    return bytes([0x82]) + bytes([len(remaining)]) + remaining


def _build_publish(topic: str, payload: bytes, packet_id: int = 0) -> bytes:
    """Build an MQTT 5.0 PUBLISH packet (QoS 0 or 1)."""
    topic_bytes = topic.encode("utf-8")
    qos = 1 if packet_id else 0
    flags = (qos << 1)
    variable_header = struct.pack("!H", len(topic_bytes)) + topic_bytes
    if qos > 0:
        variable_header += struct.pack("!H", packet_id)
    variable_header += bytes([0x00])  # properties length
    remaining = variable_header + payload
    return bytes([0x30 | flags]) + bytes([len(remaining)]) + remaining


# ---------------------------------------------------------------------------
# Tests: basic JWT auth on TCP
# ---------------------------------------------------------------------------


def test_jwt_not_required_allows_anonymous(jwt_enabled):
    """Without jwt_required, connections without a JWT are accepted."""
    sock, rc = mqtt_connect(MQTT_HOST, MQTT_PORT, client_id="anon-client")
    sock.close()
    assert rc == RC_SUCCESS


def test_jwt_required_rejects_anonymous(jwt_required):
    """jwt_required=on: connection with no JWT is rejected with NOT_AUTHORIZED."""
    sock, rc = mqtt_connect(MQTT_HOST, MQTT_PORT, client_id="no-jwt-client")
    sock.close()
    assert rc == RC_NOT_AUTHORIZED


def test_valid_jwt_accepted(jwt_required):
    """A valid JWT in the CONNECT password field is accepted."""
    token = make_jwt(sub="valid-client", exp_offset=3600)
    sock, rc = mqtt_connect(MQTT_HOST, MQTT_PORT, client_id="valid-jwt", jwt=token)
    sock.close()
    assert rc == RC_SUCCESS


def test_expired_jwt_rejected(jwt_required):
    """An expired JWT is rejected with NOT_AUTHORIZED."""
    token = make_jwt(sub="expired-client", exp_offset=-1)
    sock, rc = mqtt_connect(MQTT_HOST, MQTT_PORT, client_id="expired-jwt", jwt=token)
    sock.close()
    assert rc == RC_NOT_AUTHORIZED


def test_invalid_signature_rejected(jwt_required):
    """A JWT with a tampered signature is rejected."""
    token = make_jwt(sub="tamper-client", exp_offset=3600)
    # Corrupt the last character of the signature
    parts = token.rsplit(".", 1)
    bad_sig = parts[1][:-1] + ("A" if parts[1][-1] != "A" else "B")
    bad_token = parts[0] + "." + bad_sig
    sock, rc = mqtt_connect(MQTT_HOST, MQTT_PORT, client_id="bad-sig-jwt", jwt=bad_token)
    sock.close()
    assert rc == RC_NOT_AUTHORIZED


# ---------------------------------------------------------------------------
# Tests: topic-level claim enforcement
# ---------------------------------------------------------------------------


def test_sub_claim_enforcement(jwt_required):
    """A client with sub_claims can only subscribe to matching topics."""
    token = make_jwt(sub="sub-test", sub_claims=["allowed/topic"], pub_claims=[])
    sock, rc = mqtt_connect(MQTT_HOST, MQTT_PORT, client_id="sub-claim-test", jwt=token)
    assert rc == RC_SUCCESS

    # Allowed subscription
    sock.sendall(_build_subscribe("allowed/topic", packet_id=1))
    suback = read_packet(sock)
    assert (suback[0] >> 4) == 9, "expected SUBACK"
    # Reason code is the last byte of the payload (no props)
    assert suback[-1] == 0x00, f"expected granted QoS 0, got {suback[-1]:#04x}"

    # Disallowed subscription
    sock.sendall(_build_subscribe("other/topic", packet_id=2))
    suback2 = read_packet(sock)
    assert (suback2[0] >> 4) == 9, "expected SUBACK"
    assert suback2[-1] == 0x87, f"expected NOT_AUTHORIZED (0x87), got {suback2[-1]:#04x}"

    sock.close()


def test_pub_claim_enforcement(jwt_required):
    """A client with pub_claims can only publish to matching topics (QoS 1 gets PUBACK 0x87 on denial)."""
    token = make_jwt(sub="pub-test", sub_claims=[], pub_claims=["allowed/pub"])
    sock, rc = mqtt_connect(MQTT_HOST, MQTT_PORT, client_id="pub-claim-test", jwt=token)
    assert rc == RC_SUCCESS

    # Allowed publish (QoS 1)
    sock.sendall(_build_publish("allowed/pub", b"hello", packet_id=1))
    puback = read_packet(sock)
    assert (puback[0] >> 4) == 4, "expected PUBACK"
    # Reason code byte: for success it may be absent (implied 0) or present
    # Minimal PUBACK: type(1) + remlen(2) + pkt_id(2) = 4 bytes, no reason code byte means 0
    # With reason code byte it would be 5+ bytes
    if len(puback) >= 5:
        assert puback[4] == 0x00, f"expected PUBACK success, got {puback[4]:#04x}"

    # Disallowed publish (QoS 1)
    sock.sendall(_build_publish("other/pub", b"hello", packet_id=2))
    puback2 = read_packet(sock)
    assert (puback2[0] >> 4) == 4, "expected PUBACK"
    assert len(puback2) >= 5, "expected reason code in PUBACK"
    assert puback2[4] == 0x87, f"expected NOT_AUTHORIZED (0x87), got {puback2[4]:#04x}"

    sock.close()


def test_wildcard_sub_claim(jwt_required):
    """Wildcard sub_claims like 'sensors/+' match topics correctly."""
    token = make_jwt(sub="wildcard-test", sub_claims=["sensors/+"])
    sock, rc = mqtt_connect(MQTT_HOST, MQTT_PORT, client_id="wildcard-sub", jwt=token)
    assert rc == RC_SUCCESS

    # Should be allowed: matches sensors/+
    sock.sendall(_build_subscribe("sensors/temp", packet_id=1))
    suback = read_packet(sock)
    assert (suback[0] >> 4) == 9
    assert suback[-1] == 0x00, f"expected granted, got {suback[-1]:#04x}"

    # Should be denied: sensors/temp/deep doesn't match sensors/+
    sock.sendall(_build_subscribe("sensors/temp/deep", packet_id=2))
    suback2 = read_packet(sock)
    assert (suback2[0] >> 4) == 9
    assert suback2[-1] == 0x87, f"expected NOT_AUTHORIZED, got {suback2[-1]:#04x}"

    sock.close()


# ---------------------------------------------------------------------------
# Tests: WebSocket JWT token sources
# ---------------------------------------------------------------------------


def test_jwt_via_websocket_query_param(jwt_required):
    """JWT token in ?jwt= query parameter is accepted for WebSocket connections."""
    token = make_jwt(sub="ws-qp-client", exp_offset=3600)
    path = f"/?jwt={token}"
    sock, rc = _ws_mqtt_connect(path=path, client_id="ws-qp-test")
    sock.close()
    assert rc == RC_SUCCESS


def test_jwt_via_websocket_header(jwt_required):
    """JWT token in Authorization: Bearer header is accepted for WebSocket connections."""
    token = make_jwt(sub="ws-hdr-client", exp_offset=3600)

    sock = socket.create_connection((MQTT_HOST, WS_PORT), timeout=5.0)
    _ws_handshake(sock, extra_headers=f"Authorization: Bearer {token}\r\n")

    # No password — JWT came from header
    pkt = build_connect_packet(client_id="ws-hdr-test", clean_start=True, keep_alive=60)
    _ws_send_binary(sock, pkt)
    frame = _ws_recv_frame(sock)
    rc = parse_connack(frame)
    sock.close()
    assert rc == RC_SUCCESS


# ---------------------------------------------------------------------------
# Tests: client_id claim enforcement
# ---------------------------------------------------------------------------


def test_jwt_client_id_matches(jwt_required):
    """JWT with client_id claim matching CONNECT client_id is accepted."""
    token = make_jwt(sub="cid-test", client_id="my-device-1")
    sock, rc = mqtt_connect(MQTT_HOST, MQTT_PORT, client_id="my-device-1", jwt=token)
    sock.close()
    assert rc == RC_SUCCESS


def test_jwt_client_id_mismatch_rejected(jwt_required):
    """JWT with client_id claim NOT matching CONNECT client_id is rejected."""
    token = make_jwt(sub="cid-test", client_id="my-device-1")
    sock, rc = mqtt_connect(MQTT_HOST, MQTT_PORT, client_id="wrong-device", jwt=token)
    sock.close()
    assert rc == RC_NOT_AUTHORIZED


def test_jwt_no_client_id_claim_allows_any(jwt_required):
    """JWT without client_id claim allows any CONNECT client_id."""
    token = make_jwt(sub="no-cid")
    sock, rc = mqtt_connect(MQTT_HOST, MQTT_PORT, client_id="anything-goes", jwt=token)
    sock.close()
    assert rc == RC_SUCCESS


# ---------------------------------------------------------------------------
# Tests: jwt_required_ws (WebSocket-only JWT requirement)
# ---------------------------------------------------------------------------


def test_jwt_required_ws_allows_tcp_anonymous(jwt_required_ws_only):
    """jwt_required_ws=on still allows anonymous TCP connections."""
    sock, rc = mqtt_connect(MQTT_HOST, MQTT_PORT, client_id="tcp-anon")
    sock.close()
    assert rc == RC_SUCCESS


def test_jwt_required_ws_rejects_ws_anonymous(jwt_required_ws_only):
    """jwt_required_ws=on rejects anonymous WebSocket connections."""
    sock, rc = _ws_mqtt_connect(path="/", client_id="ws-anon-reject")
    sock.close()
    assert rc == RC_NOT_AUTHORIZED


def test_jwt_required_ws_accepts_ws_with_jwt(jwt_required_ws_only):
    """jwt_required_ws=on accepts WebSocket connections with valid JWT."""
    token = make_jwt(sub="ws-jwt-ok", exp_offset=3600)
    path = f"/?jwt={token}"
    sock, rc = _ws_mqtt_connect(path=path, client_id="ws-jwt-ok")
    sock.close()
    assert rc == RC_SUCCESS


# ---------------------------------------------------------------------------
# Tests: security edge cases
# ---------------------------------------------------------------------------


def test_jwt_missing_exp_rejected(jwt_required):
    """A JWT without an exp claim must be rejected."""
    token = make_jwt(sub="no-exp-client", exp_offset=None)
    sock, rc = mqtt_connect(MQTT_HOST, MQTT_PORT, client_id="no-exp-jwt", jwt=token)
    sock.close()
    assert rc == RC_NOT_AUTHORIZED


def test_malformed_public_key_rejects_when_required():
    """If jwt_public_key is set but malformed, jwt_required=on must reject connections."""
    set_guc("pgmqtt.jwt_public_key", "not-a-valid-base64url-key!!!")
    set_guc("pgmqtt.jwt_required", "on")

    token = make_jwt(sub="should-fail", exp_offset=3600)
    sock, rc = mqtt_connect(MQTT_HOST, MQTT_PORT, client_id="bad-key-test", jwt=token)
    sock.close()
    assert rc == RC_NOT_AUTHORIZED
