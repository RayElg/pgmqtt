"""MQTT connection helpers for pgmqtt enterprise tests."""

import socket
import ssl
import struct


def _encode_utf8(s: str) -> bytes:
    encoded = s.encode("utf-8")
    return struct.pack("!H", len(encoded)) + encoded


def _encode_variable_byte_int(value: int) -> bytes:
    out = []
    while True:
        byte = value & 0x7F
        value >>= 7
        if value > 0:
            byte |= 0x80
        out.append(byte)
        if value == 0:
            break
    return bytes(out)


def build_connect_packet(
    client_id: str = "test",
    username: str | None = None,
    password: bytes | None = None,
    clean_start: bool = True,
    keep_alive: int = 60,
) -> bytes:
    """Build an MQTT 5.0 CONNECT packet."""
    proto_name = _encode_utf8("MQTT")
    proto_version = bytes([5])

    connect_flags = 0x02 if clean_start else 0x00  # clean_start
    if username is not None:
        connect_flags |= 0x80
    if password is not None:
        connect_flags |= 0x40

    keep_alive_bytes = struct.pack("!H", keep_alive)

    # No properties
    properties = bytes([0x00])

    payload = _encode_utf8(client_id)
    if username is not None:
        payload += _encode_utf8(username)
    if password is not None:
        payload += struct.pack("!H", len(password)) + password

    variable_header = proto_name + proto_version + bytes([connect_flags]) + keep_alive_bytes + properties
    remaining = variable_header + payload

    fixed_header = bytes([0x10]) + _encode_variable_byte_int(len(remaining))
    return fixed_header + remaining


def read_packet(sock: socket.socket, timeout: float = 5.0) -> bytes:
    """Read one complete MQTT packet from sock."""
    sock.settimeout(timeout)
    # Read fixed header byte 1
    first = sock.recv(1)
    if not first:
        raise ConnectionError("connection closed")

    # Read variable-byte remaining length
    remaining_length = 0
    multiplier = 1
    while True:
        b = sock.recv(1)
        if not b:
            raise ConnectionError("connection closed reading length")
        byte = b[0]
        remaining_length += (byte & 0x7F) * multiplier
        multiplier *= 128
        if (byte & 0x80) == 0:
            break

    data = b""
    while len(data) < remaining_length:
        chunk = sock.recv(remaining_length - len(data))
        if not chunk:
            raise ConnectionError("connection closed reading payload")
        data += chunk

    return first + _encode_variable_byte_int(remaining_length) + data


def parse_connack(packet: bytes) -> int:
    """Parse a CONNACK packet and return the reason code."""
    if not packet or (packet[0] >> 4) != 2:
        raise ValueError(f"expected CONNACK, got packet type {packet[0] >> 4 if packet else 'empty'}")
    # Skip fixed header + remaining length byte(s), then session_present byte, then reason code
    # MQTT 5.0 CONNACK: fixed_hdr + remlen + session_present(1) + reason_code(1) + props
    # Find end of variable-length int
    i = 1
    while packet[i] & 0x80:
        i += 1
    i += 1  # skip remaining length field
    # i now points to session_present
    reason_code = packet[i + 1]
    return reason_code


def mqtt_connect(
    host: str,
    port: int,
    client_id: str,
    *,
    tls: bool = False,
    jwt: str | None = None,
    username: str | None = None,
    timeout: float = 5.0,
) -> tuple[socket.socket, int]:
    """Connect to the MQTT broker and return (socket, connack_reason_code).

    Args:
        host:      Broker hostname.
        port:      Broker port.
        client_id: MQTT client ID.
        tls:       If True, wrap connection in TLS (self-signed cert accepted).
        jwt:       If set, send as password bytes (username defaults to "").
        username:  Optional username (overrides jwt username default).
        timeout:   Socket timeout in seconds.

    Returns:
        (sock, reason_code) where reason_code 0 means success.
    """
    raw = socket.create_connection((host, port), timeout=timeout)

    if tls:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        sock = ctx.wrap_socket(raw, server_hostname=host)
    else:
        sock = raw

    password: bytes | None = None
    uname: str | None = username
    if jwt is not None:
        password = jwt.encode("utf-8")
        if uname is None:
            uname = ""

    pkt = build_connect_packet(
        client_id=client_id,
        username=uname,
        password=password,
        clean_start=True,
        keep_alive=60,
    )
    sock.sendall(pkt)

    connack = read_packet(sock, timeout=timeout)
    reason_code = parse_connack(connack)
    return sock, reason_code


def assert_connack(sock: socket.socket, expected_rc: int = 0, timeout: float = 5.0) -> None:
    """Read the next packet from sock and assert it is CONNACK with expected_rc."""
    pkt = read_packet(sock, timeout=timeout)
    rc = parse_connack(pkt)
    assert rc == expected_rc, f"expected CONNACK reason code {expected_rc:#04x}, got {rc:#04x}"
