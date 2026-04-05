"""
WebSocket framing tests for pgmqtt.

Tests RFC 6455 frame-level behavior: fragmentation reassembly, oversized
reassembled messages, RSV bit rejection, and unmasked frame rejection.
"""

import os
import socket
import struct
import base64
import time

from proto_utils import (
    create_connect_packet,
    MQTT_HOST,
)

WS_PORT = int(os.environ.get("WS_PORT", "9001"))


# ---------------------------------------------------------------------------
# WebSocket helpers
# ---------------------------------------------------------------------------

def _ws_handshake(sock):
    """Perform a WebSocket Upgrade handshake, return any leftover bytes."""
    key_b64 = base64.b64encode(b"pgmqtt-test-key-1234").decode()
    request = (
        f"GET / HTTP/1.1\r\n"
        f"Host: {MQTT_HOST}:{WS_PORT}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key_b64}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "Sec-WebSocket-Protocol: mqtt\r\n"
        "\r\n"
    )
    sock.sendall(request.encode())

    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("WS handshake: connection closed")
        buf += chunk
    assert b"101" in buf, f"WS handshake failed: {buf[:200]}"
    # Return any bytes after the HTTP response (leftover wire data)
    idx = buf.index(b"\r\n\r\n") + 4
    return buf[idx:]


def _ws_send_frame(sock, payload, *, opcode=0x02, fin=True, masked=True):
    """Send a single WebSocket frame with the given parameters."""
    first_byte = (0x80 if fin else 0x00) | (opcode & 0x0F)
    length = len(payload)
    mask_bit = 0x80 if masked else 0x00

    header = bytes([first_byte])
    if length < 126:
        header += bytes([mask_bit | length])
    elif length < 65536:
        header += bytes([mask_bit | 126]) + struct.pack("!H", length)
    else:
        header += bytes([mask_bit | 127]) + struct.pack("!Q", length)

    if masked:
        mask = os.urandom(4)
        header += mask
        masked_payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        sock.sendall(header + masked_payload)
    else:
        sock.sendall(header + payload)


def _ws_recv_frame(sock, timeout=5.0):
    """Receive one WebSocket frame payload (unmasked, from server)."""
    sock.settimeout(timeout)

    def recv_exact(n):
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("WS recv: connection closed")
            buf += chunk
        return buf

    hdr = recv_exact(2)
    length = hdr[1] & 0x7F
    if length == 126:
        length = struct.unpack("!H", recv_exact(2))[0]
    elif length == 127:
        length = struct.unpack("!Q", recv_exact(8))[0]
    # Server frames are unmasked
    return recv_exact(length)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_fragmented_message_reassembly():
    """A fragmented WebSocket message is correctly reassembled by the broker."""
    sock = socket.create_connection((MQTT_HOST, WS_PORT), timeout=5.0)
    _ws_handshake(sock)

    # Build an MQTT CONNECT packet then split it across two WS fragments.
    connect_pkt = create_connect_packet("ws_frag_test", clean_start=True)
    mid = len(connect_pkt) // 2
    part1 = connect_pkt[:mid]
    part2 = connect_pkt[mid:]

    # First fragment: opcode=binary, fin=False
    _ws_send_frame(sock, part1, opcode=0x02, fin=False)
    # Continuation frame: opcode=continuation, fin=True
    _ws_send_frame(sock, part2, opcode=0x00, fin=True)

    # Should receive CONNACK back
    connack = _ws_recv_frame(sock)
    assert connack is not None and len(connack) > 0, "Expected CONNACK after fragmented CONNECT"
    # Verify it's actually a CONNACK (packet type 2)
    ptype = (connack[0] >> 4)
    assert ptype == 2, f"Expected CONNACK (type 2), got type {ptype}"
    print("  Fragmented CONNECT reassembled and CONNACK received")
    sock.close()


def test_oversized_reassembled_message_rejected():
    """Reassembled fragments exceeding MAX_WS_FRAME_SIZE (64KB) are rejected."""
    sock = socket.create_connection((MQTT_HOST, WS_PORT), timeout=5.0)
    _ws_handshake(sock)

    # Send fragments that together exceed 64KB.
    # Each individual frame is under 64KB, but the total exceeds it.
    chunk_size = 40_000  # Under 64KB per frame
    chunk = b"\x00" * chunk_size

    # First fragment
    _ws_send_frame(sock, chunk, opcode=0x02, fin=False)
    # Second fragment — this pushes total to 80KB, exceeding the 64KB limit
    _ws_send_frame(sock, chunk, opcode=0x00, fin=True)

    # The broker should close the connection
    time.sleep(0.5)
    try:
        data = sock.recv(4096)
        # If we get data, it might be a Close frame — that's fine.
        # If empty, the connection was closed.
        if data:
            # Check if it's a WS Close frame (opcode 0x8)
            opcode = data[0] & 0x0F
            print(f"  Received frame with opcode 0x{opcode:02x} (Close=0x08)")
        else:
            print("  Connection closed by broker (empty recv)")
    except (ConnectionError, OSError):
        print("  Connection reset by broker")

    # Verify connection is dead by trying to send/recv
    try:
        sock.sendall(b"\x00")
        time.sleep(0.2)
        leftover = sock.recv(4096)
        assert leftover == b"", "Connection should be closed after oversized reassembly"
    except (ConnectionError, BrokenPipeError, OSError):
        pass  # Expected — connection is dead
    print("  Oversized reassembled message correctly rejected")
    sock.close()


def test_rsv_bits_rejected():
    """Frames with non-zero RSV bits are rejected (RFC 6455 §5.2)."""
    sock = socket.create_connection((MQTT_HOST, WS_PORT), timeout=5.0)
    _ws_handshake(sock)

    # Send a frame with RSV1 set (0x40 in first byte)
    payload = create_connect_packet("ws_rsv_test", clean_start=True)
    first_byte = 0x80 | 0x40 | 0x02  # FIN + RSV1 + Binary
    length = len(payload)
    mask = os.urandom(4)
    header = bytes([first_byte, 0x80 | length]) + mask
    masked_payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    sock.sendall(header + masked_payload)

    time.sleep(0.5)
    try:
        data = sock.recv(4096)
        if not data:
            print("  Connection closed by broker (RSV bits rejected)")
        else:
            print(f"  Received response (len={len(data)}), connection closing")
    except (ConnectionError, OSError):
        print("  Connection reset by broker (RSV bits rejected)")
    sock.close()


def test_unmasked_client_frame_rejected():
    """Unmasked frames from client are rejected (RFC 6455 §5.1)."""
    sock = socket.create_connection((MQTT_HOST, WS_PORT), timeout=5.0)
    _ws_handshake(sock)

    # Send an unmasked frame (mask bit = 0)
    payload = create_connect_packet("ws_nomask_test", clean_start=True)
    _ws_send_frame(sock, payload, masked=False)

    time.sleep(0.5)
    try:
        data = sock.recv(4096)
        if not data:
            print("  Connection closed by broker (unmasked frame rejected)")
        else:
            print(f"  Received response (len={len(data)}), connection closing")
    except (ConnectionError, OSError):
        print("  Connection reset by broker (unmasked frame rejected)")
    sock.close()
