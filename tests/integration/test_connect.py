"""
Connection lifecycle integration tests for pgmqtt.
Covers CONNECT, SUBSCRIBE, PING, keep-alive, auto-generated IDs,
concurrent connections, session takeover, and duplicate CONNECT.
"""

import socket
import time
import concurrent.futures

from proto_utils import (
    create_connect_packet,
    create_subscribe_packet,
    create_pingreq_packet,
    create_disconnect_packet,
    recv_packet,
    validate_connack,
    validate_suback,
    validate_pingresp,
    MQTTControlPacket,
    ReasonCode,
    MQTT_HOST,
    MQTT_PORT,
)


def test_connect_connack():
    """CONNECT -> CONNACK handshake validation."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("test_connect_connack"))

    resp = recv_packet(s, timeout=5)
    assert resp is not None, "No CONNACK received"

    _, reason_code, _ = validate_connack(resp)
    assert reason_code == ReasonCode.SUCCESS, f"CONNACK failed: {reason_code}"
    s.close()


def test_subscribe_suback():
    """SUBSCRIBE -> SUBACK validation."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("test_subscribe_suback"))
    recv_packet(s, timeout=5)

    packet_id = 77
    s.sendall(create_subscribe_packet(packet_id, "test/topic", qos=0))
    suback = recv_packet(s, timeout=5)
    assert suback is not None, "No SUBACK received"

    reason_codes = validate_suback(suback, packet_id)
    assert reason_codes[0] == ReasonCode.GRANTED_QOS_0, f"SUBACK reason_code={reason_codes[0]}"
    s.close()


def test_pingreq_pingresp():
    """PINGREQ -> PINGRESP."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("test_pingreq_resp"))
    recv_packet(s, timeout=5)

    s.sendall(create_pingreq_packet())
    resp = recv_packet(s, timeout=5)
    assert resp is not None, "No PINGRESP received"
    validate_pingresp(resp)
    s.close()


def test_multiple_pings():
    """5 PINGREQ/PINGRESP exchanges on a single connection."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("test_multiple_pings"))
    recv_packet(s, timeout=5)

    for i in range(5):
        s.sendall(create_pingreq_packet())
        resp = recv_packet(s, timeout=5)
        assert resp is not None, f"No PINGRESP for ping {i+1}"
        validate_pingresp(resp)

    s.close()


def test_keepalive_maintained_with_ping():
    """Sending pings within keep-alive interval keeps connection alive."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("test_ka_ping", keep_alive=3))
    resp = recv_packet(s, timeout=5)
    assert resp is not None, "No CONNACK received"
    validate_connack(resp)

    # Send 4 pings every 2s over 8s total — all within 3s keep-alive
    for i in range(4):
        time.sleep(2)
        s.sendall(create_pingreq_packet())
        resp = recv_packet(s, timeout=5)
        assert resp is not None, f"No PINGRESP for ping {i+1} — connection may have been closed"
        validate_pingresp(resp)
        print(f"  Ping {i+1}/4 OK")

    s.close()


def test_keepalive_broker_disconnects():
    """Broker disconnects after keep-alive expiry (1.5x + buffer)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(10)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("test_ka_expire", keep_alive=2))
    resp = recv_packet(s, timeout=5)
    assert resp is not None, "No CONNACK received"
    validate_connack(resp)

    # Wait 4s (well beyond 1.5 * 2s = 3s)
    print("  Waiting 4s for keep-alive expiry...")
    time.sleep(4)

    # Try to send a ping — connection should be closed or we get DISCONNECT
    try:
        s.sendall(create_pingreq_packet())
        resp = recv_packet(s, timeout=2)
        if resp is None:
            print("  Connection closed by broker (recv returned None)")
        elif (resp[0] >> 4) == MQTTControlPacket.DISCONNECT:
            print("  Received DISCONNECT from broker")
        else:
            # Some brokers may still respond if timing is tight; try once more
            time.sleep(1)
            s.sendall(create_pingreq_packet())
            resp2 = recv_packet(s, timeout=2)
            assert resp2 is None or (resp2[0] >> 4) == MQTTControlPacket.DISCONNECT, \
                "Expected broker to close connection after keep-alive timeout"
    except (ConnectionError, BrokenPipeError, OSError):
        print("  Connection reset by broker")

    s.close()


def test_auto_generated_client_ids():
    """3 empty-string client ID connections get unique IDs, no collisions."""
    # Connect Client A
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s1.settimeout(5)
    s1.connect((MQTT_HOST, MQTT_PORT))
    s1.sendall(create_connect_packet(""))
    resp1 = recv_packet(s1)
    assert resp1 is not None, "Client A failed to get CONNACK"
    _, reason1, _ = validate_connack(resp1)
    assert reason1 == ReasonCode.SUCCESS

    # Connect Client B
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.settimeout(5)
    s2.connect((MQTT_HOST, MQTT_PORT))
    s2.sendall(create_connect_packet(""))
    resp2 = recv_packet(s2)
    assert resp2 is not None, "Client B failed to get CONNACK"
    _, reason2, _ = validate_connack(resp2)
    assert reason2 == ReasonCode.SUCCESS

    # Disconnect Client A
    s1.close()
    time.sleep(1)

    # Connect Client C
    s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s3.settimeout(5)
    s3.connect((MQTT_HOST, MQTT_PORT))
    s3.sendall(create_connect_packet(""))
    resp3 = recv_packet(s3)
    assert resp3 is not None, "Client C failed to get CONNACK"
    _, reason3, _ = validate_connack(resp3)
    assert reason3 == ReasonCode.SUCCESS

    # Verify Client B is still alive (no ID collision)
    s2.sendall(create_pingreq_packet())
    pingresp = recv_packet(s2, timeout=2)
    assert pingresp is not None, "Client B did not respond to PING — potential ID collision"
    validate_pingresp(pingresp)

    s2.close()
    s3.close()


def _connect_client(client_index):
    """Helper for concurrent connection test."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(10)
        s.connect((MQTT_HOST, MQTT_PORT))
        s.sendall(create_connect_packet(""))
        resp = recv_packet(s)
        if resp is None:
            return None, f"Client {client_index} failed to get CONNACK"
        _, reason, _ = validate_connack(resp)
        if reason != ReasonCode.SUCCESS:
            return None, f"Client {client_index} failed with reason {reason}"
        return s, None
    except Exception as e:
        return None, f"Error connecting client {client_index}: {e}"


def test_concurrent_connections():
    """200 concurrent clients stay alive with no ID collisions."""
    num_clients = 200
    sockets = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        future_to_idx = {executor.submit(_connect_client, i): i for i in range(num_clients)}
        for future in concurrent.futures.as_completed(future_to_idx):
            s, err = future.result()
            if err:
                print(f"  {err}")
            else:
                sockets.append(s)

    print(f"  {len(sockets)} clients connected successfully")
    assert len(sockets) == num_clients, f"Only {len(sockets)}/{num_clients} connected"

    # Verify all alive
    def verify_client(i, s):
        try:
            s.sendall(create_pingreq_packet())
            pingresp = recv_packet(s, timeout=2)
            return pingresp is not None and pingresp == bytes([0xD0, 0x00])
        except Exception:
            return False

    alive_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        futures = {executor.submit(verify_client, i, s): i for i, s in enumerate(sockets)}
        for future in concurrent.futures.as_completed(futures, timeout=15):
            if future.result():
                alive_count += 1

    print(f"  {alive_count} clients alive")
    assert alive_count == num_clients, f"Only {alive_count}/{num_clients} alive"

    for s in sockets:
        try:
            s.close()
        except Exception:
            pass


def test_session_takeover():
    """Second connection with same client_id disconnects the first."""
    client_id = "takeover_client"

    # First connection
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s1.settimeout(5)
    s1.connect((MQTT_HOST, MQTT_PORT))
    s1.sendall(create_connect_packet(client_id))
    resp = recv_packet(s1, timeout=5)
    assert resp is not None, "First CONNACK not received"
    validate_connack(resp)

    # Second connection with same client_id
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.settimeout(5)
    s2.connect((MQTT_HOST, MQTT_PORT))
    s2.sendall(create_connect_packet(client_id))
    resp2 = recv_packet(s2, timeout=5)
    assert resp2 is not None, "Second CONNACK not received"
    validate_connack(resp2)

    # First connection should be disconnected — either DISCONNECT packet or closed
    resp1 = recv_packet(s1, timeout=3)
    if resp1 is not None:
        ptype = (resp1[0] >> 4)
        assert ptype == MQTTControlPacket.DISCONNECT, \
            f"Expected DISCONNECT on first connection, got packet type {ptype}"
        print("  First connection received DISCONNECT")
    else:
        # Connection was closed (recv returned None)
        print("  First connection closed by broker")

    s1.close()
    s2.close()


def test_duplicate_connect():
    """Sending CONNECT twice on same connection causes disconnect."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet("test_dup_connect"))
    resp = recv_packet(s, timeout=5)
    assert resp is not None, "No CONNACK received"
    validate_connack(resp)

    # Send a second CONNECT on the same connection (protocol violation)
    s.sendall(create_connect_packet("test_dup_connect"))
    resp2 = recv_packet(s, timeout=3)

    if resp2 is not None:
        ptype = (resp2[0] >> 4)
        assert ptype == MQTTControlPacket.DISCONNECT, \
            f"Expected DISCONNECT after duplicate CONNECT, got packet type {ptype}"
        print("  Received DISCONNECT with protocol error")
    else:
        print("  Connection closed by broker after duplicate CONNECT")

    s.close()
