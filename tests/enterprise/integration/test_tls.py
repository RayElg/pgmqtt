"""Integration tests for pgmqtt enterprise MQTTS and WSS."""

import os
import subprocess
import sys
import time
import socket
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from helpers.guc import set_guc, reset_guc
from helpers.mqtt import mqtt_connect
from helpers.tls import port_accepts_tls

pytestmark = pytest.mark.enterprise

MQTT_HOST = os.environ.get("MQTT_HOST", "127.0.0.1")
MQTTS_PORT = int(os.environ.get("MQTTS_PORT", "8883"))
WSS_PORT = int(os.environ.get("WSS_PORT", "9002"))
CONTAINER_NAME = "pgmqtt-enterprise-postgres-1"

def run_docker_cmd(*args):
    """Run a docker exec command inside the postgres container and check the result."""
    res = subprocess.run(["docker", "exec", "-u", "postgres", CONTAINER_NAME] + list(args), capture_output=True, text=True)
    if res.returncode != 0:
        print(f"Docker cmd failed: {res.stderr}")
    return res.returncode == 0

@pytest.fixture(scope="module")
def setup_certs():
    """Generate self-signed certs inside the postgres container."""
    cert_path = "/tmp/server.crt"
    key_path = "/tmp/server.key"
    
    # Check if container exists with this name before running
    # If not, fallback to docker compose exec
    success = run_docker_cmd(
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
        "-keyout", key_path, "-out", cert_path, "-days", "365",
        "-subj", "/CN=localhost"
    )
    if not success:
        # Fallback to docker compose exec
        subprocess.run([
            "docker", "compose", "exec", "-T", "postgres",
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", key_path, "-out", cert_path, "-days", "365",
            "-subj", "/CN=localhost"
        ], capture_output=True, check=True)

    yield (cert_path, key_path)

    # Cleanup (optional)
    subprocess.run(["docker", "exec", "-u", "postgres", CONTAINER_NAME, "rm", "-f", cert_path, key_path], capture_output=True)


@pytest.fixture(autouse=True)
def configure_tls(setup_certs):
    """Enable MQTTS and WSS before tests, and reset after."""
    cert_path, key_path = setup_certs
    try:
        set_guc("pgmqtt.tls_cert_file", cert_path)
        set_guc("pgmqtt.tls_key_file", key_path)
        set_guc("pgmqtt.mqtts_enabled", "on")
        set_guc("pgmqtt.wss_enabled", "on")
        # Restart container to force logical replication worker to restart and bind new ports
        subprocess.run(["docker", "compose", "restart", "postgres"], check=True)
        time.sleep(2) # Give it time to start up and plugin to initialize
        yield
    finally:
        reset_guc("pgmqtt.mqtts_enabled")
        reset_guc("pgmqtt.wss_enabled")
        reset_guc("pgmqtt.tls_cert_file")
        reset_guc("pgmqtt.tls_key_file")
        subprocess.run(["docker", "compose", "restart", "postgres"])
        time.sleep(2)


def test_mqtts_connection():
    """Verify that MQTTS listener accepts TLS connections and performs MQTT handshake."""
    assert port_accepts_tls(MQTT_HOST, MQTTS_PORT), "MQTTS port should accept TLS"
    
    sock, rc = mqtt_connect(MQTT_HOST, MQTTS_PORT, "mqtts_test_client", tls=True)
    assert rc == 0
    sock.close()


def test_wss_connection():
    """Verify that WSS listener accepts WebSocket upgrade requests over TLS."""
    import ssl
    import base64

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    ws_key = base64.b64encode(b'\x00' * 16).decode()
    upgrade_request = (
        f"GET / HTTP/1.1\r\n"
        f"Host: {MQTT_HOST}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {ws_key}\r\n"
        f"Sec-WebSocket-Version: 13\r\n"
        f"Sec-WebSocket-Protocol: mqtt\r\n"
        f"\r\n"
    ).encode()

    with socket.create_connection((MQTT_HOST, WSS_PORT), timeout=5.0) as raw:
        with ctx.wrap_socket(raw, server_hostname=MQTT_HOST) as tls:
            tls.sendall(upgrade_request)

            response = b""
            while b"\r\n\r\n" not in response:
                chunk = tls.recv(1024)
                if not chunk:
                    break
                response += chunk

    assert b"101 Switching Protocols" in response, (
        f"Expected 101 Switching Protocols, got: {response[:200]}"
    )
