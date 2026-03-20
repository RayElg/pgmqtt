"""TLS test helpers for pgmqtt enterprise tests."""

import socket
import ssl


def make_tls_context(verify: bool = False) -> ssl.SSLContext:
    """Create an SSL context for testing.

    Args:
        verify: If True, verify server certificate. Default False (for self-signed certs).
    """
    ctx = ssl.create_default_context()
    if not verify:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


def port_accepts_tls(host: str, port: int, timeout: float = 2.0) -> bool:
    """Return True if the host:port completes a TLS handshake."""
    ctx = make_tls_context(verify=False)
    try:
        with socket.create_connection((host, port), timeout=timeout) as raw:
            with ctx.wrap_socket(raw, server_hostname=host) as tls:
                return tls.version() is not None
    except (ssl.SSLError, ConnectionRefusedError, OSError):
        return False


def port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    """Return True if a plain TCP connection can be established."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (ConnectionRefusedError, OSError):
        return False
