"""License token generation helper for pgmqtt enterprise tests.

Requires the environment variable PGMQTT_TEST_SIGNING_KEY to be set to a
base64-encoded 32-byte Ed25519 private key seed. Tests that need this helper
should be skipped (or will auto-skip) when the variable is absent.
"""

import base64
import json
import os
import time

import pytest


def _get_signing_key_seed() -> bytes | None:
    raw = os.environ.get("PGMQTT_TEST_SIGNING_KEY")
    if not raw:
        return None
    seed = base64.b64decode(raw)
    if len(seed) != 32:
        raise ValueError(
            f"PGMQTT_TEST_SIGNING_KEY must decode to exactly 32 bytes, got {len(seed)}"
        )
    return seed


def signing_key_available() -> bool:
    return _get_signing_key_seed() is not None


requires_signing_key = pytest.mark.skipif(
    not signing_key_available(),
    reason="PGMQTT_TEST_SIGNING_KEY not set — license signing tests skipped",
)


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def generate_test_license(
    customer: str = "test",
    days: int = 1,
    features: list | None = None,
    max_connections: int = 100,
    grace_days: int = 7,
    expired: bool = False,
    grace_only: bool = False,
) -> str:
    """Generate a signed license token using the key from PGMQTT_TEST_SIGNING_KEY."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    seed = _get_signing_key_seed()
    if seed is None:
        raise RuntimeError("PGMQTT_TEST_SIGNING_KEY is not set")

    private_key = Ed25519PrivateKey.from_private_bytes(seed)

    if features is None:
        features = ["tls", "jwt"]

    now = int(time.time())
    if expired:
        expires_at = now - 86400 * 10
        grace_expires_at = now - 86400
    elif grace_only:
        expires_at = now - 86400
        grace_expires_at = now + 86400 * grace_days
    else:
        expires_at = now + 86400 * days
        grace_expires_at = now + 86400 * (days + grace_days)

    payload = {
        "customer": customer,
        "expires_at": expires_at,
        "grace_expires_at": grace_expires_at,
        "features": features,
        "max_connections": max_connections,
    }
    payload_bytes = json.dumps(payload, separators=(",", ":")).encode()
    signature = private_key.sign(payload_bytes)
    return f"{_b64url(payload_bytes)}.{_b64url(signature)}"
