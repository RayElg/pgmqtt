"""JWT generation helpers for pgmqtt enterprise tests.

Uses the deterministic test Ed25519 keypair (private seed = [0x42] * 32).
The corresponding public key (base64url) can be set via pgmqtt.jwt_public_key.
"""

import base64
import json
import time

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

# Deterministic test private key seed
_TEST_SEED = bytes([0x42] * 32)
_PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(_TEST_SEED)
_PUBLIC_KEY_RAW = _PRIVATE_KEY.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def public_key_b64url() -> str:
    """Return the test public key as a base64url string (for use in GUC setting)."""
    return _b64url(_PUBLIC_KEY_RAW)


def make_jwt(
    sub: str = "test-client",
    client_id: str | None = None,
    sub_claims: list[str] | None = None,
    pub_claims: list[str] | None = None,
    exp_offset: int | None = 3600,
    iat: int | None = None,
) -> str:
    """Generate a signed Ed25519 JWT token.

    Args:
        sub:         Subject (client identifier).
        client_id:   If set, the CONNECT client_id must match this value.
        sub_claims:  List of topic filters the client may subscribe to.
        pub_claims:  List of topic filters the client may publish to.
        exp_offset:  Seconds from now until expiry (negative = already expired).
                     None omits the exp claim entirely (for testing rejection).
        iat:         Issued-at timestamp override (defaults to now).

    Returns:
        A JWT string: ``header.payload.signature``
    """
    now = int(time.time())
    if iat is None:
        iat = now

    header = {"alg": "EdDSA", "typ": "JWT"}
    payload: dict = {
        "sub": sub,
        "iat": iat,
    }
    if exp_offset is not None:
        payload["exp"] = now + exp_offset
    if client_id is not None:
        payload["client_id"] = client_id
    if sub_claims is not None:
        payload["sub_claims"] = sub_claims
    if pub_claims is not None:
        payload["pub_claims"] = pub_claims

    header_b64 = _b64url(json.dumps(header, separators=(",", ":")).encode())
    payload_b64 = _b64url(json.dumps(payload, separators=(",", ":")).encode())

    signing_input = f"{header_b64}.{payload_b64}".encode()
    signature = _PRIVATE_KEY.sign(signing_input)

    return f"{header_b64}.{payload_b64}.{_b64url(signature)}"
