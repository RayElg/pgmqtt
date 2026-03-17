"""Integration tests for pgmqtt enterprise license gating."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from helpers.guc import set_guc, reset_guc  # noqa: E402
from helpers.license import generate_test_license  # noqa: E402

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "integration"))
from test_utils import run_sql  # noqa: E402

pytestmark = pytest.mark.enterprise

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _set_license(token: str) -> None:
    set_guc("pgmqtt.license_key", token)


def _clear_license() -> None:
    set_guc("pgmqtt.license_key", "")


def _license_status() -> dict:
    rows = run_sql(
        "SELECT customer, status, expires_at, grace_expires_at, features, max_connections "
        "FROM pgmqtt_license_status()"
    )
    if not rows:
        return {}
    r = rows[0]
    return {
        "customer": r[0],
        "status": r[1],
        "expires_at": r[2],
        "grace_expires_at": r[3],
        "features": list(r[4]) if r[4] else [],
        "max_connections": r[5],
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_license():
    _clear_license()
    yield
    _clear_license()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_no_license_returns_community():
    status = _license_status()
    assert status["status"] == "community"
    assert status["max_connections"] == 1000


def test_valid_license_returns_active():
    token = generate_test_license(customer="acme", days=1, features=["tls", "jwt"])
    _set_license(token)
    status = _license_status()
    assert status["status"] == "active"
    assert status["customer"] == "acme"
    assert "tls" in status["features"]
    assert "jwt" in status["features"]
    assert status["max_connections"] == 100


def test_expired_license_returns_expired():
    token = generate_test_license(customer="acme", days=1, expired=True)
    _set_license(token)
    status = _license_status()
    assert status["status"] == "expired"


def test_grace_license_returns_grace():
    token = generate_test_license(customer="acme", days=1, grace_only=True)
    _set_license(token)
    status = _license_status()
    assert status["status"] == "grace"


def test_license_reload_without_restart():
    """Switching license key via ALTER SYSTEM + reload updates the status."""
    token1 = generate_test_license(customer="first", days=1)
    _set_license(token1)
    assert _license_status()["customer"] == "first"

    token2 = generate_test_license(customer="second", days=1)
    _set_license(token2)
    assert _license_status()["customer"] == "second"
