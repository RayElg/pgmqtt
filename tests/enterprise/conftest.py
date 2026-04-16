"""
Enterprise test suite configuration.

All enterprise tests require the broker to be running and configured with a
public key that matches PGMQTT_TEST_SIGNING_KEY. This is verified by a probe
at collection time: a token is generated and set as the license GUC, then the
status is read back. If the status is not "active" (wrong key, no broker, no
env var), the entire suite is skipped.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from helpers.guc import set_guc, reset_guc  # noqa: E402
from helpers.license import generate_test_license, signing_key_available  # noqa: E402

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "integration"))
from test_utils import run_sql  # noqa: E402


def _enterprise_available() -> tuple[bool, str]:
    """Return (available, reason). Generates a probe token and checks the broker accepts it."""
    if not signing_key_available():
        return False, "PGMQTT_TEST_SIGNING_KEY not set"

    try:
        token = generate_test_license(customer="probe", days=1)
        set_guc("pgmqtt.license_key", token)
        rows = run_sql("SELECT status FROM pgmqtt_license_status()")
        status = rows[0][0] if rows else None
        return status in ("active", "grace"), f"broker license status was '{status}' (expected 'active' or 'grace')"
    except Exception as e:
        return False, f"probe failed: {e}"
    finally:
        try:
            reset_guc("pgmqtt.license_key")
        except Exception:
            pass


def pytest_collection_modifyitems(config, items):
    enterprise_items = [
        item for item in items
        if item.fspath.strpath.startswith(os.path.join(str(config.rootdir), "tests", "enterprise"))
    ]
    if not enterprise_items:
        return

    available, reason = _enterprise_available()
    if not available:
        skip = pytest.mark.skip(reason=f"enterprise suite skipped: {reason}")
        for item in enterprise_items:
            item.add_marker(skip)
