"""Tests for pgmqtt TLS-related GUCs."""

import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from helpers.guc import set_guc, reset_guc
from test_utils import run_sql

pytestmark = pytest.mark.enterprise

def show_guc(name: str) -> str:
    """Return the current value of a GUC."""
    res = run_sql(f"SHOW {name}")
    # res is often a list of tuples like [('off',)]
    if res and len(res) > 0 and len(res[0]) > 0:
        return str(res[0][0])
    return ""

def test_tls_gucs_exist():
    """Verify that all new TLS GUCs are registered and have expected defaults."""
    assert show_guc("pgmqtt.mqtts_enabled") == "off"
    assert show_guc("pgmqtt.wss_enabled") == "off"
    assert show_guc("pgmqtt.mqtts_port") == "8883"
    assert show_guc("pgmqtt.wss_port") == "9002"
    # Default for string GUCs initialized with None is usually empty string in SHOW
    assert show_guc("pgmqtt.tls_cert_file") == ""
    assert show_guc("pgmqtt.tls_key_file") == ""

def test_tls_gucs_settable():
    """Verify that TLS GUCs can be changed and reloaded."""
    try:
        set_guc("pgmqtt.mqtts_enabled", "on")
        set_guc("pgmqtt.wss_enabled", "on")
        set_guc("pgmqtt.mqtts_port", "18883")
        set_guc("pgmqtt.wss_port", "19002")
        set_guc("pgmqtt.tls_cert_file", "/tmp/cert.crt")
        set_guc("pgmqtt.tls_key_file", "/tmp/key.key")

        assert show_guc("pgmqtt.mqtts_enabled") == "on"
        assert show_guc("pgmqtt.wss_enabled") == "on"
        assert show_guc("pgmqtt.mqtts_port") == "18883"
        assert show_guc("pgmqtt.wss_port") == "19002"
        assert show_guc("pgmqtt.tls_cert_file") == "/tmp/cert.crt"
        assert show_guc("pgmqtt.tls_key_file") == "/tmp/key.key"
    finally:
        reset_guc("pgmqtt.mqtts_enabled")
        reset_guc("pgmqtt.wss_enabled")
        reset_guc("pgmqtt.mqtts_port")
        reset_guc("pgmqtt.wss_port")
        reset_guc("pgmqtt.tls_cert_file")
        reset_guc("pgmqtt.tls_key_file")
