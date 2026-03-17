"""GUC manipulation helpers for pgmqtt enterprise tests."""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "integration"))
from test_utils import run_sql, run_command  # noqa: E402


def set_guc(name: str, value: str) -> None:
    """Set a pgmqtt GUC via ALTER SYSTEM and reload."""
    run_command(["psql", "-U", "postgres", "-c", f"ALTER SYSTEM SET {name} = '{value}'"])
    run_sql("SELECT pg_reload_conf()")
    time.sleep(0.3)


def reset_guc(name: str) -> None:
    """Reset a pgmqtt GUC via ALTER SYSTEM and reload."""
    run_command(["psql", "-U", "postgres", "-c", f"ALTER SYSTEM RESET {name}"])
    run_sql("SELECT pg_reload_conf()")
    time.sleep(0.3)
