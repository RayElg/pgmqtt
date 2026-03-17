"""
Pytest configuration for pgmqtt integration tests.

This file is automatically loaded by pytest and handles:
- Path setup for test imports
- Shared fixtures
- Test environment configuration
"""

import sys
import os

# Add tests/integration to sys.path so relative imports work
# This is the ONLY place we need to do this
tests_integration_dir = os.path.join(os.path.dirname(__file__), "integration")
if tests_integration_dir not in sys.path:
    sys.path.insert(0, tests_integration_dir)


def pytest_configure(config):
    """
    Register custom marks to avoid PytestUnknownMarkWarning.
    """
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "enterprise: marks tests as requiring an enterprise license"
    )
