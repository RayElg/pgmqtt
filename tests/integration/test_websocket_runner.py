#!/usr/bin/env python3
"""
Python wrapper for the Node.js WebSocket integration test.
Ensures npm dependencies are installed and runs the test.
"""

import subprocess
import sys
import os
from pathlib import Path

def run_ws_test():
    test_dir = Path(__file__).parent
    os.chdir(test_dir)

    # 1. npm install if node_modules missing
    if not (test_dir / "node_modules").exists():
        print("==> Installing npm dependencies …")
        try:
            subprocess.run(["npm", "install"], check=True)
        except subprocess.CalledProcessError as e:
            print(f"ERROR: npm install failed: {e}")
            sys.exit(1)
        except FileNotFoundError:
            print("ERROR: npm not found in PATH")
            sys.exit(1)

    # 2. Run the Node.js test
    print("==> Running WebSocket test …")
    try:
        # We pass through environment variables like PG_CONTAINER if they exist
        result = subprocess.run(["node", "test_websocket.js"], check=False)
        sys.exit(result.returncode)
    except FileNotFoundError:
        print("ERROR: node not found in PATH")
        sys.exit(1)

if __name__ == "__main__":
    run_ws_test()
