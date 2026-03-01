#!/usr/bin/env python3
"""
integration test for the pgmqtt HTTP healthcheck endpoint
"""

import urllib.request
import urllib.error
import time
import sys
import json

HEALTH_URL = "http://localhost:8080/health"
BAD_URL = "http://localhost:8080/nonexistent"
MAX_RETRIES = 30
RETRY_INTERVAL = 1

passed = 0
failed = 0

def pass_test(msg):
    global passed
    print(f"  ✓ {msg}")
    passed += 1

def fail_test(msg):
    global failed
    print(f"  ✗ {msg}")
    failed += 1

def main():
    print("=== pgmqtt healthcheck test ===")

    # Wait for server to come up
    print(f"Waiting for HTTP server on {HEALTH_URL} ...")
    server_ready = False
    for i in range(1, MAX_RETRIES + 1):
        try:
            urllib.request.urlopen(HEALTH_URL, timeout=1)
            print(f"  Server ready after {i}s")
            server_ready = True
            break
        except Exception:
            if i == MAX_RETRIES:
                print(f"  ✗ Server did not become ready within {MAX_RETRIES}s")
                sys.exit(1)
            time.sleep(RETRY_INTERVAL)

    # Test 1: GET /health returns 200
    print("\nTest 1: GET /health returns 200")
    try:
        req = urllib.request.urlopen(HEALTH_URL)
        if req.getcode() == 200:
            pass_test("HTTP status is 200")
        else:
            fail_test(f"Expected 200, got {req.getcode()}")
    except urllib.error.HTTPError as e:
        fail_test(f"Expected 200, got {e.code}")
    except Exception as e:
        fail_test(f"Failed with exception: {e}")

    # Test 2: GET /health body is correct
    print("\nTest 2: GET /health body contains status ok")
    try:
        req = urllib.request.urlopen(HEALTH_URL)
        body = req.read().decode('utf-8')
        data = json.loads(body)
        if data.get("status") == "ok":
            pass_test('Body is {"status":"ok"}')
        else:
            fail_test(f"Unexpected body: {body}")
    except Exception as e:
        fail_test(f"Failed to read body: {e}")

    # Test 3: GET /nonexistent returns 404
    print("\nTest 3: GET /nonexistent returns 404")
    try:
        urllib.request.urlopen(BAD_URL)
        fail_test("Expected 404, got 200")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            pass_test("HTTP status is 404")
        else:
            fail_test(f"Expected 404, got {e.code}")
    except Exception as e:
        fail_test(f"Failed with exception: {e}")

    # Summary
    print(f"\n=== Results: {passed} passed, {failed} failed ===")
    if failed > 0:
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()
