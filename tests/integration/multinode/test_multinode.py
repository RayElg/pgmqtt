import subprocess
import time
import os
import urllib.request
import urllib.error

def test_read_replica_pauses_mqtt():
    # Spin up the primary-replica cluster
    print("Starting multinode cluster...")
    subprocess.check_call([
        "docker", "compose"", "-f", "tests/integration/multinode/"docker", "compose".multinode.yml", "up", "-d", "--build"
    ])
    
    try:
        # Wait for the replica to be fully online as a standby
        print("Waiting for pg_replica to start up as a standby...")
        replica_ready = False
        for i in range(30):
            try:
                subprocess.check_output([
                    "docker", "compose"", "-f", "tests/integration/multinode/"docker", "compose".multinode.yml", 
                    "exec", "-u", "postgres", "-T", "pg_replica", "psql", "-c", "SELECT 1"
                ], stderr=subprocess.STDOUT)
                replica_ready = True
                break
            except subprocess.CalledProcessError:
                pass
            time.sleep(2)
            
        assert replica_ready, "Replica database never became ready!"
        print("Replica is up.")

        # Check that the health port (8080) is not responding on the replica
        # This confirms pgmqtt_http hasn't started (due to BgWorkerStartTime::RecoveryFinished)
        try:
            urllib.request.urlopen("http://127.0.0.1:8081/health", timeout=5)
            assert False, "HTTP healthcheck responded on replica before being promoted!"
        except Exception:
            # Expected to fail (connection refused or timeout)
            print("Verified HTTP healthcheck is offline on the replica.")
        
        # Test promoting the replica
        print("Promoting replica to primary...")
        subprocess.check_call([
            "docker", "compose"", "-f", "tests/integration/multinode/"docker", "compose".multinode.yml", "exec", "-u", "postgres", "-T", "pg_replica", "pg_ctl", "promote"
        ])
        
        # Now wait for the broker to proceed with startup
        resumed_log_found = False
        for i in range(30):
            print(f"Checking pg_replica healthcheck (attempt {i+1}/30)...")
            try:
                req = urllib.request.urlopen("http://127.0.0.1:8081/health", timeout=5)
                if req.getcode() == 200:
                    body = req.read().decode('utf-8')
                    if "ok" in body:
                        resumed_log_found = True
                        print("Found healthcheck responding on replica after promotion.")
                        break
            except Exception as e:
                # print(f"  Attempt {i+1} failed: {e}")
                pass
                
            time.sleep(2)
            
        assert resumed_log_found, "Replica did not resume MQTT broker startup after promotion"

    except AssertionError as e:
        print(f"\nAssertion failed: {e}\nPrinting pg_replica logs:\n")
        logs = subprocess.check_output([
            "docker", "compose"", "-f", "tests/integration/multinode/"docker", "compose".multinode.yml", "logs", "pg_replica"
        ]).decode('utf-8')
        print(logs)
        raise
    finally:
        print("Tearing down multinode cluster...")
        subprocess.check_call([
            "docker", "compose"", "-f", "tests/integration/multinode/"docker", "compose".multinode.yml", "down", "-v"
        ])

if __name__ == "__main__":
    test_read_replica_pauses_mqtt()
