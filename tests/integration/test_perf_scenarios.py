import time
import socket
import threading
import sys
from proto_utils import (
    get_broker_config,
    create_connect_packet, create_subscribe_packet, create_publish_packet,
    create_puback_packet, recv_packet,
    validate_connack, validate_suback, validate_publish, validate_puback,
    run_psql
)

# Shared Config
SCENARIO_DURATION = 5
SCENARIO_A_CLIENTS = 5
SCENARIO_B_SUBS = 50
SCENARIO_C_ITERATIONS = 50

def send_disconnect(s):
    """Sends an MQTT DISCONNECT packet (0xE0 0x00)."""
    try:
        s.sendall(b'\xe0\x00')
    except:
        pass

def pub_worker(client_id, topic, qos=0, duration=None, num_messages=None, interval=0, concurrency=1, results=None):
    host, port = get_broker_config()
    received = 0
    sent = 0
    start_time = time.time()
    
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5.0)
            s.connect((host, port))
            
            # Connect
            s.sendall(create_connect_packet(client_id, clean_start=True))
            if not recv_packet(s):
                return

            def is_time_up():
                return duration and (time.time() - start_time) >= duration

            def is_count_reached():
                return num_messages and sent >= num_messages

            # Logic Loop
            inflight = {} # packet_id -> timestamp (for QoS 1)
            
            while not (is_time_up() or is_count_reached()) or (qos > 0 and len(inflight) > 0):
                # 1. SEND (Fill pipeline)
                if not (is_time_up() or is_count_reached()):
                    while len(inflight) < concurrency:
                        pid = (sent % 65535) + 1
                        payload = f"msg {sent}".encode()
                        try:
                            s.sendall(create_publish_packet(topic, payload, qos=qos, packet_id=pid))
                            if qos == 0:
                                sent += 1
                                received += 1 # QoS 0 is "fire and forget"
                            else:
                                inflight[pid] = time.time()
                                sent += 1
                            
                            if interval > 0:
                                time.sleep(interval)
                        except (socket.error, BrokenPipeError):
                            break

                # 2. RECEIVE (Drain ACKs)
                if qos > 0 and len(inflight) > 0:
                    try:
                        # Use a shorter timeout while active, longer if just draining at the end
                        read_timeout = 0.05 if not is_time_up() else 0.5
                        s.settimeout(read_timeout)
                        ack = recv_packet(s)
                        if ack and (ack[0] >> 4) == 4: # PUBACK
                            received += 1
                            # Simplified: we assume ordered ACKs for this test or pop first
                            if inflight:
                                inflight.pop(next(iter(inflight)))
                    except socket.timeout:
                        # If we've been draining for more than 3s past duration, give up
                        if is_time_up() and (time.time() - (start_time + duration)) > 3.0:
                            break
                        continue
                    except (socket.error, BrokenPipeError):
                        break
                
                if qos == 0 and is_time_up():
                    break

            # 3. GRACEFUL EXIT
            send_disconnect(s)
            time.sleep(0.1) # Linger to allow FIN/ACK handshake
            
    except Exception as e:
        # Only print error if it happened during the active phase
        if not (duration and (time.time() - start_time) >= duration):
            print(f"  [!] {client_id} worker error: {e}")
    finally:
        if results is not None:
            results.append(received)

def sub_worker(client_id, topic, qos=0, stop_event=None, results=None, progress_results=None):
    host, port = get_broker_config()
    count = 0
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.settimeout(1.0)
            s.sendall(create_connect_packet(client_id, clean_start=True))
            recv_packet(s)
            
            s.sendall(create_subscribe_packet(1, topic, qos=qos))
            recv_packet(s)
            
            while not stop_event.is_set():
                try:
                    pkt = recv_packet(s, timeout=0.2)
                    if pkt:
                        count += 1
                        if progress_results is not None:
                            if not progress_results: progress_results.append(0)
                            progress_results[0] = count
                        
                        # Handle PUBACK if needed
                        try:
                            _, _, p_qos, _, _, pid, _ = validate_publish(pkt)
                            if p_qos > 0 and pid:
                                s.sendall(create_puback_packet(pid))
                        except:
                            pass
                except socket.timeout:
                    continue
            
            send_disconnect(s)
    except Exception as e:
        if not stop_event.is_set():
            print(f"  [!] {client_id} sub error: {e}")
    finally:
        if results is not None:
            results.append(count)

### Scenarios

def test_scenario_a():
    print(f"\n[PERF] Scenario A: High-Throughput Ingestion ({SCENARIO_DURATION}s QoS 1, Concurrency=100)")
    topic = "perf/scenarioA"
    results = []
    threads = []
    
    start_time = time.time()
    for i in range(SCENARIO_A_CLIENTS):
        t = threading.Thread(target=pub_worker, args=(f"pub_a_{i}", topic, 1, SCENARIO_DURATION, None, 0, 100, results))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
    
    duration = time.time() - start_time
    total = sum(results)
    print(f"  ✓ Acknowledged {total} QoS 1 messages in {duration:.2f}s ({total/duration:.1f} msg/s)")

def test_scenario_b():
    print(f"\n[PERF] Scenario B: Massive Fan-out ({SCENARIO_DURATION}s, {SCENARIO_B_SUBS} subs)")
    topic = "perf/scenarioB"
    stop_event = threading.Event()
    results = []
    sub_threads = []
    
    for i in range(SCENARIO_B_SUBS):
        t = threading.Thread(target=sub_worker, args=(f"sub_b_{i}", topic, 0, stop_event, results))
        sub_threads.append(t)
        t.start()
    
    time.sleep(1)
    pub_worker("pub_b_master", topic, qos=0, duration=SCENARIO_DURATION)
    
    time.sleep(2)
    stop_event.set()
    for t in sub_threads: t.join()
    
    total = sum(results)
    print(f"  ✓ Total deliveries: {total} ({total/SCENARIO_DURATION:.1f} msg/s)")

def test_scenario_c():
    print(f"\n[PERF] Scenario C: Connection Churn ({SCENARIO_C_ITERATIONS} cycles)")
    host, port = get_broker_config()
    start = time.time()
    for i in range(SCENARIO_C_ITERATIONS):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.sendall(create_connect_packet(f"c_{i}", clean_start=True))
            recv_packet(s)
            send_disconnect(s)
    dur = time.time() - start
    print(f"  ✓ Completed in {dur:.2f}s ({SCENARIO_C_ITERATIONS/dur:.1f} conn/s)")

def test_scenario_cdc_publish():
    print(f"\n[PERF] Scenario CDC: Ingesting via DB ({SCENARIO_DURATION}s)")
    topic = "perf/cdc"
    
    run_psql("DROP TABLE IF EXISTS cdc_perf CASCADE;")
    run_psql("CREATE TABLE cdc_perf (id serial PRIMARY KEY, data text);")
    run_psql("ALTER TABLE cdc_perf REPLICA IDENTITY FULL;")
    run_psql(f"SELECT pgmqtt_add_mapping('public', 'cdc_perf', '{topic}', '{{{{columns.data}}}}', 1);")
    
    stop_event = threading.Event()
    sub_res = []
    progress = [0]
    
    sub_t = threading.Thread(target=sub_worker, args=("sub_cdc", topic, 1, stop_event, sub_res, progress))
    sub_t.start()
    time.sleep(2)
    
    print("  Starting DB inserts...")
    start_time = time.time()
    sent = 0
    while (time.time() - start_time) < SCENARIO_DURATION:
        run_psql(f"INSERT INTO cdc_perf (data) SELECT 'val' FROM generate_series(1, 100);")
        sent += 100
        time.sleep(0.05)
    
    # Wait for drain
    timeout = time.time() + 15
    while progress[0] < sent and time.time() < timeout:
        time.sleep(0.5)
        
    stop_event.set()
    sub_t.join()
    print(f"  ✓ Sent {sent}, Received {sub_res[0] if sub_res else 0}")

if __name__ == "__main__":
    test_scenario_a()
    test_scenario_b()
    test_scenario_c()
    test_scenario_cdc_publish()
    print("\nALL TESTS COMPLETE")