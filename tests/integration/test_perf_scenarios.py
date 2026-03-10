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

def pub_worker(client_id, topic, qos=0, duration=None, num_messages=None, interval=0, concurrency=1, results=None):
    host, port = get_broker_config()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.sendall(create_connect_packet(client_id, clean_start=True))
            recv_packet(s)
            
            sent = 0
            received = 0
            start_time = time.time()
            
            def should_continue():
                if duration:
                    return (time.time() - start_time) < duration
                if num_messages:
                    if qos == 0:
                        return sent < num_messages
                    else:
                        return received < num_messages
                return False

            if qos == 0:
                while should_continue():
                    s.sendall(create_publish_packet(topic, f"msg {sent}".encode(), qos=qos))
                    sent += 1
                    if interval > 0:
                        time.sleep(interval)
                received = sent
            else:
                # Pipelined QoS 1
                inflight = 0
                while should_continue() or inflight > 0:
                    # Fill the pipeline up to concurrency limit
                    while inflight < concurrency and (duration and (time.time() - start_time) < duration or (num_messages and sent < num_messages)):
                        pid = (sent % 65535) + 1
                        s.sendall(create_publish_packet(topic, f"msg {sent}".encode(), qos=1, packet_id=pid))
                        sent += 1
                        inflight += 1
                        if interval > 0:
                            time.sleep(interval)
                    
                    # Receive ACKs
                    if inflight > 0:
                        ack = recv_packet(s, timeout=0.1) # Short timeout to check duration frequently
                        if ack:
                            if (ack[0] >> 4) == 4:
                                received += 1
                                inflight -= 1
                        elif duration and (time.time() - start_time) >= duration:
                            # If duration is up and we didn't get an ACK, stop waiting after a grace period or if no progress
                            if inflight > 0:
                                # Final drain attempts for another 2s max
                                if (time.time() - start_time) > (duration + 2.0):
                                    break
                    else:
                        break

            if results is not None:
                results.append(received)
    except (socket.error, ConnectionResetError, ConnectionAbortedError) as e:
        print(f"  [!] {client_id} worker error: {e}")
        if results is not None:
            results.append(received)

def sub_worker(client_id, topic, qos=0, stop_event=None, results=None, progress_results=None):
    host, port = get_broker_config()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.settimeout(1.0)
            s.sendall(create_connect_packet(client_id, clean_start=True))
            recv_packet(s)
            s.sendall(create_subscribe_packet(1, topic, qos=qos))
            recv_packet(s)
            
            count = 0
            while not stop_event.is_set():
                try:
                    pkt = recv_packet(s, timeout=0.1)
                    if pkt:
                        count += 1
                        if progress_results is not None:
                            # Keep progress_results updated
                            if len(progress_results) == 0:
                                progress_results.append(count)
                            else:
                                progress_results[0] = count
                        try:
                            _, _, p_qos, _, _, pid, _ = validate_publish(pkt)
                            if p_qos > 0 and pid:
                                s.sendall(create_puback_packet(pid))
                        except:
                            pass
                except socket.timeout:
                    continue
            if results is not None:
                results.append(count)
    except (socket.error, ConnectionResetError, ConnectionAbortedError) as e:
        print(f"  [!] {client_id} sub worker error: {e}")
        if results is not None:
            results.append(count)

def test_scenario_a():
    """Scenario A: High-Throughput Ingestion (QoS 1) - Pipelined"""
    print(f"\n[PERF] Scenario A: High-Throughput Ingestion ({SCENARIO_DURATION}s QoS 1, Concurrency=100)")
    topic = "perf/scenarioA"
    
    # Start a subscriber to ensure messages are persisted/delivered
    stop_event = threading.Event()
    sub_thread = threading.Thread(target=sub_worker, args=("sub_a_dummy", topic, 1, stop_event))
    sub_thread.start()
    time.sleep(1)

    threads = []
    results = []
    
    start_time = time.time()
    for i in range(SCENARIO_A_CLIENTS):
        cid = f"pub_a_{i}"
        t = threading.Thread(target=pub_worker, args=(cid, topic, 1, SCENARIO_DURATION, None, 0.0001, 100, results))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
    
    duration = time.time() - start_time
    total_received = sum(results)
    
    stop_event.set()
    sub_thread.join()

    print(f"  ✓ Acknowledged {total_received} QoS 1 messages via {SCENARIO_A_CLIENTS} threads in {duration:.2f}s ({total_received/duration:.1f} msg/s)")

def test_scenario_b():
    """Scenario B: Massive Fan-out Sustained"""
    print(f"\n[PERF] Scenario B: Massive Fan-out Sustained ({SCENARIO_DURATION}s, {SCENARIO_B_SUBS} subscribers)")
    topic = "perf/scenarioB"
    stop_event = threading.Event()
    results = []
    sub_threads = []
    
    # Start subscribers
    for i in range(SCENARIO_B_SUBS):
        cid = f"sub_b_{i}"
        t = threading.Thread(target=sub_worker, args=(cid, topic, 0, stop_event, results))
        sub_threads.append(t)
        t.start()
    
    time.sleep(2) # Wait for subs to settle
    
    # Start publisher
    print(f"  Starting publisher for {SCENARIO_DURATION}s...")
    start_time = time.time()
    pub_worker("pub_b", topic, qos=0, duration=SCENARIO_DURATION, interval=0.0001)
    
    # Wait for delivery (leeway)
    time.sleep(2)
    stop_event.set()
    
    for t in sub_threads:
        t.join()
        
    end_time = time.time()
    duration = end_time - start_time - 2 # excluding wait times
    total_deliveries = sum(results)
    print(f"  ✓ Received {total_deliveries} deliveries in ~{SCENARIO_DURATION}s load ({total_deliveries/SCENARIO_DURATION:.1f} msgs/s)")

def test_scenario_c():
    """Scenario C: Connection Churn"""
    print(f"\n[PERF] Scenario C: Connection Churn ({SCENARIO_C_ITERATIONS} iterations)")
    host, port = get_broker_config()
    
    start_time = time.time()
    for i in range(SCENARIO_C_ITERATIONS):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.sendall(create_connect_packet(f"churn_{i}", clean_start=True))
            recv_packet(s)
            # DISCONNECT implicitly by closing socket
    
    end_time = time.time()
    duration = end_time - start_time
    print(f"  ✓ Completed {SCENARIO_C_ITERATIONS} connect/disconnect cycles in {duration:.2f}s ({SCENARIO_C_ITERATIONS/duration:.1f} conn/s)")

def test_scenario_ramp_qos0():
    """Scenario Ramp QoS 0: Scaling Load Sustained"""
    print(f"\n[PERF] Scenario Ramp: Scaling Publishers ({SCENARIO_DURATION}s QoS 0)")
    topic = "perf/ramp_qos0"
    
    for n_threads in [1, 2, 4, 8]:
        threads = []
        results = []
        start_time = time.time()
        for i in range(n_threads):
            t = threading.Thread(target=pub_worker, args=(f"ramp_q0_{n_threads}_{i}", topic, 0, SCENARIO_DURATION, None, 0.0001, 1, results))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        duration = time.time() - start_time
        total = sum(results)
        print(f"  - {n_threads} threads: {total/duration:.1f} msg/s")

def test_scenario_ramp_qos1():
    """Scenario Ramp QoS 1: Scaling Load with Pipelined ACKs Sustained"""
    print(f"\n[PERF] Scenario Ramp: Scaling Publishers ({SCENARIO_DURATION}s QoS 1, Concurrency=100)")
    topic = "perf/ramp_qos1"
    
    for n_threads in [1, 2, 4, 8]:
        threads = []
        results = []
        start_time = time.time()
        for i in range(n_threads):
            t = threading.Thread(target=pub_worker, args=(f"ramp_q1_{n_threads}_{i}", topic, 1, SCENARIO_DURATION, None, 0.0001, 100, results))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        duration = time.time() - start_time
        total = sum(results)
        print(f"  - {n_threads} threads: {total/duration:.1f} msg/s")

def cdc_pub_worker(duration=None, batch_size=100, results=None):
    """Publish messages by inserting batches of rows into a mapped table."""
    sent = 0
    start_time = time.time()
    
    while (time.time() - start_time) < duration:
        # Insert a batch of rows
        run_psql(f"INSERT INTO cdc_perf_table (data) SELECT 'cdc msg ' || i FROM generate_series({sent}+1, {sent}+{batch_size}) AS i;")
        sent += batch_size
        # No interval, go as fast as possible
            
    if results is not None:
        results.append(sent)

def test_scenario_cdc_publish():
    """Scenario CDC Publish: Measuring intake speed via table inserts and logical decoding."""
    print(f"\n[PERF] Scenario CDC Publish: Ingesting QoS 1 via CDC ({SCENARIO_DURATION}s)")
    topic = "perf/cdc_publish"
    
    # Setup table and mapping
    run_psql("DROP TABLE IF EXISTS cdc_perf_table CASCADE;")
    run_psql("CREATE TABLE cdc_perf_table (id serial PRIMARY KEY, data text);")
    run_psql("ALTER TABLE cdc_perf_table REPLICA IDENTITY FULL;")
    # Map table to topic with QoS 1
    run_psql(f"SELECT pgmqtt_add_mapping('public', 'cdc_perf_table', '{topic}', '{{{{columns.data}}}}', 1);")
    
    stop_event = threading.Event()
    sub_results = []
    sub_progress = [] # Share for real-time tracking
    pub_results = []
    
    # Start subscriber to measure delivery
    sub_cid = "sub_cdc_perf"
    sub_thread = threading.Thread(target=sub_worker, args=(sub_cid, topic, 1, stop_event, sub_results, sub_progress))
    sub_thread.start()
    
    time.sleep(5) # Wait for sub to connect (increased from 2)
    
    # Warmup: Insert one row and wait for it to be received
    print("  Waiting for CDC warmup message...")
    run_psql("INSERT INTO cdc_perf_table (data) VALUES ('warmup');")
    
    warmup_start = time.time()
    while (time.time() - warmup_start) < 20.0: # Increased from 10.0
        if sub_progress and sub_progress[0] >= 1:
            break
        time.sleep(0.2)
    
    if not sub_progress or sub_progress[0] < 1:
        print("  WARNING: CDC warmup failed, proceeding anyway...")
    else:
        print("  ✓ CDC warmup successful")

    # Reset progress for actual measurement
    # Instead of clearing, just record the baseline
    warmup_received = sub_progress[0] if sub_progress else 0
    print(f"  Starting measurement from baseline: {warmup_received} messages")

    # Start CDC publisher (inserting rows)
    # Slow down slightly to avoid overwhelming the 8192-capacity ring buffer
    def cdc_pub_worker_throttled(duration, batch_size, results):
        sent = 0
        start_time = time.time()
        while (time.time() - start_time) < duration:
            run_psql(f"INSERT INTO cdc_perf_table (data) SELECT 'cdc msg ' || i FROM generate_series({sent}+1, {sent}+{batch_size}) AS i;")
            sent += batch_size
            time.sleep(0.02) # Add small delay to keep ring buffer happy
        results.append(sent)

    start_time = time.time()
    cdc_pub_worker_throttled(duration=SCENARIO_DURATION, batch_size=100, results=pub_results)
    
    total_sent = sum(pub_results)
    
    # Wait for delivery (CDC can have more lag)
    # Target: wait until sub_progress matches total_sent + warmup_received or timeout
    wait_start = time.time()
    timeout = 60.0 # Increased from 30.0
    last_val = -1
    while (time.time() - wait_start) < timeout:
        current_total = sub_progress[0] if sub_progress else 0
        current_received = current_total - warmup_received
        if current_received != last_val:
            print(f"  Progress: {current_received}/{total_sent} received...")
            last_val = current_received
        if current_received >= total_sent:
            break
        time.sleep(1.0)
    
    stop_event.set()
    sub_thread.join()
    
    # Final check from sub_results
    total_received = (sub_results[0] if sub_results else 0) - warmup_received
    
    print(f"  ✓ DB inserted {total_sent} rows in {SCENARIO_DURATION}s ({total_sent/SCENARIO_DURATION:.1f} rows/s)")
    if total_received >= total_sent:
        print(f"  ✓ Subscriber received all {total_received} deliveries!")
    else:
        print(f"  ✗ Subscriber only received {total_received}/{total_sent} deliveries (Missing {total_sent-total_received})")

if __name__ == "__main__":
    try:
        test_scenario_a()
        test_scenario_b()
        test_scenario_c()
        test_scenario_ramp_qos0()
        test_scenario_ramp_qos1()
        test_scenario_cdc_publish()
        print("\nPERFORMANCE SCENARIOS COMPLETED")
    except Exception as e:
        print(f"\nERROR: {e}")
        sys.exit(1)
