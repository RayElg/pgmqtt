import time
import socket
import sys
import threading

from proto_utils import (
    ReasonCode, get_broker_config,
    create_connect_packet, create_subscribe_packet, create_publish_packet,
    create_puback_packet, recv_packet,
    validate_connack, validate_suback, validate_publish, validate_puback,
    run_psql
)

# Configuration
NUM_MESSAGES = 200
TOPIC = "perf/test"
CLIENT_ID_PUB = "perf_pub"
CLIENT_ID_SUB = "perf_sub"

def setup_cdc():
    run_psql("DROP TABLE IF EXISTS perf_table CASCADE;")
    run_psql("CREATE TABLE perf_table (id serial PRIMARY KEY, data text);")
    run_psql("ALTER TABLE perf_table REPLICA IDENTITY FULL;")

def measure_published_qos0():
    host, port = get_broker_config()
    payload = b"perf message qos 0"
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_sub:
        s_sub.connect((host, port))
        s_sub.sendall(create_connect_packet(CLIENT_ID_SUB, clean_start=True))
        recv_packet(s_sub)
        s_sub.sendall(create_subscribe_packet(1, TOPIC, qos=0))
        recv_packet(s_sub)
        
        start_time = time.time()
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
            s_pub.connect((host, port))
            s_pub.sendall(create_connect_packet(CLIENT_ID_PUB, clean_start=True))
            recv_packet(s_pub)
            
            for _ in range(NUM_MESSAGES):
                s_pub.sendall(create_publish_packet(TOPIC, payload, qos=0))
        
        received = 0
        while received < NUM_MESSAGES:
            pkt = recv_packet(s_sub, timeout=2.0)
            if not pkt: break
            received += 1
            
        end_time = time.time()
        return received, end_time - start_time

def measure_published_qos1():
    host, port = get_broker_config()
    payload = b"perf message qos 1"
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_sub:
        s_sub.connect((host, port))
        s_sub.sendall(create_connect_packet(CLIENT_ID_SUB, clean_start=True))
        recv_packet(s_sub)
        s_sub.sendall(create_subscribe_packet(1, TOPIC, qos=1))
        recv_packet(s_sub)
        
        def sub_thread_task(results):
            count = 0
            while count < NUM_MESSAGES:
                pkt = recv_packet(s_sub, timeout=10.0)
                if not pkt: break
                try:
                    _, _, _, _, _, pid, _ = validate_publish(pkt)
                    if pid:
                        s_sub.sendall(create_puback_packet(pid))
                    count += 1
                except:
                    break
            results['count'] = count

        results = {'count': 0}
        t = threading.Thread(target=sub_thread_task, args=(results,))
        t.start()
        
        start_time = time.time()
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
            s_pub.connect((host, port))
            s_pub.sendall(create_connect_packet(CLIENT_ID_PUB, clean_start=True))
            recv_packet(s_pub)
            
            # PIPELINING: Send all messages first
            for i in range(NUM_MESSAGES):
                s_pub.sendall(create_publish_packet(TOPIC, payload, qos=1, packet_id=i+1))
            
            # Then collect all ACKs
            acks_received = 0
            while acks_received < NUM_MESSAGES:
                ack = recv_packet(s_pub, timeout=5.0)
                if not ack: break
                acks_received += 1
        
        t.join()
        end_time = time.time()
        return results['count'], end_time - start_time

def measure_published_qos1_sync():
    host, port = get_broker_config()
    payload = b"perf message qos 1"
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_sub:
        s_sub.connect((host, port))
        s_sub.sendall(create_connect_packet(CLIENT_ID_SUB, clean_start=True))
        recv_packet(s_sub)
        s_sub.sendall(create_subscribe_packet(1, TOPIC, qos=1))
        recv_packet(s_sub)
        
        def sub_thread_task(results):
            count = 0
            while count < NUM_MESSAGES:
                pkt = recv_packet(s_sub, timeout=10.0)
                if not pkt: break
                try:
                    _, _, _, _, _, pid, _ = validate_publish(pkt)
                    if pid:
                        s_sub.sendall(create_puback_packet(pid))
                    count += 1
                except:
                    break
            results['count'] = count

        results = {'count': 0}
        t = threading.Thread(target=sub_thread_task, args=(results,))
        t.start()
        
        start_time = time.time()
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
            s_pub.connect((host, port))
            s_pub.sendall(create_connect_packet(CLIENT_ID_PUB, clean_start=True))
            recv_packet(s_pub)
            
            # NON-PIPELINED: Wait for ACK before sending next
            acks_received = 0
            for i in range(NUM_MESSAGES):
                s_pub.sendall(create_publish_packet(TOPIC, payload, qos=1, packet_id=i+1))
                ack = recv_packet(s_pub, timeout=5.0)
                if ack:
                    acks_received += 1
                else:
                    break
        
        t.join()
        end_time = time.time()
        return results['count'], end_time - start_time

def measure_rendered_qos0():
    host, port = get_broker_config()
    setup_cdc()
    run_psql(f"SELECT pgmqtt_add_mapping('public', 'perf_table', '{TOPIC}', '{{{{columns.data}}}}', 0);")
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_sub:
        s_sub.connect((host, port))
        s_sub.sendall(create_connect_packet(CLIENT_ID_SUB, clean_start=True))
        recv_packet(s_sub)
        s_sub.sendall(create_subscribe_packet(1, TOPIC, qos=0))
        recv_packet(s_sub)
        
        time.sleep(1)
        
        start_time = time.time()
        run_psql(f"INSERT INTO perf_table (data) SELECT 'msg-' || generate_series(1, {NUM_MESSAGES});")
        
        received = 0
        while received < NUM_MESSAGES:
            pkt = recv_packet(s_sub, timeout=10.0)
            if not pkt: break
            received += 1
            
        end_time = time.time()
        return received, end_time - start_time

def measure_rendered_qos1():
    host, port = get_broker_config()
    setup_cdc()
    run_psql(f"SELECT pgmqtt_add_mapping('public', 'perf_table', '{TOPIC}', '{{{{columns.data}}}}', 1);")
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_sub:
        s_sub.connect((host, port))
        s_sub.sendall(create_connect_packet(CLIENT_ID_SUB, clean_start=True))
        recv_packet(s_sub)
        s_sub.sendall(create_subscribe_packet(1, TOPIC, qos=1))
        recv_packet(s_sub)
        
        time.sleep(1)
        
        start_time = time.time()
        run_psql(f"INSERT INTO perf_table (data) SELECT 'msg-' || generate_series(1, {NUM_MESSAGES});")
        
        received = 0
        while received < NUM_MESSAGES:
            pkt = recv_packet(s_sub, timeout=10.0)
            if not pkt: break
            try:
                _, _, _, _, _, pid, _ = validate_publish(pkt)
                if pid:
                    s_sub.sendall(create_puback_packet(pid))
                received += 1
            except:
                break
            
        end_time = time.time()
        return received, end_time - start_time

if __name__ == "__main__":
    print(f"Starting performance characterization (n={NUM_MESSAGES})...")
    
    results = []
    
    try:
        print("Testing Published QoS 0...")
        c, t = measure_published_qos0()
        results.append(("Published QoS 0", c, t))
        
        print("Testing Published QoS 1 (Pipelined)...")
        c, t = measure_published_qos1()
        results.append(("Published QoS 1 (Pipe)", c, t))
        
        print("Testing Published QoS 1 (Sync)...")
        c, t = measure_published_qos1_sync()
        results.append(("Published QoS 1 (Sync)", c, t))
        
        print("Testing Rendered QoS 0...")
        c, t = measure_rendered_qos0()
        results.append(("Rendered QoS 0", c, t))
        
        print("Testing Rendered QoS 1...")
        c, t = measure_rendered_qos1()
        results.append(("Rendered QoS 1", c, t))
    except Exception as e:
        print(f"Error during testing: {e}")
    
    print("\n" + "="*65)
    print(f"{'Scenario':<25} | {'Count':<6} | {'Time (s)':<10} | {'Msg/s':<10}")
    print("-" * 65)
    for name, count, duration in results:
        rate = count / duration if duration > 0 else 0
        print(f"{name:<25} | {count:<6} | {duration:<10.2f} | {rate:<10.2f}")
    print("="*65)

if __name__ == "__main__":
    print(f"Starting performance characterization (n={NUM_MESSAGES})...")
    
    results = []
    
    try:
        print("Testing Published QoS 0...")
        c, t = measure_published_qos0()
        results.append(("Published QoS 0", c, t))
        
        print("Testing Published QoS 1 (Pipelined)...")
        c, t = measure_published_qos1()
        results.append(("Published QoS 1 (Pipe)", c, t))
        
        print("Testing Published QoS 1 (Sync)...")
        c, t = measure_published_qos1_sync()
        results.append(("Published QoS 1 (Sync)", c, t))
        
        print("Testing Rendered QoS 0...")
        c, t = measure_rendered_qos0()
        results.append(("Rendered QoS 0", c, t))
        
        print("Testing Rendered QoS 1...")
        c, t = measure_rendered_qos1()
        results.append(("Rendered QoS 1", c, t))
    except Exception as e:
        print(f"Error during testing: {e}")
    
    print("\n" + "="*65)
    print(f"{'Scenario':<25} | {'Count':<6} | {'Time (s)':<10} | {'Msg/s':<10}")
    print("-" * 65)
    for name, count, duration in results:
        rate = count / duration if duration > 0 else 0
        print(f"{name:<25} | {count:<6} | {duration:<10.2f} | {rate:<10.2f}")
    print("="*65)
