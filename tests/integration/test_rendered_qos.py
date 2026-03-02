import socket
import struct
import time
import sys
import os
import subprocess
import test_utils

MQTT_HOST = "localhost"
MQTT_PORT = 1883

from test_utils import run_sql

def encode_variable_byte_int(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value > 0:
            byte |= 0x80
        out.append(byte)
        if value == 0:
            break
    return out

def build_connect(client_id, clean_start=True):
    vh = bytearray()
    vh.extend(b"\x00\x04MQTT\x05")
    flags = 0x02 if clean_start else 0x00
    vh.append(flags)
    vh.extend(b"\x00\x3C")
    vh.append(0x00) # Properties
    payload = bytearray()
    payload.extend(struct.pack("!H", len(client_id)))
    payload.extend(client_id.encode())
    remaining_length = len(vh) + len(payload)
    pkt = bytearray([0x10])
    pkt.extend(encode_variable_byte_int(remaining_length))
    pkt.extend(vh)
    pkt.extend(payload)
    return pkt

def build_subscribe(packet_id, topic, qos):
    vh = bytearray(struct.pack("!H", packet_id))
    vh.append(0x00) # Properties
    payload = bytearray()
    payload.extend(struct.pack("!H", len(topic)))
    payload.extend(topic.encode())
    payload.append(qos)
    remaining_length = len(vh) + len(payload)
    pkt = bytearray([0x82])
    pkt.extend(encode_variable_byte_int(remaining_length))
    pkt.extend(vh)
    pkt.extend(payload)
    return pkt

def recv_packet(s, timeout=5.0):
    s.settimeout(timeout)
    try:
        header = s.recv(1)
        if not header: return None
        byte0 = header[0]
        multiplier = 1
        remaining_length = 0
        while True:
            byte = s.recv(1)[0]
            remaining_length += (byte & 0x7F) * multiplier
            if byte & 0x80 == 0:
                break
            multiplier *= 128
        body = s.recv(remaining_length)
        return (byte0, body)
    except socket.timeout:
        return None

def parse_publish(body):
    # topic length
    tlen = struct.unpack("!H", body[:2])[0]
    topic = body[2:2+tlen].decode()
    return topic

def test_rendered_qos():
    # 1. Setup mappings
    print("Setting up mappings for QOS 0 and QOS 1 tests...")
    run_sql("DROP TABLE IF EXISTS qos_test_table;")
    run_sql("CREATE TABLE qos_test_table (id serial primary key, name text, val text);")
    run_sql("SELECT pgmqtt_add_mapping('public', 'qos_test_table', 'test/{{ columns.name }}', '{{ columns.val }}', 0);") # Mapping 0: QOS 0
    
    # Connect subscriber
    print("Connecting subscriber with QOS 1 subscription...")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(build_connect("cdc_sub"))
    recv_packet(s) # CONNACK
    s.sendall(build_subscribe(1, "test/#", qos=1))
    recv_packet(s) # SUBACK

    # Test Rendered QOS 0
    print("Testing Rendered QOS 0...")
    run_sql("INSERT INTO qos_test_table (name, val) VALUES ('qos0', 'message0');")
    res = recv_packet(s, timeout=5.0)
    if res:
        header, body = res
        qos = (header & 0x06) >> 1
        topic = parse_publish(body)
        print(f"  Received PUBLISH: topic={topic}, qos={qos}")
        if qos != 0:
            print("  ✗ ERROR: Expected QOS 0")
            sys.exit(1)
        else:
            print("  ✓ SUCCESS: Received QOS 0")
    else:
        print("  ✗ ERROR: Timeout waiting for QOS 0 message")
        sys.exit(1)

    # Re-configure mapping for QOS 1
    print("Updating mapping to QOS 1...")
    run_sql("SELECT pgmqtt_add_mapping('public', 'qos_test_table', 'test/{{ columns.name }}', '{{ columns.val }}', 1);")
    
    print("Testing Rendered QOS 1...")
    run_sql("INSERT INTO qos_test_table (name, val) VALUES ('qos1', 'message1');")
    res = recv_packet(s, timeout=5.0)
    if res:
        header, body = res
        qos = (header & 0x06) >> 1
        topic = parse_publish(body)
        packet_id = struct.unpack("!H", body[2+len(topic):2+len(topic)+2])[0]
        print(f"  Received PUBLISH: topic={topic}, qos={qos}, pid={packet_id}")
        if qos != 1:
            print("  ✗ ERROR: Expected QOS 1")
            sys.exit(1)
        else:
            print("  ✓ SUCCESS: Received QOS 1")
            # Ack it
            ack = bytearray([0x40, 0x02])
            ack.extend(struct.pack("!H", packet_id))
            s.sendall(ack)
    else:
        print("  ✗ ERROR: Timeout waiting for QOS 1 message")
        sys.exit(1)

    print("\nALL RENDERED QOS TESTS PASSED")
    s.close()

if __name__ == "__main__":
    test_rendered_qos()
