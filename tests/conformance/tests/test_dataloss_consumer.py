import socket
import struct
import sys
import os
import time

# Add parent directory to sys.path to find proto_utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto_utils import (
    MQTTControlPacket, get_broker_config,
    create_connect_packet, create_subscribe_packet,
    create_publish_packet, recv_packet
)

def test_message_expiry():
    host, port = get_broker_config()
    topic = "test/expiry"
    client_id = "expiry_tester"
    
    print("Testing Message Expiry (MQTT 5.0)...")
    
    # 1. Consumer connects with Session Expiry and subscribes
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet(client_id, clean_start=True, properties={0x11: 3600}))
        recv_packet(s_cons)
        s_cons.sendall(create_subscribe_packet(5, topic, qos=1))
        recv_packet(s_cons)
    
    # 2. Publisher publishes with Message Expiry = 2 seconds
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
        s_pub.connect((host, port))
        s_pub.sendall(create_connect_packet("expiry_pub"))
        recv_packet(s_pub)
        
        # Message Expiry property (0x02) = 2 seconds
        s_pub.sendall(create_publish_packet(topic, b"expire me", qos=1, packet_id=101, properties={0x02: 2}))
        recv_packet(s_pub) # puback
        
    print("Message published with 2s expiry. Waiting 4s...")
    time.sleep(4)
    
    # 3. Consumer reconnects with Clean Start=False
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet(client_id, clean_start=False))
        connack = recv_packet(s_cons)
        
        data = recv_packet(s_cons, timeout=2.0)
        if data:
            print(f"FAILURE: Received message that should have expired: {data.hex()}")
        else:
            print("SUCCESS: Session present but message expired and was dropped.")

def test_session_expiry_loss():
    host, port = get_broker_config()
    topic = "test/session_expiry"
    client_id = "session_expiry_tester"
    
    print("Testing Session Expiry...")
    
    # 1. Consumer connects with Session Expiry = 2 seconds
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet(client_id, clean_start=True, properties={0x11: 2}))
        recv_packet(s_cons)
        s_cons.sendall(create_subscribe_packet(6, topic, qos=1))
        recv_packet(s_cons)
    
    # 2. Publisher sends message
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
        s_pub.connect((host, port))
        s_pub.sendall(create_connect_packet("session_expiry_pub"))
        recv_packet(s_pub)
        s_pub.sendall(create_publish_packet(topic, b"session message", qos=1, packet_id=102))
        recv_packet(s_pub)
        
    print("Session should expire in 2s. Waiting 4s...")
    time.sleep(4)
    
    # 3. Consumer reconnects with Clean Start=False
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_cons:
        s_cons.connect((host, port))
        s_cons.sendall(create_connect_packet(client_id, clean_start=False))
        connack = recv_packet(s_cons)
        session_present = connack[2] & 0x01 if connack else 0
        if session_present:
             print("FAILURE: Session still present after expiry.")
        else:
             print("SUCCESS: Session expired and data was lost as expected.")

if __name__ == "__main__":
    test_message_expiry()
    test_session_expiry_loss()
