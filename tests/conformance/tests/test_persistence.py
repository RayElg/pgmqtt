"""
MQTT 5.0 Conformance Tests: Persistence
Tests verify that configurable topic persistence stores messages tracking QoS, Retain flags into Postgres tables.
"""
import socket
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from proto_utils import (
    ReasonCode, get_broker_config,
    create_connect_packet, create_publish_packet,
    recv_packet, validate_connack
)

try:
    import psycopg2
except ImportError:
    print("Warning: psycopg2 not installed. Use 'pip install psycopg2-binary'.")
    sys.exit(0)

def test_publish_persistence():
    """
    Test: Client PUBLISH messages are inserted into pgmqtt_messages and pgmqtt_retained.
    """
    host, port = get_broker_config()
    topic = "persistent/test/1"
    payload = b"test persistence payload"
    
    # Initialize Database Tables
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host=host,
        port=5432
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS pgmqtt;")
    cur.execute("SELECT pgmqtt_add_outbound_mapping('public', 'dummy_table', 'dummy/test', '{}', true);")
    
    # Connect publisher
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_pub:
        s_pub.connect((host, port))
        s_pub.sendall(create_connect_packet("pub_pers", clean_start=True))
        connack = recv_packet(s_pub)
        validate_connack(connack)
        
        # Publish with retain=True
        s_pub.sendall(create_publish_packet(topic, payload, qos=0, retain=True))
        time.sleep(0.5) # allow time for worker to persist
        
    # Query database
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host=host,
        port=5432
    )
    cur = conn.cursor()
    
    # Check pgmqtt_messages
    cur.execute("SELECT payload, qos, retain FROM pgmqtt_messages WHERE topic = %s ORDER BY created_at DESC LIMIT 1;", (topic,))
    row = cur.fetchone()
    assert row is not None, f"No message found in pgmqtt_messages for topic {topic}"
    # fetchone returns memoryview for bytea
    db_payload, qos, retain = row
    if hasattr(db_payload, 'tobytes'):
        db_payload = db_payload.tobytes()
        
    assert db_payload == payload, "Payload mismatch in database"
    assert qos == 0, "QoS mismatch in database"
    assert retain is True, "Retain mismatch in database"
    
    # Check pgmqtt_retained
    cur.execute(
        "SELECT m.payload FROM pgmqtt_retained r JOIN pgmqtt_messages m ON r.message_id = m.id WHERE r.topic = %s;",
        (topic,)
    )
    r_row = cur.fetchone()
    assert r_row is not None, f"No message found in pgmqtt_retained for topic {topic}"
    db_payload_ret = r_row[0]
    if hasattr(db_payload_ret, 'tobytes'):
        db_payload_ret = db_payload_ret.tobytes()
        
    assert db_payload_ret == payload, "Payload mismatch in retained table"
    
    cur.close()
    conn.close()
    print("✓ test_publish_persistence passed")

if __name__ == "__main__":
    test_publish_persistence()
    print("\nAll persistence tests passed!")
