"""Observability demo simulator.

Two groups of clients:
  - Publishers: produce sensor readings at varying rates with realistic churn
    (connect, publish for a while, disconnect, pause, repeat).
  - Subscribers: one permanent client per sensor group that subscribes to the
    published topics so msgs_sent is non-zero and delivery is visible.
"""
import json
import math
import os
import random
import threading
import time

import paho.mqtt.client as mqtt

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

PUBLISHERS = [
    {"id": "sensor-alpha",   "interval": 0.4,  "qos": 0, "topic": "obs/sensor/alpha"},
    {"id": "sensor-beta",    "interval": 0.8,  "qos": 1, "topic": "obs/sensor/beta"},
    {"id": "sensor-gamma",   "interval": 0.25, "qos": 0, "topic": "obs/sensor/gamma"},
    {"id": "sensor-delta",   "interval": 1.2,  "qos": 1, "topic": "obs/sensor/delta"},
    {"id": "sensor-epsilon", "interval": 0.6,  "qos": 0, "topic": "obs/sensor/epsilon"},
    {"id": "monitor-north",  "interval": 2.0,  "qos": 0, "topic": "obs/monitor/north"},
    {"id": "monitor-south",  "interval": 2.0,  "qos": 0, "topic": "obs/monitor/south"},
]

# Subscribers stay permanently connected and subscribe to all sensor topics.
SUBSCRIBERS = [
    {"id": "dashboard-a", "topics": ["obs/sensor/#"]},
    {"id": "dashboard-b", "topics": ["obs/sensor/alpha", "obs/sensor/beta"]},
    {"id": "dashboard-c", "topics": ["obs/monitor/#"]},
]


def run_publisher(cfg: dict):
    client_id = cfg["id"]
    interval  = cfg["interval"]
    qos       = cfg["qos"]
    topic     = cfg["topic"]
    t = 0.0

    while True:
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=mqtt.MQTTv5,
        )
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=20)
        client.loop_start()

        # Each session lasts 8–30 s then reconnects (creates visible churn)
        session_secs = random.uniform(8, 30)
        deadline = time.time() + session_secs

        while time.time() < deadline:
            payload = json.dumps({
                "id":  client_id,
                "ts":  round(time.time(), 3),
                "v1":  round(20 + 8 * math.sin(t) + random.gauss(0, 0.3), 2),
                "v2":  round(50 + 15 * math.cos(t * 0.7) + random.gauss(0, 0.5), 1),
                "seq": int(t / interval),
            })
            client.publish(topic, payload, qos=qos)
            t += interval
            time.sleep(interval)

        client.loop_stop()
        client.disconnect()
        time.sleep(random.uniform(0.5, 2.5))


def run_subscriber(cfg: dict):
    client_id = cfg["id"]
    topics    = cfg["topics"]

    def on_connect(client, userdata, flags, reason_code, properties):
        print(f"[{client_id}] connected, subscribing to {userdata}", flush=True)
        for topic in userdata:
            client.subscribe(topic, qos=0)

    while True:
        try:
            client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id=client_id,
                protocol=mqtt.MQTTv5,
            )
            client.user_data_set(topics)
            client.on_connect = on_connect
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            client.loop_forever()
        except Exception as exc:
            print(f"[{client_id}] error: {exc} — retrying", flush=True)
            time.sleep(2)


def main():
    print(f"Connecting to {MQTT_HOST}:{MQTT_PORT}", flush=True)

    threads = []

    # Subscribers first so they're ready before publishers arrive
    for cfg in SUBSCRIBERS:
        t = threading.Thread(target=run_subscriber, args=(cfg,), daemon=True)
        t.start()
        threads.append(t)
        print(f"Subscriber: {cfg['id']} → {cfg['topics']}", flush=True)
        time.sleep(0.3)

    time.sleep(1)

    for i, cfg in enumerate(PUBLISHERS):
        time.sleep(i * 0.3)
        t = threading.Thread(target=run_publisher, args=(cfg,), daemon=True)
        t.start()
        threads.append(t)
        print(f"Publisher:  {cfg['id']} ({cfg['interval']}s)", flush=True)

    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
