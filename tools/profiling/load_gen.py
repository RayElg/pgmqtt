"""
Sustained pub/sub load generator for perf profiling.

Run with perf attached to the pgmqtt BGW.  Emits traffic for `duration`
seconds and prints publisher-side rate + subscriber-side delivery rate.

Usage:
    python tools/profiling/load_gen.py --mode qos0 --duration 45 \
        --publishers 4 --subscribers 4 --payload-bytes 128

Modes:
    qos0          -- fire-and-forget publishes, subscribers count deliveries
    qos1          -- QoS 1 publishes, subscribers PUBACK every message
    cdc           -- batched INSERTs driving outbound mapping
    inbound       -- QoS 0 publishes that match an inbound mapping (no sub)
"""
import argparse
import json
import os
import socket
import sys
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tests", "integration"))

from proto_utils import (  # noqa: E402
    MQTT_HOST,
    MQTT_PORT,
    MQTTControlPacket,
    create_connect_packet,
    create_publish_packet,
    create_puback_packet,
    create_subscribe_packet,
    recv_packet,
    run_psql,
    validate_publish,
)


def _connect(client_id):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.sendall(create_connect_packet(client_id, clean_start=True))
    recv_packet(s)
    return s


def _subscriber(idx, topic, qos, stop_evt, counter):
    s = _connect(f"loadgen_sub_{idx}_{os.getpid()}")
    s.sendall(create_subscribe_packet(1, topic, qos=qos))
    recv_packet(s)
    s.settimeout(0.5)
    local = 0
    while not stop_evt.is_set():
        pkt = recv_packet(s, timeout=0.5)
        if pkt is None:
            continue
        if (pkt[0] >> 4) != MQTTControlPacket.PUBLISH:
            continue
        local += 1
        if qos == 1:
            try:
                _, _, _, _, _, pid, _ = validate_publish(pkt)
                if pid is not None:
                    s.sendall(create_puback_packet(pid))
            except Exception:
                pass
        if local % 1000 == 0:
            counter["n"] += 1000
            local = 0
    counter["n"] += local
    try:
        s.close()
    except Exception:
        pass


def _publisher(idx, topic_prefix, qos, payload, stop_evt, counter, per_pub_rate):
    """
    Publisher paced at `per_pub_rate` msgs/sec.  The broker disconnects any
    client whose unparsed read buffer exceeds 64 KiB (MAX_REQUEST_BYTES), so
    pacing prevents us from outrunning the per-tick parse and getting kicked.
    """
    s = _connect(f"loadgen_pub_{idx}_{os.getpid()}")
    pid = 1
    sent = 0
    interval = 1.0 / per_pub_rate if per_pub_rate > 0 else 0.0
    next_send = time.perf_counter()
    while not stop_evt.is_set():
        t = f"{topic_prefix}/p{idx}"
        try:
            if qos == 0:
                s.sendall(create_publish_packet(t, payload, qos=0))
            else:
                s.sendall(create_publish_packet(t, payload, qos=1, packet_id=pid))
                pid = (pid % 65535) + 1
                s.settimeout(0.001)
                while True:
                    try:
                        ack = s.recv(4096)
                        if not ack:
                            break
                    except socket.timeout:
                        break
                    except BlockingIOError:
                        break
        except Exception:
            break
        sent += 1
        if sent % 1000 == 0:
            counter["n"] += 1000
            sent = 0
        if interval > 0:
            next_send += interval
            sleep_for = next_send - time.perf_counter()
            if sleep_for > 0:
                time.sleep(sleep_for)
            elif sleep_for < -0.5:
                # fell way behind; drop the lag so we don't burst
                next_send = time.perf_counter()
    counter["n"] += sent
    try:
        s.close()
    except Exception:
        pass


def run_qos(mode, duration, n_pub, n_sub, payload_bytes, per_pub_rate):
    qos = 0 if mode == "qos0" else 1
    topic_prefix = "loadgen/x"
    payload = os.urandom(payload_bytes)
    stop = threading.Event()

    subs = []
    for i in range(n_sub):
        c = {"n": 0}
        t = threading.Thread(target=_subscriber, args=(i, f"{topic_prefix}/#", qos, stop, c))
        t.daemon = True
        t.start()
        subs.append((t, c))
    time.sleep(0.5)  # let subs settle

    pubs = []
    for i in range(n_pub):
        c = {"n": 0}
        t = threading.Thread(
            target=_publisher,
            args=(i, topic_prefix, qos, payload, stop, c, per_pub_rate),
        )
        t.daemon = True
        t.start()
        pubs.append((t, c))

    print(
        f"[loadgen] mode={mode} pubs={n_pub} subs={n_sub} payload={payload_bytes}B "
        f"duration={duration}s rate={per_pub_rate}/pub",
        flush=True,
    )
    t0 = time.time()
    last_pub = 0
    last_sub = 0
    while time.time() - t0 < duration:
        time.sleep(1.0)
        now = time.time()
        pub_total = sum(c["n"] for _, c in pubs)
        sub_total = sum(c["n"] for _, c in subs)
        print(
            f"  t+{int(now - t0):02d}s  pub={pub_total - last_pub:>6}/s  "
            f"sub={sub_total - last_sub:>6}/s  (pub_total={pub_total} sub_total={sub_total})",
            flush=True,
        )
        last_pub = pub_total
        last_sub = sub_total

    stop.set()
    time.sleep(1.5)

    pub_total = sum(c["n"] for _, c in pubs)
    sub_total = sum(c["n"] for _, c in subs)
    elapsed = time.time() - t0
    print()
    print(f"[loadgen] elapsed={elapsed:.1f}s  pub_total={pub_total} ({pub_total/elapsed:.0f}/s)  "
          f"sub_total={sub_total} ({sub_total/elapsed:.0f}/s)")


def run_inbound(duration, n_pub, _payload_bytes):
    """Publish into an inbound-mapped topic so the BGW writes rows to a table."""
    run_psql("DROP TABLE IF EXISTS loadgen_inbound CASCADE;")
    run_psql(
        "CREATE TABLE loadgen_inbound ("
        "site_id text NOT NULL, sensor_id text NOT NULL, value numeric,"
        "PRIMARY KEY (site_id, sensor_id))"
    )
    run_psql(
        "SELECT pgmqtt_add_inbound_mapping("
        "'loadgen/{site_id}/data/{sensor_id}', 'loadgen_inbound',"
        "'{\"site_id\": \"{site_id}\", \"sensor_id\": \"{sensor_id}\", \"value\": \"$.value\"}'::jsonb,"
        "'upsert', ARRAY['site_id','sensor_id'], 'public', 'loadgen_inbound')"
    )
    time.sleep(2)

    stop = threading.Event()

    def worker(idx, counter):
        s = _connect(f"loadgen_inb_{idx}")
        i = 0
        while not stop.is_set():
            payload = json.dumps({"value": i * 0.1}).encode()
            topic = f"loadgen/site-{idx}/data/s{i % 128}"
            try:
                s.sendall(create_publish_packet(topic, payload, qos=0))
            except Exception:
                break
            i += 1
            if i % 500 == 0:
                counter["n"] += 500
        counter["n"] += i % 500
        s.close()

    pubs = []
    for i in range(n_pub):
        c = {"n": 0}
        t = threading.Thread(target=worker, args=(i, c))
        t.daemon = True
        t.start()
        pubs.append((t, c))

    print(f"[loadgen] mode=inbound pubs={n_pub} duration={duration}s", flush=True)
    t0 = time.time()
    last_pub = 0
    while time.time() - t0 < duration:
        time.sleep(1.0)
        now = time.time()
        pub_total = sum(c["n"] for _, c in pubs)
        rows = run_psql("SELECT count(*) FROM loadgen_inbound")
        row_ct = rows[0][0] if rows else 0
        print(f"  t+{int(now - t0):02d}s  pub={pub_total - last_pub:>6}/s  rows={row_ct}", flush=True)
        last_pub = pub_total

    stop.set()
    time.sleep(2.0)
    rows = run_psql("SELECT count(*) FROM loadgen_inbound")
    row_ct = rows[0][0] if rows else 0
    total = sum(c["n"] for _, c in pubs)
    print(f"[loadgen] inbound: published={total}, rows_written={row_ct}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["qos0", "qos1", "inbound"], default="qos0")
    ap.add_argument("--duration", type=int, default=45)
    ap.add_argument("--publishers", type=int, default=4)
    ap.add_argument("--subscribers", type=int, default=4)
    ap.add_argument("--payload-bytes", type=int, default=128)
    ap.add_argument(
        "--rate-per-pub",
        type=int,
        default=500,
        help="Per-publisher rate in msg/s.  0 = unthrottled (may trip broker 64 KiB parse-buffer cap).",
    )
    args = ap.parse_args()

    if args.mode in ("qos0", "qos1"):
        run_qos(
            args.mode,
            args.duration,
            args.publishers,
            args.subscribers,
            args.payload_bytes,
            args.rate_per_pub,
        )
    elif args.mode == "inbound":
        run_inbound(args.duration, args.publishers, args.payload_bytes)


if __name__ == "__main__":
    main()
