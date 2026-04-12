"""SSE bridge: listens on a PostgreSQL NOTIFY channel and forwards payloads
to browser clients as Server-Sent Events.
"""
import os
import queue
import select as _select
import threading
import time

import psycopg2
from flask import Flask, Response

PGHOST    = os.getenv("PGHOST", "localhost")
PGPORT    = int(os.getenv("PGPORT", "5432"))
PGUSER    = os.getenv("PGUSER", "postgres")
PGPASS    = os.getenv("PGPASSWORD", "password")
PGDB      = os.getenv("PGDATABASE", "postgres")
CHANNEL   = os.getenv("NOTIFY_CHANNEL", "pgmqtt_live")

app = Flask(__name__)

# All active SSE client queues
_subscribers: list[queue.Queue] = []
_lock = threading.Lock()


def _broadcast(payload: str):
    with _lock:
        dead = []
        for q in _subscribers:
            try:
                q.put_nowait(payload)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _subscribers.remove(q)


def _pg_listener():
    """Connects to PostgreSQL, listens on CHANNEL, broadcasts every notification."""
    dsn = f"host={PGHOST} port={PGPORT} user={PGUSER} password={PGPASS} dbname={PGDB}"
    while True:
        try:
            conn = psycopg2.connect(dsn)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"LISTEN {CHANNEL}")
            print(f"[bridge] listening on {CHANNEL}", flush=True)
            while True:
                if _select.select([conn], [], [], 2.0)[0]:
                    conn.poll()
                    for n in conn.notifies:
                        _broadcast(n.payload)
                    conn.notifies.clear()
        except Exception as exc:
            print(f"[bridge] pg error: {exc} — retrying in 3 s", flush=True)
            time.sleep(3)


# Start listener thread at module load time
threading.Thread(target=_pg_listener, daemon=True).start()


@app.route("/stream")
def stream():
    q: queue.Queue = queue.Queue(maxsize=50)
    with _lock:
        _subscribers.append(q)

    def generate():
        try:
            while True:
                try:
                    payload = q.get(timeout=25)
                    yield f"data: {payload}\n\n"
                except queue.Empty:
                    yield ": heartbeat\n\n"
        finally:
            with _lock:
                if q in _subscribers:
                    _subscribers.remove(q)

    resp = Response(generate(), mimetype="text/event-stream")
    resp.headers["Cache-Control"] = "no-cache"
    resp.headers["X-Accel-Buffering"] = "no"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


@app.route("/health")
def health():
    return "ok"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3001, threaded=True)
