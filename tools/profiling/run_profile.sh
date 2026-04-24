#!/usr/bin/env bash
# Profile the pgmqtt background worker while load_gen.py drives traffic.
#
# Produces:
#   tools/profiling/out/perf.data    -- raw samples
#   tools/profiling/out/perf.folded  -- collapsed stacks
#   tools/profiling/out/flame.svg    -- interactive flamegraph
#
# Requirements:
#   - pgmqtt running in the docker container (`docker compose up`)
#   - postgres container built with debug=line-tables-only (see extension/Cargo.toml)
#   - kernel.perf_event_paranoid <= 1  (sudo sysctl -w kernel.perf_event_paranoid=1)
#   - perf + FlameGraph scripts present under tools/profiling/FlameGraph/
#
# Usage:
#   tools/profiling/run_profile.sh [mode] [duration] [publishers] [subscribers]
#     mode:        qos0 | qos1 | inbound       (default: qos0)
#     duration:    seconds of traffic + perf   (default: 45)
#     publishers:  load_gen publisher threads  (default: 4)
#     subscribers: load_gen subscriber threads (default: 4; ignored for inbound)

set -euo pipefail

MODE="${1:-qos0}"
DURATION="${2:-45}"
PUBS="${3:-4}"
SUBS="${4:-4}"
RATE="${5:-500}"

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PROF_DIR="$ROOT/tools/profiling"
OUT_DIR="$PROF_DIR/out"
FG_DIR="$PROF_DIR/FlameGraph"

mkdir -p "$OUT_DIR"

# 1) Find the pgmqtt mqtt+cdc background-worker host PID.
CONTAINER="$(docker ps --filter name=pgmqtt-enterprise-postgres-1 --format '{{.Names}}' | head -1)"
if [[ -z "$CONTAINER" ]]; then
  echo "ERROR: postgres container not running. Run: docker compose up -d" >&2
  exit 1
fi

# docker top shows host PIDs with full command lines.  The pgmqtt mqtt+cdc
# background worker shows up as "postgres: pgmqtt_mqtt".
HOST_PID="$(docker top "$CONTAINER" -eo pid,args 2>/dev/null | awk 'NR>1 && /pgmqtt_mqtt/ {print $1; exit}')"
if [[ -z "$HOST_PID" ]]; then
  echo "ERROR: could not find pgmqtt mqtt BGW. docker top output:" >&2
  docker top "$CONTAINER" -eo pid,args >&2
  exit 1
fi

echo "[profile] pgmqtt BGW: host_pid=$HOST_PID"
echo "[profile] mode=$MODE duration=${DURATION}s pubs=$PUBS subs=$SUBS"

# 2) Start the load generator in the background.
LOADGEN_LOG="$OUT_DIR/loadgen.log"
( cd "$ROOT" && python tools/profiling/load_gen.py \
    --mode "$MODE" --duration "$DURATION" \
    --publishers "$PUBS" --subscribers "$SUBS" \
    --rate-per-pub "$RATE" ) > "$LOADGEN_LOG" 2>&1 &
LG_PID=$!
echo "[profile] load_gen pid=$LG_PID (log: $LOADGEN_LOG)"

# Give the load generator a second to establish connections before we sample.
sleep 2

# 3) perf record, on-CPU, 99Hz, with DWARF unwinding (needed for Rust frames).
PERF_DATA="$OUT_DIR/perf.data"
PERF_DURATION=$(( DURATION - 3 ))
if (( PERF_DURATION < 5 )); then PERF_DURATION=5; fi

# Requires perf binary to have cap_perfmon + cap_sys_ptrace (or run as root):
#   sudo setcap cap_perfmon,cap_sys_ptrace+ep /usr/lib/linux-tools-*/perf
echo "[profile] perf record -F 99 -g --call-graph dwarf,16384 -p $HOST_PID -- sleep $PERF_DURATION"
perf record -F 99 -g --call-graph dwarf,16384 -o "$PERF_DATA" -p "$HOST_PID" -- sleep "$PERF_DURATION"

wait "$LG_PID" || true
echo "[profile] load_gen finished; tail:" && tail -20 "$LOADGEN_LOG"

# 4) Resolve symbols against the container's filesystem.
# The .so and postgres binary live inside the container; copy them into a
# --symfs tree so perf script can resolve frames.
SYMFS="$OUT_DIR/symfs"
mkdir -p "$SYMFS/usr/lib/postgresql/16/lib" "$SYMFS/usr/lib/postgresql/16/bin"
docker cp "$CONTAINER:/usr/lib/postgresql/16/lib/pgmqtt.so" "$SYMFS/usr/lib/postgresql/16/lib/pgmqtt.so" 2>/dev/null || true
docker cp "$CONTAINER:/usr/lib/postgresql/16/bin/postgres" "$SYMFS/usr/lib/postgresql/16/bin/postgres" 2>/dev/null || true

# 5) Fold & render.
perf script -i "$PERF_DATA" --symfs="$SYMFS" > "$OUT_DIR/perf.script"
"$FG_DIR/stackcollapse-perf.pl" "$OUT_DIR/perf.script" > "$OUT_DIR/perf.folded"
"$FG_DIR/flamegraph.pl" \
    --title "pgmqtt BGW — $MODE ${DURATION}s" \
    --subtitle "pubs=$PUBS subs=$SUBS" \
    "$OUT_DIR/perf.folded" > "$OUT_DIR/flame.svg"

# Top-15 samples summary (quick tty read).
echo
echo "[profile] top 20 collapsed stacks by weight (truncated paths):"
awk '{
    w=$NF; $NF="";
    # drop frames with no symbol (unknown/libc opaque) for readability
    gsub(/;\[unknown\]/, "", $0);
    gsub(/;\[libc\.so\.6\]/, ";libc", $0);
    print w"\t"$0
}' "$OUT_DIR/perf.folded" \
    | sort -rn -k1,1 \
    | head -n 20 \
    | awk -F'\t' '{printf "%14d  %s\n", $1, $2}' || true

echo
echo "[profile] artifacts:"
echo "  $PERF_DATA"
echo "  $OUT_DIR/perf.folded"
echo "  $OUT_DIR/flame.svg"
