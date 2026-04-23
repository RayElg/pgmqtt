#!/usr/bin/env bash
# Off-CPU flamegraph for the pgmqtt BGW while load_gen.py drives traffic.
#
# Complements run_profile.sh (on-CPU).  Off-CPU sampling uses bpftrace's
# `offcputime` logic: on every context switch out, we record the stack + the
# delta-T until the next wake-up, then aggregate.  High bars here = "the BGW
# was blocked waiting for X" (kernel I/O, lock contention, socket backpressure).
#
# Produces:
#   tools/profiling/out/offcpu.folded  -- stack;stack;frame <microseconds>
#   tools/profiling/out/offcpu.svg     -- flamegraph (colour: io)
#
# Requirements:
#   - bpftrace installed (apt-get install bpftrace)
#   - /sys/kernel/tracing readable (sudo mount -o remount,mode=755 /sys/kernel/tracing)
#   - Perf caps already granted:
#       sudo setcap cap_perfmon,cap_sys_ptrace+ep /usr/lib/linux-tools-*/perf
#     bpftrace itself needs root OR CAP_BPF + CAP_PERFMON; we use sudo.
#
# Usage:
#   tools/profiling/run_offcpu.sh [mode] [duration] [publishers] [subscribers] [rate]

set -euo pipefail

MODE="${1:-qos0}"
DURATION="${2:-60}"
PUBS="${3:-4}"
SUBS="${4:-4}"
RATE="${5:-500}"

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PROF_DIR="$ROOT/tools/profiling"
OUT_DIR="$PROF_DIR/out"
FG_DIR="$PROF_DIR/FlameGraph"

mkdir -p "$OUT_DIR"

CONTAINER="$(docker ps --filter name=pgmqtt-enterprise-postgres-1 --format '{{.Names}}' | head -1)"
HOST_PID="$(docker top "$CONTAINER" -eo pid,args 2>/dev/null | awk 'NR>1 && /pgmqtt_mqtt/ {print $1; exit}')"
if [[ -z "$HOST_PID" ]]; then
  echo "ERROR: could not find pgmqtt mqtt BGW" >&2
  exit 1
fi

echo "[offcpu] pgmqtt BGW: host_pid=$HOST_PID"
echo "[offcpu] mode=$MODE duration=${DURATION}s pubs=$PUBS subs=$SUBS rate=$RATE/pub"

LOADGEN_LOG="$OUT_DIR/offcpu.loadgen.log"
( cd "$ROOT" && python tools/profiling/load_gen.py \
    --mode "$MODE" --duration "$DURATION" \
    --publishers "$PUBS" --subscribers "$SUBS" \
    --rate-per-pub "$RATE" ) > "$LOADGEN_LOG" 2>&1 &
LG_PID=$!
echo "[offcpu] load_gen pid=$LG_PID (log: $LOADGEN_LOG)"

sleep 2
TRACE_DURATION=$(( DURATION - 3 ))
if (( TRACE_DURATION < 5 )); then TRACE_DURATION=5; fi

# bpftrace script: record stack + delta-t for every off-CPU span of the target PID.
# Output format: one line per unique stack, value in microseconds.
BT_OUT="$OUT_DIR/offcpu.raw"
BT_SCRIPT=$(cat <<'BT'
BEGIN {
    printf("tracing off-CPU for pid %d\n", $1);
}

// Stamp when our target goes off-CPU; capture its stack at that moment.
tracepoint:sched:sched_switch
/ args->prev_pid == $1 /
{
    @off_stack[args->prev_pid] = kstack;
    @off_ustack[args->prev_pid] = ustack;
    @off_ts[args->prev_pid] = nsecs;
}

// When it comes back on-CPU, compute delta and aggregate.
tracepoint:sched:sched_switch
/ args->next_pid == $1 && @off_ts[args->next_pid] != 0 /
{
    $delta_us = (nsecs - @off_ts[args->next_pid]) / 1000;
    @offcpu[@off_stack[args->next_pid], @off_ustack[args->next_pid]] = sum($delta_us);
    delete(@off_stack[args->next_pid]);
    delete(@off_ustack[args->next_pid]);
    delete(@off_ts[args->next_pid]);
}

END {
    clear(@off_stack);
    clear(@off_ustack);
    clear(@off_ts);
}
BT
)

echo "[offcpu] bpftrace -p $HOST_PID -- duration ${TRACE_DURATION}s"
# bpftrace needs root and writable tracing subsystem.
sudo timeout "${TRACE_DURATION}s" bpftrace -p "$HOST_PID" -e "$BT_SCRIPT" "$HOST_PID" \
    > "$BT_OUT" 2>&1 || true
# raw output is root-owned; fix so the python fold step can read it
sudo chown "$USER:$USER" "$BT_OUT" 2>/dev/null || true

wait "$LG_PID" || true
echo "[offcpu] load_gen finished; tail:" && tail -8 "$LOADGEN_LOG"

# Fold bpftrace output into flamegraph-compatible stacks.
# bpftrace prints maps like:
#   @offcpu[kstack, ustack]:
#     <k1>
#     <k2>
#     ---
#     <u1>
#     <u2>
#     ] : <value>
# We flatten each stack-set into kstack;ustack <value>.
python3 - "$BT_OUT" > "$OUT_DIR/offcpu.folded" <<'PY'
import re, sys
from collections import defaultdict

buckets = defaultdict(int)
with open(sys.argv[1]) as f:
    text = f.read()

# Each @offcpu entry is:
# @offcpu[
#   ... kstack ...
#     ,
#   ... ustack ...
# ]: <count>
pat = re.compile(r"@offcpu\[\s*(.*?)\]:\s*(\d+)", re.DOTALL)
for m in pat.finditer(text):
    body, val = m.group(1), int(m.group(2))
    # Split on the comma that separates the two stacks.
    # Each stack's frames are printed one per line, with leading whitespace.
    parts = body.split(",", 1)
    if len(parts) == 2:
        k_raw, u_raw = parts
    else:
        k_raw, u_raw = "", parts[0]
    def clean(raw):
        frames = []
        for line in raw.splitlines():
            s = line.strip()
            if not s: continue
            # bpftrace prints addresses like "func+0x1234".  Drop the offset.
            s = re.sub(r"\+0x[0-9a-f]+$", "", s)
            frames.append(s)
        return frames
    kframes = clean(k_raw)
    uframes = clean(u_raw)
    # Flamegraph convention: leaf on right.  bpftrace prints leaf first;
    # reverse so the flame reads caller → callee.
    stack = list(reversed(uframes)) + list(reversed(kframes))
    if not stack:
        continue
    key = ";".join(stack)
    buckets[key] += val

for k, v in sorted(buckets.items(), key=lambda x: -x[1]):
    print(f"{k} {v}")
PY

"$FG_DIR/flamegraph.pl" \
    --colors=io \
    --countname=us \
    --title "pgmqtt BGW off-CPU — $MODE ${DURATION}s" \
    --subtitle "pubs=$PUBS subs=$SUBS rate=$RATE/s" \
    "$OUT_DIR/offcpu.folded" > "$OUT_DIR/offcpu.svg"

echo
echo "[offcpu] top 15 blocked stacks by microseconds waited:"
awk '{
    w=$NF; $NF="";
    gsub(/;\[unknown\]/, "", $0);
    printf "%12d us  %s\n", w, $0
}' "$OUT_DIR/offcpu.folded" | head -n 15 || true

echo
echo "[offcpu] artifacts:"
echo "  $OUT_DIR/offcpu.raw"
echo "  $OUT_DIR/offcpu.folded"
echo "  $OUT_DIR/offcpu.svg"
