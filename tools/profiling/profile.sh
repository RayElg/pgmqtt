#!/usr/bin/env bash
# profile.sh — profile a pgrx extension's PostgreSQL process while a workload
# exercises it.
#
# The script:
#   1. Resolves a target PID (passed directly, matched by pattern, or looked
#      up inside a docker container).
#   2. Starts a user-supplied workload command in the background.
#   3. Records samples for `--duration` seconds.  On-CPU samples come from
#      `perf record`; off-CPU samples come from a `bpftrace` script attached
#      to `sched_switch`.  Both can run simultaneously against the same PID.
#   4. Waits for the workload, then renders one flamegraph per profiler.
#
# Usage:
#   profile.sh --workload <cmd> [options]
#
# Required:
#   --workload <cmd>       Shell command run in the background to exercise the
#                          extension.  Quote the whole command.
#
# Target selection (one of):
#   --pid <pid>            Profile this PID directly.
#   --pid-pattern <regex>  Match against /proc/*/cmdline.  First match wins.
#   --container <name>     Look up the PID inside a docker container; pair
#                          with --pid-pattern to disambiguate.  Returns the
#                          host PID.
#
# Optional:
#   --mode <cpu|offcpu|both>
#                          Profiler(s) to run.  `both` runs perf and bpftrace
#                          concurrently against the same PID.  Default: cpu.
#   --duration <seconds>   Sampling window.  Default: 30.
#   --frequency <hz>       perf sample rate (cpu only).  Default: 99.
#   --label <name>         Output filename prefix.  Default: <timestamp>.
#   --out-dir <path>       Where to write artifacts.  Default: <script>/out.
#   --flamegraph-dir <p>   Path to brendangregg/FlameGraph clone.
#                          Default: <script>/FlameGraph.
#   --symfs <path>         --symfs argument passed to `perf script`.  Use
#                          this when binaries live in a container or chroot;
#                          mirror the in-container paths under this directory.
#                          When --container is set and --symfs is not, the
#                          script auto-builds a symfs at <out-dir>/symfs by
#                          copying the target process's mapped binaries
#                          (Postgres + extension shared libraries) out of the
#                          container.  Pass --no-symfs to disable.
#   --no-symfs             Skip auto-building a symfs.
#   --title <text>         Flamegraph title.  Default: derived from label.
#   --warmup <seconds>     Wait this long after starting the workload before
#                          sampling.  Default: 2.
#   -h, --help             Show this help.

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

WORKLOAD=""
PID=""
PID_PATTERN=""
CONTAINER=""
MODE="cpu"
DURATION=30
FREQUENCY=99
LABEL=""
OUT_DIR="$SCRIPT_DIR/out"
FG_DIR="$SCRIPT_DIR/FlameGraph"
SYMFS=""
NO_SYMFS=false
TITLE=""
WARMUP=2

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

usage() {
    sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --workload)         WORKLOAD="$2"; shift 2 ;;
        --pid)              PID="$2"; shift 2 ;;
        --pid-pattern)      PID_PATTERN="$2"; shift 2 ;;
        --container)        CONTAINER="$2"; shift 2 ;;
        --mode)             MODE="$2"; shift 2 ;;
        --duration)         DURATION="$2"; shift 2 ;;
        --frequency)        FREQUENCY="$2"; shift 2 ;;
        --label)            LABEL="$2"; shift 2 ;;
        --out-dir)          OUT_DIR="$2"; shift 2 ;;
        --flamegraph-dir)   FG_DIR="$2"; shift 2 ;;
        --symfs)            SYMFS="$2"; shift 2 ;;
        --no-symfs)         NO_SYMFS=true; shift ;;
        --title)            TITLE="$2"; shift 2 ;;
        --warmup)           WARMUP="$2"; shift 2 ;;
        -h|--help)          usage 0 ;;
        *) echo "unknown option: $1" >&2; usage 1 ;;
    esac
done

if [[ -z "$WORKLOAD" ]]; then
    echo "error: --workload is required" >&2
    usage 1
fi

case "$MODE" in
    cpu|offcpu|both) ;;
    *) echo "error: --mode must be cpu, offcpu, or both" >&2; exit 1 ;;
esac

if [[ -z "$LABEL" ]]; then
    LABEL="$(date +%Y%m%d-%H%M%S)"
fi

if [[ -z "$TITLE" ]]; then
    TITLE="$LABEL"
fi

if [[ ! -d "$FG_DIR" ]]; then
    echo "error: FlameGraph directory not found at $FG_DIR" >&2
    echo "       clone https://github.com/brendangregg/FlameGraph or pass --flamegraph-dir" >&2
    exit 1
fi

mkdir -p "$OUT_DIR"

WANT_CPU=false
WANT_OFFCPU=false
[[ "$MODE" == "cpu"    || "$MODE" == "both" ]] && WANT_CPU=true
[[ "$MODE" == "offcpu" || "$MODE" == "both" ]] && WANT_OFFCPU=true

# ---------------------------------------------------------------------------
# Target resolution
# ---------------------------------------------------------------------------

resolve_pid_from_container() {
    local container="$1" pattern="$2"
    if ! docker ps --format '{{.Names}}' | grep -qx "$container"; then
        echo "error: container '$container' is not running" >&2
        return 1
    fi
    local found
    if [[ -n "$pattern" ]]; then
        found="$(docker top "$container" -eo pid,args 2>/dev/null \
            | awk -v re="$pattern" 'NR>1 && $0 ~ re {print $1; exit}')"
    else
        found="$(docker top "$container" -eo pid,args 2>/dev/null \
            | awk 'NR>1 {print $1; exit}')"
    fi
    if [[ -z "$found" ]]; then
        echo "error: no process in container '$container' matched '${pattern:-(any)}'" >&2
        return 1
    fi
    echo "$found"
}

resolve_pid_from_host() {
    local pattern="$1"
    local found
    found="$(pgrep -f "$pattern" | head -1 || true)"
    if [[ -z "$found" ]]; then
        echo "error: no host process matched '$pattern'" >&2
        return 1
    fi
    echo "$found"
}

if [[ -z "$PID" ]]; then
    if [[ -n "$CONTAINER" ]]; then
        PID="$(resolve_pid_from_container "$CONTAINER" "$PID_PATTERN")"
    elif [[ -n "$PID_PATTERN" ]]; then
        PID="$(resolve_pid_from_host "$PID_PATTERN")"
    else
        echo "error: provide --pid, --pid-pattern, or --container" >&2
        usage 1
    fi
fi

if [[ ! -d "/proc/$PID" ]]; then
    echo "error: pid $PID does not exist" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Symbol mirror (perf only)
#
# When the target lives in a container or chroot, `perf script` on the host
# can't open the in-container binaries to resolve symbols.  We mirror them
# into a local directory tree at the same paths, then pass that tree to
# `perf script --symfs`.
#
# Generic: we don't assume which binaries the extension uses.  After
# recording, we extract every file-backed DSO that perf saw and copy each
# from the container, preserving its in-container path.
# ---------------------------------------------------------------------------

build_symfs_from_container() {
    local container="$1" perf_data="$2" symfs="$3"
    mkdir -p "$symfs"

    # `perf script` lists the DSO for every sample; dedupe and filter to
    # absolute file paths (skip anonymous mappings and pseudo-files).
    local paths
    paths="$(perf script -i "$perf_data" 2>/dev/null \
        | grep -oE '\(/[^)]+\)' \
        | tr -d '()' \
        | awk '$0 ~ /^\// && $0 !~ /^\/\// && $0 !~ /^\/(dev|proc|sys|run)\// {print $1}' \
        | sort -u)"

    if [[ -z "$paths" ]]; then
        echo "warn: no DSOs found in $perf_data; skipping symfs build" >&2
        return 1
    fi

    local copied=0 skipped=0
    while IFS= read -r path; do
        local target="$symfs$path"
        if [[ -e "$target" ]]; then
            skipped=$(( skipped + 1 ))
            continue
        fi
        mkdir -p "$(dirname "$target")"
        if docker cp -L "$container:$path" "$target" 2>/dev/null; then
            copied=$(( copied + 1 ))
        else
            skipped=$(( skipped + 1 ))
        fi
    done <<< "$paths"

    echo "[profile] symfs: $copied files copied, $skipped skipped → $symfs"
}

# ---------------------------------------------------------------------------
# Workload + sampling window
# ---------------------------------------------------------------------------

WORKLOAD_LOG="$OUT_DIR/${LABEL}.workload.log"

echo "[profile] target pid : $PID"
echo "[profile] mode       : $MODE"
echo "[profile] duration   : ${DURATION}s (warmup ${WARMUP}s)"
echo "[profile] workload   : $WORKLOAD"
echo "[profile] artifacts  : $OUT_DIR (label: $LABEL)"
echo

bash -c "$WORKLOAD" > "$WORKLOAD_LOG" 2>&1 &
WORKLOAD_PID=$!
cleanup() {
    kill "$WORKLOAD_PID"      2>/dev/null || true
    [[ -n "${PERF_PID:-}"     ]] && kill "$PERF_PID"     2>/dev/null || true
    [[ -n "${BPFTRACE_PID:-}" ]] && sudo kill "$BPFTRACE_PID" 2>/dev/null || true
}
trap cleanup EXIT

sleep "$WARMUP"

SAMPLE_DURATION=$(( DURATION - WARMUP ))
if (( SAMPLE_DURATION < 1 )); then SAMPLE_DURATION=1; fi

PERF_DATA="$OUT_DIR/${LABEL}.cpu.perf.data"
OFFCPU_RAW="$OUT_DIR/${LABEL}.offcpu.raw"

# ---------------------------------------------------------------------------
# Recording (concurrent if --mode both)
# ---------------------------------------------------------------------------

PERF_PID=""
BPFTRACE_PID=""

if $WANT_CPU; then
    echo "[profile] perf record -F $FREQUENCY -p $PID --call-graph dwarf -- sleep $SAMPLE_DURATION"
    perf record -F "$FREQUENCY" -g --call-graph dwarf,16384 \
        -o "$PERF_DATA" -p "$PID" -- sleep "$SAMPLE_DURATION" \
        > "$OUT_DIR/${LABEL}.cpu.recorder.log" 2>&1 &
    PERF_PID=$!
fi

if $WANT_OFFCPU; then
    if ! command -v bpftrace >/dev/null; then
        echo "error: bpftrace not found in PATH" >&2
        exit 1
    fi
    BT_PROGRAM=$(cat <<'BT'
tracepoint:sched:sched_switch
/ args->prev_pid == $1 /
{
    @off_kstack[args->prev_pid] = kstack;
    @off_ustack[args->prev_pid] = ustack;
    @off_ts[args->prev_pid] = nsecs;
}

tracepoint:sched:sched_switch
/ args->next_pid == $1 && @off_ts[args->next_pid] != 0 /
{
    $delta_us = (nsecs - @off_ts[args->next_pid]) / 1000;
    @offcpu[@off_kstack[args->next_pid], @off_ustack[args->next_pid]] = sum($delta_us);
    delete(@off_kstack[args->next_pid]);
    delete(@off_ustack[args->next_pid]);
    delete(@off_ts[args->next_pid]);
}

END {
    clear(@off_kstack);
    clear(@off_ustack);
    clear(@off_ts);
}
BT
)
    echo "[profile] bpftrace off-CPU for pid $PID over ${SAMPLE_DURATION}s"
    sudo timeout "${SAMPLE_DURATION}s" bpftrace -p "$PID" \
        -e "$BT_PROGRAM" "$PID" > "$OFFCPU_RAW" 2>&1 &
    BPFTRACE_PID=$!
fi

[[ -n "$PERF_PID"     ]] && wait "$PERF_PID"     || true
[[ -n "$BPFTRACE_PID" ]] && wait "$BPFTRACE_PID" || true
[[ -f "$OFFCPU_RAW"   ]] && sudo chown "$USER:$USER" "$OFFCPU_RAW" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

render_cpu() {
    local script_file="$OUT_DIR/${LABEL}.cpu.script"
    local folded="$OUT_DIR/${LABEL}.cpu.folded"
    local svg="$OUT_DIR/${LABEL}.cpu.svg"

    if ! $NO_SYMFS && [[ -z "$SYMFS" && -n "$CONTAINER" ]]; then
        SYMFS="$OUT_DIR/symfs"
        build_symfs_from_container "$CONTAINER" "$PERF_DATA" "$SYMFS" || SYMFS=""
    fi

    local script_args=(-i "$PERF_DATA")
    [[ -n "$SYMFS" ]] && script_args+=(--symfs="$SYMFS")
    perf script "${script_args[@]}" > "$script_file"

    "$FG_DIR/stackcollapse-perf.pl" "$script_file" > "$folded"
    "$FG_DIR/flamegraph.pl" --title "$TITLE (on-CPU)" "$folded" > "$svg"

    echo
    echo "[profile] on-CPU top 10 stacks (samples):"
    sort -rn -k2,2 -t' ' "$folded" | head -10 \
        | awk '{n=$NF; $NF=""; printf "  %8d  %s\n", n, $0}'
    echo "  → $svg"
}

render_offcpu() {
    local folded="$OUT_DIR/${LABEL}.offcpu.folded"
    local folded_by_reason="$OUT_DIR/${LABEL}.offcpu.by-reason.folded"
    local reason_summary="$OUT_DIR/${LABEL}.offcpu.by-reason.txt"
    local svg="$OUT_DIR/${LABEL}.offcpu.svg"
    local svg_by_reason="$OUT_DIR/${LABEL}.offcpu.by-reason.svg"

    python3 - "$OFFCPU_RAW" "$folded" "$folded_by_reason" "$reason_summary" <<'PY'
import re, sys
from collections import defaultdict

raw_path, folded_path, by_reason_path, summary_path = sys.argv[1:5]

# Each off-CPU span is classified by a pattern in its kernel stack.  The
# kernel stack at de-schedule time always ends in `schedule` / `__schedule`;
# the function above that identifies the blocking primitive that called it.
# Patterns are checked in order — first match wins, so put more specific
# patterns first.
REASON_RULES = [
    ("[fsync]",    ("ext4_sync_file", "vfs_fsync_range", "do_fsync", "__do_fsync")),
    ("[disk-io]",  ("io_schedule", "submit_bio_wait", "wait_on_buffer", "folio_wait_bit")),
    ("[epoll]",    ("do_epoll_wait", "ep_poll", "do_sys_poll", "do_select")),
    ("[net-recv]", ("sk_wait_data", "tcp_recvmsg", "unix_stream_read_generic",
                    "skb_wait_for_more_packets", "wait_for_packet")),
    ("[net-send]", ("sk_stream_wait_memory", "wait_for_tcp_memory")),
    ("[futex]",    ("futex_wait", "do_futex")),
    ("[sleep]",    ("do_nanosleep", "hrtimer_nanosleep", "schedule_hrtimeout",
                    "schedule_timeout")),
    ("[pipe]",     ("pipe_read", "pipe_wait")),
    ("[signal]",   ("do_signal_stop", "do_wait")),
]

def classify(kframes):
    blob = " ".join(kframes)
    for label, needles in REASON_RULES:
        if any(n in blob for n in needles):
            return label
    return "[other]"

def frames(raw):
    out = []
    for line in raw.splitlines():
        s = line.strip()
        if not s:
            continue
        out.append(re.sub(r"\+0x[0-9a-f]+$", "", s))
    return out

with open(raw_path) as f:
    text = f.read()

# bpftrace prints `@offcpu[<kstack>, <ustack>]: <microseconds>`.
pat = re.compile(r"@offcpu\[\s*(.*?)\]:\s*(\d+)", re.DOTALL)

flat   = defaultdict(int)
tagged = defaultdict(int)
totals = defaultdict(int)

for body, val in ((m.group(1), int(m.group(2))) for m in pat.finditer(text)):
    parts = body.split(",", 1)
    k_raw, u_raw = (parts + [""])[:2] if len(parts) == 1 else parts
    kframes = frames(k_raw)
    uframes = frames(u_raw)
    if not (kframes or uframes):
        continue

    # Caller-first order; user stack first, kernel stack on top.
    stack = list(reversed(uframes)) + list(reversed(kframes))
    flat[";".join(stack)] += val

    reason = classify(kframes)
    totals[reason] += val
    tagged[f"{reason};" + ";".join(stack)] += val

with open(folded_path, "w") as f:
    for k, v in sorted(flat.items(), key=lambda kv: -kv[1]):
        f.write(f"{k} {v}\n")

with open(by_reason_path, "w") as f:
    for k, v in sorted(tagged.items(), key=lambda kv: -kv[1]):
        f.write(f"{k} {v}\n")

total_us = sum(totals.values()) or 1
with open(summary_path, "w") as f:
    for reason, us in sorted(totals.items(), key=lambda kv: -kv[1]):
        pct = 100.0 * us / total_us
        f.write(f"  {reason:<12}  {us:>14d} us  {pct:5.1f}%\n")
PY

    "$FG_DIR/flamegraph.pl" \
        --colors=io \
        --countname=us \
        --title "$TITLE (off-CPU)" \
        "$folded" > "$svg"

    "$FG_DIR/flamegraph.pl" \
        --colors=io \
        --countname=us \
        --title "$TITLE (off-CPU by reason)" \
        "$folded_by_reason" > "$svg_by_reason"

    echo
    echo "[profile] off-CPU time by reason:"
    cat "$reason_summary"
    echo
    echo "[profile] off-CPU top 10 stacks (microseconds):"
    head -10 "$folded" \
        | awk '{n=$NF; $NF=""; printf "  %10d us  %s\n", n, $0}'
    echo "  → $svg"
    echo "  → $svg_by_reason"
}

$WANT_CPU    && render_cpu
$WANT_OFFCPU && render_offcpu

# ---------------------------------------------------------------------------
# Wait for workload, surface its tail
# ---------------------------------------------------------------------------

wait "$WORKLOAD_PID" 2>/dev/null || true
trap - EXIT

echo
echo "[profile] workload tail:"
tail -10 "$WORKLOAD_LOG" | sed 's/^/  /'
