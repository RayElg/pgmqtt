#!/usr/bin/env bash
# profile.sh — profile a pgrx extension's PostgreSQL process while a workload
# exercises it.
#
# The script:
#   1. Resolves a target PID (passed directly, matched by pattern, or looked
#      up inside a docker container).
#   2. Starts a user-supplied workload command in the background.
#   3. Records samples for `--duration` seconds.  On-CPU samples come from
#      `perf record`; off-CPU samples come from bpftrace (offcpu.bt) attached
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
#                          this when binaries live in a container or chroot.
#                          When --container is set and --symfs is not, the
#                          script auto-builds a symfs at <out-dir>/symfs by
#                          copying the target process's mapped binaries out of
#                          the container.  Pass --no-symfs to disable.
#   --no-symfs             Skip auto-building a symfs.
#   --title <text>         Flamegraph title.  Default: derived from label.
#   --warmup <seconds>     Wait this long after starting the workload before
#                          sampling.  Default: 2.
#   --bpftrace-docker      Run bpftrace inside a privileged Docker container
#                          instead of via sudo.  Requires Docker on the host.
#   --bpftrace-image <img> Docker image to use with --bpftrace-docker.
#                          Default: pgmqtt-profiler:latest (build with
#                          docker build -f tools/profiling/Dockerfile.profiler)
#   -h, --help             Show this help.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

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
BPFTRACE_DOCKER=false
BPFTRACE_IMAGE="pgmqtt-profiler:latest"

# Set by record_* functions; referenced by cleanup and wait_for_profilers.
PERF_PID=""
BPFTRACE_PID=""
BT_CONTAINER=""

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

usage() {
    sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --workload)         WORKLOAD="$2";       shift 2 ;;
        --pid)              PID="$2";            shift 2 ;;
        --pid-pattern)      PID_PATTERN="$2";    shift 2 ;;
        --container)        CONTAINER="$2";      shift 2 ;;
        --mode)             MODE="$2";           shift 2 ;;
        --duration)         DURATION="$2";       shift 2 ;;
        --frequency)        FREQUENCY="$2";      shift 2 ;;
        --label)            LABEL="$2";          shift 2 ;;
        --out-dir)          OUT_DIR="$2";        shift 2 ;;
        --flamegraph-dir)   FG_DIR="$2";         shift 2 ;;
        --symfs)            SYMFS="$2";          shift 2 ;;
        --no-symfs)         NO_SYMFS=true;       shift   ;;
        --title)            TITLE="$2";          shift 2 ;;
        --warmup)           WARMUP="$2";         shift 2 ;;
        --bpftrace-docker)  BPFTRACE_DOCKER=true; shift  ;;
        --bpftrace-image)   BPFTRACE_IMAGE="$2"; shift 2 ;;
        -h|--help)          usage 0 ;;
        *) echo "unknown option: $1" >&2; usage 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

[[ -z "$WORKLOAD" ]] && { echo "error: --workload is required" >&2; usage 1; }

case "$MODE" in
    cpu|offcpu|both) ;;
    *) echo "error: --mode must be cpu, offcpu, or both" >&2; exit 1 ;;
esac

[[ -z "$LABEL" ]] && LABEL="$(date +%Y%m%d-%H%M%S)"
[[ -z "$TITLE" ]] && TITLE="$LABEL"

if [[ ! -d "$FG_DIR" ]]; then
    if command -v git >/dev/null; then
        echo "[profile] FlameGraph not found; cloning into $FG_DIR"
        git clone --depth=1 https://github.com/brendangregg/FlameGraph "$FG_DIR"
    else
        echo "error: FlameGraph directory not found at $FG_DIR" >&2
        echo "       git clone --depth=1 https://github.com/brendangregg/FlameGraph $FG_DIR" >&2
        exit 1
    fi
fi

if [[ "$MODE" == "offcpu" || "$MODE" == "both" ]] && [[ ! -f "$SCRIPT_DIR/offcpu.bt" ]]; then
    echo "error: $SCRIPT_DIR/offcpu.bt not found" >&2; exit 1
fi

WANT_CPU=false; WANT_OFFCPU=false
[[ "$MODE" == "cpu"    || "$MODE" == "both" ]] && WANT_CPU=true
[[ "$MODE" == "offcpu" || "$MODE" == "both" ]] && WANT_OFFCPU=true

mkdir -p "$OUT_DIR"

# ---------------------------------------------------------------------------
# PID resolution
# ---------------------------------------------------------------------------

resolve_pid_from_container() {
    local container="$1" pattern="$2"
    if ! docker ps --format '{{.Names}}' | grep -qx "$container"; then
        echo "error: container '$container' is not running" >&2; return 1
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
        echo "error: no process in container '$container' matched '${pattern:-(any)}'" >&2; return 1
    fi
    echo "$found"
}

resolve_pid_from_host() {
    local pattern="$1"
    local found
    found="$(pgrep -f "$pattern" | head -1 || true)"
    if [[ -z "$found" ]]; then
        echo "error: no host process matched '$pattern'" >&2; return 1
    fi
    echo "$found"
}

if [[ -z "$PID" ]]; then
    if [[ -n "$CONTAINER" ]]; then
        PID="$(resolve_pid_from_container "$CONTAINER" "$PID_PATTERN")"
    elif [[ -n "$PID_PATTERN" ]]; then
        PID="$(resolve_pid_from_host "$PID_PATTERN")"
    else
        echo "error: provide --pid, --pid-pattern, or --container" >&2; usage 1
    fi
fi

[[ ! -d "/proc/$PID" ]] && { echo "error: pid $PID does not exist" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Symbol mirror (perf only)
#
# When the target lives in a container, perf script on the host can't open
# in-container binaries to resolve symbols.  We mirror them into a local
# directory tree at the same paths, then pass that tree to perf script
# --symfs.
# ---------------------------------------------------------------------------

build_symfs_from_container() {
    local container="$1" perf_data="$2" symfs="$3"
    mkdir -p "$symfs"

    local paths
    paths="$(perf script -i "$perf_data" 2>/dev/null \
        | grep -oE '\(/[^)]+\)' \
        | tr -d '()' \
        | awk '$0 ~ /^\// && $0 !~ /^\/\// && $0 !~ /^\/(dev|proc|sys|run)\// {print $1}' \
        | sort -u)"

    if [[ -z "$paths" ]]; then
        echo "warn: no DSOs found in $perf_data; skipping symfs build" >&2; return 1
    fi

    local copied=0 skipped=0
    while IFS= read -r path; do
        local target="$symfs$path"
        if [[ -e "$target" ]]; then
            skipped=$(( skipped + 1 )); continue
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
# Recording
# ---------------------------------------------------------------------------

record_cpu() {
    echo "[profile] perf record -F $FREQUENCY -p $PID --call-graph dwarf -- sleep $SAMPLE_DURATION"
    perf record -F "$FREQUENCY" -g --call-graph dwarf,16384 \
        -o "$PERF_DATA" -p "$PID" -- sleep "$SAMPLE_DURATION" \
        > "$OUT_DIR/${LABEL}.cpu.recorder.log" 2>&1 &
    PERF_PID=$!
}

record_offcpu() {
    echo "[profile] bpftrace off-CPU for pid $PID over ${SAMPLE_DURATION}s"
    if $BPFTRACE_DOCKER; then
        if ! command -v docker >/dev/null; then
            echo "error: docker not found in PATH (required for --bpftrace-docker)" >&2; exit 1
        fi
        BT_CONTAINER="profile_bpftrace_$$"
        echo "[profile] bpftrace via docker image=$BPFTRACE_IMAGE container=$BT_CONTAINER"
        docker run --rm \
            --name "$BT_CONTAINER" \
            --privileged \
            --pid=host \
            -v /sys/kernel/debug:/sys/kernel/debug:rw \
            -v /sys/kernel/tracing:/sys/kernel/tracing:rw \
            -v /sys/fs/bpf:/sys/fs/bpf:rw \
            -v "$SCRIPT_DIR/offcpu.bt:/offcpu.bt:ro" \
            "$BPFTRACE_IMAGE" \
            /bin/bash -c "timeout ${SAMPLE_DURATION}s bpftrace -p ${PID} /offcpu.bt ${PID}" \
            > "$OFFCPU_RAW" 2>&1 &
        BPFTRACE_PID=$!
    else
        if ! command -v bpftrace >/dev/null; then
            echo "error: bpftrace not found in PATH; use --bpftrace-docker for container-based tracing" >&2; exit 1
        fi
        sudo timeout "${SAMPLE_DURATION}s" bpftrace -p "$PID" \
            "$SCRIPT_DIR/offcpu.bt" "$PID" > "$OFFCPU_RAW" 2>&1 &
        BPFTRACE_PID=$!
    fi
}

wait_for_profilers() {
    if [[ -n "$PERF_PID" ]]; then
        wait "$PERF_PID" \
            || echo "[profile] warn: perf exited non-zero; check ${OUT_DIR}/${LABEL}.cpu.recorder.log" >&2
    fi
    if [[ -n "$BPFTRACE_PID" ]]; then
        wait "$BPFTRACE_PID" || true  # timeout kills bpftrace; non-zero exit is expected
        if ! grep -q "@offcpu" "$OFFCPU_RAW" 2>/dev/null; then
            echo "[profile] warn: bpftrace produced no @offcpu output; check $OFFCPU_RAW" >&2
        fi
    fi
}

# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

render_cpu() {
    local script_file="$OUT_DIR/${LABEL}.cpu.script"
    local folded="$OUT_DIR/${LABEL}.cpu.folded"
    local svg="$OUT_DIR/${LABEL}.cpu.svg"

    local script_args=(-i "$PERF_DATA")
    [[ -n "$SYMFS" ]] && script_args+=(--symfs="$SYMFS")
    perf script "${script_args[@]}" > "$script_file"

    "$FG_DIR/stackcollapse-perf.pl" "$script_file" > "$folded"
    "$FG_DIR/flamegraph.pl" --title "$TITLE (on-CPU)" "$folded" > "$svg"

    echo
    echo "[profile] on-CPU top 10 stacks (samples):"
    sort -rn -k2,2 -t' ' "$folded" | head -10 \
        | awk '{n=$NF; $NF=""; printf "  %8d  %s\n", n, $0}' || true
    echo "  → $svg"
}

render_offcpu() {
    local folded="$OUT_DIR/${LABEL}.offcpu.folded"
    local reason_summary="$OUT_DIR/${LABEL}.offcpu.by-reason.txt"
    local svg="$OUT_DIR/${LABEL}.offcpu.svg"

    python3 "$SCRIPT_DIR/offcpu_fold.py" \
        "$OFFCPU_RAW" "$folded" "$reason_summary"

    "$FG_DIR/flamegraph.pl" \
        --colors=io --countname=us \
        --title "$TITLE (off-CPU)" \
        "$folded" > "$svg"

    echo
    echo "[profile] off-CPU time by reason:"
    cat "$reason_summary"
    echo
    echo "[profile] off-CPU top 10 stacks (microseconds):"
    head -10 "$folded" \
        | awk '{n=$NF; $NF=""; printf "  %10d us  %s\n", n, $0}' || true
    echo "  → $svg"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

WORKLOAD_LOG="$OUT_DIR/${LABEL}.workload.log"
PERF_DATA="$OUT_DIR/${LABEL}.cpu.perf.data"
OFFCPU_RAW="$OUT_DIR/${LABEL}.offcpu.raw"

echo "[profile] target pid : $PID"
echo "[profile] mode       : $MODE"
echo "[profile] duration   : ${DURATION}s (warmup ${WARMUP}s)"
echo "[profile] workload   : $WORKLOAD"
echo "[profile] artifacts  : $OUT_DIR (label: $LABEL)"
echo

cleanup() {
    kill "$WORKLOAD_PID" 2>/dev/null || true
    [[ -n "${PERF_PID:-}"      ]] && kill "$PERF_PID" 2>/dev/null || true
    if [[ -n "${BT_CONTAINER:-}" ]]; then
        docker stop "$BT_CONTAINER" 2>/dev/null || true
    elif [[ -n "${BPFTRACE_PID:-}" ]]; then
        sudo kill "$BPFTRACE_PID" 2>/dev/null || true
    fi
}

bash -c "$WORKLOAD" > "$WORKLOAD_LOG" 2>&1 &
WORKLOAD_PID=$!
trap cleanup EXIT
sleep "$WARMUP"

SAMPLE_DURATION=$(( DURATION - WARMUP ))
if (( SAMPLE_DURATION < 1 )); then SAMPLE_DURATION=1; fi

$WANT_CPU    && record_cpu
$WANT_OFFCPU && record_offcpu

wait_for_profilers

if $WANT_CPU && ! $NO_SYMFS && [[ -z "$SYMFS" && -n "$CONTAINER" ]]; then
    SYMFS="$OUT_DIR/symfs"
    build_symfs_from_container "$CONTAINER" "$PERF_DATA" "$SYMFS" || SYMFS=""
fi

$WANT_CPU    && render_cpu
$WANT_OFFCPU && render_offcpu

( sleep 30 && kill "$WORKLOAD_PID" 2>/dev/null \
    && echo "[profile] warn: workload did not finish within 30s of profiling; killed" >&2 ) &
WATCHDOG_PID=$!
wait "$WORKLOAD_PID" 2>/dev/null || true
kill "$WATCHDOG_PID" 2>/dev/null || true
trap - EXIT

echo
echo "[profile] workload tail:"
tail -10 "$WORKLOAD_LOG" | sed 's/^/  /'
