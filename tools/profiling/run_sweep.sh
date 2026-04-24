#!/usr/bin/env bash
# Profile sweep: run a matrix of modes × rates × profiler types sequentially.
#
# Each run archives its artifacts to:
#   tools/profiling/out/sweep/<mode>_<rate>r_<pubs>p<subs>s_<profiler>/
#
# A summary table is printed at the end showing throughput per run.
#
# Usage:
#   tools/profiling/run_sweep.sh [options]
#
# Options:
#   --modes     "qos0 qos1 cdc"    space-separated list  (default: qos0 qos1 cdc)
#   --rates     "200 500 1000"     rows/msgs per pub/s   (default: 200 500 1000)
#   --pubs      4                  publisher/worker count (default: 4)
#   --subs      4                  subscriber count      (default: 4)
#   --duration  60                 seconds per run       (default: 60)
#   --profilers "cpu offcpu"       profiler types        (default: cpu)
#                                  offcpu requires sudo + bpftrace
#   --buffer-kb 256                pgmqtt.max_client_buffer_kb for the sweep (default: 256)
#
# Example — sweep QoS modes at two rates, CPU only:
#   tools/profiling/run_sweep.sh --modes "qos0 qos1" --rates "500 1000"
#
# Example — add off-CPU for targeted investigation:
#   tools/profiling/run_sweep.sh --modes "qos1" --rates "500" --profilers "cpu offcpu"

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PROF_DIR="$ROOT/tools/profiling"
OUT_DIR="$PROF_DIR/out"
SWEEP_DIR="$OUT_DIR/sweep"

# ---------- defaults ----------
MODES=(qos0 qos1 cdc)
RATES=(200 500 1000)
PUBS=4
SUBS=4
DURATION=60
PROFILERS=(cpu)
BUFFER_KB=256

# ---------- arg parsing ----------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --modes)     IFS=' ' read -ra MODES     <<< "$2"; shift 2 ;;
        --rates)     IFS=' ' read -ra RATES     <<< "$2"; shift 2 ;;
        --profilers) IFS=' ' read -ra PROFILERS <<< "$2"; shift 2 ;;
        --pubs)      PUBS="$2";     shift 2 ;;
        --subs)      SUBS="$2";     shift 2 ;;
        --duration)  DURATION="$2"; shift 2 ;;
        --buffer-kb) BUFFER_KB="$2"; shift 2 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

mkdir -p "$SWEEP_DIR"

# ---------- helpers ----------
SEP="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Associative arrays aren't reliably ordered in bash; use parallel indexed arrays.
RESULT_TAGS=()
RESULT_LINES=()

extract_throughput() {
    local log="$1"
    if [[ -f "$log" ]]; then
        grep -E '^\[loadgen\] (elapsed|inbound:)' "$log" | tail -1 || echo "(crashed — no summary)"
    else
        echo "(no log)"
    fi
}

run_one() {
    local mode="$1" rate="$2" profiler="$3" run_n="$4" run_total="$5"
    local tag="${mode}_${rate}r_${PUBS}p${SUBS}s_${profiler}"
    local dest="$SWEEP_DIR/$tag"
    mkdir -p "$dest"

    echo
    echo "$SEP"
    printf "[sweep] run %d/%d  mode=%-8s rate=%-5s pubs=%s subs=%s profiler=%s\n" \
        "$run_n" "$run_total" "$mode" "$rate" "$PUBS" "$SUBS" "$profiler"
    echo "$SEP"

    local logfile=""

    if [[ "$profiler" == "cpu" ]]; then
        "$PROF_DIR/run_profile.sh" "$mode" "$DURATION" "$PUBS" "$SUBS" "$rate" || true
        for f in perf.folded flame.svg loadgen.log; do
            [[ -f "$OUT_DIR/$f" ]] && cp "$OUT_DIR/$f" "$dest/" || true
        done
        logfile="$dest/loadgen.log"

    elif [[ "$profiler" == "offcpu" ]]; then
        "$PROF_DIR/run_offcpu.sh" "$mode" "$DURATION" "$PUBS" "$SUBS" "$rate" || true
        cp "$OUT_DIR/offcpu.folded"      "$dest/" 2>/dev/null || true
        cp "$OUT_DIR/offcpu.svg"         "$dest/" 2>/dev/null || true
        cp "$OUT_DIR/offcpu.raw"         "$dest/" 2>/dev/null || true
        # Normalize log name so extract_throughput works uniformly.
        [[ -f "$OUT_DIR/offcpu.loadgen.log" ]] && \
            cp "$OUT_DIR/offcpu.loadgen.log" "$dest/loadgen.log" || true
        logfile="$dest/loadgen.log"

    else
        echo "[sweep] unknown profiler '$profiler', skipping" >&2
        return
    fi

    local summary
    summary="$(extract_throughput "$logfile")"
    RESULT_TAGS+=("$tag")
    RESULT_LINES+=("$summary")

    echo "[sweep] archived → $dest"
    echo "[sweep] $summary"

    sleep 5
}

# ---------- main ----------
TOTAL=$(( ${#MODES[@]} * ${#RATES[@]} * ${#PROFILERS[@]} ))

echo "$SEP"
printf "[sweep] %d runs planned\n" "$TOTAL"
printf "        modes     : %s\n"  "${MODES[*]}"
printf "        rates     : %s\n"  "${RATES[*]}"
printf "        pubs/subs : %s/%s\n" "$PUBS" "$SUBS"
printf "        duration  : %ss\n" "$DURATION"
printf "        profilers : %s\n"  "${PROFILERS[*]}"
printf "        buffer    : %s KiB\n" "$BUFFER_KB"
printf "        artifacts : %s\n"  "$SWEEP_DIR"
# Rough estimate: duration + ~12s overhead per run, + 5s pause between.
EST=$(( TOTAL * (DURATION + 17) ))
printf "        est. time : ~%dm%02ds\n" $(( EST / 60 )) $(( EST % 60 ))
echo "$SEP"

# Apply buffer GUC for the duration of the sweep; restore on exit.
PG_CONTAINER="$(docker ps --filter name=pgmqtt-enterprise-postgres-1 --format '{{.Names}}' | head -1)"
_psql() { docker exec "$PG_CONTAINER" psql -U postgres -tAc "$1" 2>/dev/null; }
ORIG_BUFFER_KB="$(_psql "SELECT current_setting('pgmqtt.max_client_buffer_kb', true)" || echo 64)"
if _psql "ALTER SYSTEM SET pgmqtt.max_client_buffer_kb = ${BUFFER_KB}" > /dev/null 2>&1 \
   && _psql "SELECT pg_reload_conf()" > /dev/null 2>&1; then
    echo "[sweep] pgmqtt.max_client_buffer_kb set to ${BUFFER_KB} KiB (was ${ORIG_BUFFER_KB})"
    trap '_psql "ALTER SYSTEM SET pgmqtt.max_client_buffer_kb = ${ORIG_BUFFER_KB}" > /dev/null 2>&1; _psql "SELECT pg_reload_conf()" > /dev/null 2>&1; echo "[sweep] pgmqtt.max_client_buffer_kb restored to ${ORIG_BUFFER_KB}"' EXIT
else
    echo "[sweep] WARNING: could not set pgmqtt.max_client_buffer_kb (GUC may not exist in running server — redeploy?). Continuing with current setting."
fi

n=0
for mode in "${MODES[@]}"; do
    for rate in "${RATES[@]}"; do
        for profiler in "${PROFILERS[@]}"; do
            n=$(( n + 1 ))
            run_one "$mode" "$rate" "$profiler" "$n" "$TOTAL"
        done
    done
done

# ---------- summary table ----------
echo
echo "$SEP"
echo "[sweep] SUMMARY"
echo "$SEP"
printf "  %-45s  %s\n" "RUN" "THROUGHPUT"
printf "  %-45s  %s\n" "---" "----------"
for i in "${!RESULT_TAGS[@]}"; do
    printf "  %-45s  %s\n" "${RESULT_TAGS[$i]}" "${RESULT_LINES[$i]}"
done
echo
echo "[sweep] done.  All artifacts in: $SWEEP_DIR"
