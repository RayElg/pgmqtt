# Profiling

Tools for profiling the BGW under load.

- `profile.sh` — orchestrator. Runs a workload, records `perf` (on-CPU) and/or `bpftrace` (off-CPU), renders flamegraphs.
- `load_gen.py` — pub/sub/CDC/inbound load generator (`--mode {qos0,qos1,cdc,inbound}`).
- `offcpu.bt` — bpftrace script that samples `sched_switch` to attribute blocked time.
- `offcpu_fold.py` — folds `offcpu.bt` output into FlameGraph format, grouped by blocking reason (fsync, disk-io, epoll, net-recv, …).
- `Dockerfile.profiler` — image with `perf`, `bpftrace`, and kernel headers, used by `profile.sh --bpftrace-docker`.

## Quick start

```bash
# Build the profiler image once
docker build -f tools/profiling/Dockerfile.profiler -t pgmqtt-profiler:latest tools/profiling/

# Profile QoS 1 throughput, on-CPU + off-CPU
./tools/profiling/profile.sh \
    --container pgmqtt-postgres-1 \
    --pid-pattern 'pgmqtt_mqtt' \
    --mode both \
    --duration 45 \
    --label q1-baseline \
    --bpftrace-docker \
    --workload "python3 tools/profiling/load_gen.py --mode qos1 --duration 40 --publishers 4 --subscribers 4"
```

Outputs land in `out/<label>.{cpu,offcpu}.svg` plus folded stacks and a per-reason summary.
