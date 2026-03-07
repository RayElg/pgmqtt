#!/bin/bash

# run_perf_tests.sh - Execute MQTT performance tests against pgmqtt

# Disable MSYS path conversion for Git Bash on Windows
export MSYS_NO_PATHCONV=1

COMPOSE_FILE="docker/docker-compose.perf.yml"
HOST="pgmqtt"

log() {
    echo -e "\033[1;32m[PERF TEST] $1\033[0m"
}

run_bench() {
    local scenario=$1
    local cmd_args=$2
    local duration=${3:-60}
    log "Starting Scenario: $scenario (Duration: ${duration}s)"
    
    # We use sh -c because it handles argument splitting and -n 0 addition reliably
    local bench_cmd="/emqtt_bench/bin/emqtt_bench $cmd_args"
    if [[ $cmd_args == pub* ]]; then
        bench_cmd="$bench_cmd -n 0"
    fi

    docker compose -f $COMPOSE_FILE exec -T loadgen sh -c "/usr/bin/timeout --foreground ${duration}s $bench_cmd"
}

# Ensure environment is up
log "Ensuring performance environment is running..."
docker compose -f $COMPOSE_FILE up -d pgmqtt loadgen prometheus postgres-exporter

# Wait for pgmqtt to be healthy
log "Waiting for pgmqtt to be healthy..."
until [ "$(docker inspect -f '{{.State.Health.Status}}' $(docker compose -f $COMPOSE_FILE ps -q pgmqtt))" == "healthy" ]; do
    sleep 2
done

# Scenario A: High-Throughput Ingestion (QoS 1 focus)
run_bench "A - High-Throughput Ingestion (QoS 1)" \
    "pub -h $HOST -t 'test/scenarioA' -c 50 -i 50 -q 1 -s 1024" 30

# Scenario B: Massive Fan-out
run_bench "B - Massive Fan-out (Subscribers)" \
    "sub -h $HOST -t 'test/scenarioB' -c 2000" 40 &
sleep 5
run_bench "B - Fan-out (Publisher)" \
    "pub -h $HOST -t 'test/scenarioB' -c 50 -i 10 -q 0" 30

# Scenario C: Connection Churn & Subscribe Throughput
run_bench "C - Connection Churn & Sub Rate" \
    "conn -h $HOST -c 1000" 30

# Scenario Ramp: Push limits until failure
log "Scenario Ramp: Increasing throughput to find bottleneck..."
for target_rate in 100 500 1000 2000; do
    workers=$((target_rate / 10))
    run_bench "Ramp - $target_rate msg/sec" \
        "pub -h $HOST -t 'test/ramp' -c $workers -i 50 -q 1" 20
done

log "Performance tests completed. Check Grafana at http://localhost:3000 for results."