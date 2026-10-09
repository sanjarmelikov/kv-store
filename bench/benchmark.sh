#!/usr/bin/env bash
# Throughput of kv-store vs real Redis, measured with redis-benchmark (the official tool).
#
#   bench/benchmark.sh            # 50 clients, 200k requests per test, no pipelining
#   CLIENTS=50 REQUESTS=500000 PIPELINE=16 bench/benchmark.sh
#
# Both servers run on this machine with persistence off, one after the other,
# so they see the same conditions. Results append to results/benchmark.jsonl.
set -euo pipefail
cd "$(dirname "$0")/.."

CLIENTS="${CLIENTS:-50}"
REQUESTS="${REQUESTS:-200000}"
PIPELINE="${PIPELINE:-1}"
RUNS="${RUNS:-3}"   # repeat and keep the median, since one run can be noisy
KV_PORT=6391
REDIS_PORT=6392
mkdir -p results

./build/kv-store --port "$KV_PORT" 2>/dev/null &
KV_PID=$!
redis-server --port "$REDIS_PORT" --save "" --appendonly no --daemonize no >/dev/null &
REDIS_PID=$!
trap 'kill $KV_PID $REDIS_PID 2>/dev/null || true' EXIT
sleep 1

bench() {  # bench PORT TEST → median requests/sec over RUNS runs
  local port=$1 test=$2 i out rps=()
  for ((i = 0; i < RUNS; i++)); do
    out=$(redis-benchmark -p "$port" -c "$CLIENTS" -n "$REQUESTS" -P "$PIPELINE" -t "$test" -q 2>/dev/null | tr '\r' '\n' | grep -E "^$(echo "$test" | tr a-z A-Z):" | tail -1)
    rps+=("$(echo "$out" | sed -E 's/.*: ([0-9.]+) requests per second.*/\1/')")
  done
  printf '%s\n' "${rps[@]}" | sort -n | sed -n "$(( (RUNS + 1) / 2 ))p"
}

commit=$(git rev-parse --short HEAD)
dirty=$([ -z "$(git status --porcelain --untracked-files=no)" ] && echo false || echo true)
echo "clients=$CLIENTS requests=$REQUESTS pipeline=$PIPELINE runs=$RUNS (median)"
printf '%-6s %14s %14s %8s\n' test kv-store redis ratio
for test in set get; do
  kv=$(bench "$KV_PORT" "$test")
  rd=$(bench "$REDIS_PORT" "$test")
  ratio=$(python3 -c "print(round($kv / $rd, 3))")
  printf '%-6s %14s %14s %8s\n' "$test" "$kv" "$rd" "$ratio"
  echo "{\"test\": \"$test\", \"clients\": $CLIENTS, \"requests\": $REQUESTS, \"pipeline\": $PIPELINE, \"runs\": $RUNS, \"kv_store_rps\": $kv, \"redis_rps\": $rd, \"ratio\": $ratio, \"redis_version\": \"$(redis-server --version | grep -oE 'v=[0-9.]+' | cut -d= -f2)\", \"commit\": \"$commit\", \"dirty\": $dirty, \"timestamp\": \"$(date +%Y-%m-%dT%H:%M:%S)\"}" >> results/benchmark.jsonl
done
