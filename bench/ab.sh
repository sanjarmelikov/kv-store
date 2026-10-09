#!/usr/bin/env bash
# A/B throughput of two kv-store builds under identical conditions, alternating runs.
#
#   bench/ab.sh BASE_REF [RUNS]     e.g.  bench/ab.sh HEAD~1 5
#
# Builds BASE_REF in a temporary worktree, then alternates redis-benchmark runs
# against the base binary and the current build (pipelined, 1M random keys, 50 clients).
set -euo pipefail
cd "$(dirname "$0")/.."
BASE_REF="${1:?usage: bench/ab.sh BASE_REF [RUNS]}"
RUNS="${2:-5}"
CLIENTS="${CLIENTS:-50}" REQUESTS="${REQUESTS:-2000000}" PIPELINE="${PIPELINE:-16}" KEYSPACE="${KEYSPACE:-1000000}"
TMP=$(mktemp -d)
trap 'git worktree remove --force "$TMP/src" >/dev/null 2>&1 || true; rm -rf "$TMP"' EXIT
git worktree add -q "$TMP/src" "$BASE_REF"
cmake -S "$TMP/src" -B "$TMP/build" -DCMAKE_BUILD_TYPE=Release >/dev/null && cmake --build "$TMP/build" -j4 >/dev/null
cmake --build build -j4 >/dev/null

rps() {  # rps BINARY TEST
  "$1" --port 6394 2>/dev/null &
  local pid=$! out
  sleep 0.5
  out=$(redis-benchmark -p 6394 -c "$CLIENTS" -n "$REQUESTS" -P "$PIPELINE" -r "$KEYSPACE" -t "$2" -q 2>/dev/null | tr '\r' '\n' | grep -E "^$(echo "$2" | tr a-z A-Z):" | tail -1)
  kill "$pid"; wait "$pid" 2>/dev/null || true
  echo "$out" | sed -E 's/.*: ([0-9.]+) requests per second.*/\1/'
}
median() { sort -n | sed -n "$(( (RUNS + 1) / 2 ))p"; }

base_commit=$(git rev-parse --short "$BASE_REF"); new_commit=$(git rev-parse --short HEAD)
mkdir -p results
printf '%-5s %14s %14s %8s\n' test "$base_commit" "$new_commit" change
for test in set get; do
  a=(); b=()
  for ((i = 0; i < RUNS; i++)); do a+=("$(rps "$TMP/build/kv-store" "$test")"); b+=("$(rps ./build/kv-store "$test")"); done
  ma=$(printf '%s\n' "${a[@]}" | median); mb=$(printf '%s\n' "${b[@]}" | median)
  change=$(python3 -c "print(f'{($mb / $ma - 1) * 100:+.1f}%')")
  printf '%-5s %14s %14s %8s\n' "$test" "$ma" "$mb" "$change"
  echo "{\"test\": \"$test\", \"base\": \"$base_commit\", \"new\": \"$new_commit\", \"base_rps\": $ma, \"new_rps\": $mb, \"change\": \"$change\", \"runs\": $RUNS, \"clients\": $CLIENTS, \"pipeline\": $PIPELINE, \"keyspace\": $KEYSPACE, \"base_all\": [$(IFS=,; echo "${a[*]}")], \"new_all\": [$(IFS=,; echo "${b[*]}")], \"timestamp\": \"$(date +%Y-%m-%dT%H:%M:%S)\"}" >> results/ab.jsonl
done
