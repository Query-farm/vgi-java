#!/usr/bin/env bash
# Reproducible inline-vs-shared-memory benchmark for the example worker.
#
# Measures end-to-end query wall-clock in Haybarn for transport-heavy workloads,
# with the shared-memory transport off (inline pipe) and on.
#
# Workloads are sized into the MULTI-SECOND range on purpose: at sub-second times,
# scheduler jitter, JIT state, and timer granularity are a large fraction of the
# measurement. Several-second runs make that noise negligible. We report the
# median AND the min–max spread of the warm runs so the noise is visible, not
# hidden behind a single point estimate.
#
# Usage:  ./bench.sh            # needs `uvx` (haybarn-cli) on PATH
set -euo pipefail
cd "$(dirname "$0")"

WARMUP=2               # workload runs discarded before timing (JIT / page cache)
MEASURED=9             # warm runs actually timed, per workload per mode
SHM_BYTES=268435456    # 256 MiB segment (>= one 32 MiB batch, with headroom)

echo "==> Building worker…"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
(cd "$ROOT" && ./gradlew --quiet :examples:docs:installDist)
BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/build/install/vgi-java-examples/bin/vgi-java-examples"

gen_sql() {
  local query="$1"
  echo ".timer on"
  echo "LOAD vgi;"
  echo "ATTACH 'demo' AS demo (TYPE vgi, LOCATION 'launch:$BIN');"
  for _ in $(seq 1 $((WARMUP + MEASURED))); do echo "$query"; done
  echo "DETACH demo;"
}

# Run a workload in one mode; echo the MEASURED warm times (one per line).
# Drops the first (2 + WARMUP) timed statements: LOAD, ATTACH, and the warmups.
warm_times() {
  local query="$1" env_prefix="$2"
  pkill -f farm.query.vgi.examples >/dev/null 2>&1 || true
  sleep 1
  gen_sql "$query" \
    | env $env_prefix uvx haybarn-cli 2>/dev/null \
    | grep -oE 'real [0-9.]+' | awk '{print $2}' \
    | tail -n +$((2 + WARMUP + 1)) | head -n "$MEASURED"
}

# stats <newline-separated numbers> -> "min median max"
stats() {
  sort -n | awk '
    {a[NR]=$1}
    END {
      med = (NR%2) ? a[(NR+1)/2] : (a[NR/2]+a[NR/2+1])/2
      printf "%.3f %.3f %.3f", a[1], med, a[NR]
    }'
}

bench() {
  local label="$1" query="$2" bytes_mb="$3" rows_m="$4"
  local i s
  i=$(warm_times "$query" "" | stats)
  s=$(warm_times "$query" "VGI_RPC_SHM_SIZE_BYTES=$SHM_BYTES" | stats)
  read -r imin imed imax <<<"$i"
  read -r smin smed smax <<<"$s"
  awk -v l="$label" -v b="$bytes_mb" -v r="$rows_m" \
      -v imin="$imin" -v imed="$imed" -v imax="$imax" \
      -v smin="$smin" -v smed="$smed" -v smax="$smax" 'BEGIN{
    printf "%-26s | inline %5.2fs [%.2f-%.2f] %4.0f MB/s %3.0f Mrow/s | shm %5.2fs [%.2f-%.2f] %4.0f MB/s %3.0f Mrow/s | %.2fx\n",
      l, imed, imin, imax, b/imed, r/imed, smed, smin, smax, b/smed, r/smed, imed/smed
  }'
}

echo "==> Benchmarking ($MEASURED warm runs each, after $WARMUP warmups)…"
echo
# Scan: 2B int64 = 16 GB, in 4M-row (32 MB) batches. One-way (worker -> engine).
# `numbers` scans on up to 4 threads, so this is sized large to keep even the
# shm run comfortably multi-second (noise discipline).
bench "scan 2B rows (16 GB)" \
  "SELECT sum(n) FROM demo.numbers(2000000000, batch_size := 4000000);" 16000 2000
# Round-trip through echo: generate 200M, feed back in, emit out (~4.8 GB across
# three transfers — exercises inbound + outbound shm).
bench "echo round-trip 200M (4.8 GB)" \
  "SELECT count(*) FROM demo.echo((SELECT * FROM demo.numbers(200000000, batch_size := 4000000)));" 4800 200
# Scalar: 50M strings through upper_case. sum(length(...)) forces evaluation so
# DuckDB can't prune the call. Per-row Unicode work + the variable-length string
# round-trip dominate, so the rate is lower and shm helps less.
bench "scalar upper_case 50M strings" \
  "SELECT sum(length(demo.upper_case(i::VARCHAR))) FROM range(50000000) t(i);" 780 50
echo
echo "Machine: $(uname -msr)  |  median of $MEASURED warm runs, [min-max] in brackets"
