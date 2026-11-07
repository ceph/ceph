#!/bin/bash
#
# Ceph - scalable distributed file system
#
# Author: Gabriel BenHanokh <gbenhano@redhat.com>
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.
#
#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS="$ROOT/perf-results/phase1-instrumented"
WARP=~/go/bin/warp
EP="--host 127.0.0.1:9080 --access-key test --secret-key test --tls=false"

mkdir -p "$RESULTS"

sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

for i in 0 1 2; do
  echo "reset-latency" | socat - UNIX-CONNECT:/tmp/kvrgw-admin-${i}.sock >/dev/null 2>&1 || true
done

echo "Starting metrics collector..."
"$ROOT/scripts/collect_fdb_metrics.sh" "$RESULTS/metrics" 2 &
COLLECTOR_PID=$!
sleep 3

run_bench() {
  local label="$1" op="$2" size="$3" conc="$4" bucket="$5" extra="${6:-}"
  echo ""
  echo "=== $label: $op $size c=$conc ==="

  for i in 0 1 2; do
    echo "reset-latency" | socat - UNIX-CONNECT:/tmp/kvrgw-admin-${i}.sock >/dev/null 2>&1 || true
  done

  if [ "$op" = "put" ]; then
    $WARP put $EP --obj.size "$size" --concurrent "$conc" --duration 30s \
      --bucket "$bucket" --noclear --benchdata "$RESULTS/${label}.csv" 2>&1 | grep -E "Average|Reqs"
  elif [ "$op" = "get" ]; then
    $WARP get $EP --obj.size "$size" --concurrent "$conc" --duration 30s \
      --bucket "$bucket" --noclear --benchdata "$RESULTS/${label}.csv" 2>&1 | grep -E "Average|Reqs"
  elif [ "$op" = "list" ]; then
    $WARP list $EP --obj.size "$size" --concurrent "$conc" --duration 30s \
      --objects 10000 \
      --bucket "$bucket" --noclear --benchdata "$RESULTS/${label}.csv" 2>&1 | grep -E "Average|Reqs"
  fi

  echo "--- FDB latency (all instances) ---"
  for i in 0 1 2; do
    echo "[instance $i]"
    echo "get-latency" | socat - UNIX-CONNECT:/tmp/kvrgw-admin-${i}.sock 2>&1
  done
}

run_bench "put-128b-c64"  put 128B 64  "inst-put-128b"
run_bench "put-4kb-c64"   put 4KB  64  "inst-put-4kb"
run_bench "put-8kb-c64"   put 8KB  64  "inst-put-8kb"

run_bench "get-128b-c64"  get 128B 64  "inst-get-128b"
run_bench "get-4kb-c64"   get 4KB  64  "inst-get-4kb"
run_bench "get-8kb-c64"   get 8KB  64  "inst-get-8kb"

run_bench "list-128b-c16" list 128B 16 "inst-list-128b"

echo ""
echo "=== VERSIONED ==="
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
aws --endpoint-url http://127.0.0.1:9080 s3 mb s3://inst-versioned 2>/dev/null || true
aws --endpoint-url http://127.0.0.1:9080 s3api put-bucket-versioning \
  --bucket inst-versioned --versioning-configuration Status=Enabled 2>/dev/null

run_bench "vput-128b-c64" put 128B 64  "inst-versioned"
run_bench "vput-4kb-c64"  put 4KB  64  "inst-versioned"
run_bench "vput-8kb-c64"  put 8KB  64  "inst-versioned"

echo ""
echo "Stopping collectors..."
kill $COLLECTOR_PID 2>/dev/null || true
wait $COLLECTOR_PID 2>/dev/null || true

echo "=== ALL DONE ==="
echo "Results in: $RESULTS"
echo "Metrics in: $RESULTS/metrics/"
