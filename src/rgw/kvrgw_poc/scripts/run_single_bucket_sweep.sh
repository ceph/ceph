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
RESULTS="${ROOT}/perf-results/single-bucket-sweep"
rm -rf "${RESULTS}"

for BUCKETS in 1 3 9; do
  echo ""
  echo "========================================"
  echo "  SINGLE INSTANCE — BUCKETS=${BUCKETS}"
  echo "========================================"

  "${ROOT}/scripts/reload.sh" --clean --perf

  DIR="${RESULTS}/b${BUCKETS}"
  mkdir -p "${DIR}/metrics" "${ROOT}/data-0"

  "${ROOT}/scripts/collect_fdb_metrics.sh" "${DIR}/metrics" 2 &
  COLLECTOR_PID=$!
  sleep 3

  {
    echo "create-buckets buckets=${BUCKETS}"
    echo "set-batch size=10 timeout=1000 threads=16"
    echo "set-sim-disk-write-us 0"
    echo "put c=128 tiers=8192 duration=120"
    echo "batch-stats"
    echo "quit"
  } | FDB_CLUSTER_FILE="${ROOT}/.fdb/fdb.cluster" \
      KVRGW_MAX_INLINE=256 \
      KVRGW_MAX_KV_STORE=4096 \
      KVRGW_BATCH_SIZE=10 \
      "${ROOT}/build/kv-rgw-backend" --perf "/tmp/kvrgw-perf-0.sock" "${ROOT}/data-0" \
      > "${DIR}/perf.log" 2>&1

  kill "${COLLECTOR_PID}" 2>/dev/null || true
  wait 2>/dev/null || true

  iops=$(grep "IOPS=" "${DIR}/perf.log" | grep -oP 'IOPS=\K[0-9]+')
  avg_total=$(grep "avg_total_us=" "${DIR}/perf.log" | head -1 | grep -oP 'avg_total_us=\K[0-9]+')
  qsz=$(grep "avg_queue_size=" "${DIR}/perf.log" | grep -oP 'avg_queue_size=\K[0-9.]+')
  wait_us=$(grep "avg_wait_us=" "${DIR}/perf.log" | grep -oP 'avg_wait_us=\K[0-9]+')
  retries=$(grep "txn_retries=" "${DIR}/perf.log" | grep -oP 'txn_retries=\K[0-9]+')
  echo "  IOPS=$iops  avg_total=${avg_total}us  queue=$qsz  wait=${wait_us}us  retries=$retries"

  python3 "${ROOT}/scripts/parse_fdb_timeseries.py" "${DIR}/metrics/fdb_status.log" 2>&1 \
    | awk 'NR>1 && $2>1000' \
    | awk '{sum_txn+=$4; sum_ss+=$7; max_ss=($8>max_ss?$8:max_ss); max_log=($9>max_log?$9:max_log); max_prx=($10>max_prx?$10:max_prx); sum_q+=$13; n++} END {printf "  FDB: txn/sec=%.0f ss_avg=%.2f ss_max=%.2f log_max=%.2f proxy_max=%.2f queue_MB=%.0f (n=%d)\n", sum_txn/n, sum_ss/n, max_ss, max_log, max_prx, sum_q/n, n}'
done

echo ""
echo "=== SINGLE INSTANCE BUCKET SWEEP COMPLETE ==="
