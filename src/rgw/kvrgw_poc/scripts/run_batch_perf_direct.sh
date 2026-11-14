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
BACKEND="${ROOT}/build/kv-rgw-backend"
RESULTS="${ROOT}/perf-results/batch-put-direct"
DURATION=60
CONC=128

mkdir -p "${RESULTS}" "${ROOT}/data"

run_session() {
  local batch="$1"
  local outfile="${RESULTS}/batch${batch}.log"

  echo ""
  echo "########################################"
  echo "  batch_size=${batch} — direct perf driver"
  echo "########################################"

  {
    echo "create-buckets buckets=1"
    echo "set-batch size=${batch} timeout=1000 threads=8"
    echo "put c=${CONC} tiers=128 duration=${DURATION}"
    echo "batch-stats"
    echo "put c=${CONC} tiers=4096 duration=${DURATION}"
    echo "batch-stats"
    echo "put c=${CONC} tiers=8192 duration=${DURATION}"
    echo "batch-stats"
    echo "delete-buckets"
    echo "quit"
  } | FDB_CLUSTER_FILE="${ROOT}/.fdb/fdb.cluster" \
      KVRGW_MAX_INLINE=256 \
      KVRGW_MAX_KV_STORE=4096 \
      KVRGW_BATCH_SIZE="${batch}" \
      "${BACKEND}" --perf /tmp/kvrgw-perf.sock "${ROOT}/data" 2>&1 | tee "${outfile}"

  echo "--- saved to ${outfile} ---"
}

echo "=== Direct Perf Driver — Batch PUT ==="
echo "Duration: ${DURATION}s | Concurrency: ${CONC}"
echo "Tiers: inline(128B), child-D(4KB), storage(8KB)"
echo "Batch settings: 1, 5, 10"
echo ""

for BATCH in 1 5 10; do
  run_session "${BATCH}"
done

echo ""
echo "=== ALL DIRECT PERF RUNS COMPLETE ==="
echo ""
echo "=== SUMMARY ==="
for BATCH in 1 5 10; do
  echo "--- batch=${BATCH} ---"
  grep -E "IOPS=|batch_commits=" "${RESULTS}/batch${BATCH}.log" || true
  echo ""
done
