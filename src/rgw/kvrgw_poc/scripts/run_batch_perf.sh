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
source "${ROOT}/scripts/kvrgw-common.sh"

WARP=~/go/bin/warp
EP="--host 127.0.0.1:9080 --access-key test --secret-key test --tls=false"
DURATION=60
CONC=128
FDB_CLI="${ROOT}/third_party/fdb/usr/bin/fdbcli"
RESULTS="${ROOT}/perf-results/batch-put-tiers"
mkdir -p "${RESULTS}"

admin_cmd() {
  local i="$1" cmd="$2"
  echo "${cmd}" | socat - UNIX-CONNECT:/tmp/kvrgw-admin-${i}.sock 2>/dev/null || true
}

admin_all() {
  local cmd="$1"
  for i in 0 1 2; do admin_cmd "$i" "$cmd"; done
}

reset_all_stats() {
  for i in 0 1 2; do
    admin_cmd "$i" "reset-latency" > /dev/null
    admin_cmd "$i" "reset-error-stats" > /dev/null
  done
}

collect_stats() {
  local label="$1"
  local f="${RESULTS}/${label}.stats"
  {
    echo "=== ${label} ==="
    echo ""
    echo "--- error-stats ---"
    for i in 0 1 2; do
      echo "[instance $i] $(admin_cmd "$i" "get-error-stats")"
    done
    echo ""
    echo "--- latency ---"
    for i in 0 1 2; do
      echo "[instance $i]"
      admin_cmd "$i" "get-latency"
    done
    echo ""
    echo "--- tier-config ---"
    admin_cmd 0 "get-tier-config"
    echo ""
    echo "--- fdb status ---"
    "${FDB_CLI}" -C "${ROOT}/.fdb/fdb.cluster" --exec "status details" 2>/dev/null | head -80 || echo "(fdbcli unavailable)"
  } | tee "${f}"
}

run_put() {
  local batch="$1" tier_label="$2" size="$3" bucket="$4"
  local label="batch${batch}-${tier_label}"

  echo ""
  echo "========================================"
  echo "  PUT ${tier_label} | batch=${batch} | ${DURATION}s c=${CONC}"
  echo "========================================"

  reset_all_stats

  export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
  aws --endpoint-url http://127.0.0.1:9080 s3 mb "s3://${bucket}" 2>/dev/null || true

  ${WARP} put ${EP} --obj.size "${size}" --concurrent "${CONC}" \
    --duration "${DURATION}s" --bucket "${bucket}" --noclear 2>&1 \
    | tee "${RESULTS}/${label}.warp" | grep -E "^warp:|Thro|Reqs|Average|errors" || true

  echo ""
  collect_stats "${label}"
}

restart_with_batch() {
  local bs="$1"
  echo ""
  echo "########################################"
  echo "  Restarting backends with batch_size=${bs}"
  echo "########################################"

  kvrgw_stop_servers 3
  sleep 2

  export KVRGW_BATCH_SIZE="${bs}"
  for i in 0 1 2; do
    kvrgw_start_instance "$i"
  done
  kvrgw_stop_gw
  kvrgw_write_nginx_conf 3
  nginx -p "${NGINX_DIR}" -c "${NGINX_DIR}/nginx.conf"
  sleep 2

  echo "Tier config: $(admin_cmd 0 "get-tier-config")"
}

echo "=== Batch PUT Perf Test ==="
echo "Duration: ${DURATION}s per run | Concurrency: ${CONC}"
echo "Tiers: inline(128B), child-D(4KB), storage(8KB)"
echo "Batch settings: 1, 5, 10"
echo "Results: ${RESULTS}"
echo ""

for BATCH in 1 5 10; do
  restart_with_batch "${BATCH}"

  run_put "${BATCH}" "inline-128B"  "128B"  "perf-inline-b${BATCH}"
  run_put "${BATCH}" "childD-4KB"   "4KiB"  "perf-childd-b${BATCH}"
  run_put "${BATCH}" "storage-8KB"  "8KiB"  "perf-storage-b${BATCH}"
done

echo ""
echo "=== ALL PERF RUNS COMPLETE ==="
echo "Results in: ${RESULTS}/"
ls -la "${RESULTS}/"
