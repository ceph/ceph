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
FDB_CLI="${ROOT}/third_party/fdb/usr/bin/fdbcli"
CLUSTER_FILE="${ROOT}/.fdb/fdb.cluster"
PARSE_FDB="${ROOT}/scripts/parse_fdb_timeseries.py"

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]
Options:
  --description TEXT     Free-text description for this run
  --instances N          RGW instances (default 1)
  --concurrency N        Producer threads per instance (default 128)
  --batch-size N         Batch size (default 10)
  --batch-timeout N      Batch timeout us (default 1000)
  --batch-threads N      Batch worker threads (default 16)
  --object-size N        Object size bytes (default 8192)
  --buckets N            Buckets per instance (default 1)
  --burst-size N         Objects per bucket before round-robin (default 1)
  --duration N           Test duration seconds (default 60)
  --sim-write-us N       Simulated disk write latency us (default 0)
  --sim-read-us N        Simulated disk read latency us (default 0)
  --sample-interval N    Stats sampling interval seconds (default 2)
  --output-dir DIR       Output directory (default perf-results/runs)
  --no-reload            Skip reload --clean --perf
  --help                 Show this help
EOF
}

DESC=""
INSTANCES=1
CONC=128
BATCH_SIZE=10
BATCH_TIMEOUT=1000
BATCH_THREADS=16
OBJ_SIZE=8192
BUCKETS=1
BURST=1
DURATION=60
SIM_WRITE=0
SIM_READ=0
SAMPLE=2
OUTDIR="${ROOT}/perf-results/csv"
DO_RELOAD=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --description) DESC="$2"; shift 2 ;;
    --instances) INSTANCES="$2"; shift 2 ;;
    --concurrency) CONC="$2"; shift 2 ;;
    --batch-size) BATCH_SIZE="$2"; shift 2 ;;
    --batch-timeout) BATCH_TIMEOUT="$2"; shift 2 ;;
    --batch-threads) BATCH_THREADS="$2"; shift 2 ;;
    --object-size) OBJ_SIZE="$2"; shift 2 ;;
    --buckets) BUCKETS="$2"; shift 2 ;;
    --burst-size) BURST="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --sim-write-us) SIM_WRITE="$2"; shift 2 ;;
    --sim-read-us) SIM_READ="$2"; shift 2 ;;
    --sample-interval) SAMPLE="$2"; shift 2 ;;
    --output-dir) OUTDIR="$2"; shift 2 ;;
    --no-reload) DO_RELOAD=0; shift ;;
    --help) usage; exit 0 ;;
    *) echo "Unknown: $1" >&2; exit 1 ;;
  esac
done

GIT_COMMIT=$(cd "${ROOT}" && git rev-parse --short HEAD 2>/dev/null || echo "unknown")
TIMESTAMP=$(date -u +%Y%m%d_%H%M%S)
OUTFILE="${OUTDIR}/raw_${TIMESTAMP}.csv"
mkdir -p "${OUTDIR}"

# Count FDB processes from docker
FDB_SS=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -c 'fdb-storage' || echo 0)
FDB_LOG=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -c 'fdb-log' || echo 0)
FDB_STATELESS=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -c 'fdb-stateless' || echo 0)

if [[ "${OBJ_SIZE}" -le 256 ]]; then
  TIER="INLINE"
elif [[ "${OBJ_SIZE}" -le 4096 ]]; then
  TIER="CHILD_D"
else
  TIER="STORAGE"
fi

# Reload if requested
if [[ "${DO_RELOAD}" -eq 1 ]]; then
  "${ROOT}/scripts/reload.sh" --clean --perf
fi

# Write header
cat > "${OUTFILE}" <<HEADER
# description: ${DESC}
# git_commit: ${GIT_COMMIT}
# test_type: PUT
# test_start: $(date -u +%Y-%m-%dT%H:%M:%SZ)
# test_duration_sec: ${DURATION}
# object_size_bytes: ${OBJ_SIZE}
# tier: ${TIER}
# data_store: PerfDataStore
# sim_disk_write_us: ${SIM_WRITE}
# sim_disk_read_us: ${SIM_READ}
# rgw_instances: ${INSTANCES}
# rgw_producer_threads: ${CONC}
# rgw_batch_size: ${BATCH_SIZE}
# rgw_batch_timeout_us: ${BATCH_TIMEOUT}
# rgw_batch_threads: ${BATCH_THREADS}
# buckets_per_instance: ${BUCKETS}
# rgw_burst_size: ${BURST}
# fdb_storage_servers: ${FDB_SS}
# fdb_log_servers: ${FDB_LOG}
# fdb_stateless: ${FDB_STATELESS}
# sample_interval_sec: ${SAMPLE}
# columns: elapsed_s,fdb_txn_hz,fdb_reads_hz,fdb_writes_hz,fdb_conflict_hz,fdb_ss_cpu_avg,fdb_ss_cpu_max,fdb_log_cpu_max,fdb_proxy_cpu_max,fdb_queue_mb,fdb_durability_lag_s$(for i in $(seq 0 $((INSTANCES-1))); do echo -n ",i${i}_entries_hz,i${i}_commits_hz,i${i}_interval_wait_us,i${i}_interval_queue_size,i${i}_interval_latency_us,i${i}_errors"; done)
HEADER

echo "=== Test: ${DESC:-unnamed} ==="
echo "  ${INSTANCES} instances, c=${CONC}, batch=${BATCH_SIZE}, ${OBJ_SIZE}B, ${DURATION}s"
echo "  Output: ${OUTFILE}"

# Launch instances
for i in $(seq 0 $((INSTANCES-1))); do
  mkdir -p "${ROOT}/data-${i}"
  {
    echo "create-buckets buckets=${BUCKETS}"
    echo "set-batch size=${BATCH_SIZE} timeout=${BATCH_TIMEOUT} threads=${BATCH_THREADS}"
    echo "set-sim-disk-write-us ${SIM_WRITE}"
    echo "set-sim-disk-read-us ${SIM_READ}"
    echo "put c=${CONC} tiers=${OBJ_SIZE} duration=${DURATION} burst=${BURST}"
    echo "batch-stats"
    echo "quit"
  } | FDB_CLUSTER_FILE="${CLUSTER_FILE}" \
      KVRGW_MAX_INLINE=256 \
      KVRGW_MAX_KV_STORE=4096 \
      KVRGW_BATCH_SIZE="${BATCH_SIZE}" \
      KVRGW_ADMIN_SOCKET="/tmp/kvrgw-admin-perf-${i}.sock" \
      "${ROOT}/build/kv-rgw-backend" --perf "/tmp/kvrgw-perf-${i}.sock" "${ROOT}/data-${i}" \
      > "${OUTDIR}/instance-${i}_${TIMESTAMP}.log" 2>&1 &
done

# Wait for instances to start
sleep 5

# Prime the hz counters with a first read
for i in $(seq 0 $((INSTANCES-1))); do
  echo "get-batch-stats" | socat - UNIX-CONNECT:/tmp/kvrgw-admin-perf-${i}.sock 2>/dev/null || true
done
sleep "${SAMPLE}"

# Collect loop
END_TIME=$((SECONDS + DURATION - 5))
ELAPSED=0
while [[ $SECONDS -lt $END_TIME ]]; do
  ELAPSED=$((ELAPSED + SAMPLE))

  # FDB stats
  FDB_JSON=$("${FDB_CLI}" -C "${CLUSTER_FILE}" --exec "status json" --timeout 3 2>/dev/null || echo '{}')
  FDB_LINE=$(echo "${FDB_JSON}" | python3 -c "
import json,sys
try:
  j=json.load(sys.stdin)
  c=j.get('cluster',{})
  w=c.get('workload',{})
  ops=w.get('operations',{})
  txns=w.get('transactions',{})
  qos=c.get('qos',{})
  procs=c.get('processes',{})
  ss_c=[p.get('cpu',{}).get('usage_cores',0) for p in procs.values() if 'storage' in [r.get('role','') for r in p.get('roles',[])]]
  log_c=[p.get('cpu',{}).get('usage_cores',0) for p in procs.values() if 'log' in [r.get('role','') for r in p.get('roles',[])]]
  prx_c=[p.get('cpu',{}).get('usage_cores',0) for p in procs.values() if any(r.get('role','') in ('commit_proxy','grv_proxy','resolver') for r in p.get('roles',[]))]
  print('{:.0f},{:.0f},{:.0f},{:.1f},{:.3f},{:.3f},{:.3f},{:.3f},{:.1f},{:.2f}'.format(
    txns.get('committed',{}).get('hz',0),
    ops.get('reads',{}).get('hz',0),
    ops.get('writes',{}).get('hz',0),
    txns.get('conflicted',{}).get('hz',0),
    sum(ss_c)/len(ss_c) if ss_c else 0,
    max(ss_c) if ss_c else 0,
    max(log_c) if log_c else 0,
    max(prx_c) if prx_c else 0,
    qos.get('limiting_queue_bytes_storage_server',0)/1e6,
    qos.get('limiting_durability_lag_storage_server',{}).get('seconds',0)))
except: print('0,0,0,0,0,0,0,0,0,0')
" 2>/dev/null)

  # Per-instance stats
  INST_LINE=""
  for i in $(seq 0 $((INSTANCES-1))); do
    RAW=$(echo "get-batch-stats" | socat - UNIX-CONNECT:/tmp/kvrgw-admin-perf-${i}.sock 2>/dev/null || echo "")
    if [[ -n "${RAW}" ]]; then
      ehz=$(echo "${RAW}" | grep -oP 'entries_hz=\K[0-9.]+' || echo "0")
      chz=$(echo "${RAW}" | grep -oP 'commits_hz=\K[0-9.]+' || echo "0")
      iwait=$(echo "${RAW}" | grep -oP 'interval_wait_us=\K[0-9.]+' || echo "0")
      iqsz=$(echo "${RAW}" | grep -oP 'interval_queue_size=\K[0-9.]+' || echo "0")
      lat=$(echo "get-latency" | socat - UNIX-CONNECT:/tmp/kvrgw-admin-perf-${i}.sock 2>/dev/null || echo "")
      avg_total=$(echo "${lat}" | grep "PutObject" | grep -oP 'interval_total_us=\K[0-9]+' || echo "0")
      errs=$(echo "get-error-stats" | socat - UNIX-CONNECT:/tmp/kvrgw-admin-perf-${i}.sock 2>/dev/null || echo "")
      err_count=0
      if [[ "${errs}" != *"all zero"* && -n "${errs}" ]]; then
        err_count=$(echo "${errs}" | tr ' ' '\n' | grep -c '=' || echo "0")
      fi
      INST_LINE="${INST_LINE},${ehz},${chz},${iwait},${iqsz},${avg_total},${err_count}"
    else
      INST_LINE="${INST_LINE},0,0,0,0,0,0"
    fi
  done

  echo "${ELAPSED},${FDB_LINE}${INST_LINE}" >> "${OUTFILE}"
  sleep "${SAMPLE}"
done

# Wait for instances to finish
wait 2>/dev/null || true

# Append final perf driver output as comments
for i in $(seq 0 $((INSTANCES-1))); do
  LOG="${OUTDIR}/instance-${i}_${TIMESTAMP}.log"
  if [[ -f "${LOG}" ]]; then
    echo "# final_instance_${i}: $(grep 'IOPS=' "${LOG}" 2>/dev/null || echo 'no data')" >> "${OUTFILE}"
    echo "# final_batch_${i}: $(grep 'batch_commits=' "${LOG}" 2>/dev/null || echo 'no data')" >> "${OUTFILE}"
  fi
done

echo ""
echo "=== Test complete ==="
echo "Raw data: ${OUTFILE}"
echo "Samples: $(grep -c '^[0-9]' "${OUTFILE}") rows"
