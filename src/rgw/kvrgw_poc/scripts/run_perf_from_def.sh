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
if [[ -f "${ROOT}/System-Resources.md" ]]; then
  RESOURCES="${ROOT}/System-Resources.md"
elif [[ -f "${ROOT}/docs/System-Resources.md" ]]; then
  RESOURCES="${ROOT}/docs/System-Resources.md"
else
  RESOURCES="${ROOT}/System-Resources.md"
fi

usage() {
  echo "Usage: $(basename "$0") <Perf-Test-XXX.md> <FDB-Config-XXX.md>"
  exit 1
}

[[ $# -ge 2 ]] || usage
PERF_MD="${1}"
FDB_MD="${2}"
[[ -f "${PERF_MD}" ]] || { echo "ERROR: ${PERF_MD} not found" >&2; exit 1; }
[[ -f "${FDB_MD}" ]] || { echo "ERROR: ${FDB_MD} not found" >&2; exit 1; }

get_md_field() {
  local key="$1" file="$2"
  grep -m1 "| ${key} " "${file}" | awk -F'|' '{gsub(/^ *| *$/,"",$3); print $3}' || true
}

instances=$(get_md_field "instances" "${PERF_MD}")
mode=$(get_md_field "mode" "${PERF_MD}")
clean=$(get_md_field "clean" "${PERF_MD}")
batch_size=$(get_md_field "batch_size" "${PERF_MD}")
batch_timeout_us=$(get_md_field "batch_timeout_us" "${PERF_MD}")
batch_threads=$(get_md_field "batch_threads" "${PERF_MD}")
concurrency=$(get_md_field "concurrency" "${PERF_MD}")
producers=$(get_md_field "producers" "${PERF_MD}")
consumers=$(get_md_field "consumers" "${PERF_MD}")
max_futures=$(get_md_field "max_futures" "${PERF_MD}")
buckets=$(get_md_field "buckets" "${PERF_MD}")
burst=$(get_md_field "burst" "${PERF_MD}")
tiers=$(get_md_field "tiers" "${PERF_MD}")
sim_disk_write_us=$(get_md_field "sim_disk_write_us" "${PERF_MD}")
sim_disk_read_us=$(get_md_field "sim_disk_read_us" "${PERF_MD}")
sample_interval=$(get_md_field "sample_interval" "${PERF_MD}" | sed 's/s$//')
fdb_stats=$(get_md_field "fdb_stats" "${PERF_MD}")
kvrgw_stats=$(get_md_field "kvrgw_stats" "${PERF_MD}")
version_state=$(get_md_field "version_state" "${PERF_MD}")
max_inline=$(get_md_field "max_inline" "${PERF_MD}")
max_kv_store=$(get_md_field "max_kv_store" "${PERF_MD}")
duration=$(get_md_field "duration" "${PERF_MD}")
workload=$(get_md_field "workload" "${PERF_MD}")
prefix_len=$(get_md_field "prefix_len" "${PERF_MD}")
suffix_len=$(get_md_field "suffix_len" "${PERF_MD}")
tag_count=$(get_md_field "tag_count" "${PERF_MD}")
tag_name_base=$(get_md_field "tag_name_base" "${PERF_MD}")
tag_data_size=$(get_md_field "tag_data_size" "${PERF_MD}")

# #7: Default tier thresholds if not in config
[[ -n "$max_inline" ]] || max_inline=256
[[ -n "$max_kv_store" ]] || max_kv_store=4096

# #9: Validate all required fields are non-empty
missing=()
for field_name in instances mode clean workload buckets burst tiers batch_size \
    batch_timeout_us batch_threads sim_disk_write_us sim_disk_read_us \
    sample_interval fdb_stats kvrgw_stats version_state; do
  eval "val=\${${field_name}}"
  if [[ -z "$val" ]]; then
    missing+=("$field_name")
  fi
done
if [[ "$workload" == "get" ]]; then
  for field_name in producers consumers max_futures; do
    eval "val=\${${field_name}}"
    if [[ -z "$val" ]]; then
      missing+=("$field_name")
    fi
  done
else
  if [[ -z "$concurrency" ]]; then
    missing+=("concurrency")
  fi
fi
if [[ ${#missing[@]} -gt 0 ]]; then
  echo "ERROR: missing fields in ${PERF_MD}: ${missing[*]}" >&2; exit 1
fi

if [[ "$workload" != "put" && "$workload" != "put-overwrite" && "$workload" != "get" ]]; then
  echo "ERROR: workload must be 'put', 'put-overwrite', or 'get', got '${workload}'" >&2; exit 1
fi

if [[ "$workload" == "put" || "$workload" == "put-overwrite" ]]; then
  if [[ -z "$prefix_len" ]]; then
    echo "ERROR: prefix_len missing in ${PERF_MD}" >&2; exit 1
  fi
  if ! [[ "$prefix_len" =~ ^[0-9]+$ ]] || (( prefix_len < 1 || prefix_len > 768 )); then
    echo "ERROR: prefix_len must be in 1..768, got '${prefix_len}'" >&2; exit 1
  fi
  [[ -n "$suffix_len" ]] || suffix_len=0
  if ! [[ "$suffix_len" =~ ^[0-9]+$ ]] || (( suffix_len > 768 )); then
    echo "ERROR: suffix_len must be in 0..768, got '${suffix_len}'" >&2; exit 1
  fi
  if (( prefix_len + suffix_len > 768 )); then
    echo "ERROR: prefix_len + suffix_len must be <= 768, got ${prefix_len}+${suffix_len}" >&2; exit 1
  fi
  [[ -n "$tag_count" ]] || tag_count=0
  if ! [[ "$tag_count" =~ ^[0-9]+$ ]] || (( tag_count > 10 )); then
    echo "ERROR: tag_count must be in 0..10, got '${tag_count}'" >&2; exit 1
  fi
  if (( tag_count > 0 )); then
    if [[ -z "$tag_name_base" ]]; then
      echo "ERROR: tag_name_base required when tag_count > 0" >&2; exit 1
    fi
    if [[ -z "$tag_data_size" ]] || ! [[ "$tag_data_size" =~ ^[0-9]+$ ]] || \
       (( tag_data_size < 1 || tag_data_size > 256 )); then
      echo "ERROR: tag_data_size must be in 1..256 when tag_count > 0, got '${tag_data_size}'" >&2; exit 1
    fi
  else
    tag_name_base=""
    tag_data_size=""
  fi
else
  tag_count=0
  tag_name_base=""
  tag_data_size=""
fi

base_files=$(get_md_field "base_files" "${PERF_MD}")
overwrite_count=$(get_md_field "overwrite_count" "${PERF_MD}")
count=$(get_md_field "count" "${PERF_MD}")
source=$(get_md_field "source" "${PERF_MD}")
if [[ "$workload" == "put-overwrite" ]]; then
  if [[ -z "$base_files" || -z "$overwrite_count" ]]; then
    echo "ERROR: put-overwrite requires base_files and overwrite_count in ${PERF_MD}" >&2
    exit 1
  fi
elif [[ "$workload" == "put" ]]; then
  if [[ -z "$duration" ]]; then
    echo "ERROR: put requires a valid 'duration' in ${PERF_MD}" >&2
    exit 1
  fi
elif [[ "$workload" == "get" ]]; then
  if [[ "$clean" != "no" ]]; then
    echo "ERROR: workload get requires clean: no" >&2; exit 1
  fi
  if [[ -z "$source" ]]; then
    echo "ERROR: get requires source in ${PERF_MD}" >&2; exit 1
  fi
  if [[ -n "$duration" && -n "$count" ]]; then
    echo "ERROR: get stop mode is mutually exclusive: duration or count, not both" >&2
    exit 1
  fi
  if ! [[ "${max_futures}" =~ ^[0-9]+$ ]] || (( max_futures < 1 || max_futures > 1024 )); then
    echo "ERROR: max_futures must be in 1..1024, got '${max_futures}'" >&2; exit 1
  fi
  if [[ "${source}" != /* ]]; then
    source="${ROOT}/${source}"
  fi
  source="${source%/}"
  [[ -d "${source}" ]] || { echo "ERROR: source dir ${source} not found" >&2; exit 1; }
  SRC_META="${source}/test_metadata.txt"
  [[ -f "${SRC_META}" ]] || { echo "ERROR: ${SRC_META} not found" >&2; exit 1; }
  src_instances=$(grep '^instances=' "${SRC_META}" | head -1 | cut -d= -f2)
  src_threads=$(grep '^threads=' "${SRC_META}" | head -1 | cut -d= -f2)
  src_buckets=$(grep '^buckets=' "${SRC_META}" | head -1 | cut -d= -f2)
  src_prefix=$(grep '^prefix=' "${SRC_META}" | head -1 | cut -d= -f2)
  src_suffix=$(grep '^suffix=' "${SRC_META}" | head -1 | cut -d= -f2 || true)
  src_workload=$(grep '^workload=' "${SRC_META}" | head -1 | cut -d= -f2)
  if [[ "${src_workload}" != "put" && "${src_workload}" != "put-overwrite" ]]; then
    echo "ERROR: source workload must be put or put-overwrite, got '${src_workload}'" >&2; exit 1
  fi
  if [[ "${src_instances}" != "${instances}" ]]; then
    echo "ERROR: get instances (${instances}) must match PUT instances (${src_instances})" >&2; exit 1
  fi
  if [[ "${src_buckets}" != "${buckets}" ]]; then
    echo "ERROR: get buckets (${buckets}) must match PUT buckets (${src_buckets})" >&2; exit 1
  fi
  if [[ -z "${src_prefix}" ]]; then
    echo "ERROR: prefix missing in ${SRC_META}" >&2; exit 1
  fi
fi

if [[ "$version_state" != "none" && "$version_state" != "versioned" && "$version_state" != "suspended" ]]; then
  echo "ERROR: version_state must be 'none', 'versioned', or 'suspended', got '${version_state}'" >&2; exit 1
fi

PERF_NAME="$(basename "${PERF_MD}" .md)"
FDB_NAME="$(basename "${FDB_MD}" .md)"
TIMESTAMP=$(date -u +%Y%m%d_%H%M%S)
OUTDIR="${ROOT}/perf-results/${FDB_NAME}_${PERF_NAME}_${TIMESTAMP}"
mkdir -p "${OUTDIR}"

hex_to_len() {
  local hex="$1" want="$2"
  if (( want == 0 )); then
    echo -n ""
    return
  fi
  if (( ${#hex} >= want )); then
    echo -n "${hex:0:want}"
  else
    local pad=$((want - ${#hex}))
    echo -n "${hex}$(printf "%0${pad}d" 0)"
  fi
}

if [[ "$workload" == "get" ]]; then
  KEY_PREFIX="${src_prefix}"
  KEY_SUFFIX="${src_suffix:-}"
else
  hex=$(echo -n "${PERF_NAME}:${FDB_NAME}:${TIMESTAMP}" | sha512sum | awk '{print $1}')
  KEY_PREFIX=$(hex_to_len "$hex" "$prefix_len")
  KEY_SUFFIX=$(hex_to_len "$hex" "$suffix_len")
fi

cat > "${OUTDIR}/test_metadata.txt" <<METAEOF
prefix=${KEY_PREFIX}
suffix=${KEY_SUFFIX}
instances=${instances}
threads=${concurrency:-${src_threads:-}}
buckets=${buckets}
tiers=${tiers}
burst=${burst}
duration=${duration}
version_state=${version_state}
workload=${workload}
METAEOF
if [[ "$workload" == "put-overwrite" ]]; then
  cat >> "${OUTDIR}/test_metadata.txt" <<METAEOF
base_files=${base_files}
overwrite_count=${overwrite_count}
METAEOF
fi
if [[ "$workload" == "get" ]]; then
  cat >> "${OUTDIR}/test_metadata.txt" <<METAEOF
source=${source}
count=${count}
METAEOF
  cp "${SRC_META}" "${OUTDIR}/source_metadata.txt"
fi

echo "=== Perf Test: ${PERF_NAME} ==="
echo "  FDB config: ${FDB_NAME}"
echo "  Output: ${OUTDIR}"
echo "  prefix=${KEY_PREFIX}"
echo "  suffix=${KEY_SUFFIX}"
if [[ "$workload" == "put" || "$workload" == "put-overwrite" ]]; then
  echo "  prefix_len=${prefix_len} suffix_len=${suffix_len} xtag_count=${tag_count}"
fi
echo "  instances=${instances} mode=${mode} clean=${clean}"
if [[ "$workload" == "get" ]]; then
  echo "  producers=${producers} consumers=${consumers} max_futures=${max_futures} batch=${batch_size} buckets=${buckets}"
else
  echo "  c=${concurrency} batch=${batch_size} tiers=${tiers} burst=${burst} buckets=${buckets}"
fi
echo "  max_inline=${max_inline} max_kv_store=${max_kv_store}"

if [[ "$workload" == "put-overwrite" ]]; then
  workload_cmd="put-overwrite c=${concurrency} tiers=${tiers} base_files=${base_files} overwrite_count=${overwrite_count}"
  echo "  workload=put-overwrite base_files=${base_files} overwrite_count=${overwrite_count}"
elif [[ "$workload" == "get" ]]; then
  workload_cmd="get-test producers=${producers} consumers=${consumers} max_futures=${max_futures}"
  if [[ -n "$duration" ]]; then
    workload_cmd="${workload_cmd} duration=${duration}"
  elif [[ -n "$count" ]]; then
    workload_cmd="${workload_cmd} count=${count}"
  fi
  if [[ "$version_state" == "versioned" ]]; then
    workload_cmd="${workload_cmd} --all-versions"
  fi
  echo "  workload=get source=${source}"
  echo "  ${workload_cmd}"
else
  workload_cmd="put c=${concurrency} tiers=${tiers} duration=${duration} burst=${burst}"
  echo "  duration=${duration}s"
fi

progress_sec=10
if [[ "${workload_cmd}" =~ --progress-sec=([0-9]+) ]]; then
  progress_sec="${BASH_REMATCH[1]}"
fi

# #25: Resource oversubscription warning
if [[ -f "${RESOURCES}" ]]; then
  total_cores=$(grep -oP '= \K[0-9]+(?= logical CPUs)' "${RESOURCES}" || echo 0)
  fdb_cores=$(get_md_field "CPU cores" "${FDB_MD}" | awk '{print $1}' || echo 0)
  if [[ "$total_cores" -gt 0 && "$fdb_cores" -gt 0 ]]; then
    used=$((fdb_cores + instances))
    if (( used > total_cores )); then
      echo "  WARNING: FDB cores (${fdb_cores}) + instances (${instances}) = ${used} exceeds ${total_cores} machine cores"
    fi
  fi
fi

cp "${PERF_MD}" "${OUTDIR}/"
cp "${FDB_MD}" "${OUTDIR}/"

if [[ "${clean}" == "yes" ]]; then
  echo ">>> Stopping all existing FDB containers..."
  docker ps -q --filter name=fdb- | xargs -r docker stop 2>/dev/null || true
  docker ps -q --filter name=fdb- | xargs -r docker rm 2>/dev/null || true

  echo ">>> Generating docker-compose.yml from ${FDB_NAME}..."
  "${ROOT}/scripts/gen_docker_compose.sh" "${FDB_MD}"

  echo ">>> Reloading FDB (clean + perf)..."
  "${ROOT}/scripts/reload.sh" --clean --perf
else
  echo ">>> clean=no: leaving FDB and binary as-is"
fi

# #12: Verify container count matches config
EXPECT_SS=$(grep -cP '^\| fdb-storage' "${FDB_MD}" || echo 0)
EXPECT_LOG=$(grep -cP '^\| fdb-log' "${FDB_MD}" || echo 0)
EXPECT_SL=$(grep -cP '^\| fdb-stateless' "${FDB_MD}" || echo 0)
FDB_SS=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -c 'fdb-storage' || echo 0)
FDB_LOG=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -c 'fdb-log' || echo 0)
FDB_SL=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -c 'fdb-stateless' || echo 0)
echo "  FDB processes: ${FDB_SS} SS, ${FDB_LOG} log, ${FDB_SL} stateless"
if [[ "${FDB_SS}" -ne "${EXPECT_SS}" || "${FDB_LOG}" -ne "${EXPECT_LOG}" || "${FDB_SL}" -ne "${EXPECT_SL}" ]]; then
  echo "ERROR: expected ${EXPECT_SS} SS + ${EXPECT_LOG} LOG + ${EXPECT_SL} SL but got ${FDB_SS} SS + ${FDB_LOG} LOG + ${FDB_SL} SL" >&2
  exit 1
fi

# Apply FDB cluster settings from config
FDB_THROTTLE=$(grep -oP '^\| throttle \| \K\w+' "${FDB_MD}" || echo "disable")
if [[ "${FDB_THROTTLE}" == "enable" ]]; then
  echo ">>> Enabling FDB throttle..."
  "${FDB_CLI}" -C "${CLUSTER_FILE}" --exec "throttle enable" >/dev/null 2>&1 || true
fi

# #11: Parse mount paths from FDB config for disk free collector
FDB_MOUNTS=$(awk -F'|' '/^\| fdb-/{gsub(/^ *| *$/,"",$7); print $7}' "${FDB_MD}" | sort -u)

PIDS=()
FIFO_FDS=()
FIFO_PATHS=()
INST_LOGS=()
FIFO_FD=10
for i in $(seq 0 $((instances-1))); do
  inst_id=$(printf "%02d" "$i")
  inst_log="${OUTDIR}/instance-${inst_id}_${TIMESTAMP}.log"
  INST_LOGS+=("${inst_log}")
  mkdir -p "${ROOT}/data-${i}"

  perf_flag=""
  if [[ "${mode}" == "perf" ]]; then
    perf_flag="--perf"
  fi

  inst_fifo="${OUTDIR}/instance-${inst_id}_fifo"
  mkfifo "${inst_fifo}"

  if [[ "$workload" == "get" ]]; then
    inst_meta="${SRC_META}"
  else
    inst_meta="${OUTDIR}/test_metadata_${i}.txt"
  fi

  FDB_CLUSTER_FILE="${CLUSTER_FILE}" \
      KVRGW_INSTANCE_ID="${i}" \
      KVRGW_KEY_PREFIX="${KEY_PREFIX}" \
      KVRGW_KEY_SUFFIX="${KEY_SUFFIX}" \
      KVRGW_METADATA_FILE="${inst_meta}" \
      KVRGW_TAG_COUNT="${tag_count}" \
      KVRGW_TAG_NAME_BASE="${tag_name_base}" \
      KVRGW_TAG_DATA_SIZE="${tag_data_size}" \
      KVRGW_MAX_INLINE="${max_inline}" \
      KVRGW_MAX_KV_STORE="${max_kv_store}" \
      KVRGW_BATCH_SIZE="${batch_size}" \
      KVRGW_ADMIN_SOCKET="/tmp/kvrgw-admin-perf-${i}.sock" \
      "${ROOT}/build/kv-rgw-backend" ${perf_flag} "/tmp/kvrgw-perf-${i}.sock" "${ROOT}/data-${i}" \
      < "${inst_fifo}" > "${inst_log}" 2>&1 &
  PIDS+=($!)
  echo "  Instance ${inst_id} launched (PID ${PIDS[-1]}) → ${inst_log}"

  eval "exec ${FIFO_FD}>${inst_fifo}"
  if [[ "$workload" != "get" ]]; then
    echo "create-buckets buckets=${buckets} mode=${version_state}" >&${FIFO_FD}
  fi
  echo "set-batch size=${batch_size} timeout=${batch_timeout_us} threads=${batch_threads}" >&${FIFO_FD}
  if [[ "${mode}" == "perf" ]]; then
    echo "set-sim-disk-write-us ${sim_disk_write_us}" >&${FIFO_FD}
    echo "set-sim-disk-read-us ${sim_disk_read_us}" >&${FIFO_FD}
  fi
  echo "${workload_cmd}" >&${FIFO_FD}
  FIFO_FDS+=("${FIFO_FD}")
  FIFO_PATHS+=("${inst_fifo}")
  FIFO_FD=$((FIFO_FD + 1))
done

sleep 5

collect_fdb_stats() {
  local outfile="${OUTDIR}/fdb_stats_${TIMESTAMP}.log"

  while true; do
    local ts
    ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)

    # #11: Use mount paths from FDB config
    local disk_free_pct=""
    for mnt in ${FDB_MOUNTS}; do
      if mountpoint -q "$mnt" 2>/dev/null; then
        pct=$(df --output=pcent "$mnt" 2>/dev/null | tail -1 | tr -d '% ' || echo "0")
        free_pct=$((100 - pct))
        disk_free_pct="${disk_free_pct}${mnt}=${free_pct} "
      fi
    done

    local fdb_json_file="${OUTDIR}/.fdb_json_tmp"
    timeout 5 "${FDB_CLI}" -C "${CLUSTER_FILE}" --exec "status json" --timeout 3 > "${fdb_json_file}" 2>/dev/null || true

    # Write skip marker if fdbcli returned empty output
    if [[ ! -s "${fdb_json_file}" ]]; then
      echo "=== SKIP ===" >> "${outfile}"
      echo "---" >> "${outfile}"
      sleep "${sample_interval}"
      continue
    fi

    python3 -c "
import json,sys
ts='${ts}'
disk_free='${disk_free_pct}'.strip()
try:
  with open('${fdb_json_file}') as f: j=json.load(f)
  c=j.get('cluster',{})
  w=c.get('workload',{})
  ops=w.get('operations',{})
  txns=w.get('transactions',{})
  qos=c.get('qos',{})
  procs=c.get('processes',{})

  print(f'ts={ts}')

  print('cluster_txn_hz={:.0f} cluster_reads_hz={:.0f} cluster_writes_hz={:.0f} cluster_conflict_hz={:.1f}'.format(
    txns.get('committed',{}).get('hz',0), ops.get('reads',{}).get('hz',0),
    ops.get('writes',{}).get('hz',0), txns.get('conflicted',{}).get('hz',0)))

  print('qos_worst_queue_ss={:.0f} qos_worst_queue_log={:.0f} qos_limiting_queue={:.0f} qos_durability_lag_s={:.2f}'.format(
    qos.get('worst_queue_bytes_storage_server',0),
    qos.get('worst_queue_bytes_log_server',0),
    qos.get('limiting_queue_bytes_storage_server',0),
    qos.get('limiting_durability_lag_storage_server',{}).get('seconds',0)))

  for pid,p in sorted(procs.items()):
    for r in p.get('roles',[]):
      if r.get('role') != 'storage': continue
      addr = p.get('address','?')
      cpu = p.get('cpu',{}).get('usage_cores',0)
      mem = p.get('memory',{}).get('used_bytes',0)
      rss = p.get('memory',{}).get('rss_bytes', mem)
      inp = r.get('input_bytes',{}).get('hz',0)
      dur = r.get('durable_bytes',{}).get('hz',0)
      stored = r.get('stored_bytes',0)
      kvstore = r.get('kvstore_used_bytes',0)
      data_lag = r.get('data_lag',{}).get('seconds',0)
      dur_lag = r.get('durability_lag',{}).get('seconds',0)
      queue_disk = r.get('queue_disk_used_bytes',0)
      disk_busy = p.get('disk',{}).get('busy',0)
      disk_reads = p.get('disk',{}).get('reads',{}).get('hz',0)
      disk_writes = p.get('disk',{}).get('writes',{}).get('hz',0)
      print(f'ss addr={addr} cpu={cpu:.3f} mem_bytes={mem} rss_bytes={rss} input_hz={inp:.0f} durable_hz={dur:.0f} stored_bytes={stored} kvstore_bytes={kvstore} data_lag_s={data_lag:.3f} durability_lag_s={dur_lag:.3f} queue_disk_bytes={queue_disk} disk_busy={disk_busy:.3f} disk_reads_hz={disk_reads:.0f} disk_writes_hz={disk_writes:.0f}')

  for pid,p in sorted(procs.items()):
    for r in p.get('roles',[]):
      if r.get('role') != 'log': continue
      addr = p.get('address','?')
      cpu = p.get('cpu',{}).get('usage_cores',0)
      mem = p.get('memory',{}).get('used_bytes',0)
      rss = p.get('memory',{}).get('rss_bytes', mem)
      inp = r.get('input_bytes',{}).get('hz',0)
      dur = r.get('durable_bytes',{}).get('hz',0)
      queue_disk = r.get('queue_disk_used_bytes',0)
      queue_mem = r.get('queue_memory_bytes',0) if 'queue_memory_bytes' in r else 0
      disk_busy = p.get('disk',{}).get('busy',0)
      disk_reads = p.get('disk',{}).get('reads',{}).get('hz',0)
      disk_writes = p.get('disk',{}).get('writes',{}).get('hz',0)
      print(f'log addr={addr} cpu={cpu:.3f} mem_bytes={mem} rss_bytes={rss} input_hz={inp:.0f} durable_hz={dur:.0f} queue_disk_bytes={queue_disk} queue_mem_bytes={queue_mem} disk_busy={disk_busy:.3f} disk_reads_hz={disk_reads:.0f} disk_writes_hz={disk_writes:.0f}')

  for pid,p in sorted(procs.items()):
    for r in p.get('roles',[]):
      if r.get('role') not in ('commit_proxy','grv_proxy','resolver'): continue
      addr = p.get('address','?')
      role = r.get('role')
      cpu = p.get('cpu',{}).get('usage_cores',0)
      mem = p.get('memory',{}).get('used_bytes',0)
      rss = p.get('memory',{}).get('rss_bytes', mem)
      print(f'sl addr={addr} role={role} cpu={cpu:.3f} mem_bytes={mem} rss_bytes={rss}')

  if disk_free:
    print(f'disk_free {disk_free}')

  print('---')
except Exception:
  print('=== SKIP ===')
  print('---')
" >> "${outfile}" 2>&1

    sleep "${sample_interval}"
  done
}

collect_kvrgw_stats() {
  while true; do
    for i in $(seq 0 $((instances-1))); do
      local inst_id
      inst_id=$(printf "%02d" "$i")
      local sock="/tmp/kvrgw-admin-perf-${i}.sock"
      {
        echo "--- instance-${inst_id} $(date -u +%Y-%m-%dT%H:%M:%SZ) ---"
        echo "get-batch-stats" | socat - UNIX-CONNECT:"${sock}" 2>/dev/null || echo "(unavailable)"
        echo "get-latency" | socat - UNIX-CONNECT:"${sock}" 2>/dev/null || echo "(unavailable)"
        echo "get-ops-stats" | socat - UNIX-CONNECT:"${sock}" 2>/dev/null || echo "(unavailable)"
        echo "get-error-stats" | socat - UNIX-CONNECT:"${sock}" 2>/dev/null || echo "(unavailable)"
      } >> "${OUTDIR}/instance-${inst_id}_${TIMESTAMP}.stats" 2>&1
    done
    sleep "${sample_interval}"
  done
}

collect_host_stats() {
  local outfile="${OUTDIR}/host_stats_${TIMESTAMP}.log"
  while true; do
    echo "--- $(date -u +%Y-%m-%dT%H:%M:%SZ) ---" >> "${outfile}"
    pidstat -p ALL 1 1 2>/dev/null | grep -E 'fdb|kv-rgw' >> "${outfile}" 2>&1 || true
    iostat -xz 1 1 2>/dev/null | grep -v '^$' >> "${outfile}" 2>&1 || true
    free -m | head -2 >> "${outfile}" 2>&1 || true
    sleep "${sample_interval}"
  done
}

if [[ "${fdb_stats}" == "full" ]]; then
  collect_fdb_stats &
  echo "  FDB stats collector started"
fi

if [[ "${kvrgw_stats}" == "full" ]]; then
  collect_kvrgw_stats &
  echo "  KVRGW stats collector started"
fi

collect_host_stats &
echo "  Host stats collector started"

PROG_LAST_LINE=()
PROG_SNAP=()
PROG_NEW=()
WAIT_T0=0

init_progress_scan() {
  local i
  PROG_LAST_LINE=()
  PROG_SNAP=()
  PROG_NEW=()
  for i in $(seq 0 $((instances-1))); do
    PROG_LAST_LINE[i]=0
    PROG_SNAP[i]=""
    PROG_NEW[i]=0
  done
  WAIT_T0=$SECONDS
}

scan_progress_logs() {
  [[ "${progress_sec}" -ne 0 ]] || return 0
  local i nlines new_block pline all_new=1
  for i in $(seq 0 $((instances-1))); do
    local log="${INST_LOGS[$i]}"
    if [[ ! -f "$log" ]]; then
      all_new=0
      continue
    fi
    nlines=$(wc -l < "$log" | tr -d ' ')
    [[ -n "$nlines" ]] || nlines=0
    if (( nlines > PROG_LAST_LINE[i] )); then
      new_block=$(sed -n "$((PROG_LAST_LINE[i]+1)),${nlines}p" "$log")
      PROG_LAST_LINE[i]=$nlines
      pline=$(printf '%s\n' "${new_block}" | grep -E '^[[:space:]]*progress:' | tail -1 || true)
      if [[ -n "$pline" ]]; then
        pline="${pline#"${pline%%[![:space:]]*}"}"
        printf 'inst-%02d %s\n' "$i" "$pline"
        PROG_SNAP[i]="$pline"
        PROG_NEW[i]=1
      fi
    fi
    if [[ "${PROG_NEW[i]}" != "1" ]]; then
      all_new=0
    fi
  done
  if [[ "${all_new}" -eq 1 ]]; then
    python3 -c '
import re, sys
elapsed = float(sys.argv[1])
lines = sys.argv[2:]

def parse(line):
    tot = re.search(
        r"total:\s*\(\s*([0-9.]+)M objects\s+(\d+) iops,\s*(\d+)us latency\)",
        line,
    )
    iv = re.search(
        r"interval:\s*\(\s*([0-9.]+)M objects\s+(\d+) iops,\s*(\d+)us latency\)",
        line,
    )
    return (
        float(tot.group(1)) * 1e6 if tot else 0.0,
        float(iv.group(1)) * 1e6 if iv else 0.0,
        int(tot.group(2)) if tot else 0,
        int(tot.group(3)) if tot else 0,
        int(iv.group(2)) if iv else 0,
        int(iv.group(3)) if iv else 0,
    )

obj = obj_last = 0.0
iops = iops_last = 0
lat_num = lat_den = 0.0
latl_num = latl_den = 0.0
for line in lines:
    o, ol, io, lu, iol, ll = parse(line)
    obj += o
    obj_last += ol
    iops += io
    iops_last += iol
    if o > 0:
        lat_num += lu * o
        lat_den += o
    if ol > 0:
        latl_num += ll * ol
        latl_den += ol
lat = int(lat_num / lat_den) if lat_den else 0
latl = int(latl_num / latl_den) if latl_den else 0

print("\033[32m  TOTAL progress: %6.1f elapsed; total: (%6.1fM objects %6d iops, %6dus latency) | "
         "interval: (%6.1fM objects %6d iops, %6dus latency) \033[0m" % (
          elapsed, obj / 1e6, iops, lat, obj_last / 1e6, iops_last, latl))
' "$((SECONDS - WAIT_T0))" "${PROG_SNAP[@]}"
    for i in $(seq 0 $((instances-1))); do
      PROG_NEW[i]=0
    done
  fi
}

wait_with_progress() {
  local label="$1" total="$2" elapsed=0 step=10
  local do_prog=0
  if [[ "${3:-}" == "progress" && "${progress_sec}" -ne 0 ]]; then
    do_prog=1
    step="${progress_sec}"
    init_progress_scan
  fi
  if [[ "${do_prog}" -eq 0 ]]; then
    echo ">>> ${label}: ${elapsed}s / ${total}s"
  fi
  while (( elapsed < total )); do
    local chunk=$step remain=$((total - elapsed))
    (( remain < chunk )) && chunk=$remain
    sleep "$chunk"
    elapsed=$((elapsed + chunk))
    if [[ "${do_prog}" -eq 1 ]]; then
      scan_progress_logs
    else
      echo ">>> ${label}: ${elapsed}s / ${total}s"
    fi
  done
}

wait_cmd_complete() {
  local label="$1" complete_pat="$2" err_pat="$3"
  local elapsed=0 step=10
  local do_prog=0
  if [[ "${progress_sec}" -ne 0 ]]; then
    do_prog=1
    step="${progress_sec}"
    init_progress_scan
  fi
  #echo ">>> ${label}: ${elapsed}s (0/${instances} instances complete)"
  while true; do
    local done=0
    local idx
    for idx in "${!PIDS[@]}"; do
      local log="${INST_LOGS[$idx]}"
      local pid="${PIDS[$idx]}"
      if grep -qE "${err_pat}" "${log}" 2>/dev/null; then
        echo "ERROR: ${err_pat} in ${log}" >&2
        tail -50 "${log}" >&2 || true
        exit 1
      fi
      if grep -q "${complete_pat}" "${log}" 2>/dev/null; then
        done=$((done + 1))
        continue
      fi
      if ! kill -0 "${pid}" 2>/dev/null; then
        echo "ERROR: instance PID ${pid} exited before ${complete_pat} (${log})" >&2
        tail -50 "${log}" >&2 || true
        exit 1
      fi
    done
    if [[ "${done}" -eq "${instances}" ]]; then
      echo ">>> ${label} complete (${elapsed}s)"
      return
    fi
    sleep "${step}"
    elapsed=$((elapsed + step))
    if [[ "${do_prog}" -eq 1 ]]; then
      scan_progress_logs
    fi
    #echo ">>> ${label}: ${elapsed}s (${done}/${instances} instances complete)"
  done
}

if [[ "$workload" == "put-overwrite" ]]; then
  wait_cmd_complete "PUT-OVERWRITE" "put-overwrite complete" "PUT_ERR"
  if grep -E 'errors=[1-9]' "${INST_LOGS[@]}" >/dev/null 2>&1; then
    echo "ERROR: put-overwrite reported errors" >&2
    grep -E 'errors=[1-9]|PUT_ERR' "${INST_LOGS[@]}" >&2 || true
    exit 1
  fi
elif [[ "$workload" == "get" ]]; then
  if [[ -n "$duration" ]]; then
    wait_with_progress "GET" $((duration + 10)) progress
  else
    wait_cmd_complete "GET" "get-test complete" "GET_ERR"
  fi
  if grep -qE 'GET_ERR|missing=[1-9]' "${INST_LOGS[@]}" 2>/dev/null; then
    echo "ERROR: get-test reported missing keys or GET_ERR" >&2
    grep -E 'GET_ERR|missing=[1-9]|errors=[1-9]' "${INST_LOGS[@]}" >&2 || true
    exit 1
  fi
  if grep -E 'errors=[1-9]' "${INST_LOGS[@]}" >/dev/null 2>&1; then
    echo "ERROR: get-test reported errors" >&2
    grep -E 'errors=[1-9]|GET_ERR' "${INST_LOGS[@]}" >&2 || true
    exit 1
  fi
else
  wait_with_progress "PUT" $((duration + 10)) progress
fi

echo ">>> Workload phase done. Sending batch-stats + quit to instances..."
for idx in "${!FIFO_FDS[@]}"; do
  fd="${FIFO_FDS[$idx]}"
  echo "batch-stats" >&${fd}
  echo "quit" >&${fd}
  eval "exec ${fd}>&-"
done

for pid in "${PIDS[@]}"; do
  wait "$pid" 2>/dev/null || true
done

echo ">>> Perf drivers exited. Merging metadata..."
if [[ "$workload" != "get" ]]; then
  for i in $(seq 0 $((instances-1))); do
    inst_meta="${OUTDIR}/test_metadata_${i}.txt"
    if [[ -f "$inst_meta" ]]; then
      while IFS= read -r line; do
        echo "max_seq_${i}=${line#*=}" >> "${OUTDIR}/test_metadata.txt"
      done < "$inst_meta"
      rm -f "$inst_meta"
    fi
  done
fi

wait_until_quiet() {
  local elapsed=0 step=10
  echo ">>> Drain: until Cluster lim_q=0.0MB worst_ss_q=0.0MB"
  while true; do
    local fdb_json_file="${OUTDIR}/.fdb_json_tmp"
    timeout 5 "${FDB_CLI}" -C "${CLUSTER_FILE}" --exec "status json" --timeout 3 \
      > "${fdb_json_file}" 2>/dev/null || true
    local line
    line=$(python3 -c "
import json
try:
  with open('${fdb_json_file}') as f:
    j = json.load(f)
  qos = j.get('cluster', {}).get('qos', {})
  lq = float(qos.get('limiting_queue_bytes_storage_server', 0) or 0)
  wq = float(qos.get('worst_queue_bytes_storage_server', 0) or 0)
  def fmt_mb(v):
    if v >= 1e9:
      return f'{v/1e9:.1f}GB'
    return f'{v/1e6:.1f}MB'
  print(f\"{fmt_mb(lq)} {fmt_mb(wq)}\")
except Exception:
  print('? ?')
" 2>/dev/null || echo "? ?")
    local lq_s wq_s
    lq_s=$(echo "$line" | awk '{print $1}')
    wq_s=$(echo "$line" | awk '{print $2}')
    echo ">>> Drain: ${elapsed}s  Cluster: lim_q=${lq_s}  worst_ss_q=${wq_s}"
    if [[ "${lq_s}" == "0.0MB" && "${wq_s}" == "0.0MB" ]]; then
      break
    fi
    sleep "$step"
    elapsed=$((elapsed + step))
  done
}

wait_until_quiet

# Kill remaining collectors
kill $(jobs -p) 2>/dev/null || true
wait 2>/dev/null || true

# #14: Clean up FIFO files and temp files
rm -f "${OUTDIR}"/instance-*_fifo "${OUTDIR}/.fdb_json_tmp"

echo ""
echo "=== Test complete ==="
echo "Results: ${OUTDIR}"
echo "Files:"
ls -1 "${OUTDIR}/"
