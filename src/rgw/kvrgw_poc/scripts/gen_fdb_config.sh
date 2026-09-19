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
if [[ -f "${ROOT}/System-Resources.md" ]]; then
  RESOURCES="${ROOT}/System-Resources.md"
elif [[ -f "${ROOT}/docs/System-Resources.md" ]]; then
  RESOURCES="${ROOT}/docs/System-Resources.md"
else
  echo "ERROR: System-Resources.md not found (tried ${ROOT}/ and ${ROOT}/docs/)" >&2
  exit 1
fi
CONFIG="${1:-${ROOT}/FDB-Default-Config.txt}"
OUTPUT_NAME="$(basename "${CONFIG}" .txt).md"
OUTPUT="${ROOT}/${OUTPUT_NAME}"
if [[ ! -f "${CONFIG}" ]]; then
  echo "ERROR: ${CONFIG} not found" >&2; exit 1
fi

MAX_CPU=$(grep -oP 'Max CPU cores for FDB:\s*\K[0-9]+' "${RESOURCES}")
MAX_MEM=$(grep -oP 'Max DRAM for FDB:\s*\K[0-9]+' "${RESOURCES}")

# #15: Parse AVAIL_CORES from System-Resources.md NUMA lines
# Order: node0 physical, node0 HT, node1 physical, node1 HT
AVAIL_CORES=()
expand_range() {
  local spec="$1"
  for part in ${spec//,/ }; do
    if [[ "$part" == *-* ]]; then
      local lo=${part%-*} hi=${part#*-}
      local c
      for (( c=lo; c<=hi; c++ )); do :; AVAIL_CORES+=("$c"); done
    else
      AVAIL_CORES+=("$part")
    fi
  done
}
node0=$(grep -oP 'NUMA node0.*cores \K[0-9,\- ]+' "${RESOURCES}" | head -1 | tr -d ' ' || true)
node1=$(grep -oP 'NUMA node1.*cores \K[0-9,\- ]+' "${RESOURCES}" | head -1 | tr -d ' ' || true)
if [[ -n "$node0" ]]; then expand_range "$node0"; fi
if [[ -n "$node1" ]]; then expand_range "$node1"; fi
if [[ ${#AVAIL_CORES[@]} -eq 0 ]]; then
  echo "ERROR: could not parse NUMA core list from ${RESOURCES}" >&2; exit 1
fi

parse_line() {
  local line="$1"
  local count cpu mem
  count=$(echo "$line" | grep -oP 'count=\K[0-9]+')
  cpu=$(echo "$line" | grep -oP 'CPU-Cores=\K[0-9]+')
  mem=$(echo "$line" | grep -oP 'MEM-GB=\K[0-9]+')
  echo "$count $cpu $mem"
}

ss_line=$(grep -i '^storage-servers:' "${CONFIG}")
log_line=$(grep -i '^Log-server:' "${CONFIG}")
stateless_line=$(grep -i '^stateless-servers:' "${CONFIG}")

read ss_count ss_cpu ss_mem <<< "$(parse_line "$ss_line")"
read log_count log_cpu log_mem <<< "$(parse_line "$log_line")"
read sl_count sl_cpu sl_mem <<< "$(parse_line "$stateless_line")"

# Parse throttle, storage_hard_limit_mb, engine
throttle=$(grep -i '^throttle:' "${CONFIG}" | awk '{print $2}' || echo "disable")
storage_hard_limit_mb=$(grep -i '^storage_hard_limit_mb:' "${CONFIG}" | awk '{print $2}' || echo "1500")
engine=$(grep -i '^engine:' "${CONFIG}" | awk '{print $2}' || true)
if [[ -z "${engine}" ]]; then
  echo "ERROR: engine: is required (ssd2, ssd-redwood-1, or rocksdb)" >&2; exit 1
fi
case "${engine}" in
  ssd2) fdbcli_engine="ssd" ;;
  ssd-redwood-1) fdbcli_engine="ssd-redwood-1" ;;
  rocksdb) fdbcli_engine="ssd-rocksdb-v1" ;;
  *) echo "ERROR: engine: must be ssd2, ssd-redwood-1, or rocksdb, got '${engine}'" >&2; exit 1 ;;
esac

if [[ "$throttle" != "enable" && "$throttle" != "disable" ]]; then
  echo "ERROR: 'throttle' must be 'enable' or 'disable', got '${throttle}'" >&2; exit 1
fi
if ! [[ "$storage_hard_limit_mb" =~ ^[0-9]+$ ]] || (( storage_hard_limit_mb <= 0 )); then
  echo "ERROR: 'storage_hard_limit_mb' must be a positive integer, got '${storage_hard_limit_mb}'" >&2; exit 1
fi
storage_hard_limit_bytes=$(( storage_hard_limit_mb * 1048576 ))

total_cpu=$(( ss_count * ss_cpu + log_count * log_cpu + sl_count * sl_cpu ))
total_mem=$(( ss_count * ss_mem + log_count * log_mem + sl_count * sl_mem ))

if (( total_cpu > MAX_CPU )); then
  echo "ERROR: FDB config requests ${total_cpu} cores, limit is ${MAX_CPU}" >&2; exit 1
fi
if (( total_mem > MAX_MEM )); then
  echo "ERROR: FDB config requests ${total_mem} GB DRAM, limit is ${MAX_MEM} GB" >&2; exit 1
fi

VALID_SS_COUNTS="1 3 6 9 12"
ss_valid=0
for v in $VALID_SS_COUNTS; do
  if (( ss_count == v )); then ss_valid=1; break; fi
done
if (( ss_valid == 0 )); then
  echo "ERROR: storage-server count must be one of {1, 3, 6, 9, 12} (round-robin across 3 NVMe drives), got ${ss_count}" >&2; exit 1
fi
if (( log_count > 3 )); then
  echo "ERROR: max 3 log servers (3 log partitions available)" >&2; exit 1
fi
if (( sl_count > 3 )); then
  echo "ERROR: max 3 stateless servers (co-located with log partitions)" >&2; exit 1
fi

SS_DRIVE_MOUNTS=(/mnt/fdb0 /mnt/fdb1 /mnt/fdb2)
SS_DRIVE_ZONES=(zone0 zone1 zone2)
SS_DRIVE_MACHINES=(fdb-storage0 fdb-storage1 fdb-storage2)
SS_BASE_PORT=4500

SS_PORTS=()
SS_MOUNTS=()
SS_ZONES=()
SS_MACHINES=()
SS_NAMES=()
for (( i=0; i<ss_count; i++ )); do
  drive_idx=$(( i % 3 ))
  slot=$(( i / 3 ))
  SS_MOUNTS+=("${SS_DRIVE_MOUNTS[$drive_idx]}")
  SS_ZONES+=("${SS_DRIVE_ZONES[$drive_idx]}")
  SS_MACHINES+=("${SS_DRIVE_MACHINES[$drive_idx]}")
  SS_PORTS+=( $(( SS_BASE_PORT + i )) )
  SS_NAMES+=("fdb-storage${drive_idx}${slot}")
done

# #20: Separate port ranges to avoid collision
LOG_PORTS=(4520 4521 4522)
LOG_MOUNTS=(/mnt/fdb-log0 /mnt/fdb-log1 /mnt/fdb-log2)
LOG_ZONES=(zone3 zone4 zone5)
LOG_MACHINES=(fdb-log0 fdb-log1 fdb-log2)

SL_PORTS=(4530 4531 4532)
SL_MOUNTS=(/mnt/fdb-log0 /mnt/fdb-log1 /mnt/fdb-log2)
SL_ZONES=(zone3 zone4 zone5)
SL_MACHINES=(fdb-log0 fdb-log1 fdb-log2)
SL_NAMES=(fdb-stateless0 fdb-stateless1 fdb-stateless2)

# #24: Use global variable instead of subshell to preserve core_idx
core_idx=0
_cpuset_result=""

next_cores() {
  local need=$1
  local _nc_j
  _cpuset_result=""
  for (( _nc_j=0; _nc_j<need; _nc_j++ )); do
    if (( core_idx >= ${#AVAIL_CORES[@]} )); then
      echo "ERROR: ran out of available CPU cores (need ${need}, used ${core_idx}/${#AVAIL_CORES[@]})" >&2; exit 1
    fi
    if [[ -n "$_cpuset_result" ]]; then _cpuset_result+=","; fi
    _cpuset_result+="${AVAIL_CORES[$core_idx]}"
    (( core_idx++ )) || true
  done
}

# #17: cache_memory = 50% of MEM-GB
compute_cache() {
  local mem_gb=$1
  local cache_mb=$(( mem_gb * 1024 / 2 ))
  if (( cache_mb >= 1024 )); then
    echo "$(( cache_mb / 1024 ))GiB"
  else
    echo "${cache_mb}MiB"
  fi
}

ss_cache=$(compute_cache "$ss_mem")
log_cache=$(compute_cache "$log_mem")
sl_cache=$(compute_cache "$sl_mem")

# #18: Container mem_limit = MEM-GB * 2
ss_mem_limit="$(( ss_mem * 2 ))g"
log_mem_limit="$(( log_mem * 2 ))g"
sl_mem_limit="$(( sl_mem * 2 ))g"

{
cat <<EOF
# FDB Cluster Configuration

Generated from: $(basename "${CONFIG}")
Generated at: $(date -u +%Y-%m-%dT%H:%M:%SZ)

## Resource Budget

| Resource | Requested | Limit |
|----------|-----------|-------|
| CPU cores | ${total_cpu} | ${MAX_CPU} |
| DRAM (GB) | ${total_mem} | ${MAX_MEM} |

## Storage Servers (${ss_count})

| Container      | Port | CPUset | Cores | MEM |      Mount     | Zone  | Machine ID   |
|----------------|------|--------|-------|-----|----------------|-------|--------------|
EOF

for (( i=0; i<ss_count; i++ )); do
  next_cores "$ss_cpu"
  printf "| %-14s | %4d | %6s | %5d | %2dG | %-14s | %-5s | %-12s |\n" \
         "${SS_NAMES[$i]}" "${SS_PORTS[$i]}" "$_cpuset_result" "$ss_cpu" "$ss_mem" \
         "${SS_MOUNTS[$i]}" "${SS_ZONES[$i]}" "${SS_MACHINES[$i]}"
done

cat <<EOF

## Log Servers (${log_count})

| Container      | Port | CPUset | Cores | MEM |      Mount     | Zone  | Machine ID |
|----------------|------|--------|-------|-----|----------------|-------|------------|
EOF

for (( i=0; i<log_count; i++ )); do
  next_cores "$log_cpu"
  printf "| %-14s | %4d | %6s | %5d | %2dG | %-14s | %-5s | %-10s |\n" \
    "${LOG_MACHINES[$i]}" "${LOG_PORTS[$i]}" "$_cpuset_result" "$log_cpu" "$log_mem" \
    "${LOG_MOUNTS[$i]}" "${LOG_ZONES[$i]}" "${LOG_MACHINES[$i]}"
done

cat <<EOF

## Stateless Servers (${sl_count})

| Container      | Port | CPUset | Cores | MEM |      Mount     | Zone  | Machine ID |
|----------------|------|--------|-------|-----|----------------|-------|------------|
EOF

for (( i=0; i<sl_count; i++ )); do
  next_cores "$sl_cpu"
  printf "| %-14s | %4d | %6s | %5d | %2dG | %-14s | %-5s | %-10s |\n" \
    "${SL_NAMES[$i]}" "${SL_PORTS[$i]}" "$_cpuset_result" "$sl_cpu" "$sl_mem" \
    "${SL_MOUNTS[$i]}" "${SL_ZONES[$i]}" "${SL_MACHINES[$i]}"
done

# #16: Per-role FDB memory
cat <<EOF

## FDB Command Line

SS: \`-m ${ss_mem}GiB --cache_memory ${ss_cache} --knob_storage_hard_limit_bytes=${storage_hard_limit_bytes}\`
LOG: \`-m ${log_mem}GiB --cache_memory ${log_cache}\`
SL: \`-m ${sl_mem}GiB --cache_memory ${sl_cache}\`

Container mem_limit: SS=${ss_mem_limit} LOG=${log_mem_limit} SL=${sl_mem_limit}

## FDB Cluster Settings

| Setting | Value |
|---------|-------|
| throttle | ${throttle} |
| storage_hard_limit_mb | ${storage_hard_limit_mb} |
| engine | ${engine} |
| fdbcli_engine | ${fdbcli_engine} |
EOF
} > "${OUTPUT}"

echo "Generated: ${OUTPUT}"
echo "  Total: $((ss_count + log_count + sl_count)) containers, ${total_cpu} cores, ${total_mem} GB DRAM"
