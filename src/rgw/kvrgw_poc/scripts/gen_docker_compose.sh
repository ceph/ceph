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
FDB_MD="${1:?Usage: gen_docker_compose.sh <FDB-Config-XXX.md>}"
COMPOSE_OUT="${ROOT}/fdb-cluster/docker-compose.yml"
if [[ -f "${ROOT}/System-Resources.md" ]]; then
  RESOURCES="${ROOT}/System-Resources.md"
elif [[ -f "${ROOT}/docs/System-Resources.md" ]]; then
  RESOURCES="${ROOT}/docs/System-Resources.md"
else
  echo "ERROR: System-Resources.md not found (tried ${ROOT}/ and ${ROOT}/docs/)" >&2
  exit 1
fi
FDBSERVER_PATH="${ROOT}/third_party/fdb/usr/sbin/fdbserver"

[[ -f "${FDB_MD}" ]] || { echo "ERROR: ${FDB_MD} not found" >&2; exit 1; }
[[ -x "${FDBSERVER_PATH}" ]] || { echo "ERROR: fdbserver not executable at ${FDBSERVER_PATH} (run bash scripts/fetch_fdb.sh)" >&2; exit 1; }

# #6: Build mount allowlist from System-Resources.md
ALLOWED_MOUNTS=()
while IFS= read -r line; do
  mnt=$(echo "$line" | grep -oP '→ \K/mnt/\S+' || true)
  if [[ -n "$mnt" ]]; then
    ALLOWED_MOUNTS+=("$mnt")
  fi
done < "${RESOURCES}"

validate_mount() {
  local mount="$1" container="$2"
  for allowed in "${ALLOWED_MOUNTS[@]}"; do
    if [[ "$mount" == "$allowed" ]]; then return 0; fi
  done
  echo "ERROR: mount '${mount}' for container '${container}' not in System-Resources.md allowlist" >&2
  exit 1
}

# Parse per-role FDB memory from the .md
# Format: SS: `-m 4GiB --cache_memory 2GiB`
get_role_mem() {
  local role="$1"
  grep -oP "${role}: \`-m \K\S+" "${FDB_MD}" | head -1 || echo "1GiB"
}
get_role_cache() {
  local role="$1"
  grep -oP "${role}: .*--cache_memory \K\S+" "${FDB_MD}" | sed 's/`$//' | head -1 || echo "256MiB"
}

ss_fdb_mem=$(get_role_mem "SS")
ss_cache=$(get_role_cache "SS")
log_fdb_mem=$(get_role_mem "LOG")
log_cache=$(get_role_cache "LOG")
sl_fdb_mem=$(get_role_mem "SL")
sl_cache=$(get_role_cache "SL")

# Read SS knobs from FDB Cluster Settings table
ss_hard_limit=$(grep -oP 'knob_storage_hard_limit_bytes=\K[0-9]+' "${FDB_MD}" | head -1 || echo "")
fdbcli_engine=$(grep -oP '^\| fdbcli_engine \| \K\S+' "${FDB_MD}" | head -1 || true)
if [[ -z "${fdbcli_engine}" ]]; then
  echo "ERROR: fdbcli_engine missing in ${FDB_MD} (regenerate from FDB-Config txt with engine:)" >&2
  exit 1
fi
case "${fdbcli_engine}" in
  ssd|ssd-redwood-1|ssd-rocksdb-v1) ;;
  *) echo "ERROR: unknown fdbcli_engine '${fdbcli_engine}'" >&2; exit 1 ;;
esac

parse_table() {
  local file="$1" section="$2" class="$3" fdb_mem="$4" cache_mem="$5"
  local in_section=0 in_table=0
  while IFS= read -r line; do
    if echo "$line" | grep -qP "^## ${section}"; then
      in_section=1; in_table=0; continue
    fi
    if [[ $in_section -eq 1 ]] && echo "$line" | grep -qP '^\| *-'; then
      in_table=1; continue
    fi
    if [[ $in_section -eq 1 ]] && echo "$line" | grep -qP '^\| *Container'; then
      continue
    fi
    if [[ $in_section -eq 1 && $in_table -eq 1 ]]; then
      if ! echo "$line" | grep -qP '^\|'; then
        break
      fi
      local container port cpuset cores_col mem_col mount zone machineid
      container=$(echo "$line" | awk -F'|' '{gsub(/^ *| *$/,"",$2); print $2}')
      port=$(echo "$line" | awk -F'|' '{gsub(/^ *| *$/,"",$3); print $3}')
      cpuset=$(echo "$line" | awk -F'|' '{gsub(/^ *| *$/,"",$4); print $4}')
      cores_col=$(echo "$line" | awk -F'|' '{gsub(/^ *| *$/,"",$5); print $5}')
      mem_col=$(echo "$line" | awk -F'|' '{gsub(/^ *| *$/,"",$6); print $6}')
      mount=$(echo "$line" | awk -F'|' '{gsub(/^ *| *$/,"",$7); print $7}')
      zone=$(echo "$line" | awk -F'|' '{gsub(/^ *| *$/,"",$8); print $8}')
      machineid=$(echo "$line" | awk -F'|' '{gsub(/^ *| *$/,"",$9); print $9}')

      [[ -n "$container" && -n "$port" ]] || continue

      # #6: Validate mount
      validate_mount "$mount" "$container"

      # #3: Validate cores count matches cpuset
      local cpuset_count
      cpuset_count=$(echo "$cpuset" | tr ',' '\n' | wc -l)
      if [[ "$cpuset_count" -ne "$cores_col" ]]; then
        echo "ERROR: container '${container}' has Cores=${cores_col} but CPUset '${cpuset}' has ${cpuset_count} cores" >&2
        exit 1
      fi

      # #2: Validate MEM column format and derive mem_limit (2x)
      if ! [[ "$mem_col" =~ ^[0-9]+G$ ]]; then
        echo "ERROR: container '${container}' MEM must be in G (e.g. 4G), got '${mem_col}'" >&2; exit 1
      fi
      local mem_gb
      mem_gb=$(echo "$mem_col" | grep -oP '[0-9]+')
      local container_mem_limit="$(( mem_gb * 2 ))g"

      local datadir="/data/data/${port}"
      if [[ "$class" == "stateless" ]]; then
        datadir="/data/data-stateless/${port}"
      fi

      echo "  ${container}:"
      echo "    image: rockylinux:9-minimal"
      echo "    container_name: ${container}"
      echo "    network_mode: host"
      echo "    mem_limit: ${container_mem_limit}"
      echo "    cpuset: \"${cpuset}\""
      echo "    volumes:"
      echo "      - ${mount}:/data"
      echo "      - ${FDBSERVER_PATH}:/usr/sbin/fdbserver:ro"
      local knobs=""
      if [[ "$class" == "storage" && -n "$ss_hard_limit" ]]; then
        knobs=" --knob_storage_hard_limit_bytes=${ss_hard_limit}"
      fi
      echo "    command: [\"sh\", \"-c\", \"mkdir -p /data/logs && exec /usr/sbin/fdbserver -C /data/fdb.cluster -p 127.0.0.1:${port} -d ${datadir} -L /data/logs -m ${fdb_mem} --cache_memory ${cache_mem} --class ${class} --locality_machineid ${machineid} --locality_zoneid ${zone}${knobs}\"]"
      echo "    restart: unless-stopped"
      echo ""
    fi
  done < "$file"
}

ss_count=$(grep -cP '^\| fdb-storage' "${FDB_MD}" || echo 0)
log_count=$(grep -cP '^\| fdb-log' "${FDB_MD}" || echo 0)
sl_count=$(grep -cP '^\| fdb-stateless' "${FDB_MD}" || echo 0)
total=$((ss_count + log_count + sl_count))

{
  echo "x-fdb-engine: ${fdbcli_engine}"
  echo "services:"
  parse_table "${FDB_MD}" "Storage Servers" "storage" "${ss_fdb_mem}" "${ss_cache}"
  parse_table "${FDB_MD}" "Log Servers" "log" "${log_fdb_mem}" "${log_cache}"
  parse_table "${FDB_MD}" "Stateless Servers" "stateless" "${sl_fdb_mem}" "${sl_cache}"
} > "${COMPOSE_OUT}"

generated_services=$(grep -c 'container_name:' "${COMPOSE_OUT}")
if [[ "${generated_services}" -ne "${total}" ]]; then
  echo "ERROR: expected ${total} containers but generated ${generated_services}" >&2
  exit 1
fi

echo "Generated: ${COMPOSE_OUT}"
echo "  ${ss_count} SS + ${log_count} LOG + ${sl_count} SL = ${total} containers"
echo "  Engine (fdbcli): ${fdbcli_engine}"
echo "  SS: -m ${ss_fdb_mem} --cache_memory ${ss_cache}"
echo "  LOG: -m ${log_fdb_mem} --cache_memory ${log_cache}"
echo "  SL: -m ${sl_fdb_mem} --cache_memory ${sl_cache}"
