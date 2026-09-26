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
# Shared helpers for kv-rgw lifecycle scripts.

# Define color variables
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FDB_ROOT="${ROOT}/third_party/fdb"
export FDB_CLUSTER_FILE="${ROOT}/.fdb/fdb.cluster"
export LD_LIBRARY_PATH="${ROOT}/build:${FDB_ROOT}/usr/lib64:${LD_LIBRARY_PATH:-}"

KVRGW_HTTP_BASE_PORT="${KVRGW_HTTP_BASE_PORT:-9081}"
KVRGW_GW_PORT="${KVRGW_GW_PORT:-9080}"
DATA_ROOT="${KVRGW_DATA:-${ROOT}/data}"
ENDPOINT="http://127.0.0.1:${KVRGW_GW_PORT}"
KVRGW_S5CMD_LOG="${KVRGW_S5CMD_LOG:-${ROOT}/.logs/s5cmd.log}"

kvrgw_s5cmd_init_log() {
  mkdir -p "$(dirname "${KVRGW_S5CMD_LOG}")"
  : > "${KVRGW_S5CMD_LOG}"
}

kvrgw_s5cmd_log_stderr() {
  local label="$1" rc="$2" errfile="$3"
  local ts
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  {
    echo "[${ts}] ${label} (exit=${rc})"
    if [[ -f "${errfile}" && -s "${errfile}" ]]; then
      cat "${errfile}"
    fi
    echo
  } >> "${KVRGW_S5CMD_LOG}"
}

kvrgw_s5cmd_fail_msg() {
  local label="$1" rc="$2"
  echo -e "${RED}FAIL:${NC} s5cmd ${label} exit=${rc} see ${KVRGW_S5CMD_LOG}" >&2
  tail -20 "${KVRGW_S5CMD_LOG}" >&2 2>/dev/null || true
}

# Foreground s5cmd: stdout discarded, stderr logged.
kvrgw_s5cmd_log() {
  local label="$1"
  shift
  mkdir -p "$(dirname "${KVRGW_S5CMD_LOG}")"
  local errf rc
  errf="$(mktemp)"
  set +e
  s5cmd "$@" >/dev/null 2>"${errf}"
  rc=$?
  set -e
  kvrgw_s5cmd_log_stderr "${label}: s5cmd $*" "${rc}" "${errf}"
  rm -f "${errf}"
  if [[ "${rc}" -ne 0 ]]; then
    kvrgw_s5cmd_fail_msg "${label}" "${rc}"
  fi
  return "${rc}"
}

# Background s5cmd sync; caller sets pid=$!
kvrgw_s5cmd_bg() {
  local label="$1"
  shift
  mkdir -p "$(dirname "${KVRGW_S5CMD_LOG}")"
  local ts
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "[${ts}] ${label}: s5cmd $* (background start)" >> "${KVRGW_S5CMD_LOG}"
  s5cmd "$@" >/dev/null 2>>"${KVRGW_S5CMD_LOG}" &
}

kvrgw_s5cmd_wait() {
  local label="$1" pid="$2"
  local rc=0
  set +e
  wait "${pid}"
  rc=$?
  set -e
  local ts
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "[${ts}] ${label}: background pid=${pid} exit=${rc}" >> "${KVRGW_S5CMD_LOG}"
  if [[ "${rc}" -ne 0 ]]; then
    kvrgw_s5cmd_fail_msg "${label}" "${rc}"
  fi
  return "${rc}"
}

kvrgw_s5cmd_ls_count() {
  local label="$1" bucket="$2"
  local errf out rc
  errf="$(mktemp)"
  set +e
  out="$(s5cmd --endpoint-url "${ENDPOINT}" ls "s3://${bucket}/*" 2>"${errf}")"
  rc=$?
  set -e
  if [[ "${rc}" -ne 0 ]]; then
    kvrgw_s5cmd_log_stderr "${label}" "${rc}" "${errf}"
    rm -f "${errf}"
    kvrgw_s5cmd_fail_msg "${label}" "${rc}"
    return 1
  fi
  rm -f "${errf}"
  echo "${out}" | wc -l
}

kvrgw_aws_list_count() {
  local bucket="$1" max_count="$2"
  python3 "${ROOT}/scripts/list_verify.py" aws \
    --endpoint "${ENDPOINT}" --bucket "${bucket}" --max-count "${max_count}" 2>&1 \
    | sed -n 's/.*count=\([0-9]*\).*/\1/p'
}

# After sync exit 0: re-list once on mismatch to distinguish timing vs lost objects.
kvrgw_verify_count_after_sync() {
  local bucket="$1" target="$2" sync_label="$3"
  local count count2
  count="$(kvrgw_aws_list_count "${bucket}" "${target}")"
  if [[ -z "${count}" ]]; then
    echo -e "${RED}FAIL:${NC} could not parse list count after ${sync_label}" >&2
    return 1
  fi
  if [[ "${count}" -eq "${target}" ]]; then
    echo "${count}"
    return 0
  fi
  echo "WARN: list count ${count} != ${target} after ${sync_label}; re-list in 2s" >&2
  sleep 2
  count2="$(kvrgw_aws_list_count "${bucket}" "${target}")"
  if [[ "${count2}" -eq "${target}" ]]; then
    echo -e "${RED}FAIL:${NC} test timing: list before visible (${count} then ${count2}); should not happen on POC" >&2
    return 1
  fi
  echo -e "${RED}FAIL:${NC} lost objects suspected: ${sync_label} exit 0 but list ${count} then ${count2} != ${target}; see ${KVRGW_S5CMD_LOG}" >&2
  return 1
}

KVRGW_TENANT_NAME="${KVRGW_TENANT_NAME:-kv-poc}"
S3CMD_CFG="${ROOT}/s3cmd.cfg"
FDB_CLI="${FDB_ROOT}/usr/bin/fdbcli"
RUN_DIR="${ROOT}/.run"
KVRGW_INSTANCES_FILE="${RUN_DIR}/instances"
NGINX_DIR="${RUN_DIR}/nginx"

kvrgw_read_registered_instances() {
  if [[ -f "${KVRGW_INSTANCES_FILE}" ]]; then
    cat "${KVRGW_INSTANCES_FILE}"
  else
    echo 1
  fi
}

kvrgw_register_instances() {
  local n="$1"
  mkdir -p "${RUN_DIR}"
  echo "${n}" > "${KVRGW_INSTANCES_FILE}"
  export KVRGW_INSTANCES="${n}"
}

# Registered .run/instances wins (reload/start write it); else env; else 1.
if [[ -f "${KVRGW_INSTANCES_FILE}" ]]; then
  KVRGW_INSTANCES="$(kvrgw_read_registered_instances)"
elif [[ -v KVRGW_INSTANCES ]]; then
  :
else
  KVRGW_INSTANCES=1
fi
export KVRGW_INSTANCES

ADMIN_SOCKET=""
SOCKET=""
HTTP_ADDR=""
HTTP_HOST=""

BACKEND_PID=""
FRONTEND_PID=""
RGW_ID=""
declare -a RGW_IDS=()
FDB_STATUS="unhealthy"
BACKEND_STATUS="down"
FRONTEND_STATUS="down"
GW_STATUS="down"
S3_STATUS="skipped"
S3_DETAIL=""
FAST_TEST_STATUS="${BLUE}skipped${NC}"

kvrgw_instance_socket() { echo "/tmp/kvrgw-${1}.sock"; }
kvrgw_instance_admin_socket() { echo "/tmp/kvrgw-admin-${1}.sock"; }
kvrgw_instance_http_port() { echo $((KVRGW_HTTP_BASE_PORT + $1)); }
kvrgw_instance_http_addr() { echo ":$(kvrgw_instance_http_port "$1")"; }

kvrgw_sync_legacy_vars() {
  SOCKET="$(kvrgw_instance_socket 0)"
  ADMIN_SOCKET="$(kvrgw_instance_admin_socket 0)"
  HTTP_ADDR="$(kvrgw_instance_http_addr 0)"
  HTTP_HOST="${HTTP_ADDR#:}"
}

kvrgw_backend_pid() {
  local i="$1"
  [[ -f "${RUN_DIR}/backend-${i}.pid" ]] && cat "${RUN_DIR}/backend-${i}.pid" || true
}

kvrgw_frontend_pid() {
  local i="$1"
  [[ -f "${RUN_DIR}/frontend-${i}.pid" ]] && cat "${RUN_DIR}/frontend-${i}.pid" || true
}

kvrgw_instance_process_alive() {
  local i="$1" fp
  fp="$(kvrgw_frontend_pid "${i}")"
  [[ -n "${fp}" ]] && kill -0 "${fp}" 2>/dev/null
}

kvrgw_admin_responds() {
  local i="$1" admin gc
  admin="$(kvrgw_instance_admin_socket "${i}")"
  gc="${ROOT}/build/gc_ctl"
  [[ -x "${gc}" && -S "${admin}" ]] || return 1
  timeout 2 env KVRGW_DATA="${DATA_ROOT}" KVRGW_ADMIN_SOCKET="${admin}" \
    "${gc}" query-active-age >/dev/null 2>&1
}

kvrgw_instance_is_live() {
  local i="$1"
  kvrgw_instance_process_alive "${i}" && kvrgw_admin_responds "${i}"
}

declare -a KVRGW_LIVE_INDICES=()
KVRGW_LIVE_COUNT=0

kvrgw_refresh_live_indices() {
  KVRGW_LIVE_INDICES=()
  local i
  for ((i = 0; i < KVRGW_INSTANCES; ++i)); do
    if kvrgw_instance_is_live "${i}"; then
      KVRGW_LIVE_INDICES+=("${i}")
    fi
  done
  KVRGW_LIVE_COUNT="${#KVRGW_LIVE_INDICES[@]}"
  export KVRGW_LIVE_COUNT
}

kvrgw_live_indices_csv() {
  kvrgw_refresh_live_indices
  echo "${KVRGW_LIVE_INDICES[*]}"
}

kvrgw_kill_instance() {
  local i="$1" bp fp
  bp="$(kvrgw_backend_pid "${i}")"
  fp="$(kvrgw_frontend_pid "${i}")"
  [[ -n "${bp}" ]] && kill -9 "${bp}" 2>/dev/null || true
  [[ -n "${fp}" ]] && kill -9 "${fp}" 2>/dev/null || true
  rm -f "${RUN_DIR}/backend-${i}.pid" "${RUN_DIR}/frontend-${i}.pid"
  rm -f "$(kvrgw_instance_socket "${i}")" "$(kvrgw_instance_admin_socket "${i}")"
  sleep 0.5
}

kvrgw_wait_instance_live() {
  local i="$1" tries="${2:-60}"
  local n=0
  while [[ "${n}" -lt "${tries}" ]]; do
    if kvrgw_instance_is_live "${i}"; then
      return 0
    fi
    sleep 0.5
    n=$((n + 1))
  done
  echo -e "${RED}FAIL:${NC} instance ${i} not live after ${tries} polls" >&2
  return 1
}

kvrgw_wait_live_count() {
  local want="$1" tries="${2:-60}"
  local n=0
  while [[ "${n}" -lt "${tries}" ]]; do
    kvrgw_refresh_live_indices
    if [[ "${KVRGW_LIVE_COUNT}" -eq "${want}" ]]; then
      return 0
    fi
    sleep 0.5
    n=$((n + 1))
  done
  echo -e "${RED}FAIL:${NC} live count ${KVRGW_LIVE_COUNT} != ${want} after ${tries} polls" >&2
  return 1
}

kvrgw_restart_instance() {
  local i="$1"
  echo "Restarting instance ${i} (SIGKILL)..."
  kvrgw_kill_instance "${i}"
  kvrgw_refresh_live_indices
  kvrgw_refresh_gw_live || true
  kvrgw_start_instance "${i}" || return 1
  kvrgw_wait_instance_live "${i}" || return 1
  kvrgw_refresh_live_indices
  kvrgw_refresh_gw_live || return 1
  echo "Instance ${i} restarted (live count=${KVRGW_LIVE_COUNT})"
}

kvrgw_ensure_dirs() {
  mkdir -p "${DATA_ROOT}" "${ROOT}/.logs" "${ROOT}/frontend/pb" "${RUN_DIR}" "${NGINX_DIR}" "${NGINX_DIR}/tmp"
}

kvrgw_stop_gw() {
  if [[ -f "${NGINX_DIR}/nginx.pid" ]]; then
    nginx -p "${NGINX_DIR}" -c "${NGINX_DIR}/nginx.conf" -s stop 2>/dev/null || true
    kill "$(cat "${NGINX_DIR}/nginx.pid")" 2>/dev/null || true
    rm -f "${NGINX_DIR}/nginx.pid"
  fi
  GW_STATUS="down"
}

kvrgw_stop_servers() {
  local n="${1:-${KVRGW_INSTANCES}}"
  echo "Stopping kv-rgw (${n} instance(s))..."
  kvrgw_stop_gw
  local i
  for ((i = 0; i < n; ++i)); do
    [[ -f "${RUN_DIR}/backend-${i}.pid" ]] && kill "$(cat "${RUN_DIR}/backend-${i}.pid")" 2>/dev/null || true
    [[ -f "${RUN_DIR}/frontend-${i}.pid" ]] && kill "$(cat "${RUN_DIR}/frontend-${i}.pid")" 2>/dev/null || true
    rm -f "${RUN_DIR}/backend-${i}.pid" "${RUN_DIR}/frontend-${i}.pid"
    rm -f "$(kvrgw_instance_socket "${i}")" "$(kvrgw_instance_admin_socket "${i}")"
  done
  rm -f "${RUN_DIR}/backend.pid" "${RUN_DIR}/frontend.pid" /tmp/kvrgw.sock /tmp/kvrgw-admin.sock
  pkill -f "${ROOT}/build/kv-rgw-backend" 2>/dev/null || true
  pkill -f "${ROOT}/build/kv-rgw-frontend" 2>/dev/null || true
  sleep 2
  kvrgw_sync_legacy_vars
}

kvrgw_build() {
  echo "Building backend and frontend..."
  cmake -S "${ROOT}/backend" -B "${ROOT}/build"
  cmake --build "${ROOT}/build" -j"$(nproc)"
  if [[ "${PERF:-0}" -eq 1 ]]; then
    echo "Perf mode: skipping backend unit tests"
  else
    echo "Running backend unit tests..."
    "${ROOT}/build/object_value_test"
    "${ROOT}/build/l_key_test"
    if [[ -f "${FDB_CLUSTER_FILE}" ]]; then
      "${ROOT}/build/kv_range_test"
    else
      echo "Skipping kv_range_test (no FDB cluster file)"
    fi
  fi
  export PATH="$(go env GOPATH)/bin:${PATH}"
  protoc -I "${ROOT}/proto" \
    --go_out="${ROOT}/frontend/pb" --go_opt=paths=source_relative \
    "${ROOT}/proto/kvrgw.proto"
  cd "${ROOT}/frontend" && go mod tidy && go build -o "${ROOT}/build/kv-rgw-frontend" .
}

# Host path bind-mounted onto /usr/sbin/fdbserver in docker-compose.yml
kvrgw_fdb_compose_server() {
  local compose_file="${1:-${ROOT}/fdb-cluster/docker-compose.yml}"
  grep -oP '^\s+- \K[^:]+:/usr/sbin/fdbserver' "${compose_file}" 2>/dev/null | head -1 | cut -d: -f1
}

kvrgw_fdb_dump_failure() {
  local compose_dir="${ROOT}/fdb-cluster"
  local compose_file="${compose_dir}/docker-compose.yml"
  local host_srv expected="${FDB_ROOT}/usr/sbin/fdbserver"
  host_srv="$(kvrgw_fdb_compose_server "${compose_file}")"
  echo "ERROR: FDB cluster is not usable." >&2
  echo "  compose: ${compose_file}" >&2
  echo "  fdbserver bind-mount (host): ${host_srv:-<missing from compose>}" >&2
  echo "  expected executable: ${expected}" >&2
  if [[ -n "${host_srv}" ]]; then
    ls -ld "${host_srv}" >&2 || echo "  (bind-mount path does not exist)" >&2
  fi
  if [[ -d "${host_srv:-}" ]]; then
    echo "  Docker created a directory at the bind-mount because the binary was missing." >&2
    echo "  Containers then exec that directory and crash (exit 126: Is a directory)." >&2
  fi
  echo "  docker compose ps:" >&2
  (cd "${compose_dir}" && sudo docker compose ps) >&2 || true
  echo "  fdb-storage00 logs (last 30):" >&2
  sudo docker logs --tail 30 fdb-storage00 >&2 || true
  echo "  regenerate compose bind-mounts: bash scripts/gen_docker_compose.sh ${ROOT}/FDB-Config9.md" >&2
}

kvrgw_fdb_check_server_mount() {
  local compose_file="${ROOT}/fdb-cluster/docker-compose.yml"
  local expected="${FDB_ROOT}/usr/sbin/fdbserver"
  local host_srv
  if [[ ! -f "${compose_file}" ]]; then
    echo "ERROR: ${compose_file} missing (run gen_docker_compose.sh)" >&2
    return 1
  fi
  host_srv="$(kvrgw_fdb_compose_server "${compose_file}")"
  if [[ -z "${host_srv}" ]]; then
    echo "ERROR: no fdbserver bind-mount in ${compose_file}" >&2
    kvrgw_fdb_dump_failure
    return 1
  fi
  if [[ ! -x "${expected}" ]]; then
    echo "ERROR: missing ${expected} (run bash scripts/fetch_fdb.sh)" >&2
    return 1
  fi
  if [[ -d "${host_srv}" ]]; then
    echo "ERROR: compose mounts ${host_srv} which is a directory, not fdbserver." >&2
    kvrgw_fdb_dump_failure
    return 1
  fi
  if [[ ! -x "${host_srv}" ]]; then
    echo "ERROR: fdbserver bind-mount is not an executable file: ${host_srv}" >&2
    kvrgw_fdb_dump_failure
    return 1
  fi
  if [[ "${host_srv}" != "${expected}" ]]; then
    echo "ERROR: compose fdbserver path (${host_srv}) is not this tree (${expected})." >&2
    echo "Stale docker-compose.yml from another checkout will crash-loop fdbserver." >&2
    kvrgw_fdb_dump_failure
    return 1
  fi
  return 0
}

kvrgw_fdb_wait_containers() {
  local i status restarting
  for i in $(seq 1 20); do
    status="$(sudo docker inspect -f '{{.State.Status}}' fdb-storage00 2>/dev/null || echo missing)"
    restarting="$(sudo docker inspect -f '{{.State.Restarting}}' fdb-storage00 2>/dev/null || echo true)"
    if [[ "${status}" == "running" && "${restarting}" != "true" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "ERROR: fdb-storage00 not running (status=${status:-unknown} restarting=${restarting:-unknown})" >&2
  kvrgw_fdb_dump_failure
  return 1
}

kvrgw_fdbcli() {
  timeout "${FDB_CLI_TIMEOUT:-30}" "${FDB_CLI}" -C "${FDB_CLUSTER_FILE}" --exec "$1"
}

kvrgw_start_fdb() {
  echo "Starting FoundationDB (if needed)..."
  kvrgw_fdb_check_server_mount || return 1
  "${ROOT}/scripts/start_fdb.sh" >/dev/null
  echo "Waiting for FDB..."
  local _
  for _ in $(seq 1 30); do
    if kvrgw_fdbcli "status" 2>/dev/null | grep -q "Replication health     - Healthy"; then
      FDB_STATUS="healthy"; return 0
    fi
    sleep 1
  done
  FDB_STATUS="unhealthy"
  echo "ERROR: FDB not healthy after start_fdb.sh" >&2
  kvrgw_fdb_dump_failure
  return 1
}

kvrgw_clean_fdb() {
  local compose_dir="${ROOT}/fdb-cluster"
  local compose_file="${compose_dir}/docker-compose.yml"
  local fdbcli_engine
  fdbcli_engine=$(grep -oP '^x-fdb-engine: \K\S+' "${compose_file}" 2>/dev/null | head -1 || true)
  if [[ -z "${fdbcli_engine}" ]]; then
    echo "ERROR: x-fdb-engine missing in ${compose_file} (run gen_docker_compose.sh from FDB-Config md)" >&2
    return 1
  fi
  case "${fdbcli_engine}" in
    ssd|ssd-redwood-1|ssd-rocksdb-v1) ;;
    *) echo "ERROR: unknown x-fdb-engine '${fdbcli_engine}'" >&2; return 1 ;;
  esac
  local mounts=(/mnt/fdb0 /mnt/fdb1 /mnt/fdb2 /mnt/fdb-log0 /mnt/fdb-log1 /mnt/fdb-log2)
  echo "Stopping FDB containers..."
  (cd "${compose_dir}" && sudo docker compose down) >/dev/null 2>&1 || true
  kvrgw_fdb_check_server_mount || return 1
  echo "Wiping FDB data..."
  for m in "${mounts[@]}"; do
    sudo rm -rf "${m}/data" "${m}/logs" "${m}/fdb.cluster" 2>/dev/null || true
    sudo mkdir -p "${m}/logs"
  done
  echo "Seeding fresh cluster file..."
  local seed_id
  seed_id="$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | LC_ALL=C tr -dc 'a-zA-Z0-9' | head -c 32)"
  for m in "${mounts[@]}"; do
    echo "docker:${seed_id}@127.0.0.1:4500" | sudo tee "${m}/fdb.cluster" >/dev/null
  done
  echo "Restarting FDB containers..."
  mkdir -p "${ROOT}/.logs"
  if ! (cd "${compose_dir}" && sudo docker compose up -d) >"${ROOT}/.logs/fdb-cluster.log" 2>&1; then
    echo "ERROR: docker compose up failed (see ${ROOT}/.logs/fdb-cluster.log)" >&2
    cat "${ROOT}/.logs/fdb-cluster.log" >&2 || true
    kvrgw_fdb_dump_failure
    return 1
  fi
  kvrgw_fdb_wait_containers || return 1
  local cluster_file="/mnt/fdb0/fdb.cluster"
  local attempts=30
  while [[ ! -f "${cluster_file}" && $attempts -gt 0 ]]; do
    sleep 1; ((attempts--))
  done
  [[ -f "${cluster_file}" ]] || { echo "ERROR: ${cluster_file} not created after restart" >&2; kvrgw_fdb_dump_failure; return 1; }
  mkdir -p "$(dirname "${FDB_CLUSTER_FILE}")"
  cp "${cluster_file}" "${FDB_CLUSTER_FILE}"
  echo "Initializing fresh FDB cluster (engine=${fdbcli_engine})..."
  if ! kvrgw_fdbcli "configure new single ${fdbcli_engine}"; then
    echo "ERROR: configure new single ${fdbcli_engine} failed (coordinator not reachable within ${FDB_CLI_TIMEOUT:-30}s)" >&2
    kvrgw_fdb_dump_failure
    return 1
  fi
  kvrgw_fdbcli "configure triple" >/dev/null 2>&1 || true
  kvrgw_fdbcli "coordinators auto" >/dev/null 2>&1 || true
  sleep 3
  cp /mnt/fdb0/fdb.cluster "${FDB_CLUSTER_FILE}"
  echo "Waiting for FDB healthy (up to 60s)..."
  local i
  for i in $(seq 1 60); do
    if kvrgw_fdbcli "status" 2>/dev/null | grep -q "Replication health     - Healthy"; then
      echo "FDB healthy after ${i}s."
      return 0
    fi
    sleep 1
  done
  echo "ERROR: FDB not healthy after 60s" >&2
  kvrgw_fdb_dump_failure
  return 1
}

kvrgw_clean_data() {
  [[ "${FDB_STATUS}" == "healthy" ]] || { echo "Cannot clean: FDB unhealthy" >&2; return 1; }
  echo "Cleaning kv-rgw state (FDB + ${DATA_ROOT})..."
  "${FDB_CLI}" -C "${FDB_CLUSTER_FILE}" --exec "writemode on; clearrange \x00 \xFF" >/dev/null
  rm -rf "${DATA_ROOT}" && mkdir -p "${DATA_ROOT}"
}

kvrgw_read_rgw_id() {
  local idx="$1"
  local from_line="${2:-1}"
  local log="${ROOT}/.logs/frontend-${idx}.log"
  local id=""
  for _ in $(seq 1 20); do
    if [[ -f "${log}" ]]; then
      id="$(tail -n +"${from_line}" "${log}" 2>/dev/null | grep -oE 'rgw_id=[0-9]+' | tail -1 | cut -d= -f2 || true)"
      if [[ -n "${id}" ]]; then
        RGW_IDS[idx]="${id}"
        [[ "${idx}" -eq 0 ]] && RGW_ID="${id}"
        return 0
      fi
    fi
    sleep 0.1
  done
  return 1
}

kvrgw_start_instance() {
  local i="$1" sock admin http_addr http_host frontend_log frontend_log_from=1
  sock="$(kvrgw_instance_socket "${i}")"
  admin="$(kvrgw_instance_admin_socket "${i}")"
  http_addr="$(kvrgw_instance_http_addr "${i}")"
  http_host="${http_addr#:}"
  frontend_log="${ROOT}/.logs/frontend-${i}.log"
  rm -f "${sock}" "${admin}"
  [[ -f "${frontend_log}" ]] && frontend_log_from=$(( $(wc -l < "${frontend_log}") + 1 ))
  echo "Starting frontend instance ${i} (${http_addr})..."
  KVRGW_GOMAXPROCS="${KVRGW_GOMAXPROCS:-8}" \
    KVRGW_IAM_DIR="${KVRGW_IAM_DIR:-${ROOT}/iam}" \
    KVRGW_DATA="${DATA_ROOT}" \
    KVRGW_MAX_INLINE="${KVRGW_MAX_INLINE:-256}" \
    KVRGW_MAX_KV_STORE="${KVRGW_MAX_KV_STORE:-4096}" \
    KVRGW_KV_STORE_COALESCING="${KVRGW_KV_STORE_COALESCING:-0}" \
    KVRGW_ADMIN_SOCKET="${admin}" \
    "${ROOT}/build/kv-rgw-frontend" -addr "${http_addr}" >> "${frontend_log}" 2>&1 &
  local frontend_pid=$! ok=0
  for _ in $(seq 1 60); do
    if ! kill -0 "${frontend_pid}" 2>/dev/null; then
      break
    fi
    if [[ -S "${admin}" ]] && ss -tlnp 2>/dev/null | grep -q ":${http_host} "; then
      ok=1
      break
    fi
    sleep 0.25
  done
  [[ "${ok}" -eq 1 ]] || {
    tail -40 "${frontend_log}" >&2
    kill "${frontend_pid}" 2>/dev/null || true
    return 1
  }
  kvrgw_read_rgw_id "${i}" "${frontend_log_from}" && echo "Instance ${i} rgw_id=${RGW_IDS[$i]:-?}"
  echo "${frontend_pid}" > "${RUN_DIR}/frontend-${i}.pid"
  if [[ "${i}" -eq 0 ]]; then
    BACKEND_PID=""
    FRONTEND_PID="${frontend_pid}"
    BACKEND_STATUS="in-process"
    FRONTEND_STATUS="up"
  fi
}

kvrgw_write_nginx_conf() {
  local conf="${NGINX_DIR}/nginx.conf" i port
  local -a indices=()
  if [[ $# -eq 1 && "${1}" =~ ^[0-9]+$ ]]; then
    for ((i = 0; i < $1; ++i)); do indices+=("${i}"); done
  else
    indices=("$@")
  fi
  [[ ${#indices[@]} -ge 1 ]] || { echo -e "${RED}FAIL:${NC} nginx conf needs >=1 upstream" >&2; return 1; }
  {
    echo "pid ${NGINX_DIR}/nginx.pid;"
    echo "error_log ${ROOT}/.logs/nginx-error.log;"
    echo "events { worker_connections 1024; }"
    echo "http {"
    echo "  client_body_temp_path ${NGINX_DIR}/tmp;"
    echo "  proxy_temp_path ${NGINX_DIR}/tmp;"
    echo "  fastcgi_temp_path ${NGINX_DIR}/tmp;"
    echo "  uwsgi_temp_path ${NGINX_DIR}/tmp;"
    echo "  scgi_temp_path ${NGINX_DIR}/tmp;"
    echo "  access_log ${ROOT}/.logs/nginx-access.log;"
    echo "  upstream kvrgw_frontends {"
    for i in "${indices[@]}"; do
      port="$(kvrgw_instance_http_port "${i}")"
      echo "    server 127.0.0.1:${port};"
    done
    echo "  }"
    echo "  server {"
    echo "    listen ${KVRGW_GW_PORT};"
    echo "    location / {"
    echo "      proxy_pass http://kvrgw_frontends;"
    echo "      proxy_http_version 1.1;"
    echo "      proxy_set_header Host \$http_host;"
    echo "      proxy_set_header Connection \"\";"
    echo "      proxy_request_buffering off;"
    echo "      proxy_connect_timeout 5s;"
    echo "      proxy_read_timeout 300s;"
    echo "      client_max_body_size 0;"
    echo "    }"
    echo "  }"
    echo "}"
  } > "${conf}"
}

kvrgw_refresh_gw_live() {
  kvrgw_refresh_live_indices
  [[ "${KVRGW_LIVE_COUNT}" -ge 1 ]] || { echo -e "${RED}FAIL:${NC} no live frontends for GW" >&2; return 1; }
  command -v nginx >/dev/null || { echo "nginx not found" >&2; return 1; }
  kvrgw_write_nginx_conf "${KVRGW_LIVE_INDICES[@]}"
  if [[ -f "${NGINX_DIR}/nginx.pid" ]] && kill -0 "$(cat "${NGINX_DIR}/nginx.pid")" 2>/dev/null; then
    nginx -p "${NGINX_DIR}" -c "${NGINX_DIR}/nginx.conf" -s reload
  else
    ss -tlnp 2>/dev/null | grep -q ":${KVRGW_GW_PORT} " && { echo "GW port in use" >&2; return 1; }
    nginx -p "${NGINX_DIR}" -c "${NGINX_DIR}/nginx.conf"
  fi
  sleep 0.3
  GW_STATUS="up"
  echo "GW refreshed (${KVRGW_LIVE_COUNT} live upstream(s): ${KVRGW_LIVE_INDICES[*]})"
}

kvrgw_start_gw() {
  local n="${1:-${KVRGW_INSTANCES}}"
  command -v nginx >/dev/null || { echo "nginx not found; sudo dnf install nginx" >&2; return 1; }
  kvrgw_stop_gw
  kvrgw_write_nginx_conf "${n}"
  ss -tlnp 2>/dev/null | grep -q ":${KVRGW_GW_PORT} " && { echo "GW port in use" >&2; return 1; }
  nginx -p "${NGINX_DIR}" -c "${NGINX_DIR}/nginx.conf"
  sleep 0.5
  ss -tlnp 2>/dev/null | grep -q ":${KVRGW_GW_PORT} " || { tail -10 "${ROOT}/.logs/nginx-error.log" >&2; return 1; }
  GW_STATUS="up"
  echo "GW listening on ${ENDPOINT} (${n} upstream(s))"
}

kvrgw_startup_suspend_random() {
  local n="${1:-${KVRGW_INSTANCES}}"
  [[ "${n}" -gt 1 ]] || return 0

  local gc_ctl="${ROOT}/build/gc_ctl"
  if [[ ! -x "${gc_ctl}" ]]; then
    echo "Skipping startup suspend: gc_ctl not built" >&2
    return 0
  fi

  local pick=$((RANDOM % n))
  local admin
  admin="$(kvrgw_instance_admin_socket "${pick}")"
  echo "Startup GC suspend on instance ${pick} (${admin})..."
  KVRGW_DATA="${DATA_ROOT}" KVRGW_ADMIN_SOCKET="${admin}" \
    "${gc_ctl}" set-gc-config suspended=1 interval_sec=3600 max_objects_per_sec=0 max_mb_per_sec=0
}

kvrgw_start_servers() {
  local n="${1:-${KVRGW_INSTANCES}}" i
  kvrgw_register_instances "${n}"
  RGW_IDS=(); BACKEND_STATUS="down"; FRONTEND_STATUS="down"
  for ((i = 0; i < n; ++i)); do kvrgw_start_instance "${i}" || return 1; done
  kvrgw_startup_suspend_random "${n}"
  kvrgw_start_gw "${n}" || return 1
  kvrgw_sync_legacy_vars
  export RGW_ID="${RGW_IDS[0]:-}"
  RGW_IDS_CSV="$(IFS=,; echo "${RGW_IDS[*]}")"
  export RGW_IDS_CSV
}

kvrgw_add_tenant() {
  local name="${1:-${KVRGW_TENANT_NAME}}" code
  code="$(curl -s -o /dev/null -w '%{http_code}' -X PUT "${ENDPOINT}/_admin/tenant/${name}")"
  [[ "${code}" == "200" || "${code}" == "409" ]] || { echo "AddTenant HTTP ${code}" >&2; return 1; }
  echo "Tenant ${name} ready (HTTP ${code})."
}

kvrgw_test_list_buckets() {
  local prefix="list-test-$$" i tmp; tmp="$(mktemp -d)"
  export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}" AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}" AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
  for i in 1 2 3 4; do
    s3cmd -c "${S3CMD_CFG}" mb "s3://${prefix}-bucket${i}" >/dev/null
  done
  python3 - "${ENDPOINT}" "${prefix}" <<'PY'
import json, subprocess, sys
from datetime import datetime
endpoint, prefix = sys.argv[1:3]
expected = {f"{prefix}-bucket{i}" for i in range(1, 5)}
data = json.loads(subprocess.check_output(["aws","--endpoint-url",endpoint,"s3api","list-buckets","--output","json"], text=True))
names = {b["Name"] for b in data.get("Buckets", [])}
assert not (expected - names)
for b in data["Buckets"]:
    if b["Name"] in expected:
        dt = datetime.fromisoformat(b["CreationDate"].replace("Z","+00:00"))
        assert 2020 <= dt.year <= 2100
print("list-buckets CreationDate ok")
PY
  s3cmd -c "${S3CMD_CFG}" ls > "${tmp}/ls.txt"
  for i in 1 2 3 4; do grep -q "${prefix}-bucket${i}" "${tmp}/ls.txt"; done
  for i in 1 2 3 4; do aws --endpoint-url "${ENDPOINT}" s3 rb "s3://${prefix}-bucket${i}" >/dev/null 2>&1 || true; done
  rm -rf "${tmp}"
}

kvrgw_verify() {
  local verify_bucket="reload-check-$$" verify_tmp; verify_tmp="$(mktemp -d)"
  echo probe > "${verify_tmp}/probe.txt"
  export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
  if s3cmd -c "${S3CMD_CFG}" mb "s3://${verify_bucket}" >/dev/null 2>&1 \
     && s3cmd -c "${S3CMD_CFG}" put "${verify_tmp}/probe.txt" "s3://${verify_bucket}/probe" >/dev/null 2>&1 \
     && s3cmd -c "${S3CMD_CFG}" get "s3://${verify_bucket}/probe" "${verify_tmp}/out.txt" >/dev/null 2>&1 \
     && diff -q "${verify_tmp}/probe.txt" "${verify_tmp}/out.txt" >/dev/null \
     && s3cmd -c "${S3CMD_CFG}" del "s3://${verify_bucket}/probe" >/dev/null 2>&1 \
     && s3cmd -c "${S3CMD_CFG}" rb "s3://${verify_bucket}" >/dev/null 2>&1 \
     && kvrgw_test_list_buckets; then
    S3_STATUS="ok"; S3_DETAIL="s3cmd via GW ok"
  else
    S3_DETAIL="s3cmd check failed"
  fi
  rm -rf "${verify_tmp}"
  [[ "${S3_STATUS}" == "ok" ]]
}

kvrgw_run_fast_tests() {
  export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
  export KVRGW_INSTANCES RGW_ID RGW_IDS RGW_IDS_CSV
  "${ROOT}/scripts/test_head_bucket.sh"
  "${ROOT}/scripts/test_corrupted_etag.sh"
  "${ROOT}/scripts/test_l_namespace_fdb.sh"
  "${ROOT}/scripts/test_gc_admin.sh"
  if [[ "${KVRGW_INSTANCES}" -gt 1 ]]; then
    "${ROOT}/scripts/test_multi_rgw_rr.sh"
  fi
  FAST_TEST_STATUS="${GREEN}ok${NC}"
}

kvrgw_print_report() {
  local n="${KVRGW_INSTANCES}" i fp rid
  echo ""; echo "KV-RGW reload report"; echo "===================="

  # Colorize FDB status into a NEW variable
  if [[ "${FDB_STATUS}" == "healthy" ]]; then
      FDB_STATUS_COLORED="${GREEN}${FDB_STATUS}${NC}"
  else
      FDB_STATUS_COLORED="${RED}${FDB_STATUS}${NC}"
  fi

  if [[ "${GW_STATUS}" == "up" ]]; then
      GW_STATUS_COLORED="${GREEN}${GW_STATUS}${NC}"
  else
      GW_STATUS_COLORED="${RED}${GW_STATUS}${NC}"
  fi

  if [[ "${S3_STATUS}" == "ok" ]]; then
      S3_STATUS_COLORED="${GREEN}${S3_STATUS}${NC}"
  elif [[ "${S3_STATUS}" == "skipped" ]]; then
      S3_STATUS_COLORED="${BLUE}${S3_STATUS}${NC}"
  else
      S3_STATUS_COLORED="${RED}${S3_STATUS}${NC}"
  fi

  # Print using the colored display variables
  echo -e "FDB: ${FDB_STATUS_COLORED}  GW: ${GW_STATUS_COLORED} (${ENDPOINT})  Instances: ${n}"
  #echo -e "FDB: ${FDB_STATUS}  GW: ${GW_STATUS} (${ENDPOINT})  Instances: ${n}"
  for ((i = 0; i < n; ++i)); do
    fp="$(cat "${RUN_DIR}/frontend-${i}.pid" 2>/dev/null || echo '?')"
    rid="${RGW_IDS[${i}]:-?}"
    echo "  [${i}] frontend pid=${fp} rgw_id=${rid} :$(kvrgw_instance_http_port "${i}")"
  done
  echo -e "S3: ${S3_STATUS_COLORED} (${S3_DETAIL})  Fast tests: ${FAST_TEST_STATUS}"
}

kvrgw_sync_legacy_vars
