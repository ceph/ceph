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
FDB_ROOT="${ROOT}/third_party/fdb"
FDB_SERVER="${FDB_ROOT}/usr/sbin/fdbserver"
FDB_CLI="${FDB_ROOT}/usr/bin/fdbcli"
CLUSTER_FILE="${ROOT}/.fdb/fdb.cluster"
LOG_DIR="${ROOT}/.logs"
COMPOSE_DIR="${ROOT}/fdb-cluster"

mkdir -p "${LOG_DIR}" "$(dirname "${CLUSTER_FILE}")"
export LD_LIBRARY_PATH="${FDB_ROOT}/usr/lib64:${LD_LIBRARY_PATH:-}"

if [[ ! -x "${FDB_SERVER}" ]]; then
  echo "fdbserver not found at ${FDB_SERVER}" >&2
  exit 1
fi

if [[ ! -f "${COMPOSE_DIR}/docker-compose.yml" ]]; then
  echo "FDB cluster compose not found at ${COMPOSE_DIR}/docker-compose.yml" >&2
  exit 1
fi

MOUNTS=(/mnt/fdb0 /mnt/fdb1 /mnt/fdb2 /mnt/fdb-log0 /mnt/fdb-log1 /mnt/fdb-log2)
for m in "${MOUNTS[@]}"; do
  if ! mountpoint -q "$m" 2>/dev/null; then
    echo "ERROR: $m is not mounted. Mount NVMe drives first." >&2
    exit 1
  fi
  sudo mkdir -p "${m}/logs"
done

running=$(sudo docker ps --filter "name=fdb-storage0" --format '{{.Names}}' 2>/dev/null || true)
if [[ -z "${running}" ]]; then
  echo "Starting FDB cluster (12 containers)..."
  cd "${COMPOSE_DIR}" && sudo docker compose up -d >> "${LOG_DIR}/fdb-cluster.log" 2>&1
  sleep 5
fi

if [[ ! -f /mnt/fdb0/fdb.cluster ]]; then
  echo "ERROR: /mnt/fdb0/fdb.cluster missing — cluster not initialized" >&2
  exit 1
fi

cp /mnt/fdb0/fdb.cluster "${CLUSTER_FILE}"

if ! "${FDB_CLI}" -C "${CLUSTER_FILE}" --exec "status minimal" 2>/dev/null | grep -q "healthy\|available"; then
  echo "Initializing new FDB cluster..."
  "${FDB_CLI}" -C "${CLUSTER_FILE}" --exec "configure new single ssd" >/dev/null 2>&1 || true
  "${FDB_CLI}" -C "${CLUSTER_FILE}" --exec "configure triple" >/dev/null 2>&1 || true
  "${FDB_CLI}" -C "${CLUSTER_FILE}" --exec "coordinators auto" >/dev/null 2>&1 || true
  sleep 3
  cp /mnt/fdb0/fdb.cluster "${CLUSTER_FILE}"
fi

export FDB_CLUSTER_FILE="${CLUSTER_FILE}"
echo "FDB cluster file: ${CLUSTER_FILE}"
