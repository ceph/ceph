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
# Upload 1000 objects (100B–8MiB), suspend GC, random batch deletes, strict G:O verification.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/kvrgw-common.sh
source "${ROOT}/scripts/kvrgw-common.sh"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

SEED="${KVRGW_GC_DELETE_SEED:-42}"
NUM_OBJECTS="${KVRGW_GC_DELETE_COUNT:-1000}"
BUCKET="kv-gc-delete-$$"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Required command not found: $1" >&2
    exit 1
  }
}

require_cmd aws
require_cmd python3

require_admin() {
  kvrgw_refresh_live_indices
  [[ "${KVRGW_LIVE_COUNT}" -ge 1 ]] || { echo -e "${RED}FAIL:${NC} no live admin sockets" >&2; exit 1; }
  local i
  for i in "${KVRGW_LIVE_INDICES[@]}"; do
    kvrgw_admin_responds "${i}" || { echo -e "${RED}FAIL:${NC} admin not responding: instance ${i}" >&2; exit 1; }
  done
}

require_admin

export KVRGW_DATA="${DATA_ROOT}"

python3 "${ROOT}/scripts/test_gc_delete_queue.py" \
  --endpoint "${ENDPOINT}" \
  --gc-ctl "${ROOT}/scripts/gc_ctl.sh" \
  --bucket "${BUCKET}" \
  --seed "${SEED}" \
  --num-objects "${NUM_OBJECTS}"
