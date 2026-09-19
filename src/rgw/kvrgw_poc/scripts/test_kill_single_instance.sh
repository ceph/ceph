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
# Kill one instance (no restart), refresh GW, run integration tests on degraded cluster.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/kvrgw-common.sh
source "${ROOT}/scripts/kvrgw-common.sh"

KILL_INDEX="${KVRGW_KILL_INDEX:-1}"

if [[ "${KVRGW_INSTANCES}" -lt 3 ]]; then
  echo "SKIP: test_kill_single_instance (KVRGW_INSTANCES=${KVRGW_INSTANCES}; need >=3)"
  exit 0
fi

kvrgw_refresh_live_indices
[[ "${KVRGW_LIVE_COUNT}" -eq "${KVRGW_INSTANCES}" ]] || {
  echo -e "${RED}FAIL:${NC} not all instances live before kill (live=${KVRGW_LIVE_COUNT})" >&2
  exit 1
}

echo "=== Kill instance ${KILL_INDEX} (no restart) ==="
kvrgw_kill_instance "${KILL_INDEX}"
kvrgw_refresh_live_indices
want=$((KVRGW_INSTANCES - 1))
[[ "${KVRGW_LIVE_COUNT}" -eq "${want}" ]] || {
  echo -e "${RED}FAIL:${NC} expected live=${want} after kill, got ${KVRGW_LIVE_COUNT}" >&2
  exit 1
}
kvrgw_refresh_gw_live

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

run_subtest() {
  local name="$1"
  shift
  echo ""
  echo "=== subtest: ${name} (live=${KVRGW_LIVE_COUNT}) ==="
  kvrgw_refresh_live_indices
  "$@"
}

run_subtest "gc_admin" "${ROOT}/scripts/test_gc_admin.sh"
KVRGW_GC_DELETE_COUNT=10 run_subtest "gc_delete_queue" "${ROOT}/scripts/test_gc_delete_queue.sh"
run_subtest "multi_rgw_rr" "${ROOT}/scripts/test_multi_rgw_rr.sh"
run_subtest "l_namespace_fdb" "${ROOT}/scripts/test_l_namespace_fdb.sh"
run_subtest "head_bucket" "${ROOT}/scripts/test_head_bucket.sh"
run_subtest "corrupted_etag" "${ROOT}/scripts/test_corrupted_etag.sh"
run_subtest "byte_range" "${ROOT}/scripts/test_byte_range_get.sh"
run_subtest "smoke" "${ROOT}/scripts/smoke_test.sh" --skip-reload

echo ""
echo -e "${GREEN}PASS: kill single instance — degraded tests ok (live=${KVRGW_LIVE_COUNT})${NC}"
echo "Restore full cluster: ${ROOT}/scripts/reload.sh --clean ${KVRGW_INSTANCES}"
