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
# Run the test plan from TEST_PLAN.md.
# Every phase is executed; every failure is recorded and reported at the end.
# Exit 0 only if ALL phases pass.
#
# Usage:
#   run_test_plan.sh           Full plan (same as --slow)
#   run_test_plan.sh --fast    Build, unit, reload (+ fast integration tests)
#   run_test_plan.sh --quick   --fast + smoke
#   run_test_plan.sh --medium  --fast + smoke + byte-range + list boundary
#   run_test_plan.sh --slow    --medium + 64K/1K stress + gc delete queue
#   run_test_plan.sh --chaos   --slow + kill-instance + power-cycle easy/hard (N=3, ~2h)

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/kvrgw-common.sh
source "${ROOT}/scripts/kvrgw-common.sh"

TIER=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --fast|--quick|--medium|--slow|--chaos)
      if [[ -n "${TIER}" ]]; then
        echo "Specify only one tier flag: --fast, --quick, --medium, --slow, or --chaos" >&2
        exit 2
      fi
      TIER="${1#--}"
      shift
      ;;
    -h|--help)
      cat <<EOF
Usage: $(basename "$0") [TIER]

Tiers (each includes all prior tiers):
  --fast     build, unit, reload (+ fast integration tests)     ~1 min
  --quick    --fast + smoke                                     ~2 min
  --medium   --fast + smoke + byte-range + list boundary        ~4 min
  --slow     --medium + 64K/1K stress + gc delete queue        ~45 min
  --chaos    --slow + kill-instance + power-cycle (N=3)         ~1 hour
  (default)  same as --slow

EOF
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
  esac
done
[[ -z "${TIER}" ]] && TIER="slow"

kvrgw_s5cmd_init_log
export KVRGW_S5CMD_LOG
export KVRGW_S5CMD_LOG_APPEND=1

tier_at_least() {
  case "${TIER}" in
    fast)   [[ "$1" == "fast" ]] ;;
    quick)  [[ "$1" == "fast" || "$1" == "quick" ]] ;;
    medium) [[ "$1" == "fast" || "$1" == "quick" || "$1" == "medium" ]] ;;
    slow)   true ;;
    chaos)  true ;;
    *)      false ;;
  esac
}

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

declare -a PHASE_NAMES=()
declare -a PHASE_STATUS=()
declare -a PHASE_DETAIL=()

PLAN_ORDER=()
PLAN_ORDER+=("0 build" "1 unit tests")
if tier_at_least fast; then
  PLAN_ORDER+=("2 reload (clean)")
fi
if tier_at_least quick; then
  PLAN_ORDER+=("3 smoke")
fi
if tier_at_least medium; then
  PLAN_ORDER+=("4 byte range GET (4MiB)" "5 list boundary (1024)" "5b batch tiers (s5cmd)")
fi
if tier_at_least slow; then
  PLAN_ORDER+=("6 pagination (64K)" "7 list stress (64K upload)" "8 list buckets stress (1K buckets)" "9 gc delete queue")
fi
if [[ "${TIER}" == "chaos" ]]; then
  PLAN_ORDER+=("10 kill instance" "10b reload restore (3)" "11 power-cycle easy" "12 power-cycle hard")
fi
PHASE_TOTAL="${#PLAN_ORDER[@]}"

report_phase_progress() {
  local name="$1" status="$2"
  local done="${#PHASE_NAMES[@]}"
  local next="(none — run complete)"
  if [[ "${done}" -lt "${PHASE_TOTAL}" ]]; then
    next="${PLAN_ORDER[${done}]}"
  fi
  echo "Finished: ${name} — ${status}"
  echo "Next: ${next}"
  echo "Progress: ${done}/${PHASE_TOTAL}"
  FAIL_COUNT=$(printf '%s\n' "${PHASE_STATUS[@]}" | grep -c '^FAIL$' || true)
  if [ "$FAIL_COUNT" -gt 0 ]; then
      echo -e "Failures: ${RED}${FAIL_COUNT}${NC}"
  fi
}

record_pass() {
  PHASE_NAMES+=("$1")
  PHASE_STATUS+=("PASS")
  PHASE_DETAIL+=("${2:-}")
  echo -e "${GREEN}PASS: $1 ${NC}"
  report_phase_progress "$1" "PASS"
}

record_fail() {
  PHASE_NAMES+=("$1")
  PHASE_STATUS+=("FAIL")
  PHASE_DETAIL+=("$2")
  echo -e "${RED}FAIL:${NC} $1 — $2" >&2
  report_phase_progress "$1" "FAIL"
}

run_phase() {
  local name="$1"
  shift
  local log
  log="$(mktemp)"
  if "$@" >"${log}" 2>&1; then
    record_pass "${name}" "$(grep -E '^(PASS|ok|passed|Milestone|boundary|bucket)' "${log}" | tail -1 || tail -1 "${log}")"
  else
    local detail
    detail="$(grep -E '^(FAIL|Error|missing|expected|aws |s3cmd |Traceback|Result:)' "${log}" || true)"
    if [[ -z "${detail}" ]]; then
      detail="$(tail -5 "${log}")"
    fi
    record_fail "${name}" "$(echo "${detail}" | tail -5 | tr '\n' ' ')"
  fi
  rm -f "${log}"
}

phase_build() {
  cmake -S "${ROOT}/backend" -B "${ROOT}/build" >/dev/null
  cmake --build "${ROOT}/build" -j"$(nproc)"
  cd "${ROOT}/frontend" && go build -o "${ROOT}/build/kv-rgw-frontend" .
}

phase_unit() {
  "${ROOT}/build/object_value_test"
  "${ROOT}/build/l_key_test"
  "${ROOT}/build/data_store_test"
  "${ROOT}/build/gc_config_state_test"
  if [[ -f "${FDB_CLUSTER_FILE}" ]]; then
    "${ROOT}/build/kv_range_test"
  else
    echo "SKIP kv_range_test: no FDB cluster" >&2
    return 1
  fi
}

phase_reload() {
  "${ROOT}/scripts/reload.sh" --clean "${KVRGW_INSTANCES}"
}

phase_smoke() {
  "${ROOT}/scripts/smoke_test.sh" --skip-reload
}

phase_head_bucket() {
  "${ROOT}/scripts/test_head_bucket.sh"
}

phase_corrupted_etag() {
  "${ROOT}/scripts/test_corrupted_etag.sh"
}

phase_l_namespace_fdb() {
  "${ROOT}/scripts/test_l_namespace_fdb.sh"
}

phase_byte_range() {
  "${ROOT}/scripts/test_byte_range_get.sh"
}

phase_boundary() {
  require_cmd() { command -v "$1" >/dev/null || { echo "missing: $1"; return 1; }; }
  require_cmd aws
  require_cmd s5cmd
  require_cmd s3cmd

  local dir="/tmp/kv-test-2k-$$"
  local bucket="kv-test-boundary-$$"
  local uploaded expected
  mkdir -p "${dir}"
  for i in $(seq 1 1024); do
    printf 'x' > "${dir}/$(printf 'obj-%04d' "${i}")"
  done

  uploaded="$(find "${dir}" -maxdepth 1 -type f | wc -l)"
  expected="${uploaded}"
  if [[ "${expected}" -ne 1024 ]]; then
    echo "setup: expected 1024 files, got ${expected}"
    rm -rf "${dir}"
    return 1
  fi

  kvrgw_s5cmd_log "boundary-mb-${bucket}" --numworkers 8 --endpoint-url "${ENDPOINT}" mb "s3://${bucket}"
  kvrgw_s5cmd_log "boundary-sync-${bucket}" --numworkers 8 --endpoint-url "${ENDPOINT}" sync "${dir}/" "s3://${bucket}/"

  python3 "${ROOT}/scripts/list_verify.py" aws \
    --endpoint "${ENDPOINT}" --bucket "${bucket}" --expected "${expected}"

  python3 "${ROOT}/scripts/list_verify.py" s3cmd \
    --config "${ROOT}/s3cmd.cfg" --bucket "${bucket}" --expected "${expected}"

  python3 "${ROOT}/scripts/list_verify.py" s5cmd \
    --endpoint "${ENDPOINT}" --bucket "${bucket}" --expected "${expected}"

  aws --endpoint-url "${ENDPOINT}" s3 rb "s3://${bucket}" --force >/dev/null
  rm -rf "${dir}"
  echo "boundary ${expected} keys: aws + s3cmd + s5cmd ok (count + strict order)"
}

phase_pagination() {
  "${ROOT}/scripts/test_list_pagination.sh"
}

phase_stress() {
  "${ROOT}/scripts/test_list_stress.sh" --skip-reload
}

phase_list_buckets_stress() {
  "${ROOT}/scripts/test_list_buckets_stress.sh" --skip-reload
}

phase_batch_tiers() {
  "${ROOT}/scripts/test_batch_tiers.sh"
}

phase_gc_admin() {
  "${ROOT}/scripts/test_gc_admin.sh"
}

phase_gc_delete_queue() {
  "${ROOT}/scripts/test_gc_delete_queue.sh"
}

phase_kill_instance() {
  "${ROOT}/scripts/test_kill_single_instance.sh"
}

phase_power_cycle_easy() {
  "${ROOT}/scripts/test_power_cycle_upload_easy.sh"
}

phase_power_cycle_hard() {
  "${ROOT}/scripts/test_power_cycle_upload_hard.sh"
}

echo "=== KV-RGW Test Plan (tier: ${TIER}) ==="

run_phase "0 build" phase_build
run_phase "1 unit tests" phase_unit

if tier_at_least fast; then
  run_phase "2 reload (clean)" phase_reload
fi

if tier_at_least quick; then
  run_phase "3 smoke" phase_smoke
fi

if tier_at_least medium; then
  run_phase "4 byte range GET (4MiB)" phase_byte_range
  run_phase "5 list boundary (1024)" phase_boundary
  run_phase "5b batch tiers (s5cmd)" phase_batch_tiers
fi

if tier_at_least slow; then
  run_phase "6 pagination (64K)" phase_pagination
  run_phase "7 list stress (64K upload)" phase_stress
  run_phase "8 list buckets stress (1K buckets)" phase_list_buckets_stress
  run_phase "9 gc delete queue" phase_gc_delete_queue
fi

if [[ "${TIER}" == "chaos" ]]; then
  if [[ "${KVRGW_INSTANCES}" -lt 3 ]]; then
    record_fail "10 kill instance" "KVRGW_INSTANCES=${KVRGW_INSTANCES}; need 3 for chaos tier"
    record_fail "11 power-cycle easy" "skipped (need N=3)"
    record_fail "12 power-cycle hard" "skipped (need N=3)"
  else
    run_phase "10 kill instance" phase_kill_instance
    run_phase "10b reload restore (3)" phase_reload
    run_phase "11 power-cycle easy" phase_power_cycle_easy
    run_phase "12 power-cycle hard" phase_power_cycle_hard
  fi
fi

fail_count=0
pass_count=0
echo ""
echo "TEST PLAN SUMMARY"
echo "================="
for i in "${!PHASE_NAMES[@]}"; do
  echo "${PHASE_STATUS[$i]}: ${PHASE_NAMES[$i]}"
  if [[ -n "${PHASE_DETAIL[$i]:-}" ]]; then
    echo "       ${PHASE_DETAIL[$i]}"
  fi
  if [[ "${PHASE_STATUS[$i]}" == "FAIL" ]]; then
    fail_count=$((fail_count + 1))
  else
    pass_count=$((pass_count + 1))
  fi
done
echo ""
if [[ "${fail_count}" -gt 0 ]]; then
  echo "Result: FAILED (${pass_count} passed, ${fail_count} failed)"
  exit 1
fi
echo "Result: PASSED (${pass_count}/${pass_count} phases)"
exit 0
