#!/bin/bash
#
# Ceph - scalable distributed file system
#
# Author: Gabriel BenHanokh <gbenhano@ibm.com>
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.
#
# run_backend_fdb_tests.sh -- run the backend test binaries that talk to a live
# FoundationDB cluster and must not share it with running frontends.
#
# Each of these binaries starts its own in-process KvRgwRuntime (with its own
# GC worker) on the same FDB keyspace as any running kv-rgw-frontend. The
# frontends' GC workers would claim the tests' G: entries and delete files
# under the frontends' own data root, so the files the tests wait on are never
# freed. The frontends are therefore paused (SIGSTOP) ONCE before the first
# test and resumed (SIGCONT) ONCE after the last one, on every exit path.
#
# Included tests (name -- what it covers):
#   o_value_layout_test        :O: value layout permutations (plans/o_value_layout.md):
#                              single-part and 17 multipart shapes, CopyObject,
#                              ref-counts, no orphan C:/file/R: after delete  (~3 min)
#   multipart_children_test    multipart C:M / C:S child KVs, every delete path,
#                              CopyObject sharing                             (~4 min)
#   put_condition_test         If-Match / If-None-Match on every storage tier;
#                              run with KVRGW_BATCH_SIZE=1 and 10
#   multipart_phase2_test      multipart Phase 2/3 functional test
#   multipart_gc_test          multipart GC / sweeper integration test
#   multipart_ref_copy_test    CopyObject of multipart objects, delete order
#   multipart_cksum_test       multipart checksums (composite / full-object)
#   multipart_goa_test         GetObjectAttributes on multipart objects
#   multipart_minpart_test     minimum part size admin command and enforcement
#   d_write_test               D: key write path
#   batch_mixed_test           mixed-operation batching
#
# Usage:
#   bash scripts/run_backend_fdb_tests.sh [OPTIONS] [TEST ...]
#
#   (no TEST)            run all tests above, in the order listed
#   TEST ...             run only the named tests (any number, in the given order)
#
# Options:
#   --list               print the test names and exit
#   --skip-build         do not (re)build the test targets first
#   --timeout SEC        per-test timeout, overrides the built-in defaults
#                        (default 1500 for multipart_children_test, 900 for
#                        o_value_layout_test, 600 for everything else)
#   --keep-going         (default) run every selected test and report at the end
#   --fail-fast          stop after the first failing test
#   -h, --help           show this text
#
# Examples:
#   bash scripts/run_backend_fdb_tests.sh                       # everything
#   bash scripts/run_backend_fdb_tests.sh o_value_layout_test   # one test
#   bash scripts/run_backend_fdb_tests.sh --skip-build o_value_layout_test \
#        multipart_children_test put_condition_test
#
# Logs: .logs/backend_fdb/<test>.log (put_condition_test: <test>.bs<N>.log).
# Exit code is 0 only if every selected test passed.
#
# Requires a healthy FDB cluster (.fdb/fdb.cluster, see scripts/start_fdb.sh).
# The frontends are paused, not restarted, so no reload is needed afterwards.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=kvrgw-common.sh
source "${SCRIPT_DIR}/kvrgw-common.sh"

#----------------------------------------------------------------------------
# Test table
#----------------------------------------------------------------------------
ALL_TESTS=(
  o_value_layout_test
  multipart_children_test
  put_condition_test
  multipart_phase2_test
  multipart_gc_test
  multipart_ref_copy_test
  multipart_cksum_test
  multipart_goa_test
  multipart_minpart_test
  d_write_test
  batch_mixed_test
)

PUT_CONDITION_BATCH_SIZES="${PUT_CONDITION_BATCH_SIZES:-1 10}"
DEFAULT_TIMEOUT=600

default_timeout_for() {
  case "$1" in
    multipart_children_test) echo 1500 ;;
    o_value_layout_test)     echo 900 ;;
    *)                       echo "${DEFAULT_TIMEOUT}" ;;
  esac
}

is_known_test() {
  local t
  for t in "${ALL_TESTS[@]}"; do [[ "$t" == "$1" ]] && return 0; done
  return 1
}

print_help() { sed -n '12,/^# The frontends are paused/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }

#----------------------------------------------------------------------------
# Arguments
#----------------------------------------------------------------------------
SELECTED=()
SKIP_BUILD=0
FAIL_FAST=0
TIMEOUT_OVERRIDE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --list)       printf '%s\n' "${ALL_TESTS[@]}"; exit 0 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    --fail-fast)  FAIL_FAST=1; shift ;;
    --keep-going) FAIL_FAST=0; shift ;;
    --timeout)
      [[ $# -ge 2 && "$2" =~ ^[0-9]+$ ]] || { echo "--timeout needs a number of seconds" >&2; exit 2; }
      TIMEOUT_OVERRIDE="$2"; shift 2 ;;
    -h|--help)    print_help; exit 0 ;;
    -*)           echo "Unknown option: $1" >&2; exit 2 ;;
    *)
      if ! is_known_test "$1"; then
        echo "Unknown test: $1 (see --list)" >&2
        exit 2
      fi
      SELECTED+=("$1"); shift ;;
  esac
done
[[ ${#SELECTED[@]} -eq 0 ]] && SELECTED=("${ALL_TESTS[@]}")

#----------------------------------------------------------------------------
# Pre-flight
#----------------------------------------------------------------------------
if [[ ! -f "${FDB_CLUSTER_FILE}" ]]; then
  echo -e "${RED}FAIL:${NC} FDB cluster file not found: ${FDB_CLUSTER_FILE}" >&2
  echo "  Run:  bash scripts/start_fdb.sh" >&2
  exit 1
fi

LOG_DIR="${ROOT}/.logs/backend_fdb"
mkdir -p "${LOG_DIR}"

if [[ "${SKIP_BUILD}" -eq 0 ]]; then
  echo -e "${BLUE}Building ${#SELECTED[@]} test target(s)${NC}"
  build_log="${LOG_DIR}/build.log"
  if ! { cmake -S "${ROOT}/backend" -B "${ROOT}/build" > /dev/null &&
         cmake --build "${ROOT}/build" -j"$(nproc)" --target "${SELECTED[@]}" > "${build_log}" 2>&1; }; then
    tail -20 "${build_log}" >&2
    echo -e "${RED}FAIL:${NC} build failed (${build_log})" >&2
    exit 1
  fi
fi

for t in "${SELECTED[@]}"; do
  if [[ ! -x "${ROOT}/build/${t}" ]]; then
    echo -e "${RED}FAIL:${NC} binary not found or not executable: ${ROOT}/build/${t}" >&2
    echo "  Run without --skip-build, or: cmake --build build --target ${t}" >&2
    exit 1
  fi
done

#----------------------------------------------------------------------------
# Pause the frontends once; resume once on every exit path
#----------------------------------------------------------------------------
DATA_TMP="/tmp/kvrgw-backend-fdb-$$"
mkdir -p "${DATA_TMP}"
PAUSED_PIDS="$(pgrep -f -- "${ROOT}/build/kv-rgw-frontend" || true)"

resume_frontends() {
  if [[ -n "${PAUSED_PIDS}" ]]; then
    # shellcheck disable=SC2086
    kill -CONT ${PAUSED_PIDS} 2>/dev/null || true
    echo "Resumed frontend pid(s): ${PAUSED_PIDS//$'\n'/ }"
    PAUSED_PIDS=""
  fi
}

cleanup() {
  resume_frontends
  rm -rf "${DATA_TMP}"
}
trap cleanup EXIT
trap 'echo -e "\n${RED}Interrupted.${NC}"; exit 130' INT TERM

if [[ -n "${PAUSED_PIDS}" ]]; then
  # shellcheck disable=SC2086
  kill -STOP ${PAUSED_PIDS} 2>/dev/null || true
  echo "Paused frontend pid(s): ${PAUSED_PIDS//$'\n'/ }"
else
  echo "No running frontends to pause."
fi

#----------------------------------------------------------------------------
# Run
#----------------------------------------------------------------------------
RESULT_NAMES=()
RESULT_STATUS=()
RESULT_SECS=()

# run_one LABEL LOGFILE TIMEOUT [ENV=VAL ...] -- BINARY
run_one() {
  local label="$1" log="$2" tmo="$3"
  shift 3
  local envs=()
  while [[ "$1" != "--" ]]; do envs+=("$1"); shift; done
  shift
  local bin="$1"

  echo ""
  echo -e "${BLUE}=== ${label} (timeout ${tmo}s, log ${log}) ===${NC}"
  local start rc
  start=$(date +%s)
  env "${envs[@]}" timeout "${tmo}" "${bin}" > "${log}" 2>&1
  rc=$?
  local secs=$(( $(date +%s) - start ))

  grep -vE 'FDB_NET_OPTION|^kv-rgw backend' "${log}" | tail -5
  local status
  if [[ "${rc}" -eq 0 ]]; then
    status="PASS"
    echo -e "${GREEN}PASS: ${label}${NC} (${secs}s)"
  elif [[ "${rc}" -eq 124 ]]; then
    status="TIMEOUT"
    echo -e "${RED}FAIL: ${label} timed out after ${tmo}s${NC}" >&2
  else
    status="FAIL(rc=${rc})"
    echo -e "${RED}FAIL: ${label} rc=${rc}${NC} (see ${log})" >&2
  fi
  RESULT_NAMES+=("${label}")
  RESULT_STATUS+=("${status}")
  RESULT_SECS+=("${secs}")
  [[ "${rc}" -eq 0 ]]
}

OVERALL=0
for t in "${SELECTED[@]}"; do
  tmo="${TIMEOUT_OVERRIDE:-$(default_timeout_for "${t}")}"
  bin="${ROOT}/build/${t}"

  if [[ "${t}" == "put_condition_test" ]]; then
    for bs in ${PUT_CONDITION_BATCH_SIZES}; do
      run_one "${t} batch_size=${bs}" "${LOG_DIR}/${t}.bs${bs}.log" "${tmo}" \
        "KVRGW_BATCH_SIZE=${bs}" "KVRGW_DATA_ROOT=${DATA_TMP}/${t}-bs${bs}" -- "${bin}" || OVERALL=1
    done
  else
    extra=()
    [[ "${t}" == *gc_test || "${t}" == o_value_layout_test || "${t}" == multipart_children_test ]] &&
      extra+=("KVRGW_GC_INTERVAL_SEC=1")
    run_one "${t}" "${LOG_DIR}/${t}.log" "${tmo}" \
      "KVRGW_DATA_ROOT=${DATA_TMP}/${t}" "${extra[@]}" -- "${bin}" || OVERALL=1
  fi

  if [[ "${OVERALL}" -ne 0 && "${FAIL_FAST}" -eq 1 ]]; then
    echo -e "${RED}--fail-fast: stopping after first failure${NC}" >&2
    break
  fi
done

#----------------------------------------------------------------------------
# Summary
#----------------------------------------------------------------------------
echo ""
echo "==================== summary ===================="
for i in "${!RESULT_NAMES[@]}"; do
  if [[ "${RESULT_STATUS[$i]}" == "PASS" ]]; then
    printf '  %b%-8s%b %-45s %5ss\n' "${GREEN}" "PASS" "${NC}" "${RESULT_NAMES[$i]}" "${RESULT_SECS[$i]}"
  else
    printf '  %b%-8s%b %-45s %5ss\n' "${RED}" "${RESULT_STATUS[$i]}" "${NC}" "${RESULT_NAMES[$i]}" "${RESULT_SECS[$i]}"
  fi
done
if [[ "${OVERALL}" -eq 0 ]]; then
  echo -e "${GREEN}PASS: all ${#RESULT_NAMES[@]} run(s)${NC}"
else
  echo -e "${RED}FAIL: one or more tests failed${NC}" >&2
fi
exit "${OVERALL}"
