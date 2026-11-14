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
# run_put_condition_test.sh — run put_condition_test (If-Match / If-None-Match
# on every PUT tier and on multipart complete) against a live FDB cluster,
# once with batching off (KVRGW_BATCH_SIZE=1) and once with batching on.
#
# Usage:
#   bash scripts/run_put_condition_test.sh [-v] [--tier T]   # foreground
#   bash scripts/run_put_condition_test.sh --bg  [...]       # background
#   bash scripts/run_put_condition_test.sh --status          # running/PASS/FAIL
#   bash scripts/run_put_condition_test.sh --tail            # follow the log
#
# Background mode writes .run/put_condition_test.{pid,status} and
# .logs/put_condition_test.log.  The status file holds RUNNING, PASS or FAIL.
# Exit code is non-zero if any configuration fails.
#
# Environment:
#   PUT_CONDITION_BATCH_SIZES  space-separated KVRGW_BATCH_SIZE values
#                              (default "1 10")

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=kvrgw-common.sh
source "${SCRIPT_DIR}/kvrgw-common.sh"

BIN="${ROOT}/build/put_condition_test"
PID_FILE="${RUN_DIR}/put_condition_test.pid"
STATUS_FILE="${RUN_DIR}/put_condition_test.status"
LOG_FILE="${ROOT}/.logs/put_condition_test.log"
BATCH_SIZES="${PUT_CONDITION_BATCH_SIZES:-1 10}"

mkdir -p "${RUN_DIR}" "${ROOT}/.logs"

is_running() {
  [[ -f "${PID_FILE}" ]] && kill -0 "$(cat "${PID_FILE}")" 2>/dev/null
}

case "${1:-}" in
  --status)
    if is_running; then
      echo "RUNNING (pid $(cat "${PID_FILE}"))"
      exit 0
    fi
    if [[ -f "${STATUS_FILE}" ]]; then
      st="$(cat "${STATUS_FILE}")"
      echo "${st}"
      [[ "${st}" == PASS* ]] && exit 0
      [[ "${st}" == RUNNING* ]] && { echo "(stale: process gone)"; exit 1; }
      exit 1
    fi
    echo "no run recorded"
    exit 1
    ;;
  --tail)
    [[ -f "${LOG_FILE}" ]] || { echo "no log: ${LOG_FILE}" >&2; exit 1; }
    exec tail -n 50 -f "${LOG_FILE}"
    ;;
  --bg)
    shift
    if is_running; then
      echo "already running (pid $(cat "${PID_FILE}"))" >&2
      exit 1
    fi
    echo "RUNNING" > "${STATUS_FILE}"
    nohup bash "${BASH_SOURCE[0]}" --child "$@" > "${LOG_FILE}" 2>&1 &
    echo $! > "${PID_FILE}"
    echo "started pid $(cat "${PID_FILE}"); log: ${LOG_FILE}"
    echo "  status: bash scripts/run_put_condition_test.sh --status"
    echo "  follow: bash scripts/run_put_condition_test.sh --tail"
    exit 0
    ;;
  --child)
    shift
    CHILD=1
    ;;
  *)
    CHILD=0
    ;;
esac

# ---------------------------------------------------------------------------
# Pre-flight
# ---------------------------------------------------------------------------
finish() {
  local rc="$1"
  if [[ "${CHILD}" -eq 1 ]]; then
    if [[ "${rc}" -eq 0 ]]; then echo "PASS" > "${STATUS_FILE}"; else echo "FAIL" > "${STATUS_FILE}"; fi
    rm -f "${PID_FILE}"
  fi
  exit "${rc}"
}

if [[ ! -x "${BIN}" ]]; then
  echo -e "${RED}FAIL:${NC} binary not found: ${BIN}" >&2
  echo "  Run:  cd build && make put_condition_test" >&2
  finish 1
fi
if [[ ! -f "${FDB_CLUSTER_FILE}" ]]; then
  echo -e "${RED}FAIL:${NC} FDB cluster file not found: ${FDB_CLUSTER_FILE}" >&2
  finish 1
fi

DATA_TMP="/tmp/kvrgw-pc-test-run-$$"
mkdir -p "${DATA_TMP}"
trap 'rm -rf "${DATA_TMP}"' EXIT

# ---------------------------------------------------------------------------
# One run per batch size
# ---------------------------------------------------------------------------
overall=0
for bs in ${BATCH_SIZES}; do
  echo ""
  echo "=== put_condition_test  KVRGW_BATCH_SIZE=${bs} ==="
  if KVRGW_BATCH_SIZE="${bs}" KVRGW_DATA_ROOT="${DATA_TMP}/bs${bs}" "${BIN}" "$@"; then
    echo -e "${GREEN}PASS: put_condition_test (batch_size=${bs})${NC}"
  else
    echo -e "${RED}FAIL: put_condition_test (batch_size=${bs})${NC}" >&2
    overall=1
  fi
done

echo ""
if [[ "${overall}" -eq 0 ]]; then
  echo -e "${GREEN}PASS: all put_condition_test runs${NC}"
else
  echo -e "${RED}FAIL: put_condition_test${NC}" >&2
fi
finish "${overall}"
