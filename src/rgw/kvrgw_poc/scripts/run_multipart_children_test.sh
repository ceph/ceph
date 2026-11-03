#!/usr/bin/env bash
#
# Ceph - scalable distributed file system
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.
#
# Runs backend/tests/multipart_children_test (multipart 'M'/'S' child KVs and
# the tier 3 checksum file: placement, delete paths, copy, 10,000 parts).
#
#   1. checks FDB is healthy (starts it when not) and builds the test binary
#      (incremental);
#   2. records how many kv-rgw-frontend processes are running and stops them
#      (their GC workers would steal the GC entries the test waits on);
#   3. runs the test in the background with a timeout, logging to a file, and
#      shows the result;
#   4. restarts the frontends with scripts/reload.sh <N> if any were running.
#
# Usage: run_multipart_children_test.sh [--timeout SEC] [--log FILE]
# Takes about 4 minutes.

set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/kvrgw-common.sh
source "${ROOT}/scripts/kvrgw-common.sh"

TIMEOUT=1500
LOG="${ROOT}/.logs/multipart_children_test.log"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --timeout) TIMEOUT="$2"; shift 2 ;;
    --log) LOG="$2"; shift 2 ;;
    -h|--help) sed -n 12,24p "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "Unknown: $1" >&2; exit 1 ;;
  esac
done

TEST_BIN="${ROOT}/build/multipart_children_test"
FRONTEND_BIN="${ROOT}/build/kv-rgw-frontend"
FRONTENDS_BEFORE=0
TEST_PID=""
RESTORED=0

count_frontends() { pgrep -fc -- "${FRONTEND_BIN}" || true; }

restore_frontends() {
  [[ "${RESTORED}" -eq 1 ]] && return
  RESTORED=1
  if [[ "${FRONTENDS_BEFORE}" -gt 0 ]]; then
    echo -e "${BLUE}Restoring ${FRONTENDS_BEFORE} frontend(s) with reload.sh...${NC}"
    "${ROOT}/scripts/reload.sh" "${FRONTENDS_BEFORE}" > "${LOG}.reload" 2>&1
    local rc=$?
    tail -8 "${LOG}.reload"
    if [[ "${rc}" -ne 0 ]]; then
      echo -e "${RED}FAIL:${NC} frontend restore failed (rc=${rc}), see ${LOG}.reload" >&2
      return "${rc}"
    fi
  else
    echo "No frontends were running before; nothing to restore."
  fi
}

on_signal() {
  echo -e "\n${RED}Interrupted.${NC}"
  [[ -n "${TEST_PID}" ]] && kill "${TEST_PID}" 2>/dev/null
  restore_frontends
  exit 130
}
trap on_signal INT TERM

mkdir -p "$(dirname "${LOG}")"

# ---- 1. system state ----------------------------------------------------------
echo -e "${BLUE}[1/4] Checking system state${NC}"
if [[ -f "${FDB_CLUSTER_FILE}" ]] &&
   kvrgw_fdbcli "status" 2>/dev/null | grep -q "Replication health     - Healthy"; then
  echo "  FDB: healthy"
else
  echo "  FDB: not healthy, starting"
  kvrgw_start_fdb || { echo -e "${RED}FAIL:${NC} cannot start FDB" >&2; exit 1; }
fi

echo "  building ${TEST_BIN##*/} (incremental)"
cmake -S "${ROOT}/backend" -B "${ROOT}/build" > /dev/null &&
  cmake --build "${ROOT}/build" -j"$(nproc)" --target multipart_children_test > "${LOG}.build" 2>&1 ||
  { tail -20 "${LOG}.build" >&2; echo -e "${RED}FAIL:${NC} build failed" >&2; exit 1; }
[[ -x "${TEST_BIN}" ]] || { echo -e "${RED}FAIL:${NC} ${TEST_BIN} missing" >&2; exit 1; }

# ---- 2. stop frontends --------------------------------------------------------
echo -e "${BLUE}[2/4] Frontends${NC}"
FRONTENDS_BEFORE="$(count_frontends)"
echo "  running: ${FRONTENDS_BEFORE}"
if [[ "${FRONTENDS_BEFORE}" -gt 0 ]]; then
  pkill -TERM -f -- "${FRONTEND_BIN}" || true
  for _ in $(seq 1 20); do
    [[ "$(count_frontends)" -eq 0 ]] && break
    sleep 0.5
  done
  if [[ "$(count_frontends)" -ne 0 ]]; then
    pkill -KILL -f -- "${FRONTEND_BIN}" || true
    sleep 1
  fi
  echo "  stopped"
fi

# ---- 3. run -------------------------------------------------------------------
echo -e "${BLUE}[3/4] Running ${TEST_BIN##*/} (timeout ${TIMEOUT}s, log ${LOG})${NC}"
setsid timeout "${TIMEOUT}" "${TEST_BIN}" > "${LOG}" 2>&1 &
TEST_PID=$!
START=$(date +%s)
while kill -0 "${TEST_PID}" 2>/dev/null; do
  sleep 5
  printf '\r  elapsed %ss ' "$(( $(date +%s) - START ))"
done
wait "${TEST_PID}"
RC=$?
echo

echo "---- results ----"
grep -vE 'FDB_NET_OPTION|^kv-rgw backend' "${LOG}" | tail -40
if [[ "${RC}" -eq 0 ]]; then
  echo -e "${GREEN}PASS:${NC} multipart_children_test"
elif [[ "${RC}" -eq 124 ]]; then
  echo -e "${RED}FAIL:${NC} multipart_children_test timed out after ${TIMEOUT}s"
else
  echo -e "${RED}FAIL:${NC} multipart_children_test rc=${RC}"
fi

# ---- 4. restore ---------------------------------------------------------------
echo -e "${BLUE}[4/4] Restore${NC}"
restore_frontends || RC=1
exit "${RC}"
