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
# run_multipart_tests.sh — run multipart_phase2_test and multipart_gc_test
# against a live FDB cluster.
#
# Both binaries are built by the regular build:
#   cmake -S backend -B build && cmake --build build
#
# Usage:
#   bash scripts/run_multipart_tests.sh
#
# The script requires a healthy FDB cluster pointed to by .fdb/fdb.cluster
# (set up by scripts/start_fdb.sh or gen_docker_compose.sh).  No running
# kv-rgw-frontend is needed -- the tests connect to FDB directly.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=kvrgw-common.sh
source "${SCRIPT_DIR}/kvrgw-common.sh"

# ---------------------------------------------------------------------------
# Pre-flight: binaries and FDB cluster file
# ---------------------------------------------------------------------------
PHASE2_BIN="${ROOT}/build/multipart_phase2_test"
GC_BIN="${ROOT}/build/multipart_gc_test"

for bin in "${PHASE2_BIN}" "${GC_BIN}"; do
  if [[ ! -x "${bin}" ]]; then
    echo -e "${RED}FAIL:${NC} binary not found or not executable: ${bin}" >&2
    echo "  Run:  cmake -S backend -B build && cmake --build build" >&2
    exit 1
  fi
done

if [[ ! -f "${FDB_CLUSTER_FILE}" ]]; then
  echo -e "${RED}FAIL:${NC} FDB cluster file not found: ${FDB_CLUSTER_FILE}" >&2
  echo "  Run:  bash scripts/start_fdb.sh" >&2
  exit 1
fi

# Unique per-run data root; each binary also creates its own PID-suffixed
# subdirectory, so this is just a shared parent for cleanup.
DATA_TMP="/tmp/kvrgw-mpu-test-$$"
mkdir -p "${DATA_TMP}"

# Running frontends have GC workers on the same G: queue; they claim the tests'
# GC entries but delete files under their own data root, so the files the tests
# wait on are never freed. Pause them (SIGSTOP) for the duration of the run.
PAUSED_PIDS="$(pgrep -f -- "${ROOT}/build/kv-rgw-frontend" || true)"

resume_frontends() {
  if [[ -n "${PAUSED_PIDS}" ]]; then
    # shellcheck disable=SC2086
    kill -CONT ${PAUSED_PIDS} 2>/dev/null || true
    PAUSED_PIDS=""
  fi
}

cleanup() {
  resume_frontends
  rm -rf "${DATA_TMP}"
}
trap cleanup EXIT

if [[ -n "${PAUSED_PIDS}" ]]; then
  # shellcheck disable=SC2086
  kill -STOP ${PAUSED_PIDS} 2>/dev/null || true
fi

# ---------------------------------------------------------------------------
# Test 1: multipart_phase2_test  (Phase 2/3 functional smoke test)
# ---------------------------------------------------------------------------
echo "=== multipart_phase2_test ==="
if KVRGW_DATA_ROOT="${DATA_TMP}/phase2" "${PHASE2_BIN}"; then
  echo -e "${GREEN}PASS: multipart_phase2_test${NC}"
else
  echo -e "${RED}FAIL: multipart_phase2_test${NC}" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Test 2: multipart_gc_test  (GC / Sweeper integration test)
# ---------------------------------------------------------------------------
# KVRGW_GC_INTERVAL_SEC=1 is set inside the binary via setenv() before
# runtime.start(); exporting it here too is harmless and makes intent clear.
echo ""
echo "=== multipart_gc_test ==="
if KVRGW_GC_INTERVAL_SEC=1 KVRGW_DATA_ROOT="${DATA_TMP}/gc" "${GC_BIN}"; then
  echo -e "${GREEN}PASS: multipart_gc_test${NC}"
else
  echo -e "${RED}FAIL: multipart_gc_test${NC}" >&2
  exit 1
fi

echo ""
echo -e "${GREEN}PASS: all multipart tests${NC}"
