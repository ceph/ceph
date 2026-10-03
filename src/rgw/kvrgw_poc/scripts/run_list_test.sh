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
CLUSTER_FILE="${ROOT}/.fdb/fdb.cluster"

usage() {
  echo "Usage: $(basename "$0") <results-dir> [--blind] [--quiet] [--all-versions] [--ryw-cache=enabled|disabled] [--max-pages=N] [--progress=N]"
  echo "Note: --quiet and --progress=N are mutually exclusive"
  exit 1
}

[[ $# -ge 1 ]] || usage

RESULTS_DIR=""
LIST_OPTS=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --blind) LIST_OPTS="${LIST_OPTS} --blind"; shift ;;
    --quiet) LIST_OPTS="${LIST_OPTS} --quiet"; shift ;;
    --all-versions) LIST_OPTS="${LIST_OPTS} --all-versions"; shift ;;
    --ryw-cache=*) LIST_OPTS="${LIST_OPTS} $1"; shift ;;
    --max-pages=*) LIST_OPTS="${LIST_OPTS} $1"; shift ;;
    --progress=*) LIST_OPTS="${LIST_OPTS} $1"; shift ;;
    -*) echo "Unknown: $1" >&2; usage ;;
    *) RESULTS_DIR="$1"; shift ;;
  esac
done

if [[ "${LIST_OPTS}" == *"--quiet"* && "${LIST_OPTS}" == *"--progress="* ]]; then
  echo "ERROR: --quiet and --progress=N are mutually exclusive" >&2
  usage
fi

[[ -n "${RESULTS_DIR}" ]] || { echo "ERROR: results directory not specified" >&2; usage; }
[[ -d "${RESULTS_DIR}" ]] || { echo "ERROR: ${RESULTS_DIR} not found" >&2; exit 1; }

META="${RESULTS_DIR}/test_metadata.txt"
[[ -f "${META}" ]] || { echo "ERROR: ${META} not found" >&2; exit 1; }

PREFIX=$(grep '^prefix=' "${META}" | head -1 | cut -d= -f2)
SUFFIX=$(grep '^suffix=' "${META}" | head -1 | cut -d= -f2 || true)
[[ -n "${PREFIX}" ]] || { echo "ERROR: prefix not found in ${META}" >&2; exit 1; }

echo "=== List Test ==="
echo "  Results: ${RESULTS_DIR}"
echo "  Prefix: ${PREFIX}"
echo "  Suffix: ${SUFFIX}"
echo "  Options:${LIST_OPTS:- (none)}"

FIFO=$(mktemp -u "${RESULTS_DIR}/list_test_fifo_XXXXXX")
mkfifo "${FIFO}"
LOG="${RESULTS_DIR}/list_test.log"

FDB_CLUSTER_FILE="${CLUSTER_FILE}" \
    KVRGW_KEY_PREFIX="${PREFIX}" \
    KVRGW_KEY_SUFFIX="${SUFFIX}" \
    KVRGW_METADATA_FILE="${META}" \
    KVRGW_MAX_INLINE=256 \
    KVRGW_MAX_KV_STORE=4096 \
    "${ROOT}/build/kv-rgw-backend" --perf "/tmp/kvrgw-list-test.sock" "${ROOT}/data-list-test" \
    < "${FIFO}" > "${LOG}" 2> >(tee -a "${LOG}" >&2) &
PID=$!

exec 10>"${FIFO}"
echo "list-test${LIST_OPTS}" >&10
echo "quit" >&10
exec 10>&- 2>/dev/null

wait "$PID" 2>/dev/null || true
rm -f "${FIFO}"

echo ""
grep -E '=== LIST-TEST|pages=|Page latency|Verification|FAIL|ERROR' "${LOG}" || true
echo ""
echo "Full log: ${LOG}"
