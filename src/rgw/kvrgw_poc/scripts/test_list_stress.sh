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
# Stress test: parallel 64K upload while polling ListObjects until count reaches target.
#
# Rules per poll: keys strictly increasing (KV(n) < KV(n+1)), count <= TARGET.
# Stop when a full list returns exactly TARGET keys. Fail on crash/timeout.
#
# Usage: test_list_stress.sh [--skip-reload]

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/kvrgw-common.sh
source "${ROOT}/scripts/kvrgw-common.sh"

SKIP_RELOAD=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-reload) SKIP_RELOAD=1; shift ;;
    -h|--help)
      echo "Usage: $(basename "$0") [--skip-reload]"
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

KVRGW_DIR="/tmp/kvrgw"
BUCKET="bucket1"
TARGET=65536
POLL_SEC="${KVRGW_STRESS_POLL_SEC:-1}"
MAX_POLLS="${KVRGW_STRESS_MAX_POLLS:-900}"

if [[ "${KVRGW_S5CMD_LOG_APPEND:-}" != 1 ]]; then
  kvrgw_s5cmd_init_log
fi
export KVRGW_S5CMD_LOG

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Required command not found: $1" >&2
    exit 1
  }
}

require_cmd aws
require_cmd s5cmd
require_cmd python3

check_servers_alive() {
  if ! pgrep -f "${ROOT}/build/kv-rgw-frontend" >/dev/null 2>&1; then
    echo -e "${RED}FAIL:${NC} frontend process not running" >&2
    tail -20 "${ROOT}/.logs/frontend-0.log" 2>/dev/null >&2 || true
    return 1
  fi
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' "${ENDPOINT}/" 2>/dev/null || echo 000)"
  if [[ "${code}" == "000" ]]; then
    echo -e "${RED}FAIL:${NC} frontend not responding at ${ENDPOINT}" >&2
    return 1
  fi
}

if [[ "${SKIP_RELOAD}" -eq 0 ]]; then
  echo "=== Reloading kv-rgw (clean) ==="
  "${ROOT}/scripts/reload.sh" --clean

  echo "=== Creating ${TARGET} test files ==="
  "${ROOT}/scripts/mk_random_files.sh" -n 64K -s 1 "${KVRGW_DIR}"
else
  echo "=== Skipping reload (--skip-reload) ==="
  check_servers_alive || {
    echo "Hint: run ./scripts/reload.sh --clean first" >&2
    exit 1
  }
fi

uploaded="$(find "${KVRGW_DIR}" -maxdepth 1 -type f | wc -l)"
if [[ "${uploaded}" -ne "${TARGET}" ]]; then
  echo -e "${RED}FAIL:${NC} expected ${TARGET} local files, found ${uploaded}" >&2
  exit 1
fi

echo "=== Preparing bucket ${BUCKET} ==="
aws --endpoint-url "${ENDPOINT}" s3 rb "s3://${BUCKET}" --force >/dev/null 2>&1 || true
aws --endpoint-url "${ENDPOINT}" s3 mb "s3://${BUCKET}" >/dev/null

echo "=== Starting parallel upload (s5cmd sync) ==="
kvrgw_s5cmd_bg "list-stress-sync-${BUCKET}" --numworkers 8 --endpoint-url "${ENDPOINT}" \
  sync "${KVRGW_DIR}/" "s3://${BUCKET}/"
UPLOAD_PID=$!

poll=0
final_count=0
while [[ "${poll}" -lt "${MAX_POLLS}" ]]; do
  poll=$((poll + 1))
  check_servers_alive

  verify_line="$(python3 "${ROOT}/scripts/list_verify.py" aws \
      --endpoint "${ENDPOINT}" --bucket "${BUCKET}" --max-count "${TARGET}" 2>&1)" || {
    echo "${verify_line}" >&2
    kill "${UPLOAD_PID}" 2>/dev/null || true
    wait "${UPLOAD_PID}" 2>/dev/null || true
    exit 1
  }

  final_count="$(echo "${verify_line}" | sed -n 's/.*count=\([0-9]*\).*/\1/p')"
  if [[ -z "${final_count}" ]]; then
    echo -e "${RED}FAIL:${NC} could not parse list count from: ${verify_line}" >&2
    exit 1
  fi

  echo "poll ${poll}: list count=${final_count}/${TARGET}"

  if [[ "${final_count}" -eq "${TARGET}" ]]; then
    break
  fi

  if ! kill -0 "${UPLOAD_PID}" 2>/dev/null; then
    kvrgw_s5cmd_wait "list-stress-sync-${BUCKET}" "${UPLOAD_PID}" || exit 1
    final_count="$(kvrgw_verify_count_after_sync "${BUCKET}" "${TARGET}" "list-stress-sync")" || exit 1
    break
  fi

  sleep "${POLL_SEC}"
done

if [[ "${final_count}" -ne "${TARGET}" ]]; then
  if kill -0 "${UPLOAD_PID}" 2>/dev/null; then
    echo -e "${RED}FAIL:${NC} timed out after ${poll} polls; last count=${final_count}" >&2
    kill "${UPLOAD_PID}" 2>/dev/null || true
    wait "${UPLOAD_PID}" 2>/dev/null || true
    exit 1
  fi
  kvrgw_s5cmd_wait "list-stress-sync-${BUCKET}" "${UPLOAD_PID}" || exit 1
  final_count="$(kvrgw_verify_count_after_sync "${BUCKET}" "${TARGET}" "list-stress-sync")" || exit 1
fi

if kill -0 "${UPLOAD_PID}" 2>/dev/null; then
  kvrgw_s5cmd_wait "list-stress-sync-${BUCKET}" "${UPLOAD_PID}" || exit 1
  if [[ "${final_count}" -ne "${TARGET}" ]]; then
    final_count="$(kvrgw_verify_count_after_sync "${BUCKET}" "${TARGET}" "list-stress-sync")" || exit 1
  fi
fi

check_servers_alive
python3 "${ROOT}/scripts/list_verify.py" aws \
  --endpoint "${ENDPOINT}" --bucket "${BUCKET}" --expected "${TARGET}"
python3 "${ROOT}/scripts/list_verify.py" s5cmd \
  --endpoint "${ENDPOINT}" --bucket "${BUCKET}" --expected "${TARGET}"

echo ""
echo -e "${GREEN}PASS: list stress test (${TARGET} keys, upload + concurrent listing, no crash)${NC}"
