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
# Stress test: 1K buckets ListBuckets, objects inside buckets, concurrent 64K upload.
#
# 1. Create BUCKET_COUNT buckets, verify full list (count + strict name order).
# 2. Put OBJECTS_PER_BUCKET objects into each bucket; list buckets again (same set).
# 3. s5cmd sync 64K objects into bucket1 while polling ListBuckets in a loop.
#
# Usage: test_list_buckets_stress.sh [--skip-reload]

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

BUCKET_PREFIX="${KVRGW_LB_PREFIX:-lb-stress}"
BUCKET_COUNT="${KVRGW_LB_BUCKET_COUNT:-1000}"
OBJECTS_PER_BUCKET="${KVRGW_LB_OBJECTS_PER_BUCKET:-10}"
UPLOAD_BUCKET="${KVRGW_LB_UPLOAD_BUCKET:-bucket1}"
KVRGW_DIR="/tmp/kvrgw"
UPLOAD_TARGET=65536
PARALLEL="${KVRGW_LB_PARALLEL:-16}"
POLL_SEC="${KVRGW_LB_POLL_SEC:-1}"
MAX_POLLS="${KVRGW_LB_MAX_POLLS:-900}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Required command not found: $1" >&2
    exit 1
  }
}

require_cmd aws
require_cmd s5cmd
require_cmd python3

cleanup_all_buckets() {
    echo "=== Removing all existing buckets ==="
    aws --endpoint-url "${ENDPOINT}" s3api list-buckets --query 'Buckets[].Name' --output text \
        | tr '\t' '\n' \
        | grep -v '^$' \
        | xargs -P "${PARALLEL}" -n 1 -I {} aws --endpoint-url "${ENDPOINT}" s3 rb "s3://{}" --force >/dev/null 2>&1 || true
}

verify_list_buckets() {
  python3 "${ROOT}/scripts/list_verify.py" aws-buckets \
    --endpoint "${ENDPOINT}" \
    --prefix "${BUCKET_PREFIX}" \
    --expected "${BUCKET_COUNT}" \
    --check-creation-date
}

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

create_buckets() {
  echo "=== Creating ${BUCKET_COUNT} buckets (${BUCKET_PREFIX}-NNNN) ==="
  seq 1 "${BUCKET_COUNT}" | xargs -P "${PARALLEL}" -n 1 bash -c '
    i="$1"
    aws --endpoint-url "'"${ENDPOINT}"'" s3 mb \
      "s3://'"${BUCKET_PREFIX}"'-$(printf "%04d" "${i}")" >/dev/null
  ' _
}

put_objects() {
  echo "=== Writing ${OBJECTS_PER_BUCKET} objects to each of ${BUCKET_COUNT} buckets ==="
  seq 1 "${BUCKET_COUNT}" | xargs -P "${PARALLEL}" -n 1 bash -c '
    i="$1"
    bucket="'"${BUCKET_PREFIX}"'-$(printf "%04d" "${i}")"
    for o in $(seq -w 0 '"$((OBJECTS_PER_BUCKET - 1))"'); do
      printf "x" | aws --endpoint-url "'"${ENDPOINT}"'" s3 cp - "s3://${bucket}/obj-${o}" >/dev/null
    done
  ' _
}

if [[ "${SKIP_RELOAD}" -eq 0 ]]; then
  echo "=== Reloading kv-rgw (clean) ==="
  "${ROOT}/scripts/reload.sh" --clean

  echo "=== Creating ${UPLOAD_TARGET} test files for upload stress ==="
  "${ROOT}/scripts/mk_random_files.sh" -n 64K -s 1 "${KVRGW_DIR}"
else
  echo "=== Skipping reload (--skip-reload) ==="
  check_servers_alive || {
    echo "Hint: run ./scripts/reload.sh --clean first" >&2
    exit 1
  }
fi

uploaded="$(find "${KVRGW_DIR}" -maxdepth 1 -type f | wc -l)"
if [[ "${uploaded}" -ne "${UPLOAD_TARGET}" ]]; then
  echo -e "${RED}FAIL:${NC} expected ${UPLOAD_TARGET} local files in ${KVRGW_DIR}, found ${uploaded}" >&2
  exit 1
fi

create_buckets
verify_list_buckets
echo "=== Initial ListBuckets ok (${BUCKET_COUNT} buckets) ==="

put_objects
verify_list_buckets
echo "=== ListBuckets after ${OBJECTS_PER_BUCKET} objects/bucket still ok (${BUCKET_COUNT} buckets) ==="

echo "=== Preparing upload bucket ${UPLOAD_BUCKET} ==="
aws --endpoint-url "${ENDPOINT}" s3 rb "s3://${UPLOAD_BUCKET}" --force >/dev/null 2>&1 || true
aws --endpoint-url "${ENDPOINT}" s3 mb "s3://${UPLOAD_BUCKET}" >/dev/null

echo "=== Starting parallel upload (s5cmd sync ${UPLOAD_TARGET} objects) ==="
if [[ "${KVRGW_S5CMD_LOG_APPEND:-}" != 1 ]]; then
  kvrgw_s5cmd_init_log
fi
export KVRGW_S5CMD_LOG
kvrgw_s5cmd_bg "list-buckets-stress-sync-${UPLOAD_BUCKET}" --numworkers 8 --endpoint-url "${ENDPOINT}" \
  sync "${KVRGW_DIR}/" "s3://${UPLOAD_BUCKET}/"
UPLOAD_PID=$!

poll=0
while [[ "${poll}" -lt "${MAX_POLLS}" ]]; do
  poll=$((poll + 1))
  check_servers_alive

  verify_line="$(verify_list_buckets 2>&1)" || {
    echo "${verify_line}" >&2
    kill "${UPLOAD_PID}" 2>/dev/null || true
    wait "${UPLOAD_PID}" 2>/dev/null || true
    exit 1
  }

  echo "poll ${poll}: ${verify_line} (upload active=$(kill -0 "${UPLOAD_PID}" 2>/dev/null && echo yes || echo no))"

  if ! kill -0 "${UPLOAD_PID}" 2>/dev/null; then
    kvrgw_s5cmd_wait "list-buckets-stress-sync-${UPLOAD_BUCKET}" "${UPLOAD_PID}" || exit 1
    break
  fi

  sleep "${POLL_SEC}"
done

if kill -0 "${UPLOAD_PID}" 2>/dev/null; then
  echo -e "${RED}FAIL:${NC} timed out after ${poll} polls; upload still running" >&2
  kill "${UPLOAD_PID}" 2>/dev/null || true
  wait "${UPLOAD_PID}" 2>/dev/null || true
  exit 1
fi

check_servers_alive
verify_list_buckets

echo ""
echo -e "${GREEN}PASS: list-buckets stress (${BUCKET_COUNT} buckets, ${OBJECTS_PER_BUCKET} objs/bucket, concurrent ${UPLOAD_TARGET} upload)${NC}"
