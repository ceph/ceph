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
# Paginated ListObjects integration test: 64K objects in bucket1, 1K prefixed objects in bucket2.
#
# Listing rules: KV(n) < KV(n+1), list count == upload count (no hard-coded key names).
#
# Usage: test_list_pagination.sh [--skip-reload]
#   --skip-reload  Assume kv-rgw is already running with data loaded

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
KVRGW_OTHER="/tmp/kvrgw-other"
BUCKET1="bucket1"
BUCKET2="bucket2"
LIST_VERIFY="${ROOT}/scripts/list_verify.py"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command not found: $1" >&2
    exit 1
  fi
}

require_cmd aws
require_cmd s5cmd
require_cmd s3cmd
require_cmd python3

list_bucket_pages() {
  local bucket="$1"
  local max_keys="$2"
  local out_dir="$3"
  local prefix="${4:-}"

  if [[ "${max_keys}" -gt 1000 ]]; then
    echo -e "${RED}FAIL:${NC} max-keys ${max_keys} > 1000 (AWS/gofakes3 limit)" >&2
    exit 1
  fi

  mkdir -p "${out_dir}"
  rm -f "${out_dir}"/*

  local token=""
  local page=0

  while true; do
    page=$((page + 1))
    local args=(
      --endpoint-url "${ENDPOINT}"
      s3api list-objects-v2
      --bucket "${bucket}"
      --max-keys "${max_keys}"
      --output json
    )
    if [[ -n "${prefix}" ]]; then
      args+=(--prefix "${prefix}")
    fi
    if [[ -n "${token}" ]]; then
      args+=(--continuation-token "${token}")
    fi

    aws "${args[@]}" > "${out_dir}/page-$(printf '%04d' "${page}").json"

    local truncated
    truncated="$(python3 - "${out_dir}/page-$(printf '%04d' "${page}").json" <<'PY'
import json, sys
print("true" if json.load(open(sys.argv[1])).get("IsTruncated") else "false")
PY
)"

    if [[ "${truncated}" != "true" ]]; then
      break
    fi

    token="$(python3 - "${out_dir}/page-$(printf '%04d' "${page}").json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1])).get("NextContinuationToken") or "")
PY
)"
    if [[ -z "${token}" ]]; then
      echo "IsTruncated but no NextContinuationToken on page ${page}" >&2
      exit 1
    fi
  done

  echo "${page}"
}

verify_paginated_aws() {
  local bucket="$1"
  local max_keys="$2"
  local expected="$3"
  local prefix="${4:-}"
  local tmp pages

  tmp="$(mktemp -d)"
  pages="$(list_bucket_pages "${bucket}" "${max_keys}" "${tmp}" "${prefix}")"
  echo "${bucket} max-keys=${max_keys} prefix=${prefix:-*}: ${pages} page(s)"

  python3 "${LIST_VERIFY}" pages --dir "${tmp}" --expected "${expected}"
  rm -rf "${tmp}"
}

if [[ "${SKIP_RELOAD}" -eq 0 ]]; then
  echo "=== Reloading kv-rgw (clean) ==="
  "${ROOT}/scripts/reload.sh" --clean

  echo "=== Creating test files ==="
  "${ROOT}/scripts/mk_random_files.sh" -n 64K -s 1 "${KVRGW_DIR}"
  "${ROOT}/scripts/mk_random_files.sh" -n 1000 -s 1 -p other "${KVRGW_OTHER}"

  EXPECTED1="$(find "${KVRGW_DIR}" -maxdepth 1 -type f | wc -l)"
  EXPECTED2="$(find "${KVRGW_OTHER}" -maxdepth 1 -type f | wc -l)"

  echo "=== Uploading via s5cmd ==="
  if [[ "${KVRGW_S5CMD_LOG_APPEND:-}" != 1 ]]; then
    kvrgw_s5cmd_init_log
  fi
  export KVRGW_S5CMD_LOG
  kvrgw_s5cmd_log "pagination-mb-${BUCKET1}" --numworkers 8 --endpoint-url "${ENDPOINT}" mb "s3://${BUCKET1}"
  kvrgw_s5cmd_log "pagination-mb-${BUCKET2}" --numworkers 8 --endpoint-url "${ENDPOINT}" mb "s3://${BUCKET2}"
  kvrgw_s5cmd_log "pagination-sync-${BUCKET1}" --numworkers 8 --endpoint-url "${ENDPOINT}" sync "${KVRGW_DIR}/" "s3://${BUCKET1}/"
  kvrgw_s5cmd_log "pagination-sync-${BUCKET2}" --numworkers 8 --endpoint-url "${ENDPOINT}" sync "${KVRGW_OTHER}/" "s3://${BUCKET2}/"
else
  EXPECTED1="$(find "${KVRGW_DIR}" -maxdepth 1 -type f 2>/dev/null | wc -l)"
  EXPECTED2="$(find "${KVRGW_OTHER}" -maxdepth 1 -type f 2>/dev/null | wc -l)"
  if [[ "${EXPECTED1}" -eq 0 || "${EXPECTED2}" -eq 0 ]]; then
    echo -e "${RED}FAIL:${NC} --skip-reload but ${KVRGW_DIR} or ${KVRGW_OTHER} empty" >&2
    exit 1
  fi
  echo "=== Skipping reload (--skip-reload); verifying existing data ==="
fi

echo "=== Verifying paginated listings (aws s3api, max-keys ≤1000 — gofakes3 cap) ==="
for mk in 500 1000; do
  verify_paginated_aws "${BUCKET1}" "${mk}" "${EXPECTED1}"
done
verify_paginated_aws "${BUCKET2}" 500 "${EXPECTED2}" "other-"

echo "=== Verifying full listings (aws + s3cmd, delimiter=/ path) ==="
python3 "${LIST_VERIFY}" aws --endpoint "${ENDPOINT}" --bucket "${BUCKET1}" --expected "${EXPECTED1}"
python3 "${LIST_VERIFY}" s3cmd --config "${S3CMD_CFG}" --bucket "${BUCKET1}" --expected "${EXPECTED1}"
python3 "${LIST_VERIFY}" s5cmd --endpoint "${ENDPOINT}" --bucket "${BUCKET1}" --expected "${EXPECTED1}"
python3 "${LIST_VERIFY}" aws --endpoint "${ENDPOINT}" --bucket "${BUCKET2}" --prefix "other-" --expected "${EXPECTED2}"
python3 "${LIST_VERIFY}" s3cmd --config "${S3CMD_CFG}" --bucket "${BUCKET2}" --prefix "other-" --expected "${EXPECTED2}"
python3 "${LIST_VERIFY}" s5cmd --endpoint "${ENDPOINT}" --bucket "${BUCKET2}" --prefix "other-" --expected "${EXPECTED2}"

echo ""
echo -e "${GREEN}PASS: ListObjects pagination test complete${NC}"
echo "  bucket1: ${EXPECTED1} keys"
echo "  bucket2: ${EXPECTED2} keys (prefix other-)"
