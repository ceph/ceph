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
source "${ROOT}/scripts/kvrgw-common.sh"
SKIP_RELOAD=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-reload) SKIP_RELOAD=1; shift ;;
    -h|--help) echo "Usage: $0 [--skip-reload]"; exit 0 ;;
    *) echo "Unknown: $1" >&2; exit 2 ;;
  esac
done
if [[ "${SKIP_RELOAD}" -eq 0 ]]; then
  export KVRGW_GC_INTERVAL_SEC="${KVRGW_GC_INTERVAL_SEC:-1}"
  export KVRGW_SWEEPER_INTERVAL_SEC="${KVRGW_SWEEPER_INTERVAL_SEC:-2}"
  export KVRGW_SWEEPER_MIN_AGE_SEC="${KVRGW_SWEEPER_MIN_AGE_SEC:-1}"
  "${ROOT}/scripts/reload.sh" --clean "${KVRGW_INSTANCES}"
fi
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
TMPDIR="$(mktemp -d)"; BUCKET="mybucket-$$"
echo hello > "${TMPDIR}/file.txt"
aws --endpoint-url "${ENDPOINT}" s3 mb "s3://${BUCKET}"
aws --endpoint-url "${ENDPOINT}" s3 cp "${TMPDIR}/file.txt" "s3://${BUCKET}/key"
aws --endpoint-url "${ENDPOINT}" s3 cp "s3://${BUCKET}/key" "${TMPDIR}/download.txt"
aws --endpoint-url "${ENDPOINT}" s3api head-object --bucket "${BUCKET}" --key key
diff "${TMPDIR}/file.txt" "${TMPDIR}/download.txt"
echo "hello range-get" > "${TMPDIR}/range.txt"
aws --endpoint-url "${ENDPOINT}" s3 cp "${TMPDIR}/range.txt" "s3://${BUCKET}/range-key"
aws --endpoint-url "${ENDPOINT}" s3api get-object --bucket "${BUCKET}" --key range-key --range bytes=0-4 "${TMPDIR}/range-part.bin" >/dev/null
printf 'hello' | diff - "${TMPDIR}/range-part.bin"
command -v s5cmd >/dev/null && kvrgw_s5cmd_log "smoke-cp-range" --endpoint-url "${ENDPOINT}" cp "s3://${BUCKET}/range-key" "${TMPDIR}/range-s5.bin" && diff "${TMPDIR}/range.txt" "${TMPDIR}/range-s5.bin"
echo overwrite > "${TMPDIR}/file2.txt"
aws --endpoint-url "${ENDPOINT}" s3 cp "${TMPDIR}/file2.txt" "s3://${BUCKET}/key"
aws --endpoint-url "${ENDPOINT}" s3 ls "s3://${BUCKET}/"
aws --endpoint-url "${ENDPOINT}" s3 rm "s3://${BUCKET}/key"
aws --endpoint-url "${ENDPOINT}" s3 rm "s3://${BUCKET}/range-key"
for _ in $(seq 1 30); do [[ "$(find "${DATA_ROOT}" -maxdepth 1 -type f 2>/dev/null | wc -l)" -eq 0 ]] && break; sleep 1; done
aws --endpoint-url "${ENDPOINT}" s3 cp "${TMPDIR}/file.txt" "s3://${BUCKET}/hold"
aws --endpoint-url "${ENDPOINT}" s3 rb "s3://${BUCKET}" 2>/dev/null && { echo fail; exit 1; } || true
aws --endpoint-url "${ENDPOINT}" s3 rm "s3://${BUCKET}/hold"
aws --endpoint-url "${ENDPOINT}" s3 rb "s3://${BUCKET}"
kvrgw_test_list_buckets
rm -rf "${TMPDIR}"
echo "Smoke test passed."
