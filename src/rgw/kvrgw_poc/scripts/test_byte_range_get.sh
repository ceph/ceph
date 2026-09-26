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
# Upload 4MiB uint32-sequence object and verify random byte-range GETs.
#
# Usage: test_byte_range_get.sh [--skip-upload]
#   --skip-upload  Object already in bucket (still runs GET loop)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/kvrgw-common.sh
source "${ROOT}/scripts/kvrgw-common.sh"

SKIP_UPLOAD=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-upload) SKIP_UPLOAD=1; shift ;;
    -h|--help)
      echo "Usage: $(basename "$0") [--skip-upload]"
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

OBJECT_SIZE=$((4 * 1024 * 1024))
BUCKET="kv-byte-range-$$"
KEY="seq-u32-4mb"
TMPDIR="$(mktemp -d)"
GOLDEN="${TMPDIR}/golden.bin"
SEED="${KVRGW_BYTE_RANGE_SEED:-}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Required command not found: $1" >&2
    exit 1
  }
}

require_cmd aws
require_cmd python3

cleanup() {
  aws --endpoint-url "${ENDPOINT}" s3 rb "s3://${BUCKET}" --force >/dev/null 2>&1 || true
  rm -rf "${TMPDIR}"
}
trap cleanup EXIT

echo "=== Building 4MiB golden object (uint32 0..$((OBJECT_SIZE / 4 - 1))) ==="
python3 - "${GOLDEN}" <<'PY'
import struct, sys
UINT32_LE = "<I"
path = sys.argv[1]
with open(path, "wb") as f:
    for i in range(4 * 1024 * 1024 // 4):
        f.write(struct.pack(UINT32_LE, i))
PY

if [[ "$(stat -c%s "${GOLDEN}")" -ne "${OBJECT_SIZE}" ]]; then
  echo -e "${RED}FAIL:${NC} golden file size != ${OBJECT_SIZE}" >&2
  exit 1
fi

if [[ "${SKIP_UPLOAD}" -eq 0 ]]; then
  echo "=== Uploading to s3://${BUCKET}/${KEY} ==="
  aws --endpoint-url "${ENDPOINT}" s3 mb "s3://${BUCKET}" >/dev/null
  aws --endpoint-url "${ENDPOINT}" s3 cp "${GOLDEN}" "s3://${BUCKET}/${KEY}" >/dev/null
fi

echo "=== Random byte-range GET verification ==="
args=(--endpoint "${ENDPOINT}" --bucket "${BUCKET}" --key "${KEY}")
if [[ -n "${SEED}" ]]; then
  args+=(--seed "${SEED}")
fi
python3 "${ROOT}/scripts/test_byte_range_get.py" "${args[@]}"
