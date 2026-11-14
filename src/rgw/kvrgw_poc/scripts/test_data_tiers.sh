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
TMPDIR="$(mktemp -d)"
trap 'rm -rf "${TMPDIR}"' EXIT
BUCKET="tier-test-$$"

aws --endpoint-url "${ENDPOINT}" s3 mb "s3://${BUCKET}"

pass() { echo -e "  ${GREEN}PASS: $1 ${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1 ${NC}" >&2; exit 1; }

wait_gc_to() {
  local target="${1:-0}"
  for _ in $(seq 1 30); do
    sleep 1
    local count
    count="$(find "${DATA_ROOT}" -maxdepth 1 -type f 2>/dev/null | wc -l)"
    [[ "${count}" -le "${target}" ]] && return 0
  done
  return 1
}

echo "=== Tier 1: inline (< 256B) ==="
FILE_COUNT_PRE_T1="$(find "${DATA_ROOT}" -maxdepth 1 -type f 2>/dev/null | wc -l)"
dd if=/dev/urandom bs=100 count=1 of="${TMPDIR}/t1.bin" 2>/dev/null
aws --endpoint-url "${ENDPOINT}" s3 cp "${TMPDIR}/t1.bin" "s3://${BUCKET}/tier1"
aws --endpoint-url "${ENDPOINT}" s3 cp "s3://${BUCKET}/tier1" "${TMPDIR}/t1-get.bin"
diff "${TMPDIR}/t1.bin" "${TMPDIR}/t1-get.bin" || fail "Tier 1 GET mismatch"
FILE_COUNT_POST_T1="$(find "${DATA_ROOT}" -maxdepth 1 -type f 2>/dev/null | wc -l)"
[[ "${FILE_COUNT_POST_T1}" -eq "${FILE_COUNT_PRE_T1}" ]] || fail "Tier 1 created a storage-tier file"
pass "Tier 1 PUT+GET"

echo "=== Tier 1: byte-range GET ==="
aws --endpoint-url "${ENDPOINT}" s3api get-object --bucket "${BUCKET}" --key tier1 --range bytes=0-9 "${TMPDIR}/t1-range.bin" >/dev/null
GOT_SIZE=$(wc -c < "${TMPDIR}/t1-range.bin")
[[ "${GOT_SIZE}" -eq 10 ]] || fail "Tier 1 range GET size=${GOT_SIZE} expected=10"
pass "Tier 1 byte-range"

echo "=== Tier 2: child C:D (256B - 8KB) ==="
FILE_COUNT_PRE_T2="$(find "${DATA_ROOT}" -maxdepth 1 -type f 2>/dev/null | wc -l)"
dd if=/dev/urandom bs=2048 count=1 of="${TMPDIR}/t2.bin" 2>/dev/null
aws --endpoint-url "${ENDPOINT}" s3 cp "${TMPDIR}/t2.bin" "s3://${BUCKET}/tier2"
aws --endpoint-url "${ENDPOINT}" s3 cp "s3://${BUCKET}/tier2" "${TMPDIR}/t2-get.bin"
diff "${TMPDIR}/t2.bin" "${TMPDIR}/t2-get.bin" || fail "Tier 2 GET mismatch"
FILE_COUNT_POST_T2="$(find "${DATA_ROOT}" -maxdepth 1 -type f 2>/dev/null | wc -l)"
[[ "${FILE_COUNT_POST_T2}" -eq "${FILE_COUNT_PRE_T2}" ]] || fail "Tier 2 created a storage-tier file"
pass "Tier 2 PUT+GET"

echo "=== Tier 2: byte-range GET ==="
aws --endpoint-url "${ENDPOINT}" s3api get-object --bucket "${BUCKET}" --key tier2 --range bytes=100-199 "${TMPDIR}/t2-range.bin" >/dev/null
GOT_SIZE=$(wc -c < "${TMPDIR}/t2-range.bin")
[[ "${GOT_SIZE}" -eq 100 ]] || fail "Tier 2 range GET size=${GOT_SIZE} expected=100"
pass "Tier 2 byte-range"

echo "=== Tier 3: storage tier (> 8KB) ==="
FILE_COUNT_PRE_T3="$(find "${DATA_ROOT}" -maxdepth 1 -type f 2>/dev/null | wc -l)"
dd if=/dev/urandom bs=16384 count=1 of="${TMPDIR}/t3.bin" 2>/dev/null
aws --endpoint-url "${ENDPOINT}" s3 cp "${TMPDIR}/t3.bin" "s3://${BUCKET}/tier3"
aws --endpoint-url "${ENDPOINT}" s3 cp "s3://${BUCKET}/tier3" "${TMPDIR}/t3-get.bin"
diff "${TMPDIR}/t3.bin" "${TMPDIR}/t3-get.bin" || fail "Tier 3 GET mismatch"
FILE_COUNT_POST_T3="$(find "${DATA_ROOT}" -maxdepth 1 -type f 2>/dev/null | wc -l)"
[[ "${FILE_COUNT_POST_T3}" -gt "${FILE_COUNT_PRE_T3}" ]] || fail "Tier 3 did not create a storage-tier file"
pass "Tier 3 PUT+GET"

echo "=== Overwrite: Tier 3 -> Tier 1 ==="
dd if=/dev/urandom bs=50 count=1 of="${TMPDIR}/ow-small.bin" 2>/dev/null
aws --endpoint-url "${ENDPOINT}" s3 cp "${TMPDIR}/ow-small.bin" "s3://${BUCKET}/tier3"
aws --endpoint-url "${ENDPOINT}" s3 cp "s3://${BUCKET}/tier3" "${TMPDIR}/ow-small-get.bin"
diff "${TMPDIR}/ow-small.bin" "${TMPDIR}/ow-small-get.bin" || fail "Overwrite T3->T1 GET mismatch"
pass "Overwrite Tier3->Tier1 PUT+GET"

echo "=== GC: old Tier 3 file removed ==="
wait_gc_to "${FILE_COUNT_PRE_T3}" || fail "GC did not remove old Tier 3 file"
pass "GC cleaned storage-tier file"

echo "=== Overwrite: Tier 1 -> Tier 2 ==="
dd if=/dev/urandom bs=1024 count=1 of="${TMPDIR}/ow-med.bin" 2>/dev/null
aws --endpoint-url "${ENDPOINT}" s3 cp "${TMPDIR}/ow-med.bin" "s3://${BUCKET}/tier3"
aws --endpoint-url "${ENDPOINT}" s3 cp "s3://${BUCKET}/tier3" "${TMPDIR}/ow-med-get.bin"
diff "${TMPDIR}/ow-med.bin" "${TMPDIR}/ow-med-get.bin" || fail "Overwrite T1->T2 GET mismatch"
pass "Overwrite Tier1->Tier2 PUT+GET"

echo "=== Overwrite: Tier 2 -> Tier 3 ==="
dd if=/dev/urandom bs=16384 count=1 of="${TMPDIR}/ow-big.bin" 2>/dev/null
aws --endpoint-url "${ENDPOINT}" s3 cp "${TMPDIR}/ow-big.bin" "s3://${BUCKET}/tier3"
aws --endpoint-url "${ENDPOINT}" s3 cp "s3://${BUCKET}/tier3" "${TMPDIR}/ow-big-get.bin"
diff "${TMPDIR}/ow-big.bin" "${TMPDIR}/ow-big-get.bin" || fail "Overwrite T2->T3 GET mismatch"
pass "Overwrite Tier2->Tier3 PUT+GET"

echo "=== DELETE all objects ==="
aws --endpoint-url "${ENDPOINT}" s3 rm "s3://${BUCKET}/tier1"
aws --endpoint-url "${ENDPOINT}" s3 rm "s3://${BUCKET}/tier2"
aws --endpoint-url "${ENDPOINT}" s3 rm "s3://${BUCKET}/tier3"

echo "=== GC: all storage-tier files cleaned ==="
wait_gc_to "${FILE_COUNT_PRE_T1}" || fail "GC did not clean all files"
pass "GC cleaned all storage-tier data"

echo "=== HEAD returns 404 for deleted objects ==="
aws --endpoint-url "${ENDPOINT}" s3api head-object --bucket "${BUCKET}" --key tier1 2>/dev/null && fail "tier1 HEAD should 404" || true
aws --endpoint-url "${ENDPOINT}" s3api head-object --bucket "${BUCKET}" --key tier2 2>/dev/null && fail "tier2 HEAD should 404" || true
aws --endpoint-url "${ENDPOINT}" s3api head-object --bucket "${BUCKET}" --key tier3 2>/dev/null && fail "tier3 HEAD should 404" || true
pass "Deleted objects return 404"

echo "=== Cleanup bucket ==="
aws --endpoint-url "${ENDPOINT}" s3 rb "s3://${BUCKET}"
pass "Bucket removed"

echo ""
echo "=== No-orphan test: Tier 1+2 never touch data/ ==="
BUCKET_NO="no-orphan-$$"
aws --endpoint-url "${ENDPOINT}" s3 mb "s3://${BUCKET_NO}"

# Upload Tier 1 and Tier 2 objects on an empty system (data/ should be empty)
FILE_COUNT_BEFORE="$(find "${DATA_ROOT}" -maxdepth 1 -type f 2>/dev/null | wc -l)"
for i in $(seq 1 10); do
  dd if=/dev/urandom bs=100 count=1 of="${TMPDIR}/t1-${i}.bin" 2>/dev/null
  aws --endpoint-url "${ENDPOINT}" s3 cp "${TMPDIR}/t1-${i}.bin" "s3://${BUCKET_NO}/small-${i}" --quiet
done
for i in $(seq 1 5); do
  dd if=/dev/urandom bs=4096 count=1 of="${TMPDIR}/t2-${i}.bin" 2>/dev/null
  aws --endpoint-url "${ENDPOINT}" s3 cp "${TMPDIR}/t2-${i}.bin" "s3://${BUCKET_NO}/med-${i}" --quiet
done
FILE_COUNT_AFTER="$(find "${DATA_ROOT}" -maxdepth 1 -type f 2>/dev/null | wc -l)"
[[ "${FILE_COUNT_AFTER}" -eq "${FILE_COUNT_BEFORE}" ]] || fail "Tier 1/2 uploads created files in data/ (before=${FILE_COUNT_BEFORE} after=${FILE_COUNT_AFTER})"
pass "Tier 1+2 objects did not create storage-tier files"

# Add some Tier 3 objects
for i in $(seq 1 3); do
  dd if=/dev/urandom bs=16384 count=1 of="${TMPDIR}/t3-${i}.bin" 2>/dev/null
  aws --endpoint-url "${ENDPOINT}" s3 cp "${TMPDIR}/t3-${i}.bin" "s3://${BUCKET_NO}/big-${i}" --quiet
done
FILE_COUNT_WITH_T3="$(find "${DATA_ROOT}" -maxdepth 1 -type f 2>/dev/null | wc -l)"
[[ "${FILE_COUNT_WITH_T3}" -ge 3 ]] || fail "Tier 3 uploads should have created files"
pass "Tier 3 objects created storage-tier files"

echo "=== No-orphan test: rm --recursive + rb leaves no data ==="
s3cmd -c "${S3CMD_CFG}" rm --recursive "s3://${BUCKET_NO}" >/dev/null 2>&1
aws --endpoint-url "${ENDPOINT}" s3 rb "s3://${BUCKET_NO}"
wait_gc_to "${FILE_COUNT_BEFORE}" || fail "GC did not clean all orphaned data"
FINAL_FILES="$(find "${DATA_ROOT}" -maxdepth 1 -type f 2>/dev/null | wc -l)"
[[ "${FINAL_FILES}" -le "${FILE_COUNT_BEFORE}" ]] || fail "Orphan files remain in data/ (count=${FINAL_FILES}, baseline=${FILE_COUNT_BEFORE})"
pass "No orphan data after full cleanup"

echo ""
echo "All data-tier tests passed."
