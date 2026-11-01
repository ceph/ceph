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
# Test: DeleteBucket must reject when V: entries exist, and succeed when all versions removed.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/scripts/kvrgw-common.sh"

export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
BUCKET="version-delete-bucket-$$"

aws_s3() { aws --endpoint-url "${ENDPOINT}" s3 "$@"; }
aws_api() { aws --endpoint-url "${ENDPOINT}" s3api "$@"; }

put_versioning() {
  aws_api put-bucket-versioning --bucket "${BUCKET}" \
    --versioning-configuration "Status=$1"
}

get_versioning() {
  aws_api get-bucket-versioning --bucket "${BUCKET}" --query 'Status' --output text
}

cleanup() {
  # Force-delete all versions then bucket
  local versions
  versions="$(aws_api list-object-versions --bucket "${BUCKET}" 2>/dev/null || true)"
  if [[ -n "${versions}" ]]; then
    echo "${versions}" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for v in data.get('Versions', []):
    print(v['Key'], v['VersionId'])
for d in data.get('DeleteMarkers', []):
    print(d['Key'], d['VersionId'])
" 2>/dev/null | while read -r key vid; do
      aws_api delete-object --bucket "${BUCKET}" --key "${key}" --version-id "${vid}" >/dev/null 2>&1 || true
    done
  fi
  aws_s3 rb "s3://${BUCKET}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Setup
echo "=== Creating bucket ${BUCKET} ==="
aws_s3 mb "s3://${BUCKET}" >/dev/null

echo "=== Enabling versioning ==="
put_versioning "Enabled"
state="$(get_versioning)"
[[ "${state}" == "Enabled" ]] || { echo -e "${RED}FAIL:${NC} versioning not Enabled, got '${state}'"; exit 1; }

echo "=== PUT v1 ==="
echo "content-v1" | aws_s3 cp - "s3://${BUCKET}/obj1"

echo "=== PUT v2 (displaces v1 to V:) ==="
echo "content-v2" | aws_s3 cp - "s3://${BUCKET}/obj1"

echo "=== DELETE without vid (writes DM to O:, displaces v2 to V:) ==="
aws_s3 rm "s3://${BUCKET}/obj1"

echo "=== Listing versions ==="
VERSIONS_JSON="$(aws_api list-object-versions --bucket "${BUCKET}")"
echo "${VERSIONS_JSON}" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for v in data.get('Versions', []):
    print(f\"  Version: key={v['Key']} vid={v['VersionId']} latest={v['IsLatest']}\")
for d in data.get('DeleteMarkers', []):
    print(f\"  DeleteMarker: key={d['Key']} vid={d['VersionId']} latest={d['IsLatest']}\")
"

# Extract version IDs
DM_VID="$(echo "${VERSIONS_JSON}" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for d in data.get('DeleteMarkers', []):
    if d['IsLatest']: print(d['VersionId']); break
")"
V_VIDS="$(echo "${VERSIONS_JSON}" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for v in data.get('Versions', []):
    print(v['VersionId'])
")"

echo "=== Removing DM (vid=${DM_VID}) ==="
aws_api delete-object --bucket "${BUCKET}" --key obj1 --version-id "${DM_VID}" >/dev/null

echo "=== TEST 1: DeleteBucket should fail (V: has versions) ==="
if aws_s3 rb "s3://${BUCKET}" 2>/dev/null; then
  echo -e "${RED}FAIL:${NC} DeleteBucket succeeded but V: entries exist"
  exit 1
fi
echo -e "${GREEN}PASS: DeleteBucket correctly rejected (BucketNotEmpty)${NC}"

echo "=== Removing all remaining versions ==="
for vid in ${V_VIDS}; do
  echo "  delete obj1 vid=${vid}"
  aws_api delete-object --bucket "${BUCKET}" --key obj1 --version-id "${vid}" >/dev/null
done

echo "=== TEST 2: DeleteBucket should succeed (V: empty) ==="
if ! aws_s3 rb "s3://${BUCKET}" 2>/dev/null; then
  echo -e "${RED}FAIL:${NC} DeleteBucket failed but bucket should be empty"
  exit 1
fi
echo -e "${GREEN}PASS: DeleteBucket succeeded after all versions removed${NC}"

echo ""
echo -e "${GREEN}PASS: delete_bucket_versioning test complete${NC}"
