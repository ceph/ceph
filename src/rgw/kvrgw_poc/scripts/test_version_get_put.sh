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
# Test: PutObject returns x-amz-version-id when versioning is enabled,
# and GetObject with ?versionId returns the correct version content.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/scripts/kvrgw-common.sh"

export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
BUCKET="version-getput-$$"

aws_s3() { aws --endpoint-url "${ENDPOINT}" s3 "$@"; }
aws_api() { aws --endpoint-url "${ENDPOINT}" s3api "$@"; }

cleanup() {
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

echo "=== Setup: create bucket + enable versioning ==="
aws_s3 mb "s3://${BUCKET}" >/dev/null
aws_api put-bucket-versioning --bucket "${BUCKET}" \
  --versioning-configuration Status=Enabled

echo "=== TEST 1: PutObject returns VersionId ==="
TMP1="$(mktemp)"; echo -n "content-v1" > "${TMP1}"
V1_RESP="$(aws_api put-object --bucket "${BUCKET}" --key obj1 --body "${TMP1}" --output json)"
V1_VID="$(echo "${V1_RESP}" | python3 -c "import json,sys; print(json.load(sys.stdin).get('VersionId',''))")"
if [[ -z "${V1_VID}" ]]; then
  echo -e "${RED}FAIL:${NC} PutObject v1 did not return VersionId"
  echo "Response: ${V1_RESP}"
  rm -f "${TMP1}"
  exit 1
fi
echo -e "${GREEN}PASS: PutObject v1 returned VersionId=${V1_VID} ${NC}"

TMP2="$(mktemp)"; echo -n "content-v2" > "${TMP2}"
V2_RESP="$(aws_api put-object --bucket "${BUCKET}" --key obj1 --body "${TMP2}" --output json)"
V2_VID="$(echo "${V2_RESP}" | python3 -c "import json,sys; print(json.load(sys.stdin).get('VersionId',''))")"
if [[ -z "${V2_VID}" ]]; then
  echo -e "${RED}FAIL:${NC} PutObject v2 did not return VersionId"
  echo "Response: ${V2_RESP}"
  rm -f "${TMP1}" "${TMP2}"
  exit 1
fi
echo -e "${GREEN}PASS: PutObject v2 returned VersionId=${V2_VID} ${NC}"
rm -f "${TMP1}" "${TMP2}"

[[ "${V1_VID}" != "${V2_VID}" ]] || { echo -e "${RED}FAIL:${NC} v1 and v2 have same VersionId"; exit 1; }
echo -e "${GREEN}PASS: v1 and v2 have different VersionIds (${V1_VID} vs ${V2_VID}) ${NC}"

echo "=== TEST 2: GetObject without versionId returns latest (v2) ==="
TMPOUT="$(mktemp)"
aws_api get-object --bucket "${BUCKET}" --key obj1 "${TMPOUT}" >/dev/null 2>&1
LATEST="$(cat "${TMPOUT}")"
[[ "${LATEST}" == "content-v2" ]] || { echo -e "${RED}FAIL:${NC} latest content='${LATEST}', expected 'content-v2'"; rm -f "${TMPOUT}"; exit 1; }
echo -e "${GREEN}PASS: GET without versionId returns v2 content ${NC}"

echo "=== TEST 3: GetObject with versionId=V1 returns v1 content ==="
aws_api get-object --bucket "${BUCKET}" --key obj1 --version-id "${V1_VID}" "${TMPOUT}" >/dev/null 2>&1
GOT_V1="$(cat "${TMPOUT}")"
if [[ "${GOT_V1}" != "content-v1" ]]; then
  echo -e "${RED}FAIL:${NC} GET versionId=${V1_VID} returned '${GOT_V1}', expected 'content-v1'"
  rm -f "${TMPOUT}"
  exit 1
fi
echo -e "${GREEN}PASS: GET with versionId=${V1_VID} returns v1 content ${NC}"

echo "=== TEST 4: GetObject with versionId=V2 returns v2 content ==="
aws_api get-object --bucket "${BUCKET}" --key obj1 --version-id "${V2_VID}" "${TMPOUT}" >/dev/null 2>&1
GOT_V2="$(cat "${TMPOUT}")"
if [[ "${GOT_V2}" != "content-v2" ]]; then
  echo -e "${RED}FAIL:${NC} GET versionId=${V2_VID} returned '${GOT_V2}', expected 'content-v2'"
  rm -f "${TMPOUT}"
  exit 1
fi
echo -e "${GREEN}PASS: GET with versionId=${V2_VID} returns v2 content ${NC}"
rm -f "${TMPOUT}"

echo "=== TEST 5: HeadObject with versionId=V1 returns v1 metadata ==="
HEAD_V1="$(aws_api head-object --bucket "${BUCKET}" --key obj1 --version-id "${V1_VID}" --output json 2>&1)"
HEAD_V1_SIZE="$(echo "${HEAD_V1}" | python3 -c "import json,sys; print(json.load(sys.stdin).get('ContentLength',0))")"
if [[ "${HEAD_V1_SIZE}" != "10" ]]; then
  echo -e "${RED}FAIL:${NC} HEAD versionId=${V1_VID} ContentLength=${HEAD_V1_SIZE}, expected 10"
  exit 1
fi
echo -e "${GREEN}PASS: HEAD with versionId=${V1_VID} returns correct size (10) ${NC}"

echo "=== TEST 6: HeadObject with versionId=V2 returns v2 metadata ==="
HEAD_V2="$(aws_api head-object --bucket "${BUCKET}" --key obj1 --version-id "${V2_VID}" --output json 2>&1)"
HEAD_V2_SIZE="$(echo "${HEAD_V2}" | python3 -c "import json,sys; print(json.load(sys.stdin).get('ContentLength',0))")"
if [[ "${HEAD_V2_SIZE}" != "10" ]]; then
  echo -e "${RED}FAIL:${NC} HEAD versionId=${V2_VID} ContentLength=${HEAD_V2_SIZE}, expected 10"
  exit 1
fi
echo -e "${GREEN}PASS: HEAD with versionId=${V2_VID} returns correct size (10) ${NC}"

echo "=== TEST 7: DeleteObjects with per-object VersionId (batch) ==="
TMP3="$(mktemp)"; echo -n "content-v3" > "${TMP3}"
V3_RESP="$(aws_api put-object --bucket "${BUCKET}" --key obj2 --body "${TMP3}" --output json)"
V3_VID="$(echo "${V3_RESP}" | python3 -c "import json,sys; print(json.load(sys.stdin).get('VersionId',''))")"
rm -f "${TMP3}"
[[ -n "${V3_VID}" ]] || { echo -e "${RED}FAIL:${NC} PutObject obj2 did not return VersionId"; exit 1; }

DELETE_RESP="$(aws_api delete-objects --bucket "${BUCKET}" --delete "{\"Objects\":[{\"Key\":\"obj1\",\"VersionId\":\"${V1_VID}\"},{\"Key\":\"obj2\",\"VersionId\":\"${V3_VID}\"}]}" --output json 2>&1)"
DEL_COUNT="$(echo "${DELETE_RESP}" | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('Deleted',[])))")"
if [[ "${DEL_COUNT}" != "2" ]]; then
  echo -e "${RED}FAIL:${NC} DeleteObjects batch returned ${DEL_COUNT} deleted, expected 2"
  echo "Response: ${DELETE_RESP}"
  exit 1
fi
echo -e "${GREEN}PASS: DeleteObjects with per-object VersionId deleted 2 objects ${NC}"

echo ""
echo -e "${GREEN}PASS: version_get_put test complete (PutObject VersionId + GetObject/HeadObject ?versionId + DeleteObjects batch) ${NC}"
