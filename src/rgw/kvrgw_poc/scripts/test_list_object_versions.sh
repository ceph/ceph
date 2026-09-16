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
# Comprehensive ListObjectVersions test covering all versioning states.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/scripts/kvrgw-common.sh"

export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
BUCKET="list-versions-$$"

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

PASS=0
FAIL=0

check() {
  local label="$1" expected="$2" actual="$3"
  if [[ "${actual}" == "${expected}" ]]; then
    echo -e "  ${GREEN}PASS: ${label} (${actual}) ${NC}"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: ${label}: expected=${expected}, got=${actual}"
    FAIL=$((FAIL + 1))
  fi
}

list_versions_json() {
  aws_api list-object-versions --bucket "${BUCKET}" "$@" --output json 2>&1
}

echo "=== Setup: create bucket ==="
aws_s3 mb "s3://${BUCKET}" >/dev/null

echo ""
echo "=== CASE 1: Empty bucket (versioning enabled) ==="
aws_api put-bucket-versioning --bucket "${BUCKET}" --versioning-configuration Status=Enabled
JSON="$(list_versions_json)"
VER_COUNT="$(echo "${JSON}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d.get('Versions',[])))")"
check "empty bucket has 0 versions" "0" "${VER_COUNT}"

echo ""
echo "=== CASE 2: Single key, multiple versions ==="
TMP="$(mktemp)"
echo -n "v1" > "${TMP}"; aws_api put-object --bucket "${BUCKET}" --key obj1 --body "${TMP}" >/dev/null
echo -n "v2" > "${TMP}"; aws_api put-object --bucket "${BUCKET}" --key obj1 --body "${TMP}" >/dev/null
echo -n "v3" > "${TMP}"; aws_api put-object --bucket "${BUCKET}" --key obj1 --body "${TMP}" >/dev/null
JSON="$(list_versions_json)"
VER_COUNT="$(echo "${JSON}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d.get('Versions',[])))")"
IS_LATEST="$(echo "${JSON}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['Versions'][0]['IsLatest'])")"
check "3 versions for obj1" "3" "${VER_COUNT}"
check "first version is latest" "True" "${IS_LATEST}"

echo ""
echo "=== CASE 3: Delete creates DM ==="
aws_s3 rm "s3://${BUCKET}/obj1" >/dev/null 2>&1
JSON="$(list_versions_json)"
DM_COUNT="$(echo "${JSON}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d.get('DeleteMarkers',[])))")"
VER_COUNT="$(echo "${JSON}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d.get('Versions',[])))")"
check "1 delete marker" "1" "${DM_COUNT}"
check "3 versions remain" "3" "${VER_COUNT}"

echo ""
echo "=== CASE 4: Multiple keys ==="
echo -n "a1" > "${TMP}"; aws_api put-object --bucket "${BUCKET}" --key keyA --body "${TMP}" >/dev/null
echo -n "a2" > "${TMP}"; aws_api put-object --bucket "${BUCKET}" --key keyA --body "${TMP}" >/dev/null
echo -n "b1" > "${TMP}"; aws_api put-object --bucket "${BUCKET}" --key keyB --body "${TMP}" >/dev/null
JSON="$(list_versions_json)"
TOTAL="$(echo "${JSON}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d.get('Versions',[])))")"
check "total versions (3+2+1)=6" "6" "${TOTAL}"

echo ""
echo "=== CASE 5: Suspended + version_id=0 (null version) ==="
# Clean up first
aws_api delete-objects --bucket "${BUCKET}" --delete "$(echo "${JSON}" | python3 -c "
import json,sys
d=json.load(sys.stdin)
objs=[]
for v in d.get('Versions',[]):
    objs.append({'Key':v['Key'],'VersionId':v['VersionId']})
for dm in d.get('DeleteMarkers',[]):
    objs.append({'Key':dm['Key'],'VersionId':dm['VersionId']})
print(json.dumps({'Objects':objs}))
")" >/dev/null 2>&1 || true

# Versioning enabled: PUT creates versioned object
echo -n "enabled-v1" > "${TMP}"; aws_api put-object --bucket "${BUCKET}" --key sus-obj --body "${TMP}" >/dev/null
# Suspend versioning
aws_api put-bucket-versioning --bucket "${BUCKET}" --versioning-configuration Status=Suspended
# PUT while suspended: creates null version (version_id=0 → "null")
echo -n "suspended-v1" > "${TMP}"; aws_api put-object --bucket "${BUCKET}" --key sus-obj --body "${TMP}" >/dev/null

JSON="$(list_versions_json)"
SUS_RESULT="$(echo "${JSON}" | python3 -c "
import json,sys
d=json.load(sys.stdin)
versions = d.get('Versions',[])
print(len(versions))
for v in versions:
    print(f\"  key={v['Key']} vid={v['VersionId']} latest={v['IsLatest']}\")
")"
SUS_COUNT="$(echo "${SUS_RESULT}" | head -1)"
check "suspended: 2 versions (1 versioned + 1 null)" "2" "${SUS_COUNT}"
echo "${SUS_RESULT}" | tail -n +2

echo ""
echo "=== CASE 6: Pre-versioning object (PUT before enable) ==="
# New bucket for clean test
BUCKET2="list-versions-pre-$$"
aws_s3 mb "s3://${BUCKET2}" >/dev/null
echo -n "pre-ver" > "${TMP}"; aws_api put-object --bucket "${BUCKET2}" --key pre-obj --body "${TMP}" >/dev/null
aws_api put-bucket-versioning --bucket "${BUCKET2}" --versioning-configuration Status=Enabled
JSON2="$(aws_api list-object-versions --bucket "${BUCKET2}" --output json)"
PRE_COUNT="$(echo "${JSON2}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d.get('Versions',[])))")"
PRE_VID="$(echo "${JSON2}" | python3 -c "import json,sys; d=json.load(sys.stdin); v=d.get('Versions',[]); print(v[0]['VersionId'] if v else 'NONE')")"
check "pre-versioning object listed" "1" "${PRE_COUNT}"
check "pre-versioning object has null versionId" "null" "${PRE_VID}"
# Cleanup bucket2
aws_api delete-object --bucket "${BUCKET2}" --key pre-obj --version-id "${PRE_VID}" >/dev/null 2>&1 || true
aws_s3 rb "s3://${BUCKET2}" >/dev/null 2>&1 || true

echo ""
echo "=== CASE 7: Prefix filtering ==="
aws_api put-bucket-versioning --bucket "${BUCKET}" --versioning-configuration Status=Enabled
echo -n "x" > "${TMP}"; aws_api put-object --bucket "${BUCKET}" --key "dir/file1" --body "${TMP}" >/dev/null
echo -n "y" > "${TMP}"; aws_api put-object --bucket "${BUCKET}" --key "dir/file2" --body "${TMP}" >/dev/null
echo -n "z" > "${TMP}"; aws_api put-object --bucket "${BUCKET}" --key "other" --body "${TMP}" >/dev/null
JSON="$(aws_api list-object-versions --bucket "${BUCKET}" --prefix "dir/" --output json)"
PREFIX_COUNT="$(echo "${JSON}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d.get('Versions',[])))")"
check "prefix dir/ returns 2" "2" "${PREFIX_COUNT}"

echo ""
echo "=== CASE 8: Pagination (MaxKeys=2) ==="
JSON="$(aws_api list-object-versions --bucket "${BUCKET}" --max-keys 2 --no-paginate --output json)"
PAGE_TRUNC="$(echo "${JSON}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('IsTruncated',False))")"
PAGE_COUNT="$(echo "${JSON}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d.get('Versions',[]))+len(d.get('DeleteMarkers',[])))")"
check "page 1 is truncated" "True" "${PAGE_TRUNC}"
check "page 1 has <=2 entries" "2" "${PAGE_COUNT}"

PAGE_WALK="$(python3 -c "
import json, subprocess, os
endpoint = os.environ.get('ENDPOINT', 'http://127.0.0.1:9080')
bucket = '${BUCKET}'

def list_page(key_marker=None, vid_marker=None):
    cmd = ['aws', '--endpoint-url', endpoint, 's3api', 'list-object-versions',
           '--bucket', bucket, '--max-keys', '2', '--no-paginate', '--output', 'json']
    if key_marker:
        cmd += ['--key-marker', key_marker, '--version-id-marker', vid_marker]
    return json.loads(subprocess.check_output(cmd))

entries = []
seen = set()
d = list_page()
pages = 0
while True:
    pages += 1
    for v in d.get('Versions', []) + d.get('DeleteMarkers', []):
        tup = (v['Key'], v['VersionId'])
        if tup in seen:
            raise SystemExit(f'dup {tup}')
        seen.add(tup)
        entries.append(tup)
    if not d.get('IsTruncated'):
        break
    km = d.get('NextKeyMarker')
    vm = d.get('NextVersionIdMarker')
    if not km:
        raise SystemExit('truncated without NextKeyMarker')
    d = list_page(km, vm)
    if pages > 1000:
        raise SystemExit('too many pages')

full = json.loads(subprocess.check_output(
    ['aws', '--endpoint-url', endpoint, 's3api', 'list-object-versions',
     '--bucket', bucket, '--no-paginate', '--output', 'json']))
full_entries = [(v['Key'], v['VersionId']) for v in full.get('Versions', []) + full.get('DeleteMarkers', [])]
print(f'pages={pages} paged={len(entries)} full={len(full_entries)}')
if entries != full_entries:
    raise SystemExit(f'order/count mismatch paged={entries} full={full_entries}')
print('ok')
")"
check "paged listing matches full listing" "ok" "$(echo "${PAGE_WALK}" | tail -1)"
echo "  ${PAGE_WALK}"

rm -f "${TMP}"

echo ""
echo "========================="
echo "PASSED: ${PASS}  FAILED: ${FAIL}"
if [[ "${FAIL}" -gt 0 ]]; then
  echo -e "${RED}FAIL:${NC} list_object_versions test"
  exit 1
fi
echo -e "${GREEN}PASS: list_object_versions test complete${NC}"
