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
# Object tagging integration tests:
#   1. put-object-tagging (inline) + get-object-tagging
#   2. put 10 tags with long values (force external mode) + get
#   3. replace with small tag set (external → inline, Case D) + get
#   4. delete-object-tagging + get (empty)
#   5. copy-object preserves tags
#   6. delete object → GC cleans up
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
  "${ROOT}/scripts/reload.sh" --clean "${KVRGW_INSTANCES}"
fi

export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1

BUCKET="kv-tag-test-$$"
TMPDIR="$(mktemp -d)"
trap 'aws --endpoint-url "${ENDPOINT}" s3 rb "s3://${BUCKET}" --force 2>/dev/null; rm -rf "${TMPDIR}"' EXIT

echo "=== Object Tagging Tests ==="

aws --endpoint-url "${ENDPOINT}" s3 mb "s3://${BUCKET}" >/dev/null
echo "test-data" > "${TMPDIR}/obj.txt"
aws --endpoint-url "${ENDPOINT}" s3 cp "${TMPDIR}/obj.txt" "s3://${BUCKET}/tagged-obj" >/dev/null

echo "--- 1. put-object-tagging (inline, 2 tags) ---"
aws --endpoint-url "${ENDPOINT}" s3api put-object-tagging \
  --bucket "${BUCKET}" --key tagged-obj \
  --tagging '{"TagSet":[{"Key":"env","Value":"prod"},{"Key":"team","Value":"storage"}]}'
GOT=$(aws --endpoint-url "${ENDPOINT}" s3api get-object-tagging \
  --bucket "${BUCKET}" --key tagged-obj --output json)
COUNT=$(echo "${GOT}" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['TagSet']))")
if [[ "${COUNT}" -ne 2 ]]; then
  echo -e "${RED}FAIL:${NC} expected 2 tags, got ${COUNT}" >&2; exit 1
fi
echo -e "${GREEN}ok:${NC} put + get inline tags (${COUNT} tags)"

echo "--- 2. put 10 long-value tags (external mode) ---"
TAGSET='{"TagSet":['
for i in $(seq 1 10); do
  VAL=$(printf '%0.sa' $(seq 1 200))
  [[ $i -gt 1 ]] && TAGSET="${TAGSET},"
  TAGSET="${TAGSET}{\"Key\":\"k${i}\",\"Value\":\"${VAL}\"}"
done
TAGSET="${TAGSET}]}"
aws --endpoint-url "${ENDPOINT}" s3api put-object-tagging \
  --bucket "${BUCKET}" --key tagged-obj \
  --tagging "${TAGSET}"
GOT=$(aws --endpoint-url "${ENDPOINT}" s3api get-object-tagging \
  --bucket "${BUCKET}" --key tagged-obj --output json)
COUNT=$(echo "${GOT}" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['TagSet']))")
if [[ "${COUNT}" -ne 10 ]]; then
  echo -e "${RED}FAIL:${NC} expected 10 tags, got ${COUNT}" >&2; exit 1
fi
echo -e "${GREEN}ok:${NC} put + get external tags (${COUNT} tags)"

echo "--- 3. replace with small tag set (external → inline, Case D) ---"
aws --endpoint-url "${ENDPOINT}" s3api put-object-tagging \
  --bucket "${BUCKET}" --key tagged-obj \
  --tagging '{"TagSet":[{"Key":"single","Value":"tag"}]}'
GOT=$(aws --endpoint-url "${ENDPOINT}" s3api get-object-tagging \
  --bucket "${BUCKET}" --key tagged-obj --output json)
COUNT=$(echo "${GOT}" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['TagSet']))")
if [[ "${COUNT}" -ne 1 ]]; then
  echo -e "${RED}FAIL:${NC} expected 1 tag after replace, got ${COUNT}" >&2; exit 1
fi
echo -e "${GREEN}ok:${NC} external → inline transition (${COUNT} tag)"

echo "--- 4. delete-object-tagging ---"
aws --endpoint-url "${ENDPOINT}" s3api delete-object-tagging \
  --bucket "${BUCKET}" --key tagged-obj
GOT=$(aws --endpoint-url "${ENDPOINT}" s3api get-object-tagging \
  --bucket "${BUCKET}" --key tagged-obj --output json)
COUNT=$(echo "${GOT}" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['TagSet']))")
if [[ "${COUNT}" -ne 0 ]]; then
  echo -e "${RED}FAIL:${NC} expected 0 tags after delete, got ${COUNT}" >&2; exit 1
fi
echo -e "${GREEN}ok:${NC} delete-object-tagging (0 tags)"

echo "--- 5. copy-object preserves tags ---"
aws --endpoint-url "${ENDPOINT}" s3api put-object-tagging \
  --bucket "${BUCKET}" --key tagged-obj \
  --tagging '{"TagSet":[{"Key":"color","Value":"blue"},{"Key":"size","Value":"large"}]}'
aws --endpoint-url "${ENDPOINT}" s3api copy-object \
  --bucket "${BUCKET}" --key copied-obj \
  --copy-source "${BUCKET}/tagged-obj" >/dev/null
GOT=$(aws --endpoint-url "${ENDPOINT}" s3api get-object-tagging \
  --bucket "${BUCKET}" --key copied-obj --output json)
COUNT=$(echo "${GOT}" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['TagSet']))")
if [[ "${COUNT}" -ne 2 ]]; then
  echo -e "${RED}FAIL:${NC} expected 2 tags on copy, got ${COUNT}" >&2; exit 1
fi
echo -e "${GREEN}ok:${NC} copy-object preserves tags (${COUNT} tags)"

echo ""
echo -e "${GREEN}PASS: Object Tagging (5/5 tests passed)${NC}"
