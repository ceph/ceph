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
# Test: ListObjects v1 (s3cmd marker) and v2 (s5cmd/aws continuation_token)
# both paginate correctly over >1000 objects.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/scripts/kvrgw-common.sh"

export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
BUCKET="list-v1v2-$$"
EXPECTED=1024
DIR="/tmp/kv-list-v1v2-$$"

echo "=== Setup: creating ${EXPECTED} files ==="
mkdir -p "${DIR}"
for i in $(seq 1 ${EXPECTED}); do
  printf 'x' > "${DIR}/$(printf 'obj-%04d' "${i}")"
done
actual="$(find "${DIR}" -maxdepth 1 -type f | wc -l)"
[[ "${actual}" -eq "${EXPECTED}" ]] || { echo -e "${RED}FAIL:${NC} created ${actual}, expected ${EXPECTED}"; exit 1; }

echo "=== Upload to s3://${BUCKET} ==="
s5cmd --endpoint-url "${ENDPOINT}" mb "s3://${BUCKET}"
s5cmd --endpoint-url "${ENDPOINT}" sync "${DIR}/" "s3://${BUCKET}/" >/dev/null

echo "=== TEST 1: s5cmd ls (ListObjectsV2) ==="
count_v2="$(s5cmd --endpoint-url "${ENDPOINT}" ls "s3://${BUCKET}/*" | wc -l)"
if [[ "${count_v2}" -ne "${EXPECTED}" ]]; then
  echo -e "${RED}FAIL:${NC} s5cmd listed ${count_v2}, expected ${EXPECTED}"
  exit 1
fi
echo -e "${GREEN}PASS: v2 listing = ${count_v2}${NC}"

echo "=== TEST 2: s3cmd ls (ListObjects v1 with marker pagination) ==="
count_v1="$(s3cmd -c "${ROOT}/s3cmd.cfg" ls "s3://${BUCKET}" | wc -l)"
if [[ "${count_v1}" -ne "${EXPECTED}" ]]; then
  echo -e "${RED}FAIL:${NC} s3cmd listed ${count_v1}, expected ${EXPECTED}"
  exit 1
fi
echo -e "${GREEN}PASS: v1 listing = ${count_v1}${NC}"

echo "=== TEST 3: s3cmd rb --recursive (v1 pagination during delete) ==="
s3cmd -c "${ROOT}/s3cmd.cfg" rb --recursive "s3://${BUCKET}" >/dev/null 2>&1
echo -e "${GREEN}PASS: recursive bucket remove succeeded${NC}"

rm -rf "${DIR}"
echo ""
echo -e "${GREEN}PASS: list_v1_v2 test complete (${EXPECTED} objects, both APIs)${NC}"
