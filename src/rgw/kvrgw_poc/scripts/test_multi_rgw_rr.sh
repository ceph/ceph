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
# Round-robin via GW: PUT once, many GETs; proves shared FDB + shared KVRGW_DATA.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/kvrgw-common.sh
source "${ROOT}/scripts/kvrgw-common.sh"

INSTANCES="${KVRGW_INSTANCES}"
kvrgw_refresh_live_indices
if [[ "${KVRGW_LIVE_COUNT}" -lt 2 ]]; then
  echo "SKIP: test_multi_rgw_rr (live=${KVRGW_LIVE_COUNT}; need >=2)"
  exit 0
fi
kvrgw_refresh_gw_live

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

BUCKET="kv-rr-$$"
KEY="obj"
TMP="$(mktemp -d)"
PAYLOAD="${TMP}/payload.bin"
GETS="${KVRGW_RR_GET_COUNT:-30}"

echo "=== Multi-RGW round-robin (configured=${INSTANCES}, live=${KVRGW_LIVE_COUNT}, GETs=${GETS}) ==="
head -c 4096 /dev/urandom > "${PAYLOAD}"

cleanup() {
  aws --endpoint-url "${ENDPOINT}" s3 rb "s3://${BUCKET}" --force >/dev/null 2>&1 || true
  rm -rf "${TMP}"
}
trap cleanup EXIT

aws --endpoint-url "${ENDPOINT}" s3 mb "s3://${BUCKET}" >/dev/null
aws --endpoint-url "${ENDPOINT}" s3 cp "${PAYLOAD}" "s3://${BUCKET}/${KEY}" >/dev/null

i=0
while [[ "${i}" -lt "${GETS}" ]]; do
  out="${TMP}/get-${i}.bin"
  aws --endpoint-url "${ENDPOINT}" s3 cp "s3://${BUCKET}/${KEY}" "${out}" >/dev/null
  diff -q "${PAYLOAD}" "${out}" >/dev/null || {
    echo -e "${RED}FAIL:${NC} GET ${i} content mismatch via GW ${ENDPOINT}" >&2
    exit 1
  }
  i=$((i + 1))
done

echo -e "${GREEN}PASS: ${GETS} GETs via GW after PUT (round-robin over ${KVRGW_LIVE_COUNT} live frontends)${NC}"
