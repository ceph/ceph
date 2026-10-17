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
# Explicit HeadBucket (HTTP HEAD / aws s3api head-bucket) tests.
#
# 1. head-bucket on missing bucket → 404
# 2. mb; head-bucket → 200
# 2.5 resolve tenant_id from FDB T key (tenant name)
# 3. rb; head-bucket → 404
# 4. mb; delete B key via FDB (simulate other RGW); head-bucket → 404 (stale cache)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/kvrgw-common.sh
source "${ROOT}/scripts/kvrgw-common.sh"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

BUCKET="kv-head-bucket-$$"
TENANT_NAME="${KVRGW_TENANT_NAME:-kv-poc}"
TENANT_ID=""

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Required command not found: $1" >&2
    exit 1
  }
}

require_cmd aws
require_cmd python3
require_cmd curl

head_bucket_http_code() {
  local code
  code="$(aws --endpoint-url "${ENDPOINT}" s3api head-bucket --bucket "${BUCKET}" 2>&1 && echo 200 || echo "$?")"
  if echo "${code}" | grep -q "200"; then echo 200
  elif echo "${code}" | grep -q "(404)"; then echo 404
  elif echo "${code}" | grep -q "(403)"; then echo 403
  elif echo "${code}" | grep -q "(301)"; then echo 301
  else echo 000; fi
}

expect_head_bucket() {
  local want="$1"
  local label="$2"
  local code
  code="$(head_bucket_http_code)"
  if [[ "${code}" != "${want}" ]]; then
    echo -e "${RED}FAIL:${NC} ${label}: HEAD ${BUCKET} HTTP ${code}, expected ${want}" >&2
    exit 1
  fi
  echo -e "${GREEN}ok:${NC} ${label} (HTTP ${code})"
}

fdb_tenant_id_from_name() {
  local tenant_name="$1"
  TENANT_ID="$(
    FDB_CLI="${FDB_CLI}" FDB_CLUSTER_FILE="${FDB_CLUSTER_FILE}" \
      python3 - "${tenant_name}" <<'PY'
import os
import struct
import subprocess
import sys

tenant_name = sys.argv[1]
key = b"T" + tenant_name.encode("utf-8")
hexesc = "".join(f"\\x{b:02x}" for b in key)
cluster = os.environ["FDB_CLUSTER_FILE"]
fdb = os.environ["FDB_CLI"]
out = subprocess.check_output(
    [fdb, "-C", cluster, "--exec", f"get {hexesc}"],
    stderr=subprocess.STDOUT,
    text=True,
)
for line in out.splitlines():
    if "is `" not in line:
        continue
    raw = line.split("' is `", 1)[1].rstrip("\n").rstrip("'")
    buf = bytearray()
    i = 0
    while i < len(raw):
        if raw[i : i + 2] == "\\x" and i + 3 < len(raw):
            buf.append(int(raw[i + 2 : i + 4], 16))
            i += 4
        else:
            buf.append(ord(raw[i]))
            i += 1
    if len(buf) >= 4:
        print(struct.unpack(">I", buf[0:4])[0])
        sys.exit(0)
sys.exit(1)
PY
  )" || {
    echo -e "${RED}FAIL:${NC} no T key for tenant_name=${tenant_name}" >&2
    return 1
  }
  echo "resolved tenant_id=${TENANT_ID} for tenant_name=${tenant_name} from FDB T key"
}

fdb_delete_bucket_key() {
  local bucket="$1"
  local tenant="$2"
  FDB_CLI="${FDB_CLI}" FDB_CLUSTER_FILE="${FDB_CLUSTER_FILE}" \
    python3 - "${bucket}" "${tenant}" <<'PY'
import os
import struct
import subprocess
import sys

bucket = sys.argv[1]
tenant = int(sys.argv[2])
key = b"B" + struct.pack(">I", tenant) + bucket.encode("utf-8")
hexesc = "".join(f"\\x{b:02x}" for b in key)
cluster = os.environ["FDB_CLUSTER_FILE"]
fdb = os.environ["FDB_CLI"]
subprocess.check_call(
    [fdb, "-C", cluster, "--exec", f"writemode on; clear {hexesc}"],
)
print(f"fdb clear B key for bucket={bucket!r} tenant={tenant}")
PY
}

cleanup() {
  aws --endpoint-url "${ENDPOINT}" s3 rb "s3://${BUCKET}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "=== HeadBucket test bucket=${BUCKET} tenant=${TENANT_NAME} ==="

echo "--- step 1: missing bucket → 404 ---"
expect_head_bucket 404 "head-bucket before create"

echo "--- step 2: mb; head-bucket → 200 ---"
aws --endpoint-url "${ENDPOINT}" s3 mb "s3://${BUCKET}" >/dev/null
expect_head_bucket 200 "head-bucket after mb"

echo "--- step 2.5: resolve tenant_id from FDB T key ---"
fdb_tenant_id_from_name "${TENANT_NAME}"

echo "--- step 3: rb; head-bucket → 404 ---"
aws --endpoint-url "${ENDPOINT}" s3 rb "s3://${BUCKET}" >/dev/null
expect_head_bucket 404 "head-bucket after rb"

echo "--- step 4: mb; FDB delete B; head-bucket → 404 (cache stale) ---"
aws --endpoint-url "${ENDPOINT}" s3 mb "s3://${BUCKET}" >/dev/null
expect_head_bucket 200 "head-bucket after mb (warm cache)"
fdb_delete_bucket_key "${BUCKET}" "${TENANT_ID}"
expect_head_bucket 404 "head-bucket after FDB B delete"

echo ""
echo -e "${GREEN}PASS: HeadBucket (404/200/404 + stale-cache FDB delete)${NC}"
