#!/usr/bin/env bash
set -euo pipefail

# IAM account name uniqueness (#81348). Run from a vstart build directory with
# RGW on port 8000 and STS enabled, or set RGW_ENDPOINT / PATH / CEPH_CONF.
#
# Example:
#   cd build-local
#   MON=1 OSD=1 MDS=0 MGR=1 RGW=1 ../src/vstart.sh -n \
#     -o "rgw s3 auth use sts = true" \
#     -o "rgw sts key = abcdefghijklmnop"
#   PATH=$PWD/bin:$PATH CEPH_CONF=$PWD/ceph.conf \
#     ../qa/workunits/rgw/test_iam_duplicate_account_names.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CEPH_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

export RGW_ENDPOINT="${RGW_ENDPOINT:-http://localhost:8000}"

if ! command -v radosgw-admin >/dev/null; then
  echo "radosgw-admin not on PATH" >&2
  exit 1
fi

python3 -c "import boto3" 2>/dev/null || {
  echo "boto3 required (pip install boto3)" >&2
  exit 1
}

exec python3 "${SCRIPT_DIR}/test_iam_duplicate_account_names.py"
