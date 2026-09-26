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
# FDB-level assertions for :L: namespace counters (type N).
#
# Requires running backend (post-reload). Verifies:
#   L N tenant_id  — exists; value == tenant_id in T+name row
#   L N bucket_id  — increments on CreateBucket; matches allocated bucket_id
#   L N rgw_id     — exists; value >= 1; matches backend startup allocation

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/kvrgw-common.sh
source "${ROOT}/scripts/kvrgw-common.sh"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

TENANT_NAME="${KVRGW_TENANT_NAME:-kv-poc}"
BUCKET="kv-l-ns-$$"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Required command not found: $1" >&2
    exit 1
  }
}

require_cmd aws
require_cmd python3

kvrgw_refresh_live_indices
export KVRGW_LIVE_COUNT

cleanup() {
  aws --endpoint-url "${ENDPOINT}" s3 rb "s3://${BUCKET}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "=== L namespace FDB assertions tenant=${TENANT_NAME} (live=${KVRGW_LIVE_COUNT}) ==="

FDB_CLI="${FDB_CLI}" FDB_CLUSTER_FILE="${FDB_CLUSTER_FILE}" \
  RGW_ID="${RGW_ID:-}" KVRGW_INSTANCES="${KVRGW_LIVE_COUNT:-${KVRGW_INSTANCES}}" TENANT_NAME="${TENANT_NAME}" BUCKET="${BUCKET}" ENDPOINT="${ENDPOINT}" \
  python3 <<'PY'
import os
import struct
import subprocess
import sys

tenant_name = os.environ["TENANT_NAME"]
bucket_name = os.environ["BUCKET"]
endpoint = os.environ["ENDPOINT"]
cluster = os.environ["FDB_CLUSTER_FILE"]
fdb = os.environ["FDB_CLI"]
rgw_id_env = os.environ.get("RGW_ID", "")
instances = int(os.environ.get("KVRGW_INSTANCES", "1"))

def fdb_exec(cmd: str) -> str:
    return subprocess.check_output(
        [fdb, "-C", cluster, "--exec", cmd],
        stderr=subprocess.STDOUT,
        text=True,
    )

def key_hex(key: bytes) -> str:
    return "".join(f"\\x{b:02x}" for b in key)

def make_l_key(type_byte: str, name: str) -> bytes:
    if type_byte not in ("N", "I"):
        raise ValueError(type_byte)
    if not name or len(name) > 64:
        raise ValueError(name)
    return b"L" + type_byte.encode("ascii") + name.encode("utf-8")

def parse_fdb_value(raw: str) -> bytes:
    for line in raw.splitlines():
        if " is `" not in line:
            continue
        payload = line.split("' is `", 1)[1].rstrip("\n").rstrip("'")
        out = bytearray()
        i = 0
        while i < len(payload):
            if payload[i : i + 2] == "\\x" and i + 3 < len(payload):
                out.append(int(payload[i + 2 : i + 4], 16))
                i += 4
            else:
                out.append(ord(payload[i]))
                i += 1
        return bytes(out)
    raise RuntimeError(f"unexpected fdb get: {raw!r}")

def fdb_get_u64_le(key: bytes) -> int:
    """FDB ADD stores counter cells as little-endian uint64."""
    raw = fdb_exec(f"get {key_hex(key)}")
    val = parse_fdb_value(raw)
    if len(val) < 8:
        val = val.ljust(8, b"\x00")
    if len(val) != 8:
        raise RuntimeError(f"counter value not 8 bytes: {val!r}")
    return struct.unpack("<Q", val)[0]

def tenant_id_from_t_key() -> int:
    key = b"T" + tenant_name.encode("utf-8")
    val = parse_fdb_value(fdb_exec(f"get {key_hex(key)}"))
    if len(val) < 4:
        raise RuntimeError("T value too short")
    return struct.unpack(">I", val[0:4])[0]

def bucket_id_from_b_key() -> int:
    tenant_id = tenant_id_from_t_key()
    key = b"B" + struct.pack(">I", tenant_id) + bucket_name.encode("utf-8")
    val = parse_fdb_value(fdb_exec(f"get {key_hex(key)}"))
    if len(val) < 8:
        raise RuntimeError("B value too short")
    return struct.unpack(">Q", val[0:8])[0]

# --- L N tenant_id ---
tenant_id = tenant_id_from_t_key()
tenant_counter_key = make_l_key("N", "tenant_id")
assert tenant_counter_key[:3] == b"LNt", tenant_counter_key
tenant_counter = fdb_get_u64_le(tenant_counter_key)

# Define color constants
RED='\033[0;31m'
GREEN = "\033[0;32m"
NC = "\033[0m"  # No Color / Reset

print(f"{GREEN}ok:{NC} L N tenant_id counter={tenant_counter} (T row tenant_id={tenant_id})")
if tenant_counter < tenant_id:
    raise SystemExit(f"{RED}FAIL:{NC} tenant counter {tenant_counter} < allocated tenant_id {tenant_id}")
if tenant_counter != tenant_id:
    print(f"note: tenant counter {tenant_counter} > tenant_id {tenant_id} (prior AddTenant runs)")

# --- L N rgw_id ---
rgw_counter_key = make_l_key("N", "rgw_id")
rgw_counter = fdb_get_u64_le(rgw_counter_key)
print(f"{GREEN}ok:{NC} L N rgw_id counter={rgw_counter}")
if rgw_counter < instances:
    raise SystemExit(f"{RED}FAIL:{NC} rgw counter {rgw_counter} < instance count {instances}")
if instances == 1 and rgw_id_env.isdigit() and int(rgw_id_env) != rgw_counter:
    raise SystemExit(f"{RED}FAIL:{NC} rgw counter {rgw_counter} != RGW_ID {rgw_id_env}")
print(f"{GREEN}ok:{NC} rgw counter {rgw_counter} >= instances {instances}")

# --- L N bucket_id (before/after CreateBucket) ---
bucket_counter_key = make_l_key("N", "bucket_id")
before = fdb_get_u64_le(bucket_counter_key)
print(f"{GREEN}ok:{NC} L N bucket_id counter before mb={before}")

subprocess.check_call(
    ["aws", "--endpoint-url", endpoint, "s3", "mb", f"s3://{bucket_name}"],
    stdout=subprocess.DEVNULL,
)

after = fdb_get_u64_le(bucket_counter_key)
allocated = bucket_id_from_b_key()
print(f"{GREEN}ok:{NC} L N bucket_id counter after mb={after} (B row bucket_id={allocated})")
if after != before + 1:
    raise SystemExit(f"{RED}FAIL:{NC} bucket counter {before} -> {after}, expected {before + 1}")
if after != allocated:
    raise SystemExit(f"{RED}FAIL:{NC} bucket counter {after} != allocated bucket_id {allocated}")

subprocess.check_call(
    ["aws", "--endpoint-url", endpoint, "s3", "rb", f"s3://{bucket_name}"],
    stdout=subprocess.DEVNULL,
)
PY

echo -e "${GREEN}PASS: L namespace FDB counter assertions${NC}"
