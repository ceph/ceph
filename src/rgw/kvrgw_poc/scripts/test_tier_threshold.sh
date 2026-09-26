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
# Tier threshold test: verify tier routing under default and changed thresholds.
# Checks data/ filesystem and D: FDB keys to confirm correct tier placement.
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

GC_CTL="${ROOT}/scripts/gc_ctl.sh"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "${TMPDIR}"' EXIT
BUCKET="tier-thresh-$$"
TENANT_NAME="${KVRGW_TENANT_NAME:-kv-poc}"

aws --endpoint-url "${ENDPOINT}" s3 mb "s3://${BUCKET}"

pass() { echo -e "  ${GREEN}PASS: $1 ${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1 ${NC}" >&2; exit 1; }

reload_with_tiers() {
  local max_inline="$1" max_kv_store="$2"
  export KVRGW_MAX_INLINE="${max_inline}"
  export KVRGW_MAX_KV_STORE="${max_kv_store}"
  "${ROOT}/scripts/reload.sh" "${KVRGW_INSTANCES}" >/dev/null 2>&1
  unset KVRGW_MAX_INLINE KVRGW_MAX_KV_STORE
}

file_count() {
  find "${DATA_ROOT}" -maxdepth 1 -type f 2>/dev/null | wc -l
}

# Resolve bucket_id + object O: key, extract ref_tag, check D: key existence
# Usage: check_d_key OBJECT_NAME expect_exists
# Prints ref_tag hex on stdout
check_d_key() {
  local object_name="$1"
  local expect="$2"
  python3 - "${TENANT_NAME}" "${BUCKET}" "${object_name}" "${expect}" <<'PY'
import math, os, struct, subprocess, sys

tenant_name, bucket_name, object_name, expect = sys.argv[1:5]
gc_ctl = os.environ.get("GC_CTL", "gc_ctl")

def raw_get(key_hex):
    proc = subprocess.run([gc_ctl, "raw-get", key_hex], capture_output=True)
    if proc.returncode != 0:
        return None
    return proc.stdout

# Resolve tenant_id
tenant_key = (b"T" + tenant_name.encode()).hex()
tenant_val = raw_get(tenant_key)
assert tenant_val, f"tenant {tenant_name} not found"
tenant_id = struct.unpack(">I", tenant_val[0:4])[0]

# Resolve bucket_id
bucket_key = (b"B" + struct.pack(">I", tenant_id) + bucket_name.encode()).hex()
bucket_val = raw_get(bucket_key)
assert bucket_val, f"bucket {bucket_name} not found"
bucket_id = bucket_val[0:8]

# Read O: value
object_key = (
    b"S" + struct.pack(">H", 1) + struct.pack(">H", 0)
    + bucket_id + b"O" + object_name.encode()
)
obj_val = raw_get(object_key.hex())
assert obj_val, f"object {object_name} not found"

ref_tag = obj_val[0:12]
size = struct.unpack(">Q", obj_val[32:40])[0]
mtime = struct.unpack(">I", obj_val[40:44])[0]
chunk_type = chr(obj_val[48])

# Compute D: key fields
def d_size_tier(s):
    if s <= 1:
        return 0
    return (s - 1).bit_length() - 1

def fnv1a_byte(data):
    h = 0x811c9dc5
    for b in data:
        h ^= b
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h

st = d_size_tier(size)
hp = fnv1a_byte(ref_tag) % 32

d_key = (
    b"D"
    + struct.pack(">H", 1)
    + struct.pack(">H", 0)
    + bucket_id
    + struct.pack("B", st)
    + struct.pack("B", hp)
    + struct.pack(">I", mtime)
    + ref_tag
)

d_val = raw_get(d_key.hex())
has_d = d_val is not None and len(d_val) > 0

if expect == "yes" and not has_d:
    print(f"FAIL: D: key missing for {object_name} (chunk={chunk_type} size={size} st={st} hp={hp} mtime={mtime} ref={ref_tag.hex()} d_key={d_key.hex()})", file=sys.stderr)
    sys.exit(1)
if expect == "no" and has_d:
    print(f"FAIL: D: key exists for {object_name} (chunk={chunk_type} size={size} d_len={len(d_val)})", file=sys.stderr)
    sys.exit(1)

print(ref_tag.hex())
PY
}

put_and_verify() {
  local label="$1" size="$2" expect_file="$3" expect_d="$4"
  local fname="${TMPDIR}/${label}.bin"
  local fname_get="${TMPDIR}/${label}-get.bin"

  dd if=/dev/urandom "bs=${size}" count=1 of="${fname}" 2>/dev/null
  local files_before
  files_before="$(file_count)"

  aws --endpoint-url "${ENDPOINT}" s3 cp "${fname}" "s3://${BUCKET}/${label}" --quiet

  local files_after
  files_after="$(file_count)"

  if [[ "${expect_file}" == "yes" ]]; then
    [[ "${files_after}" -gt "${files_before}" ]] || fail "${label}: expected file in data/"
  else
    [[ "${files_after}" -eq "${files_before}" ]] || fail "${label}: unexpected file in data/"
  fi

  GC_CTL="${GC_CTL}" check_d_key "${label}" "${expect_d}" >/dev/null || exit 1

  aws --endpoint-url "${ENDPOINT}" s3 cp "s3://${BUCKET}/${label}" "${fname_get}" --quiet
  diff "${fname}" "${fname_get}" || fail "${label}: GET content mismatch"

  pass "${label}"
}

echo "=== Default thresholds (max_inline=256, max_kv_store=8192) ==="

echo "--- Step 2: Tier 1 INLINE (100B) ---"
put_and_verify "default-inline" 100 no no

echo "--- Step 3: Tier 2 KV_STORE (2048B) ---"
put_and_verify "default-kvstore" 2048 no yes

echo "--- Step 4: Tier 3 STORAGE (16384B) ---"
put_and_verify "default-storage" 16384 yes no

echo ""
echo "=== Change thresholds: max_inline=512, max_kv_store=4096 (restart) ==="
reload_with_tiers 512 4096
aws --endpoint-url "${ENDPOINT}" s3 mb "s3://${BUCKET}" 2>/dev/null || true

echo "--- Step 5: New INLINE (400B, was KV_STORE) ---"
put_and_verify "new-inline" 400 no no

echo "--- Step 6: New KV_STORE (2048B) ---"
put_and_verify "new-kvstore" 2048 no yes

echo "--- Step 7: New STORAGE (5120B, was KV_STORE) ---"
put_and_verify "new-storage" 5120 yes no

echo ""
echo "=== Restore thresholds (restart) ==="
reload_with_tiers 256 8192

echo ""
echo "=== Cleanup ==="
aws --endpoint-url "${ENDPOINT}" s3 rm --recursive "s3://${BUCKET}/" --quiet
aws --endpoint-url "${ENDPOINT}" s3 rb "s3://${BUCKET}"
pass "Cleanup"

echo ""
echo "All tier threshold tests passed."
