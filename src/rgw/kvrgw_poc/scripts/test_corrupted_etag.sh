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
# ETag integrity tests via s3cmd get (both variants expect the same warning).
#
# Variant A — corrupt KV metadata: increment first byte of stored etag (MD5 hex).
# Variant B — corrupt storage tier: increment byte 0 of blob at data_root/<ref_tag_hex>.
#
# Pass (both): stderr contains "MD5 signatures do not match".

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/kvrgw-common.sh
source "${ROOT}/scripts/kvrgw-common.sh"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

TENANT_NAME="${KVRGW_TENANT_NAME:-kv-poc}"
TMP="$(mktemp -d)"
BUCKET_A="kv-etag-kv-$$"
BUCKET_B="kv-etag-blob-$$"
OBJECT="obj1"
LOCAL="${TMP}/file1"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Required command not found: $1" >&2
    exit 1
  }
}

require_cmd s3cmd
require_cmd python3

cleanup() {
  s3cmd -c "${S3CMD_CFG}" del "s3://${BUCKET_A}/${OBJECT}" >/dev/null 2>&1 || true
  s3cmd -c "${S3CMD_CFG}" del "s3://${BUCKET_B}/${OBJECT}" >/dev/null 2>&1 || true
  s3cmd -c "${S3CMD_CFG}" rb "s3://${BUCKET_A}" >/dev/null 2>&1 || true
  s3cmd -c "${S3CMD_CFG}" rb "s3://${BUCKET_B}" >/dev/null 2>&1 || true
  rm -rf "${TMP}"
}
trap cleanup EXIT

baseline_get() {
  local bucket="$1"
  local out="$2"
  local label="$3"
  if ! s3cmd -c "${S3CMD_CFG}" get "s3://${bucket}/${OBJECT}" "${out}" >"${TMP}/${label}.out" 2>"${TMP}/${label}.err"; then
    echo -e "${RED}FAIL:${NC} ${label} s3cmd get failed" >&2
    cat "${TMP}/${label}.err" >&2
    exit 1
  fi
  if grep -qi "MD5 signatures do not match" "${TMP}/${label}.err"; then
    echo -e "${RED}FAIL:${NC} ${label} reported MD5 mismatch" >&2
    cat "${TMP}/${label}.err" >&2
    exit 1
  fi
  if ! diff -q "${LOCAL}" "${out}" >/dev/null; then
    echo -e "${RED}FAIL:${NC} ${label} content mismatch" >&2
    exit 1
  fi
  echo -e "${GREEN}ok:${NC} ${label} baseline get clean"
}

expect_md5_mismatch_get() {
  local bucket="$1"
  local out="$2"
  local label="$3"
  set +e
  s3cmd -c "${S3CMD_CFG}" get "s3://${bucket}/${OBJECT}" "${out}" >"${TMP}/${label}.out" 2>"${TMP}/${label}.err"
  local get_rc=$?
  set -e
  if [[ "${get_rc}" -ne 0 ]]; then
    echo -e "${RED}FAIL:${NC} ${label} s3cmd get exited ${get_rc}" >&2
    cat "${TMP}/${label}.err" >&2
    exit 1
  fi
  #cat "${TMP}/${label}.err"
  if ! grep -q "MD5 signatures do not match" "${TMP}/${label}.err"; then
    echo -e "${RED}FAIL:${NC} ${label} did not report MD5/ETag mismatch" >&2
    cat "${TMP}/${label}.err" >&2
    exit 1
  fi
  echo -e "${GREEN}ok:${NC} ${label} MD5 signatures do not match"
}

put_object() {
  local bucket="$1"
  aws --endpoint-url "${ENDPOINT}" s3 mb "s3://${bucket}" >/dev/null
  aws --endpoint-url "${ENDPOINT}" s3 cp "${LOCAL}" "s3://${bucket}/${OBJECT}" >/dev/null
}

# Object must be > 8KB to land on the storage tier (Tier 3) for Variant B blob corruption.
dd if=/dev/urandom bs=9000 count=1 of="${LOCAL}" 2>/dev/null

echo "=== Variant A: corrupt KV etag (bucket=${BUCKET_A}) ==="
put_object "${BUCKET_A}"
baseline_get "${BUCKET_A}" "${TMP}/a.good" "a-baseline"

FDB_CLI="${FDB_CLI}" FDB_CLUSTER_FILE="${FDB_CLUSTER_FILE}" \
  GC_CTL="${ROOT}/scripts/gc_ctl.sh" \
  python3 - "${TENANT_NAME}" "${BUCKET_A}" "${OBJECT}" <<'PY'
import os
import struct
import subprocess
import sys

tenant_name, bucket_name, object_name = sys.argv[1:4]
gc_ctl = os.environ["GC_CTL"]

def resolve_object_key(tenant_name: str, bucket_name: str, object_name: str) -> tuple[bytes, bytes]:
    tenant_key = b"T" + tenant_name.encode("utf-8")
    tenant_val = raw_get(tenant_key)
    tenant_id = struct.unpack(">I", tenant_val[0:4])[0]

    bucket_key = b"B" + struct.pack(">I", tenant_id) + bucket_name.encode("utf-8")
    bucket_val = raw_get(bucket_key)
    bucket_id = bucket_val[0:8]

    object_key = (
        b"S"
        + struct.pack(">H", 1)
        + struct.pack(">H", 0)
        + bucket_id
        + b"O"
        + object_name.encode("utf-8")
    )
    obj_val = raw_get(object_key)
    return object_key, obj_val

def raw_get(key: bytes) -> bytes:
    key_hex = key.hex()
    proc = subprocess.run(
        [gc_ctl, "raw-get", key_hex],
        capture_output=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"raw-get failed: {proc.stderr.decode()}")
    return proc.stdout

def raw_set(key: bytes, value: bytes) -> None:
    key_hex = key.hex()
    proc = subprocess.run(
        [gc_ctl, "raw-set", key_hex],
        input=value,
        capture_output=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"raw-set failed: {proc.stderr.decode()}")

object_key, obj_val = resolve_object_key(tenant_name, bucket_name, object_name)
# Binary O: layout: etag is 16 bytes at offset 16
old_etag = obj_val[16:32]
old_etag_hex = old_etag.hex()

bad_etag = bytearray(old_etag)
bad_etag[0] = (bad_etag[0] + 1) & 0xFF
bad_etag_hex = bytes(bad_etag).hex()

new_val = bytearray(obj_val)
new_val[16:32] = bad_etag
raw_set(object_key, bytes(new_val))
print(f"corrupted etag byte0: {old_etag_hex[:2]} -> {bad_etag_hex[:2]} ({old_etag_hex} -> {bad_etag_hex})")
PY

expect_md5_mismatch_get "${BUCKET_A}" "${TMP}/a.bad" "a-corrupt"
if ! diff -q "${LOCAL}" "${TMP}/a.bad" >/dev/null; then
  echo -e "${RED}FAIL:${NC} variant A downloaded body changed (blob tier should be intact)" >&2
  exit 1
fi
echo -e "${GREEN}ok:${NC} variant A blob bytes unchanged"

echo ""
echo "=== Variant B: corrupt storage blob (bucket=${BUCKET_B}) ==="
put_object "${BUCKET_B}"
baseline_get "${BUCKET_B}" "${TMP}/b.good" "b-baseline"

FDB_CLI="${FDB_CLI}" FDB_CLUSTER_FILE="${FDB_CLUSTER_FILE}" \
  DATA_ROOT="${DATA_ROOT}" \
  GC_CTL="${ROOT}/scripts/gc_ctl.sh" \
  python3 - "${TENANT_NAME}" "${BUCKET_B}" "${OBJECT}" <<'PY'
import os
import struct
import subprocess
import sys

tenant_name, bucket_name, object_name = sys.argv[1:4]
data_root = os.environ["DATA_ROOT"]
gc_ctl = os.environ["GC_CTL"]

def raw_get(key: bytes) -> bytes:
    proc = subprocess.run([gc_ctl, "raw-get", key.hex()], capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(f"raw-get failed: {proc.stderr.decode()}")
    return proc.stdout

tenant_key = b"T" + tenant_name.encode("utf-8")
tenant_val = raw_get(tenant_key)
tenant_id = struct.unpack(">I", tenant_val[0:4])[0]

bucket_key = b"B" + struct.pack(">I", tenant_id) + bucket_name.encode("utf-8")
bucket_val = raw_get(bucket_key)
bucket_id = bucket_val[0:8]

object_key = (
    b"S"
    + struct.pack(">H", 1)
    + struct.pack(">H", 0)
    + bucket_id
    + b"O"
    + object_name.encode("utf-8")
)
obj_val = raw_get(object_key)
# Binary O: layout: ref_tag is 12 bytes at offset 0
ref_tag = obj_val[0:12]
if len(ref_tag) != 12:
    raise RuntimeError(f"unexpected ref_tag length: {len(ref_tag)}")

blob_path = os.path.join(data_root, ref_tag.hex())
with open(blob_path, "r+b") as blob:
    blob.seek(0)
    x = blob.read(1)[0]
    blob.seek(0)
    blob.write(bytes([(x + 1) & 0xFF]))

print(f"corrupted blob byte0 at {blob_path}: {x:#04x} -> {(x + 1) & 0xFF:#04x}")
PY

expect_md5_mismatch_get "${BUCKET_B}" "${TMP}/b.bad" "b-corrupt"
if diff -q "${LOCAL}" "${TMP}/b.bad" >/dev/null; then
  echo -e "${RED}FAIL:${NC} variant B downloaded body unchanged (blob tier should differ)" >&2
  exit 1
fi
echo -e "${GREEN}ok:${NC} variant B blob bytes differ as expected"

echo ""
echo -e "${GREEN}PASS: corrupted ETag (variant A KV + variant B blob)${NC}"
