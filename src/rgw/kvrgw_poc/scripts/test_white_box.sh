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
set -euo pipefail

# White-box test: Fault injection for batch Phase 2 abort.
# Verifies: stale group P:O → sweeper → group G:O → GcWorker cleans blobs.
#
# Requires: backend running with KVRGW_SWEEPER_MIN_AGE_SEC=2 KVRGW_SWEEPER_INTERVAL_SEC=1
#           KVRGW_GC_INTERVAL_SEC=1

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJ_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/kvrgw-common.sh"

ADMIN_SOCK="/tmp/kvrgw-admin-0.sock"
S3_ENDPOINT="http://127.0.0.1:9081"
BUCKET="wb-batch-abort"
S3CMD_CFG="$PROJ_DIR/s3cmd.cfg"

export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1

INSTANCES=$(cat "$PROJ_DIR/.run/instances" 2>/dev/null || echo 1)

aws_s3api() {
  aws --endpoint-url="$S3_ENDPOINT" s3api "$@"
}

admin_cmd() {
  echo "$1" | socat - UNIX-CONNECT:"$ADMIN_SOCK"
}

admin_cmd_all() {
  for i in $(seq 0 $((INSTANCES - 1))); do
    echo "$1" | socat - UNIX-CONNECT:"/tmp/kvrgw-admin-${i}.sock" 2>/dev/null || true
  done
}

fail() {
  echo -e "${RED}FAIL:${NC} $1" >&2
  exit 1
}

echo "=== White-box test: single Phase 2 abort ==="

# 1) Suspend GC on all instances (shared data dir)
admin_cmd_all "SET suspended=1"
sleep 1
echo "  [1] GC suspended on all instances"

# 2) Set error insertion on instance 0 only (we hit :9081 directly)
resp=$(admin_cmd "set-error kAbortAfterSinglePhase2 period=1 burst=1")
[[ "$resp" == "OK" ]] || fail "set-error failed: $resp"
echo "  [2] Fault set on instance 0: kAbortAfterSinglePhase2 period=1 burst=1"

# Enable single-mode (batch_size=1 is default, ensure it)

# 3) Create fresh bucket and PUT one 16KB object (directly to instance 0, no retry)
aws --endpoint-url="$S3_ENDPOINT" s3 rb "s3://$BUCKET" --force 2>/dev/null || true
s3cmd -c "$S3CMD_CFG" --host=127.0.0.1:9081 --host-bucket=127.0.0.1:9081 --no-ssl mb "s3://$BUCKET" 2>/dev/null || true
dd if=/dev/zero of=/tmp/wb-obj16k bs=16384 count=1 2>/dev/null
s3cmd -c "$S3CMD_CFG" --host=127.0.0.1:9081 --host-bucket=127.0.0.1:9081 --no-ssl --max-retries=0 put /tmp/wb-obj16k "s3://$BUCKET/obj1" 2>/dev/null || true
echo "  [3] PUT s3://$BUCKET/obj1 (16KB via s3cmd → :9081)"

# 4) Wait 3 seconds
sleep 3
echo "  [4] Waited 3s"

# 5) GET should fail (Phase 3 never committed)
if aws_s3api head-object --bucket "$BUCKET" --key "obj1" 2>/dev/null; then
  fail "obj1 is accessible — fault did not fire"
fi
echo "  [5] GET obj1 → NoSuchKey (correct: Phase 3 aborted)"

# 6) Search for blob file on disk — expect exactly 1
blob_count=$(find "$DATA_ROOT" -type f 2>/dev/null | wc -l)
echo "  [6] Blob files in $DATA_ROOT: $blob_count"
if [ "$blob_count" -ne 1 ]; then
  fail "Expected exactly 1 orphan blob, found $blob_count"
fi

# 7) Clear fault on instance 0, enable GC with immediate mode
admin_cmd "clear-error kAbortAfterSinglePhase2" >/dev/null
admin_cmd_all "SET suspended=0 interval_sec=1"
echo "  [7] Fault cleared, GC resumed"

# 8) Wait 3 seconds for sweeper (min_age=2s) + GcWorker (interval=1s)
sleep 5
echo "  [8] Waited 5s for sweeper + GcWorker"

# 9) Search again — blobs should be gone
final_blobs=$(find "$DATA_ROOT" -type f 2>/dev/null | wc -l)
echo "  [9] Final blob count: $final_blobs"
if [ "$final_blobs" -ne 0 ]; then
  fail "Orphan blobs not cleaned (remaining=$final_blobs)"
fi

# Cleanup
aws_s3api delete-bucket --bucket "$BUCKET" 2>/dev/null || true

echo -e "${GREEN}PASS: white-box single Phase 2 abort${NC}"
