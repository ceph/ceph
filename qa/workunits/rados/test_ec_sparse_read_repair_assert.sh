#!/usr/bin/env bash
#
# Regression test for: ceph_assert(!pool.info.is_erasure()) in
# PrimaryLogPG::rep_repair_primary_object(), triggered via do_sparse_read()
# when an EC direct-read (CEPH_OSD_FLAG_EC_DIRECT_READ) hits a shard whose
# bluestore readv returns -EIO.
#
# Assumptions:
#   - bluestore objectstore on all OSDs
#   - >= 3 OSDs available (k=2 m=1 profile)
#   - ceph_test_rados_io_sequence on PATH
#
# No parameters required.  Run as:
#   bash test_ec_sparse_read_repair_assert.sh
#
set -ex

POOL="test-ec-sparse-$$"
OBJ="test-obj"
PROFILE="ec-profile-$$"
BLOCKSIZE=4096

# --- setup -------------------------------------------------------------------

# ec_optimizations causes SplitOp to set CEPH_OSD_FLAG_EC_DIRECT_READ on
# SPARSE_READ ops, routing them to individual shard OSDs and taking the
# buggy else-branch in do_sparse_read().
ceph config set osd osd_pool_default_flag_ec_optimizations true

# Required for ceph tell osd.X injectdataerr to make bluestore readv return -EIO.
ceph config set osd bluestore_debug_inject_read_err true

ceph osd erasure-code-profile set "$PROFILE" \
    k=2 m=1 crush-failure-domain=osd \
    plugin=jerasure technique=reed_sol_van

ceph osd pool create "$POOL" erasure "$PROFILE"
ceph osd pool set "$POOL" allow_ec_overwrites true
ceph osd pool application enable "$POOL" rados

# Confirm the pool has ec_optimizations and split_reads flags — these are
# required for the CEPH_OSD_FLAG_EC_DIRECT_READ path to be taken.
ceph osd pool get "$POOL" all | grep -q allow_ec_optimizations

# --- write an object ---------------------------------------------------------

printf 'create 2\ndone\n' \
    | ceph_test_rados_io_sequence \
        --pool "$POOL" --object "$OBJ" --blocksize "$BLOCKSIZE" --interactive

# --- find the OSD that holds shard 0 -----------------------------------------

PRIMARY_OSD=$(ceph osd map "$POOL" "$OBJ" --format json \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["acting"][0])')

# --- inject a bluestore EIO on shard 0 ---------------------------------------
#
# The shard ID argument (0) is required: bluestore stores EC shards as
# ghobject_t(hoid, NO_GEN, shard_id).  Without it injectdataerr uses
# NO_SHARD which never matches and the bug is not triggered.

ceph tell "osd.$PRIMARY_OSD" injectdataerr "$POOL" "$OBJ" 0

# --- trigger the bug ---------------------------------------------------------
#
# 'sparseread balanced' issues CEPH_OSD_OP_SPARSE_READ with
# OPERATION_BALANCE_READS.  SplitOp picks this up, adds
# CEPH_OSD_FLAG_EC_DIRECT_READ, and sends the op to a shard OSD.  That OSD
# runs do_sparse_read -> ECBackend::objects_readv_sync -> store->readv, which
# returns -EIO from the inject above, then calls rep_repair_primary_object
# which contains ceph_assert(!pool.info.is_erasure()).
# Before the fix: the OSD crashes.  After the fix: EIO is returned to client.

ceph crash archive-all 2>/dev/null || true
OSDS_BEFORE=$(ceph osd stat --format json \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["num_up_osds"])')

printf 'sparseread 0 2 balanced\ndone\n' \
    | ceph_test_rados_io_sequence \
        --pool "$POOL" --object "$OBJ" --blocksize "$BLOCKSIZE" --interactive \
    || true   # client-side EIO is acceptable after the fix

sleep 5   # allow a crashed OSD to be marked down

# --- verify no OSD crashed ---------------------------------------------------

OSDS_AFTER=$(ceph osd stat --format json \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["num_up_osds"])')

ASSERT_CRASHES=$(ceph crash ls --format json 2>/dev/null \
    | python3 -c '
import sys, json
crashes = json.load(sys.stdin)
print(sum(1 for c in crashes
          if "rep_repair_primary_object" in c.get("assert_msg", "")
          or "is_erasure" in c.get("assert_msg", "")))
' 2>/dev/null || echo 0)

if [ "$ASSERT_CRASHES" -gt 0 ] || [ "$OSDS_AFTER" -lt "$OSDS_BEFORE" ]; then
    echo "FAIL: OSD crashed with !is_erasure assert (bug not fixed)"
    ceph crash ls
    ceph osd stat
    exit 1
fi

# --- cleanup -----------------------------------------------------------------

ceph config rm osd osd_pool_default_flag_ec_optimizations || true
ceph config rm osd bluestore_debug_inject_read_err || true
ceph osd pool delete "$POOL" "$POOL" --yes-i-really-really-mean-it
ceph osd erasure-code-profile rm "$PROFILE" || true

echo "PASS"
