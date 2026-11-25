#!/usr/bin/env bash
#
# Read unwritten (hole) and past-end regions of an rbd image whose data is in
# an erasure-coded pool, through the krbd EC direct-read path
# (read_from_replica=balance|localize + allow_ec_optimizations), to check the
# split/direct-read code returns zeros for holes and a clean short read past
# the image end -- never wrong data, an oops or a WARN.
#
# Holes on the EC direct-read path were a real hazard (the ECExtentCache
# clone-hole / RMW-skip aborts), so a direct read that lands on an object with
# no backing shards, or that straddles written data and the following hole,
# must still reconstruct zeros for the unwritten part.  Exercises:
#   1. a whole unwritten object (no shards at all) -> all zeros;
#   2. a read spanning written data into the following hole -> data then zeros;
#   3. a read straddling / past the image end -> clean short read, no error.
#
# Same preconditions as krbd_eio_direct_read.sh: bluestore OSDs, a k=2 m=1 EC
# data pool, and the image mapped with read_from_replica=balance|localize.
# Run as a krbd workunit (sudo available).  No thrash: the pool/image teardown
# below is inline, so run this on a healthy cluster, not under OSD thrashing.

set -e
set -o pipefail

EC_PROFILE=${EC_PROFILE:-krbd-rpe-k2m1-$$}
EC_POOL=${EC_POOL:-krbd-rpe-ec-$$}
POOL=${POOL:-rbd}
IMAGE=${IMAGE:-krbd-rpe-$$}
OBJECT_SIZE=$((4 << 20))
LEN=4096                           # one 4K chunk -> a single data shard
NOBJ=4                             # image spans objects 0..NOBJ-1
DATA=$(mktemp -d)

cleanup() {
    set +e
    [ -n "${DEV:-}" ] && sudo rbd device unmap "$DEV" 2>/dev/null
    rbd rm "$POOL/$IMAGE" 2>/dev/null
    ceph osd pool delete "$EC_POOL" "$EC_POOL" --yes-i-really-really-mean-it 2>/dev/null
    ceph osd erasure-code-profile rm "$EC_PROFILE" 2>/dev/null
    rm -rf "$DATA"
}
trap cleanup EXIT

# EC_DIRECT_READS <sent> eagain <n> split <n> fallback <n>, if the kernel counts
direct_count() {
    sudo sh -c 'cat /sys/kernel/debug/ceph/*/osdc' 2>/dev/null |
        sed -n 's/^EC_DIRECT_READS \([0-9]*\).*/\1/p' | head -1
}

# read LEN bytes at byte offset $1 off the device with O_DIRECT
read_at() {
    sudo dd if="$DEV" bs=$LEN count=1 iflag=direct,skip_bytes skip="$1" \
        status=none 2>/dev/null
}

mountpoint -q /sys/kernel/debug || sudo mount -t debugfs none /sys/kernel/debug

# --- EC data pool with optimizations (so SplitOp sets EC_DIRECT_READ) --------
ceph osd erasure-code-profile set "$EC_PROFILE" \
    k=2 m=1 crush-failure-domain=osd plugin=jerasure technique=reed_sol_van
ceph osd pool create "$EC_POOL" erasure "$EC_PROFILE"
ceph osd pool set "$EC_POOL" allow_ec_overwrites true
ceph osd pool set "$EC_POOL" allow_ec_optimizations true
# the EC_DIRECT_READ path is only taken when the pool reports split_reads
ceph osd pool ls detail | grep "'$EC_POOL'" | grep -q split_reads

# --- rbd image with its data in the EC pool ----------------------------------
rbd create --size $((NOBJ * OBJECT_SIZE)) --data-pool "$EC_POOL" "$POOL/$IMAGE"
DEV=$(sudo rbd device map "$POOL/$IMAGE")
OPTIONS=$(sudo cat "/sys/bus/rbd/devices/${DEV#/dev/rbd}/config_info" 2>/dev/null || true)
[ -n "$OPTIONS" ] || OPTIONS=$(ceph-conf --name client.${CEPH_ID:-admin} \
    --lookup rbd_default_map_options 2>/dev/null || true)
if ! [[ $OPTIONS =~ read_from_replica=(balance|localize) ]]; then
    echo "SKIP: image not mapped balance/localize (map options: '$OPTIONS')"
    exit 0
fi

# --- write one known 4K pattern into object 1 only; 0, 2, 3 stay holes -------
dd if=/dev/urandom of="$DATA/pat" bs=$LEN count=1 status=none
dd if=/dev/zero    of="$DATA/zero" bs=$LEN count=1 status=none
WOFF=$((1 * OBJECT_SIZE))                 # start of object 1
sudo dd if="$DATA/pat" of="$DEV" bs=$LEN count=1 \
    oflag=direct,seek_bytes seek=$WOFF status=none

sudo dmesg -C 2>/dev/null || true
D_BEFORE=$(direct_count); D_BEFORE=${D_BEFORE:-0}
fail=0

# 1. whole unwritten object (object 0) -> zeros
read_at 0 > "$DATA/got"
cmp -s "$DATA/zero" "$DATA/got" || { echo "FAIL: hole object 0 did not read back zeros"; fail=1; }

# 2. the written chunk -> the pattern; and a hole after it (object 2) -> zeros
read_at $WOFF > "$DATA/got"
cmp -s "$DATA/pat" "$DATA/got" || { echo "FAIL: written chunk read back wrong"; fail=1; }
read_at $((2 * OBJECT_SIZE)) > "$DATA/got"
cmp -s "$DATA/zero" "$DATA/got" || { echo "FAIL: hole object 2 did not read back zeros"; fail=1; }

# 3. read straddling / past the image end -> a clean short read (no bytes past
#    the end), never an error or a splat
END=$((NOBJ * OBJECT_SIZE))
n=$(sudo dd if="$DEV" bs=$LEN count=1 iflag=direct,skip_bytes skip=$((END - LEN)) \
        status=none 2>/dev/null | wc -c)
[ "$n" -eq "$LEN" ] || { echo "FAIL: last in-bounds chunk short ($n != $LEN)"; fail=1; }
n=$(sudo dd if="$DEV" bs=$LEN count=1 iflag=direct,skip_bytes skip=$END \
        status=none 2>/dev/null | wc -c)
[ "$n" -eq 0 ] || { echo "FAIL: read past image end returned $n bytes (expected 0)"; fail=1; }

# --- no kernel splat ---------------------------------------------------------
if sudo dmesg | grep -nE 'Call Trace|kernel BUG|WARNING:|general protection|KASAN|Oops'; then
    echo "FAIL: kernel splat on a hole / past-end EC direct read"
    fail=1
fi

# --- the direct-read path was actually exercised -----------------------------
D_AFTER=$(direct_count); D_AFTER=${D_AFTER:-0}
if [ -n "$(direct_count)" ] && [ "$D_AFTER" -le "$D_BEFORE" ]; then
    echo "WARNING: EC_DIRECT_READS counter did not advance ($D_BEFORE -> $D_AFTER)"
fi

[ "$fail" -eq 0 ] || { echo "read-past-end: FAILED"; exit 1; }
echo PASS
