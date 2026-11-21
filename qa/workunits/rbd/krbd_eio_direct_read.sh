#!/usr/bin/env bash
#
# Inject a bluestore -EIO on one EC shard and read the object back through
# krbd on the EC direct-read path (read_from_replica=balance/localize +
# allow_ec_optimizations), to check the kernel client and the OSD both
# survive a shard read error there:
#
#   1. the OSD does NOT crash -- regression guard for the
#      rep_repair_primary_object() ceph_assert(!pool.info.is_erasure())
#      that do_sparse_read() hit on an EC_DIRECT_READ shard EIO (ceph PR 72651);
#   2. the kernel does NOT oops/WARN and does NOT return silently-wrong data --
#      it either reads the correct bytes (fell back off the direct-read path,
#      debugfs EC_DIRECT_READS "fallback" counter) or returns a clean error;
#   3. the direct-read path was actually taken (the pool carries split_reads and
#      the image is mapped balance/localize), so the test can't pass vacuously.
#
# Port of qa/workunits/rados/test_ec_sparse_read_repair_assert.sh to a kernel
# client.  Needs: bluestore OSDs, >= 3 OSDs (k=2 m=1), and the image mapped
# with read_from_replica=balance|localize (the krbd/thrash map-options facet
# sets rbd_default_map_options).  Run as a krbd workunit (sudo available).

set -e
set -o pipefail

EC_PROFILE=${EC_PROFILE:-krbd-eio-k2m1-$$}
EC_POOL=${EC_POOL:-krbd-eio-ec-$$}
POOL=${POOL:-rbd}
IMAGE=${IMAGE:-krbd-eio-$$}
OBJECT_SIZE=$((4 << 20))
OBJ_INDEX=${OBJ_INDEX:-1}          # write one object away from the image head
OFFSET=$((OBJ_INDEX * OBJECT_SIZE))
LEN=4096                           # a single 4K chunk -> one data shard
DATA=$(mktemp -d)

cleanup() {
    set +e
    [ -n "${DEV:-}" ] && sudo rbd device unmap "$DEV" 2>/dev/null
    rbd rm "$POOL/$IMAGE" 2>/dev/null
    ceph config rm osd bluestore_debug_inject_read_err 2>/dev/null
    ceph osd pool delete "$EC_POOL" "$EC_POOL" --yes-i-really-really-mean-it 2>/dev/null
    ceph osd erasure-code-profile rm "$EC_PROFILE" 2>/dev/null
    rm -rf "$DATA"
}
trap cleanup EXIT

# EC_DIRECT_READS <sent> eagain <n> split <n> fallback <n>, if the kernel counts
fallback_count() {
    sudo sh -c 'cat /sys/kernel/debug/ceph/*/osdc' 2>/dev/null |
        sed -n 's/^EC_DIRECT_READS .* fallback \([0-9]*\).*/\1/p' | head -1
}

mountpoint -q /sys/kernel/debug || sudo mount -t debugfs none /sys/kernel/debug

# --- EC data pool with optimizations (so SplitOp sets EC_DIRECT_READ) --------
ceph config set osd bluestore_debug_inject_read_err true
ceph osd erasure-code-profile set "$EC_PROFILE" \
    k=2 m=1 crush-failure-domain=osd plugin=jerasure technique=reed_sol_van
ceph osd pool create "$EC_POOL" erasure "$EC_PROFILE"
ceph osd pool set "$EC_POOL" allow_ec_overwrites true
ceph osd pool set "$EC_POOL" allow_ec_optimizations true
# the EC_DIRECT_READ path is only taken when the pool reports split_reads
ceph osd pool ls detail | grep "'$EC_POOL'" | grep -q split_reads

# --- rbd image with its data in the EC pool ----------------------------------
rbd create --size $(((OBJ_INDEX + 2) * OBJECT_SIZE)) \
    --data-pool "$EC_POOL" "$POOL/$IMAGE"
DEV=$(sudo rbd device map "$POOL/$IMAGE")
# The options the image was really mapped with: krbd's config_info, the "rbd
# add" string (root only).  rbd_default_map_options can come from ceph.conf
# (teuthology's map-options facet) or from CEPH_ARGS, and ceph-conf --lookup
# sees only the first, so it is the fallback for a kernel without the file.
OPTIONS=$(sudo cat "/sys/bus/rbd/devices/${DEV#/dev/rbd}/config_info" 2>/dev/null || true)
[ -n "$OPTIONS" ] || OPTIONS=$(ceph-conf --name client.${CEPH_ID:-admin} \
    --lookup rbd_default_map_options 2>/dev/null || true)
if ! [[ $OPTIONS =~ read_from_replica=(balance|localize) ]]; then
    echo "SKIP: image not mapped balance/localize (map options: '$OPTIONS')"
    exit 0
fi

# --- write a known 4K pattern and read it back clean once --------------------
dd if=/dev/urandom of="$DATA/pat" bs=$LEN count=1 status=none
sudo dd if="$DATA/pat" of="$DEV" bs=$LEN count=1 \
    oflag=direct,seek_bytes seek=$OFFSET status=none
cmp "$DATA/pat" <(sudo dd if="$DEV" bs=$LEN count=1 \
    iflag=direct,skip_bytes skip=$OFFSET status=none)

# --- find the single data object + the OSD holding shard 0 -------------------
# with a data-pool image the EC pool holds only rbd_data.* objects, and we have
# written exactly one, so list rather than guess the %0Nx object-name format.
OBJ=$(rados -p "$EC_POOL" ls | grep '^rbd_data' | head -1)
[ -n "$OBJ" ] || { echo "FAIL: no rbd_data object in $EC_POOL after write"; exit 1; }
SHARD0_OSD=$(ceph osd map "$EC_POOL" "$OBJ" --format json |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["acting"][0])')

# --- inject the EIO on shard 0, record pre-state -----------------------------
# shard id 0 is required: bluestore keys EC shards by ghobject_t shard_id;
# NO_SHARD never matches and the direct-read path is not exercised.
ceph crash archive-all 2>/dev/null || true
OSDS_BEFORE=$(ceph osd stat --format json |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["num_up_osds"])')
FB_BEFORE=$(fallback_count); FB_BEFORE=${FB_BEFORE:-0}
ceph tell "osd.$SHARD0_OSD" injectdataerr "$EC_POOL" "$OBJ" 0

# --- read it back through krbd (direct read -> shard 0 -> EIO) ----------------
sudo dmesg -C 2>/dev/null || true
RC=0
sudo dd if="$DEV" of="$DATA/got" bs=$LEN count=1 \
    iflag=direct,skip_bytes skip=$OFFSET status=none || RC=$?
sleep 5     # let a crashed OSD get marked down, if the guard regressed

# --- check 1: no OSD crash (the !is_erasure assert) --------------------------
OSDS_AFTER=$(ceph osd stat --format json |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["num_up_osds"])')
CRASHES=$(ceph crash ls --format json 2>/dev/null | python3 -c '
import json, sys
cs = json.load(sys.stdin)
print(sum(1 for c in cs
          if "rep_repair_primary_object" in c.get("assert_msg", "")
          or "is_erasure" in c.get("assert_msg", "")))' 2>/dev/null || echo 0)
if [ "$CRASHES" -gt 0 ] || [ "$OSDS_AFTER" -lt "$OSDS_BEFORE" ]; then
    echo "FAIL: OSD crashed on an EC_DIRECT_READ shard EIO (PR 72651 guard)"
    ceph crash ls; ceph osd stat
    exit 1
fi

# --- check 2: no kernel splat ------------------------------------------------
if sudo dmesg | grep -nE 'Call Trace|kernel BUG|WARNING:|general protection|KASAN|Oops'; then
    echo "FAIL: kernel splat after an EC direct-read shard EIO"
    exit 1
fi

# --- check 3: correct data (fell back) or a clean error, never bad data ------
if [ "$RC" -eq 0 ]; then
    cmp "$DATA/pat" "$DATA/got" || { echo "FAIL: wrong data after shard EIO"; exit 1; }
    FB_AFTER=$(fallback_count); FB_AFTER=${FB_AFTER:-0}
    echo "read recovered correct data after shard EIO (fallback $FB_BEFORE -> $FB_AFTER)"
    if [ -n "$(fallback_count)" ] && [ "$FB_AFTER" -le "$FB_BEFORE" ]; then
        echo "WARNING: direct-read fallback counter did not advance on the shard EIO"
    fi
else
    echo "read returned a clean error (rc=$RC) after shard EIO -- acceptable"
fi

echo PASS
