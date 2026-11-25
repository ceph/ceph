#!/usr/bin/env bash
#
# Take the OSD holding EC shard 0 DOWN (not merely inject an -EIO) and read the
# object back through the krbd EC direct-read path (read_from_replica=
# balance|localize + allow_ec_optimizations), to check the kernel detects the
# down shard from the osdmap and reconstructs via the surviving shards rather
# than sending a one-shot direct read to a dead OSD and hanging or returning
# wrong data.  This is a different path from krbd_eio_direct_read.sh: there the
# OSD replies -EIO; here the shard's OSD is absent from the acting set, so the
# kernel must route around it (fall back to the primary / reconstruct) off the
# osdmap, not off a reply error.
#
# Checks, like the EIO test: (1) no OSD crash, (2) no kernel oops/WARN,
# (3) correct bytes (reconstructed) or a clean error -- never wrong data, and
# (4) the direct-read path was actually taken (pool split_reads, image mapped
# balance|localize), so it can't pass vacuously.
#
# Preconditions: bluestore, >= 3 OSDs (k=2 m=1, failure-domain=osd), image
# mapped balance|localize.  Run as a krbd workunit (sudo).  No thrash: the
# pool/image teardown is inline, so run on an otherwise-healthy cluster.

set -e
set -o pipefail

EC_PROFILE=${EC_PROFILE:-krbd-ms-k2m1-$$}
EC_POOL=${EC_POOL:-krbd-ms-ec-$$}
POOL=${POOL:-rbd}
IMAGE=${IMAGE:-krbd-ms-$$}
OBJECT_SIZE=$((4 << 20))
OBJ_INDEX=${OBJ_INDEX:-1}
OFFSET=$((OBJ_INDEX * OBJECT_SIZE))
LEN=4096
DATA=$(mktemp -d)
DOWN_OSD=""

cleanup() {
    set +e
    [ -n "$DOWN_OSD" ] && ceph osd unset noup 2>/dev/null
    [ -n "${DEV:-}" ] && sudo rbd device unmap "$DEV" 2>/dev/null
    rbd rm "$POOL/$IMAGE" 2>/dev/null
    ceph osd pool delete "$EC_POOL" "$EC_POOL" --yes-i-really-really-mean-it 2>/dev/null
    ceph osd erasure-code-profile rm "$EC_PROFILE" 2>/dev/null
    rm -rf "$DATA"
}
trap cleanup EXIT

fallback_count() {
    sudo sh -c 'cat /sys/kernel/debug/ceph/*/osdc' 2>/dev/null |
        sed -n 's/^EC_DIRECT_READS .* fallback \([0-9]*\).*/\1/p' | head -1
}
num_up() {
    ceph osd stat --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["num_up_osds"])'
}

mountpoint -q /sys/kernel/debug || sudo mount -t debugfs none /sys/kernel/debug

ceph osd erasure-code-profile set "$EC_PROFILE" \
    k=2 m=1 crush-failure-domain=osd plugin=jerasure technique=reed_sol_van
ceph osd pool create "$EC_POOL" erasure "$EC_PROFILE"
ceph osd pool set "$EC_POOL" allow_ec_overwrites true
ceph osd pool set "$EC_POOL" allow_ec_optimizations true
ceph osd pool ls detail | grep "'$EC_POOL'" | grep -q split_reads

rbd create --size $(((OBJ_INDEX + 2) * OBJECT_SIZE)) --data-pool "$EC_POOL" "$POOL/$IMAGE"
DEV=$(sudo rbd device map "$POOL/$IMAGE")
OPTIONS=$(sudo cat "/sys/bus/rbd/devices/${DEV#/dev/rbd}/config_info" 2>/dev/null || true)
[ -n "$OPTIONS" ] || OPTIONS=$(ceph-conf --name client.${CEPH_ID:-admin} \
    --lookup rbd_default_map_options 2>/dev/null || true)
if ! [[ $OPTIONS =~ read_from_replica=(balance|localize) ]]; then
    echo "SKIP: image not mapped balance/localize (map options: '$OPTIONS')"
    exit 0
fi

# write a known 4K pattern and read it back clean once
dd if=/dev/urandom of="$DATA/pat" bs=$LEN count=1 status=none
sudo dd if="$DATA/pat" of="$DEV" bs=$LEN count=1 \
    oflag=direct,seek_bytes seek=$OFFSET status=none
cmp "$DATA/pat" <(sudo dd if="$DEV" bs=$LEN count=1 \
    iflag=direct,skip_bytes skip=$OFFSET status=none)

OBJ=$(rados -p "$EC_POOL" ls | grep '^rbd_data' | head -1)
[ -n "$OBJ" ] || { echo "FAIL: no rbd_data object in $EC_POOL after write"; exit 1; }
SHARD0_OSD=$(ceph osd map "$EC_POOL" "$OBJ" --format json |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["acting"][0])')

ceph crash archive-all 2>/dev/null || true
UP_BEFORE=$(num_up)
FB_BEFORE=$(fallback_count); FB_BEFORE=${FB_BEFORE:-0}

# take shard 0's OSD down and hold it down (noup) so it stays out of the acting
# set across the read; the k=2 m=1 PG (min_size 2) stays active on 2 shards
ceph osd set noup
DOWN_OSD=$SHARD0_OSD
ceph osd down "osd.$SHARD0_OSD"
for _ in $(seq 60); do [ "$(num_up)" -lt "$UP_BEFORE" ] && break; sleep 1; done
[ "$(num_up)" -lt "$UP_BEFORE" ] || { echo "FAIL: osd.$SHARD0_OSD did not go down"; exit 1; }
# let the PG re-peer without the down shard
sleep 10

# read it back: direct read must route around the down shard and reconstruct
sudo dmesg -C 2>/dev/null || true
RC=0
sudo dd if="$DEV" of="$DATA/got" bs=$LEN count=1 \
    iflag=direct,skip_bytes skip=$OFFSET status=none || RC=$?

UP_AFTER=$(num_up)
CRASHES=$(ceph crash ls --format json 2>/dev/null | python3 -c '
import json, sys
cs = json.load(sys.stdin)
print(sum(1 for c in cs if "is_erasure" in c.get("assert_msg", "")))' 2>/dev/null || echo 0)
# UP_AFTER should be UP_BEFORE-1 (only the one we downed); fewer => another crashed
if [ "$CRASHES" -gt 0 ] || [ "$UP_AFTER" -lt $((UP_BEFORE - 1)) ]; then
    echo "FAIL: an OSD crashed on an EC direct read with a down shard"
    ceph crash ls; ceph osd stat
    exit 1
fi

if sudo dmesg | grep -nE 'Call Trace|kernel BUG|WARNING:|general protection|KASAN|Oops'; then
    echo "FAIL: kernel splat on an EC direct read with a down shard"
    exit 1
fi

if [ "$RC" -eq 0 ]; then
    cmp "$DATA/pat" "$DATA/got" || { echo "FAIL: wrong data with a down shard"; exit 1; }
    FB_AFTER=$(fallback_count); FB_AFTER=${FB_AFTER:-0}
    echo "read reconstructed correct data with shard 0 OSD down (fallback $FB_BEFORE -> $FB_AFTER)"
    if [ -n "$(fallback_count)" ] && [ "$FB_AFTER" -le "$FB_BEFORE" ]; then
        echo "WARNING: direct-read fallback counter did not advance on the down shard"
    fi
else
    echo "read returned a clean error (rc=$RC) with a down shard -- acceptable"
fi

echo PASS
