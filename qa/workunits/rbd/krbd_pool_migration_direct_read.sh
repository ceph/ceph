#!/usr/bin/env bash
#
# Keep krbd EC direct-read I/O going while the server migrates the image's
# data pool ("ceph osd pool migrate" through a series of pg_num changes), to
# check the kernel follows the migration -- it re-targets reads to the new
# interval and never returns wrong data, oopses or stalls.  Exercises the
# kernel's "follow pool migrations" path (decode the migration watermark,
# route/resend across migration_src/target) on the EC direct-read path.
#
# Preconditions: bluestore, >= 6 OSDs (k=4 m=2 default; k=2 m=1 also works),
# image mapped read_from_replica=balance|localize + allow_ec_optimizations.
# Run as a krbd workunit (sudo).  No OSD thrash: the migration is the churn.

set -e
set -o pipefail

EC_PROFILE=${EC_PROFILE:-krbd-pmig-k2m1-$$}
EC_POOL=${EC_POOL:-krbd-pmig-ec-$$}
POOL=${POOL:-rbd}
IMAGE=${IMAGE:-krbd-pmig-$$}
OBJECTS=${OBJECTS:-32}
BLOCK=${BLOCK:-65536}
DURATION=${DURATION:-900}
OBJECT_SIZE=$((4 << 20))
OFFSET=8192
DATA=$(mktemp -d)
MIGPID=""

cleanup() {
    set +e
    [ -n "$MIGPID" ] && kill "$MIGPID" 2>/dev/null
    [ -n "${DEV:-}" ] && sudo rbd device unmap "$DEV" 2>/dev/null
    rm -rf "$DATA"
    # leave the image/pool for teuthology teardown (removal under migration can
    # block), like the other krbd workunits.
}
trap cleanup EXIT

ec_counters() {
    sudo sh -c 'cat /sys/kernel/debug/ceph/*/osdc' 2>/dev/null |
        grep -m1 '^EC_DIRECT_READS' || true
}
# true while a migration of $EC_POOL is in progress
migrating() {
    ceph osd pool ls detail -f json |
        python3 -c 'import json,sys
pools=json.load(sys.stdin)
sys.exit(0 if any(p["pool_name"]==sys.argv[1] and
    (p.get("migration_src") not in (None,-1) or p.get("migration_target") not in (None,-1) or
     "migrating" in p.get("flags_names","")) for p in pools) else 1)' "$EC_POOL"
}

mountpoint -q /sys/kernel/debug || sudo mount -t debugfs none /sys/kernel/debug

ceph osd erasure-code-profile set "$EC_PROFILE" \
    k=2 m=1 crush-failure-domain=osd plugin=jerasure technique=reed_sol_van
ceph osd pool create "$EC_POOL" 16 16 erasure "$EC_PROFILE"
ceph osd pool set "$EC_POOL" allow_ec_overwrites true
ceph osd pool set "$EC_POOL" allow_ec_optimizations true
ceph osd pool ls detail | grep "'$EC_POOL'" | grep -q split_reads
ceph osd pool application enable "$EC_POOL" rbd 2>/dev/null || true

rbd create --size $((OBJECTS * OBJECT_SIZE)) --data-pool "$EC_POOL" "$POOL/$IMAGE"
DEV=$(sudo rbd device map "$POOL/$IMAGE")
OPTIONS=$(sudo cat "/sys/bus/rbd/devices/${DEV#/dev/rbd}/config_info" 2>/dev/null || true)
[ -n "$OPTIONS" ] || OPTIONS=$(ceph-conf --name client.${CEPH_ID:-admin} \
    --lookup rbd_default_map_options 2>/dev/null || true)
if ! [[ $OPTIONS =~ read_from_replica=(balance|localize) ]]; then
    echo "SKIP: image not mapped balance/localize (map options: '$OPTIONS')"
    exit 0
fi

write_object() {
    local i=$1 skip=$((i * OBJECT_SIZE + OFFSET))
    dd if=/dev/urandom of="$DATA/$i" bs=$BLOCK count=1 status=none
    sudo dd if="$DATA/$i" of="$DEV" bs=$BLOCK count=1 \
        oflag=direct,seek_bytes seek=$skip status=none
}
verify_object() {
    local i=$1 skip=$((i * OBJECT_SIZE + OFFSET))
    sudo dd if="$DEV" of="$DATA/got" bs=$BLOCK count=1 \
        iflag=direct,skip_bytes skip=$skip status=none
    cmp -s "$DATA/$i" "$DATA/got" ||
        { echo "FAIL: object $i read back differ (during/after migration)"; return 1; }
}

for ((i = 0; i < OBJECTS; i++)); do write_object $i; done
for ((i = 0; i < OBJECTS; i++)); do verify_object $i; done

# background: migrate the data pool through a sequence of pg_num changes; the
# kernel must keep serving reads across each migration interval.
( for pg in 32 64 48 32 16; do
    ceph osd pool migrate "$EC_POOL" "$pg" "$pg" --yes-i-really-mean-it 2>/dev/null ||
        ceph osd pool migrate "$EC_POOL" "$pg" "$pg" 2>/dev/null || true
    for _ in $(seq 180); do migrating || break; sleep 2; done
    sleep 5
  done ) &
MIGPID=$!

end=$((SECONDS + DURATION))
cycle=0
while ((SECONDS < end)) && kill -0 "$MIGPID" 2>/dev/null; do
    cycle=$((cycle + 1))
    for ((i = cycle % 2; i < OBJECTS; i += 2)); do write_object $i; done
    for ((i = 0; i < OBJECTS; i++)); do verify_object $i; done
    echo "cycle $cycle OK $(ec_counters)"
done
wait "$MIGPID" 2>/dev/null || true
MIGPID=""

if sudo dmesg | grep -nE 'Call Trace|kernel BUG|WARNING:|general protection|KASAN|Oops'; then
    echo "FAIL: kernel splat during pool migration"
    exit 1
fi

counters=$(ec_counters)
echo "after $cycle cycles across migrations: ${counters:-no EC direct read counters}"
if [ -n "$counters" ] && [[ $OPTIONS =~ read_from_replica=(balance|localize) ]]; then
    read -r _ direct _ _ _ split _ _ <<< "$counters"
    if ((direct == 0 || split == 0)); then
        echo "no EC direct reads ($direct) or split reads ($split) across migration"
        exit 1
    fi
fi

echo PASS
