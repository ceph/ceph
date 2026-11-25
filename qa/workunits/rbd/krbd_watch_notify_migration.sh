#!/usr/bin/env bash
#
# Watch/notify (linger requests) under a pool migration.  krbd maps an image,
# which registers a watch on its header object; the pool holding that header is
# then migrated (pg_num changes drive the migration framework) while rbd resize
# is run repeatedly.  Each resize notifies the kernel's watcher, so the kernel
# must receive the notify through the migration *redirect* reply and refresh the
# mapped device size.  If the linger (watch) request were dropped or not
# re-targeted across the migration, the device size would stop tracking the
# resizes and/or the watcher would disappear -- which is exactly the linger
# redirect handling Bill asked to see exercised
# ("libceph: move linger requests on a pool migration redirect").
#
# Checks: (1) a watcher is registered after map and stays registered through the
# migration, (2) every resize done mid-migration is seen by the kernel (device
# size updates), and (3) no kernel splat.  Runs on a replicated image and, when
# EC_POOL is set, on an image whose data is in an EC pool (header/watch still in
# the replicated WPOOL).  krbd workunit (sudo); teuthology reaps the pools.

set -e
set -o pipefail

WPOOL=${WPOOL:-krbd-wn-pool-$$}        # replicated pool holding the image header + watch
EC_POOL=${EC_POOL:-}                   # optional EC data pool
IMAGE=${IMAGE:-krbd-wn-$$}
DATA=$(mktemp -d)
DEV=""

cleanup() {
    set +e
    [ -n "$DEV" ] && sudo rbd device unmap -o force "$DEV" 2>/dev/null
    rm -rf "$DATA"
}
trap cleanup EXIT

mountpoint -q /sys/kernel/debug || sudo mount -t debugfs none /sys/kernel/debug 2>/dev/null || true

watchers() {
    rbd status "$WPOOL/$IMAGE" -f json |
        python3 -c 'import json,sys; print(len(json.load(sys.stdin)["watchers"]))'
}
pgnum() {
    ceph osd pool get "$WPOOL" pg_num -f json |
        python3 -c 'import json,sys; print(json.load(sys.stdin)["pg_num"])'
}
migrating() {
    ceph osd pool ls detail -f json | python3 -c '
import json, sys
pools = json.load(sys.stdin)["pools"]
p = next(p for p in pools if p["pool_name"] == "'"$WPOOL"'")
print(bool(p.get("migrating_pgs")) or p.get("pg_num") != p.get("pg_num_target"))'
}

# --- a replicated pool for the image header (+ watch) ------------------------
ceph osd pool create "$WPOOL" 8 8
ceph osd pool application enable "$WPOOL" rbd
dataopt=""
if [ -n "$EC_POOL" ]; then
    ceph osd erasure-code-profile set "wn-$$" \
        k=2 m=1 crush-failure-domain=osd plugin=jerasure technique=reed_sol_van
    ceph osd pool create "$EC_POOL" erasure "wn-$$"
    ceph osd pool set "$EC_POOL" allow_ec_overwrites true
    dataopt="--data-pool $EC_POOL"
fi

rbd create --size 64 $dataopt "$WPOOL/$IMAGE"
DEV=$(sudo rbd device map "$WPOOL/$IMAGE")

[ "$(watchers)" -ge 1 ] || { echo "FAIL: no watcher registered after map"; exit 1; }

dev_size() { sudo blockdev --getsize64 "$DEV"; }

# --- migrate the header's pool in the background (pg_num sequence) ------------
BASE_PG=$(pgnum)
(
    for pg in $((BASE_PG * 4)) $((BASE_PG * 2)) $((BASE_PG * 4)) $BASE_PG; do
        ceph osd pool migrate "$WPOOL" "$pg" "$pg" 2>/dev/null || true
        for _ in $(seq 60); do [ "$(migrating)" = True ] || break; sleep 2; done
    done
) &
MIGPID=$!

# --- resize during the migration; the kernel must see each new size ----------
fail=0
for mb in 128 192 256 320 384 448; do
    rbd resize --size $mb "$WPOOL/$IMAGE"
    want=$((mb * 1024 * 1024))
    seen=0
    for _ in $(seq 30); do [ "$(dev_size)" = "$want" ] && { seen=1; break; }; sleep 1; done
    if [ "$seen" = 1 ]; then
        echo "resize ${mb}MiB: kernel saw $want (watch alive through migration)"
    else
        echo "FAIL: kernel missed resize to ${mb}MiB (dev=$(dev_size) want=$want) -- notify lost across migration?"
        fail=1
    fi
    [ "$(watchers)" -ge 1 ] || { echo "FAIL: watcher gone during migration"; fail=1; }
done

wait $MIGPID 2>/dev/null || true

if sudo dmesg | grep -nE 'Call Trace|kernel BUG|WARNING:|KASAN|Oops|general protection'; then
    echo "FAIL: kernel splat during watch/notify under migration"
    fail=1
fi

[ "$fail" = 0 ] || { echo "watch-notify-migration: FAILED"; exit 1; }
echo PASS
