#!/usr/bin/env bash
#
# Write and verify files in a cephfs directory whose layout points at an
# optimized erasure-coded data pool, over a kernel mount mapped
# read_from_replica=balance|localize, to exercise EC direct/split reads from
# the *cephfs* kernel client (the same libceph direct-read path as krbd, plus
# the cephfs "version read from the primary" case).  Reads use O_DIRECT so they
# bypass the page cache and actually go to the OSDs.
#
# The suite's setup creates the EC pool (EC_POOL), enables optimizations and
# runs "ceph fs add_data_pool" before this workunit; the kernel mount is this
# workunit's cwd (teuthology runs fs workunits inside the mount).  A kernel
# that counts EC direct reads exposes them in debugfs; then at least one direct
# and one split read are required on a balance/localize mount so the run cannot
# pass without exercising the path.

set -e
set -o pipefail

EC_POOL=${EC_POOL:-cephfs_ec_data}
DIR=${DIR:-ecdir-$$}
OBJECTS=${OBJECTS:-32}
BLOCK=${BLOCK:-65536}
DURATION=${DURATION:-600}
DATA=$(mktemp -d)

cleanup() { set +e; rm -rf "$DIR" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT

ec_counters() {
    sudo sh -c 'cat /sys/kernel/debug/ceph/*/osdc' 2>/dev/null |
        grep -m1 '^EC_DIRECT_READS' || true
}

mountpoint -q /sys/kernel/debug || sudo mount -t debugfs none /sys/kernel/debug 2>/dev/null || true

# cwd is the kernel cephfs mount; point a subdir's layout at the EC pool so the
# files below are striped there (new files only, so set it before writing).
mkdir -p "$DIR"
setfattr -n ceph.dir.layout.pool -v "$EC_POOL" "$DIR"
getfattr -n ceph.dir.layout.pool "$DIR" 2>/dev/null | grep -q "$EC_POOL" ||
    { echo "SKIP: could not set dir layout to $EC_POOL"; exit 0; }

OPTIONS=$(cat /proc/mounts | awk '$3=="ceph"{print $4}' | head -1)
echo "cephfs mount options: ${OPTIONS:-<none>}"
# Assert the direct-read counter only when balance/localize is confirmed from
# the mount string, but still run the I/O and report the counter regardless, so
# a first run always shows whether the cephfs client takes the direct-read path.
localized=0
[[ $OPTIONS =~ read_from_replica=(balance|localize) ]] && localized=1

write_f() {
    local i=$1
    dd if=/dev/urandom of="$DATA/$i" bs=$BLOCK count=1 status=none
    dd if="$DATA/$i" of="$DIR/f$i" bs=$BLOCK count=1 oflag=direct status=none
}
verify_f() {
    local i=$1
    dd if="$DIR/f$i" of="$DATA/got" bs=$BLOCK count=1 iflag=direct status=none
    cmp -s "$DATA/$i" "$DATA/got" ||
        { echo "FAIL: file f$i read back differ"; return 1; }
}

for ((i = 0; i < OBJECTS; i++)); do write_f $i; done
for ((i = 0; i < OBJECTS; i++)); do verify_f $i; done

end=$((SECONDS + DURATION))
cycle=0
while ((SECONDS < end)); do
    cycle=$((cycle + 1))
    for ((i = cycle % 2; i < OBJECTS; i += 2)); do write_f $i; done
    for ((i = 0; i < OBJECTS; i++)); do verify_f $i; done
    echo "cycle $cycle OK $(ec_counters)"
done

counters=$(ec_counters)
echo "after $cycle cycles: ${counters:-no EC direct read counters}"
if [ -n "$counters" ] && [ "$localized" = 1 ]; then
    read -r _ direct _ _ _ split _ _ <<< "$counters"
    if ((direct == 0 || split == 0)); then
        echo "no EC direct reads ($direct) or split reads ($split) from cephfs"
        exit 1
    fi
fi

echo OK
