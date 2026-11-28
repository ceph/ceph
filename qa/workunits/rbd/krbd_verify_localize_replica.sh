#!/usr/bin/env bash
#
# Localize reads on a REPLICATED pool over two zones (stretch-replica): map an
# image in the suite's replicated-over-zones pool with read_from_replica=localize
# + crush_location and run continuous write/read/verify.  Checks the kernel's
# localize path on a replicated (not EC) stretch pool returns correct data with
# no splat.  The replicated-over-zones localize path was only covered by the
# squad5 chaos, never a dedicated teuthology regression test; this is it.
#
# Replicated pools do not take the EC direct/split-read path, so there is no
# EC_DIRECT_READS counter to assert on here -- this is a correctness/robustness
# check of LOCALIZE_READS against a stretch-replica pool.

set -e
set -o pipefail

POOL=${POOL:-reprep}
IMAGE=${IMAGE:-krbd-locrep-$$}
OBJECTS=${OBJECTS:-32}
BLOCK=${BLOCK:-65536}
DURATION=${DURATION:-300}
DATA=$(mktemp -d)
DEV=""

cleanup() {
    set +e
    [ -n "$DEV" ] && sudo rbd device unmap -o force "$DEV" 2>/dev/null
    rm -rf "$DATA"
}
trap cleanup EXIT

rbd create --size 128 "$POOL/$IMAGE"
DEV=$(sudo rbd device map "$POOL/$IMAGE")
OPTIONS=$(sudo cat "/sys/bus/rbd/devices/${DEV#/dev/rbd}/config_info" 2>/dev/null || true)
if ! [[ $OPTIONS =~ read_from_replica=localize ]]; then
    echo "SKIP: image not mapped localize (map options: '$OPTIONS')"
    exit 0
fi

write_b() {
    local i=$1
    dd if=/dev/urandom of="$DATA/$i" bs=$BLOCK count=1 status=none
    sudo dd if="$DATA/$i" of="$DEV" bs=$BLOCK count=1 \
        oflag=direct,seek_bytes seek=$((i * BLOCK)) status=none
}
verify_b() {
    local i=$1
    sudo dd if="$DEV" of="$DATA/got" bs=$BLOCK count=1 \
        iflag=direct,skip_bytes skip=$((i * BLOCK)) status=none 2>/dev/null
    cmp -s "$DATA/$i" "$DATA/got" || { echo "FAIL: block $i read back differ"; return 1; }
}

for ((i = 0; i < OBJECTS; i++)); do write_b $i; done
for ((i = 0; i < OBJECTS; i++)); do verify_b $i; done

sudo dmesg -C 2>/dev/null || true
end=$((SECONDS + DURATION))
cycle=0
while ((SECONDS < end)); do
    cycle=$((cycle + 1))
    for ((i = cycle % 2; i < OBJECTS; i += 2)); do write_b $i; done
    for ((i = 0; i < OBJECTS; i++)); do verify_b $i; done
done

if sudo dmesg | grep -nE 'Call Trace|kernel BUG|WARNING:|KASAN|Oops|general protection'; then
    echo "FAIL: kernel splat on localize reads of a replicated stretch pool"
    exit 1
fi

echo "localize-replica: $cycle cycles OK on $POOL (localize, stretch-replica)"
echo PASS
