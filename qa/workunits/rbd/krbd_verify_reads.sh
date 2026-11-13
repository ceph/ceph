#!/usr/bin/env bash
#
# Write and verify data on a krbd device for DURATION seconds, reading it
# back both whole and as single 4 KiB chunks.  With the image's data in an
# erasure-coded pool and read_from_replica=balance or localize in the map
# options, the whole reads are split across shards and the chunk reads go to
# a single shard, if the kernel supports EC direct reads.  Such a kernel
# counts them in debugfs; then at least one of each is required, so that
# the run can't pass without exercising those paths.

set -e
set -o pipefail

POOL=${POOL:-rbd}
IMAGE=${IMAGE:-verify-reads}
OBJECTS=${OBJECTS:-48}
BLOCK=${BLOCK:-65536}
DURATION=${DURATION:-600}

OBJECT_SIZE=$((4 << 20))
OFFSET=8192
DATA=$(mktemp -d)

function write_object() {
    local i=$1

    dd if=/dev/urandom of=$DATA/$i bs=$BLOCK count=1 status=none
    sudo dd if=$DATA/$i of=$DEV bs=$BLOCK count=1 oflag=direct,seek_bytes \
        seek=$((i * OBJECT_SIZE + OFFSET)) status=none
}

function verify_object() {
    local i=$1
    local len=$2

    if ! cmp -n $len $DATA/$i <(sudo dd if=$DEV bs=$len count=1 \
            iflag=direct,skip_bytes skip=$((i * OBJECT_SIZE + OFFSET)) \
            status=none); then
        echo "object $i: the first $len bytes read back differ"
        return 1
    fi
}

function verify_all() {
    local i
    for ((i = 0; i < OBJECTS; i++)); do
        verify_object $i $BLOCK
        verify_object $i 4096
    done
}

# EC_DIRECT_READS <sent> eagain <n> split <n> fallback <n>, if the kernel
# counts them
function ec_counters() {
    sudo sh -c 'cat /sys/kernel/debug/ceph/*/osdc' 2>/dev/null |
        grep -m1 '^EC_DIRECT_READS' || true
}

function data_pool_is_ec() {
    local data_pool
    data_pool=$(rbd info $POOL/$IMAGE --format json |
        python3 -c 'import json, sys; print(json.load(sys.stdin).get("data_pool", ""))')
    [ -n "$data_pool" ] &&
        ceph osd pool ls detail --format json |
        python3 -c 'import json, sys
pools = json.load(sys.stdin)
sys.exit(not any(p["pool_name"] == sys.argv[1] and p["type"] == 3 for p in pools))' "$data_pool"
}

mountpoint -q /sys/kernel/debug || sudo mount -t debugfs none /sys/kernel/debug

rbd create --size $((OBJECTS * OBJECT_SIZE)) $POOL/$IMAGE
DEV=$(sudo rbd device map $POOL/$IMAGE)
OPTIONS=$(ceph-conf --name client.${CEPH_ID:-admin} \
    --lookup rbd_default_map_options 2>/dev/null || true)

for ((i = 0; i < OBJECTS; i++)); do
    write_object $i
done
verify_all

end=$((SECONDS + DURATION))
cycle=0
while ((SECONDS < end)); do
    cycle=$((cycle + 1))
    for ((i = cycle % 2; i < OBJECTS; i += 2)); do
        write_object $i
    done
    verify_all
    echo "cycle $cycle OK $(ec_counters)"
done

counters=$(ec_counters)
echo "after $cycle cycles: ${counters:-no EC direct read counters}"
if [ -n "$counters" ] && [[ $OPTIONS =~ read_from_replica=(balance|localize) ]] &&
        data_pool_is_ec; then
    read -r _ direct _ _ _ split _ _ <<< "$counters"
    if ((direct == 0 || split == 0)); then
        echo "no EC direct reads ($direct) or split reads ($split)"
        exit 1
    fi
fi

sudo rbd device unmap $DEV
rbd rm $POOL/$IMAGE
rm -rf $DATA
echo OK
