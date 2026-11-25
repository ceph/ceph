#!/usr/bin/env bash
#
# Write and verify data on a krbd device for DURATION seconds, reading it
# back both whole and as single 4 KiB chunks.  With the image's data in an
# erasure-coded pool and read_from_replica=balance or localize in the map
# options, the whole reads are split across shards and the chunk reads go to
# a single shard, if the kernel supports EC direct reads.  Such a kernel
# counts them in debugfs; then at least one of each is required, so that
# the run can't pass without exercising those paths.
#
# Under OSD thrashing the data pool can drop below min_size.  I/O to an
# inactive PG is EXPECTED to stall and then recover once enough shards come
# back, so a slow op is never killed outright -- that would turn normal
# below-min_size behaviour into a spurious failure.  If an op is still
# pending after READ_TIMEOUT we snapshot the kernel's outstanding OSD
# requests (/sys/kernel/debug/ceph/*/osdc), the osdmap and the libceph
# dmesg tail and keep waiting.  The snapshot tells an expected
# cluster-unavailable stall (whole PGs inactive, many requests queued) from
# a kernel bug (a single request stuck on a down OSD while the rest churn,
# i.e. not redriven).  Only an op that never recovers within HANG_CEIL
# fails the test, with the snapshots already in the log.

set -e
set -o pipefail

POOL=${POOL:-rbd}
IMAGE=${IMAGE:-verify-reads}
OBJECTS=${OBJECTS:-48}
BLOCK=${BLOCK:-65536}
DURATION=${DURATION:-600}
READ_TIMEOUT=${READ_TIMEOUT:-120}   # snapshot a still-pending op this often
HANG_CEIL=${HANG_CEIL:-900}         # fail only if an op never recovers in this

OBJECT_SIZE=$((4 << 20))
OFFSET=8192
DATA=$(mktemp -d)

# Snapshot the kernel's view of in-flight I/O.  The outstanding OSD requests
# name the target OSD/shard of a stalled op, so an expected below-min_size
# stall (whole PGs inactive) is distinguishable from a kernel stall (a
# request pending on a down OSD while the rest churn, not redriven).
function dump_stuck() {
    echo "--- outstanding OSD requests (/sys/kernel/debug/ceph/*/osdc) ---"
    sudo sh -c 'cat /sys/kernel/debug/ceph/*/osdc' 2>/dev/null || true
    echo "--- osdmap (up/down) ---"
    sudo sh -c 'cat /sys/kernel/debug/ceph/*/osdmap' 2>/dev/null | head -20 || true
    echo "--- libceph dmesg (tail) ---"
    sudo dmesg 2>/dev/null | grep -aE 'libceph|rbd[0-9]|ceph:' | tail -40 || true
}

# Trace the osd_client request lifecycle so the dmesg tail above shows the
# direct-read redrive decisions (best-effort; needs CONFIG_DYNAMIC_DEBUG).
function enable_dyndbg() {
    local c=/sys/kernel/debug/dynamic_debug/control
    [ -e "$c" ] || return 0
    echo 'file net/ceph/osd_client.c +p' | sudo tee "$c" >/dev/null 2>&1 || true
}

# Run a blocking device I/O.  Do not kill it when it is merely slow: while
# the pool is below min_size the op is expected to stall and recover.  Take
# a diagnostic snapshot every READ_TIMEOUT it stays pending; give up (and
# fail) only once it has not recovered within HANG_CEIL.
function timed_io() {
    local desc=$1; shift
    "$@" &
    local pid=$! waited=0 next=$READ_TIMEOUT
    while kill -0 "$pid" 2>/dev/null; do
        sleep 5
        waited=$((waited + 5))
        if ((waited >= next)); then
            echo "=== SLOW OP: $desc pending ${waited}s" \
                 "(below-min_size stalls are expected to recover; waiting) $(date -u +%T) ==="
            dump_stuck
            next=$((next + READ_TIMEOUT))
        fi
        if ((waited >= HANG_CEIL)); then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
            echo "object I/O did not recover after ${HANG_CEIL}s: $desc"
            return 1
        fi
    done
    wait "$pid"
}

function write_object() {
    local i=$1
    local skip=$((i * OBJECT_SIZE + OFFSET))

    dd if=/dev/urandom of=$DATA/$i bs=$BLOCK count=1 status=none
    timed_io "write object $i ($BLOCK bytes) at offset $skip" \
        sudo dd if=$DATA/$i of=$DEV bs=$BLOCK count=1 \
        oflag=direct,seek_bytes seek=$skip status=none
}

function verify_object() {
    local i=$1
    local len=$2
    local skip=$((i * OBJECT_SIZE + OFFSET))

    timed_io "read object $i ($len bytes) at offset $skip" \
        sudo dd if=$DEV of=$DATA/got bs=$len count=1 \
        iflag=direct,skip_bytes skip=$skip status=none
    if ! cmp -n $len $DATA/$i $DATA/got; then
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

# The direct-read path is only taken on an *optimized* EC pool (one that
# reports split_reads).  A plain erasure pool reads whole objects from the
# primary, so no direct/split reads are expected there and the assertion
# below must not fire for it.
function data_pool_is_optimized_ec() {
    local data_pool
    data_pool=$(rbd info $POOL/$IMAGE --format json |
        python3 -c 'import json, sys; print(json.load(sys.stdin).get("data_pool", ""))')
    [ -n "$data_pool" ] &&
        ceph osd pool ls detail | grep "'$data_pool'" | grep -q split_reads
}

mountpoint -q /sys/kernel/debug || sudo mount -t debugfs none /sys/kernel/debug
enable_dyndbg

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
        data_pool_is_optimized_ec; then
    read -r _ direct _ _ _ split _ _ <<< "$counters"
    if ((direct == 0 || split == 0)); then
        echo "no EC direct reads ($direct) or split reads ($split)"
        exit 1
    fi
fi

# Unmap the device but do not remove the image: like diff_continuous.sh and
# most krbd workunits, leave image teardown to teuthology's cluster cleanup,
# which runs once the thrasher has stopped and the cluster has recovered.
# Removing it here issues a cluster-wide object delete that blocks on an
# inactive PG while the OSD thrasher is still running, which ran the job into
# its timeout long after every read had already verified.
sudo rbd device unmap $DEV
rm -rf $DATA
echo OK
