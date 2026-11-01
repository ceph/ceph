#!/usr/bin/env bash
#
# Reads and pulled writes out of band, from librados, with
# ceph_test_osd_oob_read: the OSDs write each read's data into a window of
# the client's memory, and pull each write's payload out of one, over the
# libfabric tcp provider, and the client checks every byte and the OSDs'
# CRC-64/NVME. The client lends its windows on 127.0.0.1, so the pools of
# this workunit put their PGs on the OSDs of this host only: a replicated
# pool, read from the primary and with balanced reads, and an
# erasure-coded pool (k=2, m=1, allow_ec_optimizations and
# allow_ec_overwrites) when the host has three OSDs or more.
#
# The OSDs of this host must serve tcp to clients (oob_client_transports),
# or this skips.
#
# Environment:
#   OOB_MODE   strict: the OSDs place every read and pull every write
#              (default); mixed: a read may come back inline
#   OOB_SIZES  object sizes (default "4096 1048583 4194304 4194305")
#   OOB_COUNT  objects of each size (default 4)
#
# Against a vstart cluster, from the build directory:
#
#   ../src/vstart.sh -n -x -o 'oob_client_transports = tcp' \
#       -o 'oob_network = 127.0.0.0/8' -o 'oob_memory = 268435456'
#   PATH=$PWD/bin:$PATH CEPH_CONF=$PWD/ceph.conf \
#       ../qa/workunits/rados/test_oob_read.sh

set -ex

: ${OOB_MODE:=strict}
: ${OOB_SIZES:="4096 1048583 4194304 4194305"}
: ${OOB_COUNT:=4}

if ! type ceph_test_osd_oob_read > /dev/null 2>&1; then
    echo "ceph_test_osd_oob_read not found: the build has no WITH_OOB_OFI"
    exit 1
fi
case "$OOB_MODE" in
    strict|mixed) ;;
    *) echo "OOB_MODE is strict or mixed, not $OOB_MODE"; exit 1 ;;
esac

# the OSDs of this host
me=$(hostname)
local_osds=$(ceph osd metadata -f json |
                 jq -r --arg h "$me" '.[] | select(.hostname == $h) | .id')
if [ -z "$local_osds" ]; then
    echo "no OSD runs on $me"
    exit 1
fi
first=$(echo $local_osds | awk '{ print $1 }')
nlocal=$(echo $local_osds | wc -w)
transport=$(ceph tell osd.$first ofi status 2>/dev/null | jq -r '.transport // ""')
case "$transport" in
    "provider tcp,"*) ;;
    *)
        echo "osd.$first serves no tcp to clients (${transport:-no ofi status}):" \
             "ceph_test_osd_oob_read speaks tcp on 127.0.0.1 only, skipping"
        exit 0
        ;;
esac
host=$(ceph osd find $first -f json | jq -r '.crush_location.host')

wait_active() {
    local pool=$1 i
    for i in $(seq 1 120); do
        if [ "$(ceph pg ls-by-pool $pool -f json |
                    jq '[.pg_stats[] | select(.state | test("^active\\+clean") | not)] | length')" = 0 ]; then
            return 0
        fi
        sleep 2
    done
    echo "the PGs of $pool are not active+clean"
    ceph pg ls-by-pool $pool
    return 1
}

pools=()
cleanup() {
    for pool in "${pools[@]}"; do
        ceph osd pool rm $pool $pool --yes-i-really-really-mean-it || true
    done
    ceph osd crush rule rm oob-local || true
    ceph osd erasure-code-profile rm oob-local-ec || true
}
trap cleanup EXIT

ceph osd crush rule create-replicated oob-local "$host" osd
ceph osd pool create oob-read-rep 8 8 replicated oob-local
ceph osd pool set oob-read-rep size 2 --yes-i-really-mean-it
ceph osd pool application enable oob-read-rep rados
pools+=(oob-read-rep)
wait_active oob-read-rep

if [ $nlocal -ge 3 ]; then
    ceph osd erasure-code-profile set oob-local-ec k=2 m=1 \
        crush-root="$host" crush-failure-domain=osd
    ceph osd pool create oob-read-ec 8 8 erasure oob-local-ec
    ceph osd pool set oob-read-ec allow_ec_optimizations true
    # the other ops of a get overwrite the start of its first object
    ceph osd pool set oob-read-ec allow_ec_overwrites true
    ceph osd pool application enable oob-read-ec rados
    pools+=(oob-read-ec)
    wait_active oob-read-ec
fi

# every line of $2 that starts with $1 must say what $3 matches
check_lines() {
    local what=$1 out=$2 want=$3 n bad
    n=$(grep -c "^$what " <<< "$out" || true)
    bad=$(grep "^$what " <<< "$out" | grep -v -E -- "$want" || true)
    if [ "$n" -ne $OOB_COUNT ] || [ -n "$bad" ]; then
        echo "expected $OOB_COUNT $what lines matching '$want':"
        echo "$out"
        return 1
    fi
}

for pool in "${pools[@]}"; do
    for size in $OOB_SIZES; do
        oids=$(seq -f "obj-$size-%g" 1 $OOB_COUNT)
        # from the primary, with other ops of the PG while the reads are
        # out; and balanced, from any OSD of the PG, without: the first
        # of the other ops writes the start of the first object, which a
        # replica can apply before it serves the read
        modes=("--others 20")
        if [ $pool = oob-read-rep ]; then
            modes+=(--balance)
        fi
        for mode in "${modes[@]}"; do
            ceph_test_osd_oob_read write $pool $size $oids
            out=$(ceph_test_osd_oob_read get $pool $size $mode $oids)
            echo "$out"
            if [ "$OOB_MODE" = strict ]; then
                check_lines get "$out" "r=0 bytes=$size inline=0 .* data=ok crc=ok"
            else
                check_lines get "$out" "r=0 .* data=ok crc=ok"
            fi
            if [ "$mode" != --balance ]; then
                grep -q '^others: n=20 failed=0 ' <<< "$out"
            fi
        done

        out=$(ceph_test_osd_oob_read put $pool $size --others 20 $oids)
        echo "$out"
        check_lines put "$out" "r=0 bytes=$size .* crc=ok"
        check_lines check "$out" "data=ok"
        grep -q '^others: n=20 failed=0 ' <<< "$out"
    done
done

echo OK
