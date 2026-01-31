#!/bin/bash

. $(dirname $0)/../../standalone/ceph-helpers.sh

set -x

function wait_for_merge_completion() {
    local pool=$1
    local target=$2
    local actual=0
    echo "Waiting for pool $pool to reach $target PGs..."
    
    while true; do
        wait_for_clean || return 1
        actual=$(ceph pg ls-by-pool $pool --format=json 2>/dev/null | jq -r ".pg_stats | length")
        echo "Current PG count: $actual (Target: $target)"
        if [ "$actual" -eq "$target" ]; then
            break
        fi
        # If we are clean but not at target, the next merge hasn't triggered yet
        sleep 2
    done
}

function test_pg_merging() {
    local pool="merge_pool"
    ceph osd pool delete $pool $pool --yes-i-really-really-mean-it || true
    create_pool $pool 16
    wait_for_clean || return 1

    ceph osd pool set $pool nopgchange 0

    # Induce merges
    ceph osd pool set $pool pg_num 4
    ceph osd pool set $pool pg_num_min 4
    
    wait_for_merge_completion $pool 4 || return 1
    
    ceph osd pool set $pool pgp_num 4
    wait_for_clean || return 1
}

function test_pg_merging_with_radosbench() {
    local pool="merge_bench"
    ceph osd pool delete $pool $pool --yes-i-really-really-mean-it || true
    create_pool $pool 16
    wait_for_clean || return 1

    # Start radosbench writes
    timeout 120 rados bench -p $pool 60 write -b 4096 --no-cleanup &
    BENCH_PID=$!
    sleep 5

    # merge
    ceph osd pool set $pool nopgchange 0
    ceph osd pool set $pool pg_num 4
    ceph osd pool set $pool pg_num_min 4
    
    # Use the loop to ensure we don't stop until all 12 PGs are merged away
    wait_for_merge_completion $pool 4 || return 1
    
    ceph osd pool set $pool pgp_num 4
    wait_for_clean || return 1

    # Ensure the bench finished successfully
    wait $BENCH_PID
    
    # Final check
    actual=$(ceph pg ls-by-pool $pool --format=json 2>/dev/null | jq -r ".pg_stats | length")
    test "$actual" -eq 4 || return 1
}

test_pg_merging || { echo "test_pg_merging failed"; exit 1; }
test_pg_merging_with_radosbench || { echo "test_pg_merging_with_radosbench failed"; exit 1; }

echo "OK"
