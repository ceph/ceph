#!/bin/bash

. $(dirname $0)/../../standalone/ceph-helpers.sh

set -x

function wait_for_merge_completion() {
    local pool=$1
    local target=$2
    local stall_timeout=60
    local actual=0
    local last_actual=0
    local last_progress_time

    wait_for_clean || return 1
    last_actual=$(ceph pg ls-by-pool $pool --format=json 2>/dev/null | jq -r ".pg_stats | length")
    last_progress_time=$(date +%s)

    echo "Waiting for pool $pool to reach $target PGs..."

    while true; do
        actual=$last_actual
        echo "Current PG count: $actual (Target: $target)"
        if [ "$actual" -eq "$target" ]; then
            break
        fi

        sleep 2
        wait_for_clean || return 1
        actual=$(ceph pg ls-by-pool $pool --format=json 2>/dev/null | jq -r ".pg_stats | length")

        if [ "$actual" -lt "$last_actual" ]; then
            last_progress_time=$(date +%s)
        fi
        last_actual=$actual

        if [ $(( $(date +%s) - last_progress_time )) -ge "$stall_timeout" ]; then
            echo "PG merge made no progress for ${stall_timeout}s in pool $pool (still at $actual PGs)"
            return 1
        fi
    done
}

function test_pg_merging() {
    local pool="merge_pool"
    ceph osd pool delete $pool $pool --yes-i-really-really-mean-it || true
    create_pool $pool 16
    wait_for_clean || return 1

    ceph osd pool set $pool nopgchange 0
    ceph osd pool set $pool crimson_allow_pg_merge true

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
    ceph osd pool set $pool crimson_allow_pg_merge true
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
