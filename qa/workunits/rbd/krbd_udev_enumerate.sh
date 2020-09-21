#!/usr/bin/env bash

# This is a test for https://tracker.ceph.com/issues/41036, but it also
# triggers https://tracker.ceph.com/issues/41404 in some environments.

set -ex

function assert_exit_codes() {
    declare -a pids=($@)

    for pid in ${pids[@]}; do
       wait $pid
    done
}

function run_map() {
    declare -a pids

    for i in {1..300}; do
        sudo rbd map img$i &
        pids+=($!)
    done

    assert_exit_codes ${pids[@]}
    [[ $(rbd showmapped | wc -l) -eq 301 ]]
}

function run_unmap_by_dev() {
    declare -a pids

    run_map
    for i in {0..299}; do
        sudo rbd unmap /dev/rbd$i &
        pids+=($!)
    done

    assert_exit_codes ${pids[@]}
    [[ $(rbd showmapped | wc -l) -eq 0 ]]
}

function run_unmap_by_spec() {
    declare -a pids

    run_map
    for i in {1..300}; do
        sudo rbd unmap img$i &
        pids+=($!)
    done

    assert_exit_codes ${pids[@]}
    [[ $(rbd showmapped | wc -l) -eq 0 ]]
}

# Can't test with exclusive-lock, don't bother enabling deep-flatten.
# See https://tracker.ceph.com/issues/42492.
for i in {1..300}; do
    rbd create --size 1 --image-feature '' img$i
done

for i in {1..30}; do
    echo Iteration $i
    run_unmap_by_dev
    run_unmap_by_spec
done

echo OK
