#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7320" # git grep '\<7320\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7321" # git grep '\<7321\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7322" # git grep '\<7322\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests "$@"
}

# A stretch pool create that fails must not leave the monitors in stretch mode
function TEST_failed_stretch_pool_create_leaves_mons() {
    local dir=$1
    two_zone_cluster $dir || return 1
    # dc2 weighs twice as much as dc1
    for osd in 3 4 5; do
        ceph osd crush reweight osd.$osd 2.0 || return 1
    done

    expect_failure $dir "Failed to validate pool stretch mode" \
        ceph osd pool create data0 erasure --num-zones 2 --k 2 --m 1 || return 1
    # commits are ordered, so any monmap change the create proposed is in
    ceph osd set noout || return 1
    test "$(ceph mon dump -f json | jq .stretch_mode)" = false || return 1
    test "$(ceph mon dump -f json | jq -r .tiebreaker_mon)" = "" || return 1
}

main mon-stretch-failed-create-mons "$@"
