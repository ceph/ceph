#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON="127.0.0.1:7323" # git grep '\<7323\>' : there must be only one
    run_stretch_tests "$@"
}

function TEST_failed_stretch_pool_create_leaves_no_pool() {
    local dir=$1
    run_mon $dir a || return 1
    for dc in dc1 dc2; do
        ceph osd crush add-bucket $dc datacenter || return 1
        ceph osd crush move $dc root=default || return 1
    done
    # mon.a has no datacenter location, so stretch mode cannot be enabled.
    # An existing rule gets the create past building a stretch rule, which
    # needs OSDs, to that check after the pool is allocated.
    expect_failure $dir "Failed to validate monitor stretch mode" \
        ceph osd pool create badpool --rule replicated_rule --num-zones 2 || return 1
    timeout 60 ceph osd pool create goodpool 12 || return 1
    ceph osd pool ls | grep -qx goodpool || return 1
    ! ceph osd pool ls | grep -q badpool || return 1
}

main mon-stretch-failed-pool-create "$@"
