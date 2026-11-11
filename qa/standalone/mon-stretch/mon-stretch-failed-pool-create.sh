#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON="127.0.0.1:7323" # git grep '\<7323\>' : there must be only one
    run_stretch_tests "$@"
}

# osd pool create of the first pool over two zones allocates the pool
# before it checks that the monitors can take the cluster stretch flags.
# When that check refuses the create, no pool may be left behind, and the
# next create must work. The cluster has one monitor, a, with no location,
# and two datacenters, dc1 and dc2, with no OSDs. badpool, given
# replicated_rule so that no rule has to be made, gets as far as that check
# and is refused. goodpool is then created, and badpool is not listed.
function TEST_failed_stretch_pool_create_leaves_no_pool() {
    local dir=$1
    run_mon $dir a || return 1
    for dc in dc1 dc2; do
        ceph osd crush add-bucket $dc datacenter || return 1
        ceph osd crush move $dc root=default || return 1
    done
    # mon.a has no datacenter location, so the cluster stretch flags cannot
    # be set. An existing rule gets the create past building a stretch rule,
    # which needs OSDs, to that check after the pool is allocated.
    expect_failure $dir "Failed to validate monitor stretch mode" \
        ceph osd pool create badpool --rule replicated_rule --num_zones 2 || return 1
    timeout 60 ceph osd pool create goodpool 12 || return 1
    ceph osd pool ls | grep -qx goodpool || return 1
    ! ceph osd pool ls | grep -q badpool || return 1
}

main mon-stretch-failed-pool-create "$@"
