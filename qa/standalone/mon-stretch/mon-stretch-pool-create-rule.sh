#!/usr/bin/env bash
#
# A pool over two zones created with a given CRUSH rule: the rule must place
# the pool's copies in both zones.
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7304" # git grep '\<7304\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7305" # git grep '\<7305\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7306" # git grep '\<7306\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests $dir "$@"
}

function TEST_pool_create_rule_must_span_zones() {
    local dir=$1

    two_zone_cluster $dir 2 || return 1

    # replicated_rule chooses hosts anywhere under the root
    ! ceph osd pool create bad --num_zones 2 --replica 2 --rule replicated_rule \
        > $dir/out 2>&1 || return 1
    grep "does not place the pool in 2 buckets of type datacenter" $dir/out || return 1
    ! ceph osd pool ls | grep -qx bad || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1

    ceph osd crush rule create-stretch-replicated --rule-name two_dc \
        --num-replica-per-zone 2 || return 1
    ceph osd pool create good --num_zones 2 --replica 2 --rule two_dc || return 1
    test "$(pool_field good num_zones)" = 2 || return 1
}

# min_size of a pool over two zones counts the copies in each zone
function TEST_pool_create_min_size_per_zone() {
    local dir=$1

    two_zone_cluster $dir 2 || return 1

    ceph osd pool create s --num_zones 2 --replica 2 --min_size 2 || return 1
    test "$(pool_field s min_size)" = 2 || return 1
    test "$(pool_field s size)" = 4 || return 1
    ! ceph osd pool create s2 --num_zones 2 --replica 2 --min_size 3 \
        > $dir/out 2>&1 || return 1
    grep "pool min_size must be between 1 and replica" $dir/out || return 1
    ! ceph osd pool ls | grep -qx s2 || return 1
}

main mon-stretch-pool-create-rule "$@"
