#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7296" # git grep '\<7296\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7297" # git grep '\<7297\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7298" # git grep '\<7298\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests "$@"
}

# Creating a stretch EC pool in degraded stretch mode must not end degraded
# mode: the healthy transition restores the other pools and lets the lost
# zone's monitor lead again.
function TEST_ec_pool_create_in_degraded_stretch_mode() {
    local dir=$1

    ec_stretch_cluster_without_dc2 $dir || return 1

    ceph osd pool create data1 erasure --num-zones 2 --k 2 --m 1 || return 1
    ceph osd pool ls detail
    # data1 joins degraded stretch mode in the surviving datacenter
    local dc1=$(ceph osd crush dump -f json | jq '.buckets[]|select(.name=="dc1")|.id')
    test "$(pool_field data1 peering_crush_bucket_count)" == 1 || return 1
    test "$(pool_field data1 peering_crush_bucket_mandatory_member)" == "$dc1" || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.degraded_stretch_mode)" == 1 || return 1
    ceph osd getmap -o $dir/osdmap || return 1
    timeout 120 rados -p data1 put obj $dir/osdmap || return 1

    activate_mon $dir b --public-addr $CEPH_MON_B || return 1
    wait_for_quorum 300 3 || return 1
    for osd in 3 4 5; do
        activate_osd $dir $osd || return 1
    done
    wait_for_stretch_state 0 0 || return 1
    ceph osd pool ls detail
    for pool in data0 data1; do
        test "$(pool_field $pool peering_crush_bucket_count)" == 2 || return 1
        test "$(pool_field $pool peering_crush_bucket_mandatory_member)" == 2147483647 || return 1
    done
    wait_for_clean || return 1

    kill_daemons $dir KILL mon.a || return 1
    for osd in 0 1 2; do
        kill_daemons $dir KILL osd.$osd || return 1
    done
    wait_for_quorum 300 2 || return 1
    ceph osd down osd.0 osd.1 osd.2 || return 1
    wait_for_stretch_state 1 0 || return 1
    rados -p data1 get obj $dir/obj || return 1
    cmp $dir/osdmap $dir/obj || return 1
}

main mon-stretch-ec-pool-create-degraded "$@"
