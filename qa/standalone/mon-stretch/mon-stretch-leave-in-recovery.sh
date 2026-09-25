#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7265" # git grep '\<7265\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7266" # git grep '\<7266\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7267" # git grep '\<7267\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests "$@"
}

# the stretch EC pool data0 in recovery stretch mode, after dc2 came back
function ec_stretch_cluster_recovering() {
    local dir=$1

    ec_stretch_cluster_without_dc2 $dir || return 1
    activate_mon $dir b --public-addr $CEPH_MON_B || return 1
    wait_for_quorum 300 3 || return 1
    ceph tell mon.\* config set mon_stretch_recovery_min_wait 3600 || return 1
    for osd in 3 4 5; do
        activate_osd $dir $osd || return 1
    done
    wait_for_stretch_state 1 1 || return 1
}

# Leaving stretch mode during recovery must end recovery stretch mode on the
# leader too, so that force_healthy_stretch_mode fails afterwards.
function leave_stretch_mode_while_recovering() {
    local dir=$1
    shift

    ec_stretch_cluster_recovering $dir || return 1
    local leader=$(ceph quorum_status -f json | jq -r .quorum_leader_name)
    "$@" || return 1
    wait_for_quorum 300 3 || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1
    test "$(ceph quorum_status -f json | jq -r .quorum_leader_name)" = $leader || return 1
    expect_failure $dir "recovery stretch mode" \
        ceph osd force_healthy_stretch_mode --yes-i-really-mean-it || return 1
}

function TEST_rm_last_stretch_pool_while_recovering() {
    leave_stretch_mode_while_recovering $1 \
        ceph osd pool rm data0 data0 --yes-i-really-really-mean-it
}

function TEST_unstretch_last_pool_while_recovering() {
    leave_stretch_mode_while_recovering $1 \
        ceph osd pool set data0 num_zones 1
}

main mon-stretch-leave-in-recovery "$@"
