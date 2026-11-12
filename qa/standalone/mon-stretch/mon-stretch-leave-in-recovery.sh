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

# the erasure coded pool data0 over zones in recovery mode, after dc2 came
# back
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

# When the last pool over two zones leaves them, the cluster stretch flags
# are cleared, and the monitors, the leader too, must leave recovery mode.
# Erasure coded pool data0 (k=2 m=1) over the datacenters dc1 and dc2 is the
# only pool over zones. dc2's monitor and OSDs go down, and the cluster
# enters degraded mode. When they come back, the cluster enters recovery
# mode, and stays in it, as mon_stretch_recovery_min_wait is raised to an
# hour. data0 then leaves the zones, and the OSDMap's stretch flag is
# cleared. The monitor that led in recovery mode is made the leader again,
# and osd force_healthy_stretch_mode, which only works in recovery mode,
# must be refused.
# The tests remove data0 (osd pool rm) or move it to one zone (osd pool set
# data0 num_zones 1).
function leave_stretch_mode_while_recovering() {
    local dir=$1
    shift

    ec_stretch_cluster_recovering $dir || return 1
    local leader=$(ceph quorum_status -f json | jq -r .quorum_leader_name)
    "$@" || return 1
    wait_for_quorum 300 3 || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1
    # Clearing the cluster stretch flags allows every monitor to lead again,
    # and another one can win the election that follows: only the monitor
    # that led in recovery mode may lead
    local mon
    for mon in a b c; do
        if [ $mon != $leader ]; then
            ceph mon add disallowed_leader $mon || return 1
        fi
    done
    for i in $(seq 1 60); do
        wait_for_quorum 300 3 || return 1
        if [ "$(ceph quorum_status -f json | jq -r .quorum_leader_name)" = $leader ]; then
            break
        fi
        sleep 2
    done
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
        ceph osd pool set data0 num_zones 1 --yes-i-really-mean-it
}

main mon-stretch-leave-in-recovery "$@"
