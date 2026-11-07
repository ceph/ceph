#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7470" # git grep '\<7470\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7471" # git grep '\<7471\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7472" # git grep '\<7472\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests "$@"
}

# The zone of the leader Monitor is lost after its OSDs are marked down: the
# cluster must still enter degraded stretch mode. The new leader checks for it
# when it wins the election, before it has a pending MonMap.
function TEST_lose_leader_zone_after_its_osds() {
    local dir=$1

    two_zone_cluster $dir || return 1
    ceph osd pool create data0 erasure --num_zones 2 --k 2 --m 1 || return 1
    wait_for_clean || return 1
    # The pool enables stretch mode, whose connectivity elections can make
    # either zone's monitor the leader.
    local leader=$(ceph quorum_status -f json | jq -r .quorum_leader_name)
    local osds
    case $leader in
        a) osds="0 1 2" ;;
        b) osds="3 4 5" ;;
        *) return 1 ;;
    esac

    for osd in $osds; do
        kill_daemons $dir KILL osd.$osd || return 1
    done
    ceph osd down $osds || return 1
    test "$(ceph osd dump -f json | jq '[.osds[] | select(.up == 0)] | length')" = 3 || return 1
    wait_for_stretch_state 0 0 || return 1

    kill_daemons $dir KILL mon.$leader || return 1
    wait_for_quorum 300 2 || return 1
    wait_for_stretch_state 1 0 || return 1
}

main mon-stretch-leader-zone-down "$@"
