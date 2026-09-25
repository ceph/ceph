#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7476" # git grep '\<7476\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7477" # git grep '\<7477\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7478" # git grep '\<7478\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests "$@"
}

function wait_for_pg_state() {
    local pgid=$1 re=$2 i
    for i in $(seq 1 120); do
        [[ "$(ceph pg $pgid query -f json 2>/dev/null | jq -r .state)" =~ $re ]] &&
            return 0
        sleep 2
    done
    ceph pg $pgid query
    return 1
}

function osds_down() {
    local dir=$1 osd
    shift
    for osd in "$@"; do
        kill_daemons $dir KILL osd.$osd || return 1
    done
    for osd in "$@"; do
        ceph osd down osd.$osd || return 1
    done
}

# An interval in which the PG of a pool over zones could only be peered,
# because its mandatory zone was below min_size, is not one that may have
# taken writes: the PG must not wait for that interval's OSDs later.
function TEST_peered_interval_not_waited_for() {
    local dir=$1

    # osd.0-3 in dc1 with mon.a, osd.4-7 in dc2 with mon.b, one per host
    two_zone_cluster $dir 4 || return 1
    # min_size 3 in each zone: a zone of 2 shards can decode but not write
    ceph osd pool create p 1 1 erasure --num_zones 2 --k 2 --m 2 || return 1
    ceph osd pool set p pg_autoscale_mode off || return 1
    test "$(pool_field p min_size)" = 3 || return 1
    wait_for_clean || return 1
    local pgid=$(ceph osd dump -f json | jq '.pools[] | select(.pool_name == "p") | .pool').0
    local -a dc1 dc2
    local osd
    for osd in $(ceph pg map $pgid -f json | jq -r '.acting[]'); do
        if [ $osd -lt 4 ]; then dc1+=($osd); else dc2+=($osd); fi
    done
    test ${#dc1[@]} = 4 -a ${#dc2[@]} = 4 || return 1
    echo v1 > $dir/v1
    rados -p p put obj $dir/v1 || return 1

    # lose dc1: degraded stretch mode with dc2 as the mandatory member
    osds_down $dir ${dc1[@]} || return 1
    kill_daemons $dir KILL mon.a || return 1
    wait_for_quorum 300 2 || return 1
    wait_for_stretch_state 1 0 || return 1
    wait_for_pg_state $pgid '^active' || return 1
    echo v2 > $dir/v2
    timeout 120 rados -p p put obj $dir/v2 || return 1

    # dc2 down to two shards, then dc1 back: recovery stretch mode, still
    # with dc2 mandatory, so the PG can only be peered
    osds_down $dir ${dc2[2]} ${dc2[3]} || return 1
    activate_mon $dir a --public-addr $CEPH_MON_A || return 1
    wait_for_quorum 300 3 || return 1
    for osd in ${dc1[@]}; do
        activate_osd $dir $osd || return 1
    done
    wait_for_stretch_state 1 1 || return 1
    wait_for_pg_state $pgid 'peered' || return 1
    ! [[ "$(ceph pg $pgid query -f json | jq -r .state)" =~ ^active ]] || return 1

    # dc2 back to min_size with one of the peered interval's OSDs, the rest
    # of them down: v2 is in dc2, so the PG goes active
    osds_down $dir ${dc1[@]} ${dc2[1]} || return 1
    for osd in ${dc2[2]} ${dc2[3]}; do
        activate_osd $dir $osd || return 1
    done
    wait_for_pg_state $pgid '^active' || return 1
    timeout 60 rados -p p get obj $dir/got || return 1
    cmp $dir/v2 $dir/got || return 1
}

main mon-stretch-ec-peered-interval "$@"
