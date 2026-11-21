#!/usr/bin/env bash
#
# osd ok-to-stop --max on a stretch pool: min_size applies to each zone, so
# the set must not grow past the last copy in a zone.
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7654" # git grep '\<7654\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7655" # git grep '\<7655\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7656" # git grep '\<7656\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests $dir "$@"
}

function TEST_ok_to_stop_max_per_zone_min_size() {
    local dir=$1

    # dc1: osd.0 osd.1, dc2: osd.2 osd.3
    two_zone_cluster $dir 2 || return 1
    add_stretch_rule $dir two_dc default datacenter 2 host 2 || return 1
    # 2 copies in each zone, min_size 1 in each zone. osd pool stretch set
    # would make it one zone, with min_size over all copies
    ceph osd pool create p --num_zones 2 --replica 2 --rule two_dc || return 1
    test "$(pool_field p num_zones)" = 2 || return 1
    test "$(pool_field p size)" = 4 || return 1
    test "$(pool_field p min_size)" = 1 || return 1
    wait_for_clean || return 1

    # one OSD per zone can stop, the last copy of a zone can't
    ceph osd ok-to-stop 0 || return 1
    ceph osd ok-to-stop 0 2 || return 1
    ! ceph osd ok-to-stop 0 1 || return 1

    # growing from osd.0 must stop at dc1's last copy. Counting copies over
    # the whole pool instead would add osd.1 and osd.2, and then the full
    # check of that set would refuse
    local res=$(ceph osd ok-to-stop 0 --max 4 --format=json)
    test "$(echo $res | jq '.ok_to_stop')" = true || return 1
    test "$(echo $res | jq -c '.osds')" = "[0]" || return 1
}

main mon-stretch-ok-to-stop "$@"
