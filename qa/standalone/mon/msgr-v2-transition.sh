#!/usr/bin/env bash
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_V1="v1:127.0.0.1:7148" # git grep '\<7148\>' : there must be only one
    export CEPH_MON_V2="v2:127.0.0.1:7149" # git grep '\<7149\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "

    local funcs=${@:-$(set | ${SED} -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_mon_v1_osd_addrs() {
    local dir=$1

    export CEPH_ARGS="$CEPH_ARGS --mon-host=$CEPH_MON_V1 --mon-debug-no-require-octopus --mon-debug-no-require-nautilus"
    run_mon $dir a || return 1

    ceph mon dump | grep mon.a | grep $CEPH_MON_V1

    run_osd $dir 0 || return 1
    wait_for_osd up 0 || return 1
    ceph osd dump | grep osd.0 | grep v1: || return 1
    ceph osd dump | grep osd.0 | grep v2: && return 1

    ceph osd require-osd-release nautilus

    ceph osd down 0
    wait_for_osd up 0 || return 1

    # public should be v1, cluster v2
    ceph osd dump | grep osd.0 | grep v1: || return 1
    ceph osd dump -f json | jq '.osds[0].public_addrs.addrvec[0]' | grep v1 || return 1
    ceph osd dump -f json | jq '.osds[0].cluster_addrs.addrvec[0]' | grep v2 || return 1

    # enable v2 port on mon
    ceph mon set-addrs a "[$CEPH_MON_V2,$CEPH_MON_V1]"

    ceph osd down 0
    wait_for_osd up 0 || return 1

    # both public and cluster should be v2+v1
    ceph osd dump | grep osd.0 | grep v1: || return 1
    ceph osd dump -f json | jq '.osds[0].public_addrs.addrvec[0]' | grep v2 || return 1
    ceph osd dump -f json | jq '.osds[0].cluster_addrs.addrvec[0]' | grep v2 || return 1
}

function TEST_mon_v2v1_osd_addrs() {
    local dir=$1

    export CEPH_ARGS="$CEPH_ARGS --mon-host=[$CEPH_MON_V2,$CEPH_MON_V1] --mon-debug-no-require-octopus --mon-debug-no-require-nautilus"
    run_mon $dir a || return 1

    ceph mon dump | grep mon.a | grep $CEPH_MON_V1

    run_osd $dir 0 || return 1
    wait_for_osd up 0 || return 1
    ceph osd dump | grep osd.0 | grep v1: || return 1
    ceph osd dump | grep osd.0 | grep v2: && return 1

    ceph osd require-osd-release nautilus

    ceph osd down 0
    wait_for_osd up 0 || return 1

    # both public and cluster should be v2+v1
    ceph osd dump | grep osd.0 | grep v1: || return 1
    ceph osd dump -f json | jq '.osds[0].public_addrs.addrvec[0]' | grep v2 || return 1
    ceph osd dump -f json | jq '.osds[0].cluster_addrs.addrvec[0]' | grep v2 || return 1
}

main msgr-v2-transition "$@"
