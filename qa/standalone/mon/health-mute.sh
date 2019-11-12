#!/bin/bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7143" # git grep '\<714\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none --mon-pg-warn-min-per-osd 0 --mon-max-pg-per-osd 1000 "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_mute() {
    local dir=$1
    setup $dir || return 1

    set -o pipefail

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    ceph osd pool create foo 8
    ceph osd pool application enable foo rbd --yes-i-really-mean-it
    wait_for_clean || return 1

    ceph -s
    ceph health | grep HEALTH_OK || return 1
    # test warning on setting pool size=1
    ceph osd pool set foo size 1
    ceph -s
    ceph health | grep HEALTH_WARN || return 1
    ceph health detail | grep POOL_NO_REDUNDANCY || return 1
    ceph health mute POOL_NO_REDUNDANCY
    ceph -s
    ceph health | grep HEALTH_OK | grep POOL_NO_REDUNDANCY || return 1
    ceph health unmute POOL_NO_REDUNDANCY
    ceph -s
    ceph health | grep HEALTH_WARN || return 1
    # restore pool size to default
    ceph osd pool set foo size 3
    ceph -s
    ceph health | grep HEALTH_OK || return 1
    ceph osd set noup
    ceph -s
    ceph health detail | grep OSDMAP_FLAGS || return 1
    ceph osd down 0
    ceph -s
    ceph health detail | grep OSD_DOWN || return 1
    ceph health detail | grep HEALTH_WARN || return 1

    ceph health mute OSD_DOWN
    ceph health mute OSDMAP_FLAGS
    ceph -s
    ceph health | grep HEALTH_OK | grep OSD_DOWN | grep OSDMAP_FLAGS || return 1
    ceph health unmute OSD_DOWN
    ceph -s
    ceph health | grep HEALTH_WARN || return 1

    # ttl
    ceph health mute OSD_DOWN 10s
    ceph -s
    ceph health | grep HEALTH_OK || return 1
    sleep 15
    ceph -s
    ceph health | grep HEALTH_WARN || return 1

    # sticky
    ceph health mute OSDMAP_FLAGS --sticky
    ceph osd unset noup
    sleep 5
    ceph -s
    ceph health | grep OSDMAP_FLAGS || return 1
    ceph osd set noup
    ceph -s
    ceph health | grep HEALTH_OK || return 1

    # rachet down on OSD_DOWN count
    ceph osd down 0 1
    ceph -s
    ceph health detail | grep OSD_DOWN || return 1

    ceph health mute OSD_DOWN
    kill_daemons $dir TERM osd.0
    ceph osd unset noup
    sleep 10
    ceph -s
    ceph health detail | grep OSD_DOWN || return 1
    ceph health detail | grep '1 osds down' || return 1
    ceph health | grep HEALTH_OK || return 1

    sleep 10 # give time for mon tick to rachet the mute
    ceph osd set noup
    ceph health mute OSDMAP_FLAGS
    ceph -s
    ceph health detail
    ceph health | grep HEALTH_OK || return 1

    ceph osd down 1
    ceph -s
    ceph health detail
    ceph health detail | grep '2 osds down' || return 1

    sleep 10 # give time for mute to clear
    ceph -s
    ceph health detail
    ceph health | grep HEALTH_WARN || return 1
    ceph health detail | grep '2 osds down' || return 1

    teardown $dir || return 1
}

main health-mute "$@"
