#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    # Fix port????
    export CEPH_MON="127.0.0.1:7132" # git grep '\<7132\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    export margin=10
    export objects=200
    export poolname=test

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_ec_error_rollforward() {
    local dir=$1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1

    ceph osd erasure-code-profile set ec-profile m=2 k=2 crush-failure-domain=osd
    ceph osd pool create ec 1 1 erasure ec-profile

    rados -p ec put foo /etc/passwd

    kill -STOP `cat $dir/osd.2.pid`

    rados -p ec rm foo &
    pids="$!"
    sleep 1
    rados -p ec rm a &
    pids+=" $!"
    rados -p ec rm b &
    pids+=" $!"
    rados -p ec rm c &
    pids+=" $!"
    sleep 1
    kill -9 `cat $dir/osd.?.pid`
    kill $pids
    wait

    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1

    wait_for_clean || return 1
}

main ec-error-rollforward "$@"
