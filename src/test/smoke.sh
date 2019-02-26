#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

mon_port=$(get_unused_port)

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:$mon_port"
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    set -e

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
	$func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_minimal() {
    local dir=$1

    run_mon $dir a
    run_mgr $dir x
    run_osd $dir 0
    run_osd $dir 1
    run_osd $dir 2
    create_rbd_pool
    wait_for_clean
}

function TEST_multimon() {
    local dir=$1
    MONA="127.0.0.1:$((mon_port++))"
    MONB="127.0.0.1:$((mon_port++))"
    MONC="127.0.0.1:$((mon_port++))"

    run_mon $dir a --public-addr $MONA
    run_mon $dir b --public-addr $MONB
    run_mon $dir c --public_addr $MONC
    run_mgr $dir x
    run_mgr $dir y
    run_osd $dir 0
    run_osd $dir 1
    run_osd $dir 2

    ceph osd pool create foo 32
    ceph osd out 0
    wait_for_clean

    timeout 8 rados -p foo bench 4 write -b 4096 --no-cleanup || return 1
    wait_for_clean

    ceph osd in 0
    flush_pg_stats
    wait_for_clean
}

main smoke "$@"
