#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

set -e

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:$(get_unused_port)"
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

function TEST_safe_to_destroy() {
    local dir=$1

    run_mon $dir a
    run_mgr $dir x
    run_osd $dir 0
    run_osd $dir 1
    run_osd $dir 2
    run_osd $dir 3

    flush_pg_stats

    ceph osd safe-to-destroy 0
    ceph osd safe-to-destroy 1
    ceph osd safe-to-destroy 2
    ceph osd safe-to-destroy 3

    ceph osd pool create foo 128
    sleep 2
    flush_pg_stats
    wait_for_clean

    expect_failure $dir 'pgs currently' osd safe-to-destroy 0
    expect_failure $dir 'pgs currently' ceph osd safe-to-destroy 1
    expect_failure $dir 'pgs currently' ceph osd safe-to-destroy 2
    expect_failure $dir 'pgs currently' ceph osd safe-to-destroy 3

    ceph osd out 0
    sleep 2
    flush_pg_stats
    wait_for_clean

    ceph osd safe-to-destroy 0

    # even osds without osd_stat are ok if all pgs are active+clean
    id=`ceph osd create`
    ceph osd safe-to-destroy $id
}

function TEST_ok_to_stop() {
    local dir=$1

    run_mon $dir a
    run_mgr $dir x
    run_osd $dir 0
    run_osd $dir 1
    run_osd $dir 2
    run_osd $dir 3

    ceph osd pool create foo 128
    ceph osd pool set foo size 3
    ceph osd pool set foo min_size 2
    sleep 1
    flush_pg_stats
    wait_for_clean

    ceph osd ok-to-stop 0
    ceph osd ok-to-stop 1
    ceph osd ok-to-stop 2
    ceph osd ok-to-stop 3
    expect_failure $dir degraded ceph osd ok-to-stop 0 1

    ceph osd pool set foo min_size 1
    sleep 1
    flush_pg_stats
    wait_for_clean
    ceph osd ok-to-stop 0 1
    ceph osd ok-to-stop 1 2
    ceph osd ok-to-stop 2 3
    ceph osd ok-to-stop 3 4
    expect_failure $dir degraded ceph osd ok-to-stop 0 1 2
    expect_failure $dir degraded ceph osd ok-to-stop 0 1 2 3
}

main safe-to-destroy "$@"
