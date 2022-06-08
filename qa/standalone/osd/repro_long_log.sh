#!/usr/bin/env bash
#
# Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
# Copyright (C) 2018 Red Hat <contact@redhat.com>
#
# Author: Josh Durgin <jdurgin@redhat.com>
# Author: David Zafman <dzafman@redhat.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7100" # git grep '\<7100\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

PGID=

function test_log_size()
{
    local PGID=$1
    local EXPECTED=$2
    local DUPS_EXPECTED=${3:-0}
    ceph tell osd.\* flush_pg_stats
    sleep 3
    ceph pg $PGID query | jq .info.stats.log_size
    ceph pg $PGID query | jq .info.stats.log_size | grep "${EXPECTED}"
    ceph pg $PGID query | jq .info.stats.log_dups_size
    ceph pg $PGID query | jq .info.stats.log_dups_size | grep "${DUPS_EXPECTED}"
}

function setup_log_test() {
    local dir=$1
    local which=$2

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    ceph osd pool create test 1 1 || true
    POOL_ID=$(ceph osd dump --format json | jq '.pools[] | select(.pool_name == "test") | .pool')
    PGID="${POOL_ID}.0"

    # With 1 PG setting entries per osd 20 results in a target log of 20
    ceph tell osd.\* injectargs -- --osd_target_pg_log_entries_per_osd 20 || return 1
    ceph tell osd.\* injectargs -- --osd-min-pg-log-entries 20 || return 1
    ceph tell osd.\* injectargs -- --osd-max-pg-log-entries 30 || return 1
    ceph tell osd.\* injectargs -- --osd-pg-log-trim-min 10 || return 1
    ceph tell osd.\* injectargs -- --osd_pg_log_dups_tracked 20 || return 1

    touch $dir/foo
    for i in $(seq 1 20)
    do
        rados -p test put foo $dir/foo || return 1
    done

    test_log_size $PGID 20 || return 1

    rados -p test rm foo || return 1

    # generate error entries
    for i in $(seq 1 20)
    do
        rados -p test rm foo
    done

    # log should have been trimmed down to min_entries with one extra
    test_log_size $PGID 21 || return 1
}

function TEST_repro_long_log1()
{
    local dir=$1

    setup_log_test $dir || return 1
    # regular write should trim the log
    rados -p test put foo $dir/foo || return 1
    test_log_size $PGID 22 || return 1
}

function TEST_repro_long_log2()
{
    local dir=$1

    setup_log_test $dir || return 1
    local PRIMARY=$(ceph pg $PGID query  | jq '.info.stats.up_primary')
    kill_daemons $dir TERM osd.$PRIMARY || return 1
    CEPH_ARGS="--osd-max-pg-log-entries=2 --osd-pg-log-dups-tracked=3 --no-mon-config" ceph-objectstore-tool --data-path $dir/$PRIMARY --pgid $PGID --op trim-pg-log || return 1
    activate_osd $dir $PRIMARY || return 1
    wait_for_clean || return 1
    test_log_size $PGID 21 18 || return 1
}

function TEST_trim_max_entries()
{
    local dir=$1

    setup_log_test $dir || return 1

    ceph tell osd.\* injectargs -- --osd_target_pg_log_entries_per_osd 2 || return 1
    ceph tell osd.\* injectargs -- --osd-min-pg-log-entries 2
    ceph tell osd.\* injectargs -- --osd-pg-log-trim-min 2
    ceph tell osd.\* injectargs -- --osd-pg-log-trim-max 4
    ceph tell osd.\* injectargs -- --osd_pg_log_dups_tracked 0

    # adding log entries, should only trim 4 and add one each time
    rados -p test rm foo
    test_log_size $PGID 18 || return 1
    rados -p test rm foo
    test_log_size $PGID 15 || return 1
    rados -p test rm foo
    test_log_size $PGID 12 || return 1
    rados -p test rm foo
    test_log_size $PGID 9 || return 1
    rados -p test rm foo
    test_log_size $PGID 6 || return 1
    rados -p test rm foo
    test_log_size $PGID 3 || return 1

    # below trim_min
    rados -p test rm foo
    test_log_size $PGID 4 || return 1
    rados -p test rm foo
    test_log_size $PGID 3 || return 1
    rados -p test rm foo
    test_log_size $PGID 4 || return 1
    rados -p test rm foo
    test_log_size $PGID 3 || return 1
}

function TEST_trim_max_entries_with_dups()
{
    local dir=$1

    setup_log_test $dir || return 1

    ceph tell osd.\* injectargs -- --osd_target_pg_log_entries_per_osd 2 || return 1
    ceph tell osd.\* injectargs -- --osd-min-pg-log-entries 2
    ceph tell osd.\* injectargs -- --osd-pg-log-trim-min 2
    ceph tell osd.\* injectargs -- --osd-pg-log-trim-max 4
    ceph tell osd.\* injectargs -- --osd_pg_log_dups_tracked 20 || return 1

    # adding log entries, should only trim 4 and add one each time
    # dups should be trimmed to 1
    rados -p test rm foo
    test_log_size $PGID 18 2 || return 1
    rados -p test rm foo
    test_log_size $PGID 15 5 || return 1
    rados -p test rm foo
    test_log_size $PGID 12 8 || return 1
    rados -p test rm foo
    test_log_size $PGID 9 11 || return 1
    rados -p test rm foo
    test_log_size $PGID 6 14 || return 1
    rados -p test rm foo
    test_log_size $PGID 3 17 || return 1

    # below trim_min
    rados -p test rm foo
    test_log_size $PGID 4 17 || return 1
    rados -p test rm foo
    test_log_size $PGID 3 17 || return 1
    rados -p test rm foo
    test_log_size $PGID 4 17 || return 1
    rados -p test rm foo
    test_log_size $PGID 3 17 || return 1
}

main repro-long-log "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && ../qa/run-standalone.sh repro_long_log.sh"
# End:
