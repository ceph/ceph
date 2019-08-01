#!/usr/bin/env bash
#
# Copyright (C) 2019 Red Hat <contact@redhat.com>
#
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

    # Fix port????
    export CEPH_MON="127.0.0.1:7129" # git grep '\<7129\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON --osd_max_backfills=1 --debug_reserver=20 "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}


function _common_test() {
    local dir=$1
    local extra_opts="$2"
    local loglen="$3"
    local dupslen="$4"
    local objects="$5"
    local moreobjects=${6:-0}

    local OSDS=6

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    export CEPH_ARGS

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd $extra_opts || return 1
    done

    create_pool test 1 1

    for j in $(seq 1 $objects)
    do
       rados -p test put obj-${j} /etc/passwd
    done

    # Mark out all OSDs for this pool
    ceph osd out $(ceph pg dump pgs --format=json | jq '.[0].up[]')
    if [ "$moreobjects" != "0" ]; then
      for j in $(seq 1 $moreobjects)
      do
        rados -p test put obj-more-${j} /etc/passwd
      done
    fi
    sleep 1
    wait_for_clean

    newprimary=$(ceph pg dump pgs --format=json | jq '.[0].up_primary')
    kill_daemons

    ERRORS=0
    _objectstore_tool_nodown $dir $newprimary --no-mon-config --pgid 1.0 --op log | tee $dir/result.log
    LOGLEN=$(jq '.pg_log_t.log | length' $dir/result.log)
    if [ $LOGLEN != "$loglen" ]; then
	echo "FAILED: Wrong log length got $LOGLEN (expected $loglen)"
	ERRORS=$(expr $ERRORS + 1)
    fi
    DUPSLEN=$(jq '.pg_log_t.dups | length' $dir/result.log)
    if [ $DUPSLEN != "$dupslen" ]; then
	echo "FAILED: Wrong dups length got $DUPSLEN (expected $dupslen)"
	ERRORS=$(expr $ERRORS + 1)
    fi
    grep "copy_up_to\|copy_after" $dir/osd.*.log
    rm -f $dir/result.log
    if [ $ERRORS != "0" ]; then
	 echo TEST FAILED
	 return 1
    fi
}


# Cause copy_up_to() to only partially copy logs, copy additional dups, and trim dups
function TEST_backfill_log_1() {
    local dir=$1

    _common_test $dir "--osd_min_pg_log_entries=1 --osd_max_pg_log_entries=2 --osd_pg_log_dups_tracked=10" 1 9 150
}


# Cause copy_up_to() to only partially copy logs, copy additional dups
function TEST_backfill_log_2() {
    local dir=$1

    _common_test $dir "--osd_min_pg_log_entries=1 --osd_max_pg_log_entries=2" 1 149 150
}


# Cause copy_after() to only copy logs, no dups
function TEST_recovery_1() {
    local dir=$1

    _common_test $dir "--osd_min_pg_log_entries=50 --osd_max_pg_log_entries=50 --osd_pg_log_dups_tracked=60 --osd_pg_log_trim_min=10" 40 0 40
}


# Cause copy_after() to copy logs with dups
function TEST_recovery_2() {
    local dir=$1

    _common_test $dir "--osd_min_pg_log_entries=150 --osd_max_pg_log_entries=150 --osd_pg_log_dups_tracked=3000 --osd_pg_log_trim_min=10" 151 10 141 20
}

main osd-backfill-recovery-log "$@"

# Local Variables:
# compile-command: "make -j4 && ../qa/run-standalone.sh osd-backfill-recovery-log.sh"
# End:
