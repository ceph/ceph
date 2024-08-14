#! /usr/bin/env bash
#
# Copyright (C) 2017 Red Hat <contact@redhat.com>
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

    export CEPH_MON="127.0.0.1:7124" # git grep '\<7124\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--osd-op-queue=wpq "

    export -n CEPH_CLI_TEST_DUP_COMMAND
    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done
}

# Simple test for "recovery in progress. Only high priority scrubs allowed."
# OSD::sched_scrub() called on all OSDs during ticks
function TEST_recovery_scrub_1() {
    local dir=$1
    local poolname=test

    TESTDATA="testdata.$$"
    OSDS=4
    PGS=1
    OBJECTS=100
    ERRORS=0

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd-scrub-interval-randomize-ratio=0 "
    ceph_osd_args+="--osd_scrub_backoff_ratio=0 "
    ceph_osd_args+="--osd_stats_update_period_not_scrubbing=3 "
    ceph_osd_args+="--osd_stats_update_period_scrubbing=2"
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
        run_osd $dir $osd --osd_scrub_during_recovery=false || return 1
    done

    # Create a pool with $PGS pgs
    create_pool $poolname $PGS $PGS
    wait_for_clean || return 1
    poolid=$(ceph osd dump | grep "^pool.*[']test[']" | awk '{ print $2 }')

    ceph pg dump pgs

    dd if=/dev/urandom of=$TESTDATA bs=1M count=50
    for i in $(seq 1 $OBJECTS)
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    ceph osd pool set $poolname size 4

    # Wait for recovery to start
    set -o pipefail
    count=0
    while(true)
    do
      if ceph --format json pg dump pgs |
        jq '.pg_stats | [.[] | .state | contains("recovering")]' | grep -q true
      then
        break
      fi
      sleep 2
      if test "$count" -eq "10"
      then
        echo "Recovery never started"
        return 1
      fi
      count=$(expr $count + 1)
    done
    set +o pipefail
    ceph pg dump pgs

    sleep 10
    # Work around for http://tracker.ceph.com/issues/38195
    kill_daemons $dir #|| return 1

    declare -a err_strings
    err_strings[0]="recovery in progress. Only high priority scrubs allowed."

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
        grep "recovery in progress. Only high priority scrubs allowed." $dir/osd.${osd}.log
    done
    for err_string in "${err_strings[@]}"
    do
        found=false
	count=0
        for osd in $(seq 0 $(expr $OSDS - 1))
        do
            if grep -q "$err_string" $dir/osd.${osd}.log
            then
                found=true
		count=$(expr $count + 1)
            fi
        done
        if [ "$found" = "false" ]; then
            echo "Missing log message '$err_string'"
            ERRORS=$(expr $ERRORS + 1)
        fi
        [ $count -eq $OSDS ] || return 1
    done

    teardown $dir || return 1

    if [ $ERRORS != "0" ];
    then
        echo "TEST FAILED WITH $ERRORS ERRORS"
        return 1
    fi

    echo "TEST PASSED"
    return 0
}

##
# a modified version of wait_for_scrub(), which terminates if the Primary
# of the to-be-scrubbed PG changes
#
# Given the *last_scrub*, wait for scrub to happen on **pgid**. It
# will fail if scrub does not complete within $TIMEOUT seconds. The
# repair is complete whenever the **get_last_scrub_stamp** function
# reports a timestamp different from the one given in argument.
#
# @param pgid the id of the PG
# @param the primary OSD when started
# @param last_scrub timestamp of the last scrub for *pgid*
# @return 0 on success, 1 on error
#
function wait_for_scrub_mod() {
    local pgid=$1
    local orig_primary=$2
    local last_scrub="$3"
    local sname=${4:-last_scrub_stamp}

    for ((i=0; i < $TIMEOUT; i++)); do
        sleep 0.2
        if test "$(get_last_scrub_stamp $pgid $sname)" '>' "$last_scrub" ; then
            return 0
        fi
        sleep 1
        # are we still the primary?
        local current_primary=`bin/ceph pg $pgid query | jq '.acting[0]' `
        if [ $orig_primary != $current_primary ]; then
            echo $orig_primary no longer primary for $pgid
            return 0
        fi
    done
    return 1
}

##
# A modified version of pg_scrub()
#
# Run scrub on **pgid** and wait until it completes. The pg_scrub
# function will fail if repair does not complete within $TIMEOUT
# seconds. The pg_scrub is complete whenever the
# **get_last_scrub_stamp** function reports a timestamp different from
# the one stored before starting the scrub, or whenever the Primary
# changes.
#
# @param pgid the id of the PG
# @return 0 on success, 1 on error
#
function pg_scrub_mod() {
    local pgid=$1
    local last_scrub=$(get_last_scrub_stamp $pgid)
    # locate the primary
    local my_primary=`bin/ceph pg $pgid query | jq '.acting[0]' `
    local recovery=false
    ceph pg scrub $pgid
    #ceph --format json pg dump pgs | jq ".pg_stats | .[] | select(.pgid == \"$pgid\") | .state"
    if ceph --format json pg dump pgs | jq ".pg_stats | .[] | select(.pgid == \"$pgid\") | .state" | grep -q recovering
    then
      recovery=true
    fi
    wait_for_scrub_mod $pgid $my_primary "$last_scrub" || return 1
    if test $recovery = "true"
    then
      return 2
    fi
}

# Same as wait_background() except that it checks for exit code 2 and bumps recov_scrub_count
function wait_background_check() {
    # We extract the PIDS from the variable name
    pids=${!1}

    return_code=0
    for pid in $pids; do
        wait $pid
	retcode=$?
	if test $retcode -eq 2
	then
	  recov_scrub_count=$(expr $recov_scrub_count + 1)
	elif test $retcode -ne 0
	then
            # If one process failed then return 1
            return_code=1
        fi
    done

    # We empty the variable reporting that all process ended
    eval "$1=''"

    return $return_code
}

# osd_scrub_during_recovery=true make sure scrub happens
function TEST_recovery_scrub_2() {
    local dir=$1
    local poolname=test

    TESTDATA="testdata.$$"
    OSDS=8
    PGS=32
    OBJECTS=40

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd-scrub-interval-randomize-ratio=0 "
    ceph_osd_args+="--osd_scrub_backoff_ratio=0 "
    ceph_osd_args+="--osd_stats_update_period_not_scrubbing=3 "
    ceph_osd_args+="--osd_stats_update_period_scrubbing=2"
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
        run_osd $dir $osd --osd_scrub_during_recovery=true --osd_recovery_sleep=10 \
                          $ceph_osd_args || return 1
    done

    # Create a pool with $PGS pgs
    create_pool $poolname $PGS $PGS
    wait_for_clean || return 1
    poolid=$(ceph osd dump | grep "^pool.*[']test[']" | awk '{ print $2 }')

    dd if=/dev/urandom of=$TESTDATA bs=1M count=50
    for i in $(seq 1 $OBJECTS)
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    ceph osd pool set $poolname size 3

    ceph pg dump pgs

    # note that the following will be needed if the mclock scheduler is specified
    #ceph tell osd.* config get osd_mclock_override_recovery_settings

    # the '_max_active' is expected to be 0
    ceph tell osd.1 config get osd_recovery_max_active
    # both next parameters are expected to be >=3
    ceph tell osd.1 config get osd_recovery_max_active_hdd
    ceph tell osd.1 config get osd_recovery_max_active_ssd

    # Wait for recovery to start
    count=0
    while(true)
    do
      #ceph --format json pg dump pgs | jq '.pg_stats | [.[].state]'
      if test $(ceph --format json pg dump pgs |
	      jq '.pg_stats | [.[].state]'| grep recovering | wc -l) -ge 2
      then
        break
      fi
      sleep 2
      if test "$count" -eq "10"
      then
        echo "Not enough recovery started simultaneously"
        return 1
      fi
      count=$(expr $count + 1)
    done
    ceph pg dump pgs

    pids=""
    recov_scrub_count=0
    for pg in $(seq 0 $(expr $PGS - 1))
    do
        run_in_background pids pg_scrub_mod $poolid.$(printf "%x" $pg)
    done
    wait_background_check pids
    return_code=$?
    if [ $return_code -ne 0 ]; then return $return_code; fi

    ERRORS=0
    if test $recov_scrub_count -eq 0
    then
      echo "No scrubs occurred while PG recovering"
      ERRORS=$(expr $ERRORS + 1)
    fi

    pidfile=$(find $dir 2>/dev/null | grep $name_prefix'[^/]*\.pid')
    pid=$(cat $pidfile)
    if ! kill -0 $pid
    then
        echo "OSD crash occurred"
        #tail -100 $dir/osd.0.log
        ERRORS=$(expr $ERRORS + 1)
    fi

    # Work around for http://tracker.ceph.com/issues/38195
    kill_daemons $dir #|| return 1

    declare -a err_strings
    err_strings[0]="not scheduling scrubs due to active recovery"

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
        grep "not scheduling scrubs" $dir/osd.${osd}.log
    done
    for err_string in "${err_strings[@]}"
    do
        found=false
        for osd in $(seq 0 $(expr $OSDS - 1))
        do
            if grep "$err_string" $dir/osd.${osd}.log > /dev/null;
            then
                found=true
            fi
        done
        if [ "$found" = "true" ]; then
            echo "Found log message not expected '$err_string'"
	    ERRORS=$(expr $ERRORS + 1)
        fi
    done

    teardown $dir || return 1

    if [ $ERRORS != "0" ];
    then
        echo "TEST FAILED WITH $ERRORS ERRORS"
        return 1
    fi

    echo "TEST PASSED"
    return 0
}

main osd-recovery-scrub "$@"

# Local Variables:
# compile-command: "cd build ; make -j4 && \
#    ../qa/run-standalone.sh osd-recovery-scrub.sh"
# End:
