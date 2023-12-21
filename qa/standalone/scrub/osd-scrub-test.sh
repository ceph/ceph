#!/usr/bin/env bash
#
# Copyright (C) 2018 Red Hat <contact@redhat.com>
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
source $CEPH_ROOT/qa/standalone/scrub/scrub-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7138" # git grep '\<7138\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    export -n CEPH_CLI_TEST_DUP_COMMAND
    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        echo "-------------- Prepare Test $func -------------------"
        setup $dir || return 1
        echo "-------------- Run Test $func -----------------------"
        $func $dir || return 1
        echo "-------------- Teardown Test $func ------------------"
        teardown $dir || return 1
        echo "-------------- Complete Test $func ------------------"
    done
}

function perf_counters() {
    local dir=$1
    local OSDS=$2
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      ceph tell osd.$osd counter dump | jq 'with_entries(select(.key | startswith("osd_scrub")))'
    done
}

function TEST_scrub_test() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local objects=15

    TESTDATA="testdata.$$"

    run_mon $dir a --osd_pool_default_size=3 || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd-scrub-interval-randomize-ratio=0 --osd-deep-scrub-randomize-ratio=0 "
    ceph_osd_args+="--osd_scrub_backoff_ratio=0 --osd_stats_update_period_not_scrubbing=3 "
    ceph_osd_args+="--osd_stats_update_period_scrubbing=2"
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd $ceph_osd_args || return 1
    done

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1
    poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')

    dd if=/dev/urandom of=$TESTDATA bs=1032 count=1
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    local primary=$(get_primary $poolname obj1)
    local otherosd=$(get_not_primary $poolname obj1)
    if [ "$otherosd" = "2" ];
    then
      local anotherosd="0"
    else
      local anotherosd="2"
    fi

    CORRUPT_DATA="corrupt-data.$$"
    dd if=/dev/urandom of=$CORRUPT_DATA bs=512 count=1
    objectstore_tool $dir $anotherosd obj1 set-bytes $CORRUPT_DATA
    rm -f $CORRUPT_DATA

    local pgid="${poolid}.0"
    pg_deep_scrub "$pgid" || return 1

    ceph pg dump pgs | grep ^${pgid} | grep -q -- +inconsistent || return 1
    test "$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_scrub_errors')" = "2" || return 1

    ceph osd out $primary
    wait_for_clean || return 1

    pg_deep_scrub "$pgid" || return 1

    test "$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_scrub_errors')" = "2" || return 1
    test "$(ceph pg $pgid query | jq '.peer_info[0].stats.stat_sum.num_scrub_errors')" = "2" || return 1
    ceph pg dump pgs | grep ^${pgid} | grep -q -- +inconsistent || return 1

    ceph osd in $primary
    wait_for_clean || return 1

    repair "$pgid" || return 1
    wait_for_clean || return 1

    # This sets up the test after we've repaired with previous primary has old value
    test "$(ceph pg $pgid query | jq '.peer_info[0].stats.stat_sum.num_scrub_errors')" = "2" || return 1
    ceph pg dump pgs | grep ^${pgid} | grep -vq -- +inconsistent || return 1

    ceph osd out $primary
    wait_for_clean || return 1

    test "$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_scrub_errors')" = "0" || return 1
    test "$(ceph pg $pgid query | jq '.peer_info[0].stats.stat_sum.num_scrub_errors')" = "0" || return 1
    test "$(ceph pg $pgid query | jq '.peer_info[1].stats.stat_sum.num_scrub_errors')" = "0" || return 1
    ceph pg dump pgs | grep ^${pgid} | grep -vq -- +inconsistent || return 1
    perf_counters $dir $OSDS
}

# Grab year-month-day
DATESED="s/\([0-9]*-[0-9]*-[0-9]*\).*/\1/"
DATEFORMAT="%Y-%m-%d"

function check_dump_scrubs() {
    local primary=$1
    local sched_time_check="$2"
    local deadline_check="$3"

    DS="$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) dump_scrubs)"
    # use eval to drop double-quotes
    eval SCHED_TIME=$(echo $DS | jq '.[0].sched_time')
    test $(echo $SCHED_TIME | sed $DATESED) = $(date +${DATEFORMAT} -d "now + $sched_time_check") || return 1
    # use eval to drop double-quotes
    eval DEADLINE=$(echo $DS | jq '.[0].deadline')
    test $(echo $DEADLINE | sed $DATESED) = $(date +${DATEFORMAT} -d "now + $deadline_check") || return 1
}

function TEST_interval_changes() {
    local poolname=test
    local OSDS=2
    local objects=10
    # Don't assume how internal defaults are set
    local day="$(expr 24 \* 60 \* 60)"
    local week="$(expr $day \* 7)"
    local min_interval=$day
    local max_interval=$week
    local WAIT_FOR_UPDATE=15

    TESTDATA="testdata.$$"

    # This min scrub interval results in 30 seconds backoff time
    run_mon $dir a --osd_pool_default_size=$OSDS || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd --osd_scrub_min_interval=$min_interval --osd_scrub_max_interval=$max_interval --osd_scrub_interval_randomize_ratio=0 || return 1
    done

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1
    local poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')

    dd if=/dev/urandom of=$TESTDATA bs=1032 count=1
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    local primary=$(get_primary $poolname obj1)

    # Check initial settings from above (min 1 day, min 1 week)
    check_dump_scrubs $primary "1 day" "1 week" || return 1

    # Change global osd_scrub_min_interval to 2 days
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) config set osd_scrub_min_interval $(expr $day \* 2)
    sleep $WAIT_FOR_UPDATE
    check_dump_scrubs $primary "2 days" "1 week" || return 1

    # Change global osd_scrub_max_interval to 2 weeks
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) config set osd_scrub_max_interval $(expr $week \* 2)
    sleep $WAIT_FOR_UPDATE
    check_dump_scrubs $primary "2 days" "2 week" || return 1

    # Change pool osd_scrub_min_interval to 3 days
    ceph osd pool set $poolname scrub_min_interval $(expr $day \* 3)
    sleep $WAIT_FOR_UPDATE
    check_dump_scrubs $primary "3 days" "2 week" || return 1

    # Change pool osd_scrub_max_interval to 3 weeks
    ceph osd pool set $poolname scrub_max_interval $(expr $week \* 3)
    sleep $WAIT_FOR_UPDATE
    check_dump_scrubs $primary "3 days" "3 week" || return 1
    perf_counters $dir $OSDS
}

function TEST_scrub_extended_sleep() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local objects=15

    TESTDATA="testdata.$$"

    DAY=$(date +%w)
    # Handle wrap
    if [ "$DAY" -ge "4" ];
    then
      DAY="0"
    fi
    # Start after 2 days in case we are near midnight
    DAY_START=$(expr $DAY + 2)
    DAY_END=$(expr $DAY + 3)

    run_mon $dir a --osd_pool_default_size=3 || return 1
    run_mgr $dir x || return 1

    local ceph_osd_args="--osd-scrub-interval-randomize-ratio=0 --osd-deep-scrub-randomize-ratio=0 "
    ceph_osd_args+="--osd_scrub_backoff_ratio=0 --osd_stats_update_period_not_scrubbing=3 "
    ceph_osd_args+="--osd_stats_update_period_scrubbing=2 --osd_scrub_sleep=0 "
    ceph_osd_args+="--osd_scrub_extended_sleep=20 --osd_scrub_begin_week_day=$DAY_START "
    ceph_osd_args+="--osd_op_queue=wpq --osd_scrub_end_week_day=$DAY_END "
    ceph_osd_args+="--bluestore_cache_autotune=false" # why needed?

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd $ceph_osd_args || return 1
    done

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1

    # Trigger a periodic scrub on a PG (no 'extended sleep' for h.p. scrubs)
    local pgid=$(get_pg $poolname SOMETHING)
    local primary=$(get_primary $poolname SOMETHING)
    local last_scrub=$(get_last_scrub_stamp $pgid)
    ceph tell $pgid schedule-scrub || return 1

    # Allow scrub to start extended sleep
    PASSED="false"
    for ((i=0; i < 15; i++)); do
      if grep -q "scrub state.*, sleeping" $dir/osd.${primary}.log
      then
	PASSED="true"
        break
      fi
      sleep 1
    done

    # Check that extended sleep was triggered
    if [ $PASSED = "false" ];
    then
      return 1
    fi

    # release scrub to run after extended sleep finishes
    ceph tell osd.$primary config set osd_scrub_begin_week_day 0
    ceph tell osd.$primary config set osd_scrub_end_week_day 0

    # Due to extended sleep, the scrub should not be done within 20 seconds
    # but test up to 10 seconds and make sure it happens by 25 seconds.
    count=0
    PASSED="false"
    for ((i=0; i < 25; i++)); do
	count=$(expr $count + 1)
        if test "$(get_last_scrub_stamp $pgid)" '>' "$last_scrub" ; then
	    # Did scrub run too soon?
	    if [ $count -lt "10" ];
	    then
              return 1
            fi
	    PASSED="true"
	    break
        fi
        sleep 1
    done

    # Make sure scrub eventually ran
    if [ $PASSED = "false" ];
    then
      return 1
    fi
}

function _scrub_abort() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local objects=1000
    local type=$2

    TESTDATA="testdata.$$"
    if test $type = "scrub";
    then
      stopscrub="noscrub"
      check="noscrub"
    else
      stopscrub="nodeep-scrub"
      check="nodeep_scrub"
    fi

    run_mon $dir a --osd_pool_default_size=3 || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
        # Set scheduler to "wpq" until there's a reliable way to query scrub
        # states with "--osd-scrub-sleep" set to 0. The "mclock_scheduler"
        # overrides the scrub sleep to 0 and as a result the checks in the
        # test fail.
        run_osd $dir $osd --osd_pool_default_pg_autoscale_mode=off \
            --osd_deep_scrub_randomize_ratio=0.0 \
            --osd_scrub_sleep=5.0 \
            --osd_scrub_interval_randomize_ratio=0 \
            --osd_op_queue=wpq || return 1
    done

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1
    poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')

    dd if=/dev/urandom of=$TESTDATA bs=1032 count=1
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    local primary=$(get_primary $poolname obj1)
    local pgid="${poolid}.0"

    ceph tell $pgid schedule-$type || return 1

    # Wait for scrubbing to start
    set -o pipefail
    found="no"
    for i in $(seq 0 200)
    do
      flush_pg_stats
      if ceph pg dump pgs | grep  ^$pgid| grep -q "scrubbing"
      then
        found="yes"
        #ceph pg dump pgs
        break
      fi
    done
    set +o pipefail

    if test $found = "no";
    then
      echo "Scrubbing never started"
      return 1
    fi

    ceph osd set $stopscrub
    if [ "$type" = "deep-scrub" ];
    then
      ceph osd set noscrub
    fi

    # Wait for scrubbing to end
    set -o pipefail
    for i in $(seq 0 200)
    do
      flush_pg_stats
      if ceph pg dump pgs | grep ^$pgid | grep -q "scrubbing"
      then
        continue
      fi
      #ceph pg dump pgs
      break
    done
    set +o pipefail

    sleep 5

    if ! grep "$check set, aborting" $dir/osd.${primary}.log
    then
      echo "Abort not seen in log"
      return 1
    fi

    local last_scrub=$(get_last_scrub_stamp $pgid)
    ceph config set osd "osd_scrub_sleep" "0.1"

    ceph osd unset $stopscrub
    if [ "$type" = "deep-scrub" ];
    then
      ceph osd unset noscrub
    fi
    TIMEOUT=$(($objects / 2))
    wait_for_scrub $pgid "$last_scrub" || return 1
    perf_counters $dir $OSDS
}

function TEST_scrub_abort() {
    local dir=$1
    _scrub_abort $dir scrub
}

function TEST_deep_scrub_abort() {
    local dir=$1
    _scrub_abort $dir deep-scrub
}

function TEST_scrub_permit_time() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local objects=15

    TESTDATA="testdata.$$"

    run_mon $dir a --osd_pool_default_size=3 || return 1
    run_mgr $dir x || return 1
    local scrub_begin_hour=$(date -d '2 hour ago' +"%H" | sed 's/^0//')
    local scrub_end_hour=$(date -d '1 hour ago' +"%H" | sed 's/^0//')
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd --bluestore_cache_autotune=false \
	                --osd_deep_scrub_randomize_ratio=0.0 \
	                --osd_scrub_interval_randomize_ratio=0 \
                        --osd_scrub_begin_hour=$scrub_begin_hour \
                        --osd_scrub_end_hour=$scrub_end_hour || return 1
    done

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1

    # Trigger a scrub on a PG
    local pgid=$(get_pg $poolname SOMETHING)
    local primary=$(get_primary $poolname SOMETHING)
    local last_scrub=$(get_last_scrub_stamp $pgid)
    # If we don't specify an amount of time to subtract from
    # current time to set last_scrub_stamp, it sets the deadline
    # back by osd_max_interval which would cause the time permit checking
    # to be skipped.  Set back 1 day, the default scrub_min_interval.
    ceph tell $pgid schedule-scrub $(( 24 * 60 * 60 )) || return 1

    # Scrub should not run
    for ((i=0; i < 30; i++)); do
        if test "$(get_last_scrub_stamp $pgid)" '>' "$last_scrub" ; then
            return 1
        fi
        sleep 1
    done
    perf_counters $dir $OSDS
}

#  a test to recreate the problem described in bug #52901 - setting 'noscrub'
#  without explicitly preventing deep scrubs made the PG 'unscrubable'.
#  Fixed by PR#43521
function TEST_just_deep_scrubs() {
    local dir=$1
    local -A cluster_conf=(
        ['osds_num']="3" 
        ['pgs_in_pool']="4"
        ['pool_name']="test"
    )

    standard_scrub_cluster $dir cluster_conf
    local poolid=${cluster_conf['pool_id']}
    local poolname=${cluster_conf['pool_name']}
    echo "Pool: $poolname : $poolid"

    TESTDATA="testdata.$$"
    local objects=15
    dd if=/dev/urandom of=$TESTDATA bs=1032 count=1
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    # set both 'no scrub' & 'no deep-scrub', then request a deep-scrub.
    # we do not expect to see the scrub scheduled.

    ceph osd set noscrub || return 1
    ceph osd set nodeep-scrub || return 1
    sleep 6 # the 'noscrub' command takes a long time to reach the OSDs
    local now_is=`date -I"ns"`
    declare -A sched_data
    local pgid="${poolid}.2"

    # turn on the publishing of test data in the 'scrubber' section of 'pg query' output
    set_query_debug $pgid

    extract_published_sch $pgid $now_is $now_is sched_data
    local saved_last_stamp=${sched_data['query_last_stamp']}
    local dbg_counter_at_start=${sched_data['query_scrub_seq']}
    echo "test counter @ start: $dbg_counter_at_start"

    ceph tell $pgid schedule-deep-scrub

    sleep 5 # 5s is the 'pg dump' interval
    declare -A sc_data_2
    extract_published_sch $pgid $now_is $now_is sc_data_2
    echo "test counter @ should show no change: " ${sc_data_2['query_scrub_seq']}
    (( ${sc_data_2['dmp_last_duration']} == 0)) || return 1
    (( ${sc_data_2['query_scrub_seq']} == $dbg_counter_at_start)) || return 1

    # unset the 'no deep-scrub'. Deep scrubbing should start now.
    ceph osd unset nodeep-scrub || return 1
    sleep 5
    declare -A expct_qry_duration=( ['query_last_duration']="0" ['query_last_duration_neg']="not0" )
    sc_data_2=()
    echo "test counter @ should be higher than before the unset: " ${sc_data_2['query_scrub_seq']}
    wait_any_cond $pgid 10 $saved_last_stamp expct_qry_duration "WaitingAfterScrub " sc_data_2 || return 1
    perf_counters $dir ${cluster_conf['osds_num']}
}

function TEST_dump_scrub_schedule() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local objects=15

    TESTDATA="testdata.$$"

    run_mon $dir a --osd_pool_default_size=$OSDS || return 1
    run_mgr $dir x || return 1

    # Set scheduler to "wpq" until there's a reliable way to query scrub states
    # with "--osd-scrub-sleep" set to 0. The "mclock_scheduler" overrides the
    # scrub sleep to 0 and as a result the checks in the test fail.
    local ceph_osd_args="--osd_deep_scrub_randomize_ratio=0 \
            --osd_scrub_interval_randomize_ratio=0 \
            --osd_scrub_backoff_ratio=0.0 \
            --osd_op_queue=wpq \
            --osd_stats_update_period_not_scrubbing=3 \
            --osd_stats_update_period_scrubbing=2 \
            --osd_scrub_sleep=0.2"

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd $ceph_osd_args|| return 1
    done

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1
    poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')

    dd if=/dev/urandom of=$TESTDATA bs=1032 count=1
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    local pgid="${poolid}.0"
    local now_is=`date -I"ns"`

    # before the scrubbing starts

    # last scrub duration should be 0. The scheduling data should show
    # a time in the future:
    # e.g. 'periodic scrub scheduled @ 2021-10-12T20:32:43.645168+0000'

    declare -A expct_starting=( ['query_active']="false" ['query_is_future']="true" ['query_schedule']="scrub scheduled" )
    declare -A sched_data
    extract_published_sch $pgid $now_is "2019-10-12T20:32:43.645168+0000" sched_data
    schedule_against_expected sched_data expct_starting "initial"
    (( ${sched_data['dmp_last_duration']} == 0)) || return 1
    echo "last-scrub  --- " ${sched_data['query_last_scrub']}

    #
    # step 1: scrub once (mainly to ensure there is no urgency to scrub)
    #

    saved_last_stamp=${sched_data['query_last_stamp']}
    ceph tell osd.* config set osd_scrub_sleep "0"
    ceph tell $pgid deep-scrub

    # wait for the 'last duration' entries to change. Note that the 'dump' one will need
    # up to 5 seconds to sync

    sleep 5
    sched_data=()
    declare -A expct_qry_duration=( ['query_last_duration']="0" ['query_last_duration_neg']="not0" )
    wait_any_cond $pgid 10 $saved_last_stamp expct_qry_duration "WaitingAfterScrub " sched_data || return 1
    # verify that 'pg dump' also shows the change in last_scrub_duration
    sched_data=()
    declare -A expct_dmp_duration=( ['dmp_last_duration']="0" ['dmp_last_duration_neg']="not0" )
    wait_any_cond $pgid 10 $saved_last_stamp expct_dmp_duration "WaitingAfterScrub_dmp " sched_data || return 1

    sleep 2

    #
    # step 2: set noscrub and request a "periodic scrub". Watch for the change in the 'is the scrub
    #         scheduled for the future' value
    #

    ceph tell osd.* config set osd_scrub_chunk_max "3" || return 1
    ceph tell osd.* config set osd_scrub_sleep "1.0" || return 1
    ceph osd set noscrub || return 1
    sleep 2
    saved_last_stamp=${sched_data['query_last_stamp']}

    ceph tell $pgid schedule-scrub
    sleep 1
    sched_data=()
    declare -A expct_scrub_peri_sched=( ['query_is_future']="false" )
    wait_any_cond $pgid 10 $saved_last_stamp expct_scrub_peri_sched "waitingBeingScheduled" sched_data || return 1

    # note: the induced change in 'last_scrub_stamp' that we've caused above, is by itself not a publish-stats
    # trigger. Thus it might happen that the information in 'pg dump' will not get updated here. Do not expect
    # 'dmp_is_future' to follow 'query_is_future' without a good reason
    ## declare -A expct_scrub_peri_sched_dmp=( ['dmp_is_future']="false" )
    ## wait_any_cond $pgid 15 $saved_last_stamp expct_scrub_peri_sched_dmp "waitingBeingScheduled" sched_data || echo "must be fixed"

    #
    # step 3: allow scrubs. Watch for the conditions during the scrubbing
    #

    saved_last_stamp=${sched_data['query_last_stamp']}
    ceph osd unset noscrub

    declare -A cond_active=( ['query_active']="true" )
    sched_data=()
    wait_any_cond $pgid 10 $saved_last_stamp cond_active "WaitingActive " sched_data || return 1

    # check for pg-dump to show being active. But if we see 'query_active' being reset - we've just
    # missed it.
    declare -A cond_active_dmp=( ['dmp_state_has_scrubbing']="true" ['query_active']="false" )
    sched_data=()
    wait_any_cond $pgid 10 $saved_last_stamp cond_active_dmp "WaitingActive " sched_data || return 1
    perf_counters $dir $OSDS
}

function TEST_pg_dump_objects_scrubbed() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local objects=15
    local timeout=10

    TESTDATA="testdata.$$"

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=$OSDS || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd || return 1
    done

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1
    poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')

    dd if=/dev/urandom of=$TESTDATA bs=1032 count=1
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    local pgid="${poolid}.0"
    #Trigger a scrub on a PG
    pg_scrub $pgid || return 1
    test "$(ceph pg $pgid query | jq '.info.stats.objects_scrubbed')" '=' $objects || return 1
    perf_counters $dir $OSDS

    teardown $dir || return 1
}

main osd-scrub-test "$@"

# Local Variables:
# compile-command: "cd build ; make -j4 && \
#    ../qa/run-standalone.sh osd-scrub-test.sh"
# End:
