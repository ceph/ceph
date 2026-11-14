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
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
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

    run_mon $dir a --osd_pool_default_size=3 || return 1
    run_mgr $dir x --mgr_stats_period=1 || return 1
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

    local testdata_file=$(file_with_random_data 1032)
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $testdata_file || return 1
    done
    rm -f $testdata_file

    local primary=$(get_primary $poolname obj1)
    local otherosd=$(get_not_primary $poolname obj1)
    if [ "$otherosd" = "2" ];
    then
      local anotherosd="0"
    else
      local anotherosd="2"
    fi

    local corrupt_data_file=$(file_with_random_data 512)
    objectstore_tool $dir $anotherosd obj1 set-bytes $corrupt_data_file || return 1
    rm -f $corrupt_data_file

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

    DS="$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) dump_scrubs)"
    # use eval to drop double-quotes
    eval SCHED_TIME=$(echo $DS | jq '.[0].sched_time')
    test $(echo $SCHED_TIME | sed $DATESED) = $(date +${DATEFORMAT} -d "now + $sched_time_check") || return 1
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
    run_mgr $dir x --mgr_stats_period=1 || return 1
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
    check_dump_scrubs $primary "1 day" || return 1

    # Change global osd_scrub_min_interval to 2 days
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) config set osd_scrub_min_interval $(expr $day \* 2)
    sleep $WAIT_FOR_UPDATE
    check_dump_scrubs $primary "2 days" || return 1

    # Change global osd_scrub_max_interval to 2 weeks
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) config set osd_scrub_max_interval $(expr $week \* 2)
    sleep $WAIT_FOR_UPDATE
    check_dump_scrubs $primary "2 days" || return 1

    # Change pool osd_scrub_min_interval to 3 days
    ceph osd pool set $poolname scrub_min_interval $(expr $day \* 3)
    sleep $WAIT_FOR_UPDATE
    check_dump_scrubs $primary "3 days" || return 1

    # Change pool osd_scrub_max_interval to 3 weeks
    ceph osd pool set $poolname scrub_max_interval $(expr $week \* 3)
    sleep $WAIT_FOR_UPDATE
    check_dump_scrubs $primary "3 days" || return 1
    perf_counters $dir $OSDS
}

# RRR 6aug24: this test cannot work as expected, following the changes in the
#   scrub type to overrides matrix. Disabled for now.
function NO_scrub_extended_sleep() {
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
    run_mgr $dir x --mgr_stats_period=1 || return 1

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
    run_mgr $dir x --mgr_stats_period=1 || return 1
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
    run_mgr $dir x --mgr_stats_period=1 || return 1
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
    local objects=90
    dd if=/dev/urandom of=$TESTDATA bs=1032 count=1
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    # set both 'no scrub' & 'no deep-scrub', then request a deep-scrub.
    # we do not expect to see the scrub scheduled.

    ceph tell osd.* config set osd_scrub_retry_after_noscrub 2
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

    ceph pg dump pgs --format=json-pretty | jq -r '.pg_stats[] | "\(.pgid) \(.stat_sum.num_objects)"'
    echo "Objects # in pg $pgid: " $(ceph pg $pgid query --format=json-pretty | jq -r '.info.stats.stat_sum.num_objects')

    declare -A sc_data_2
    extract_published_sch $pgid $now_is $now_is sc_data_2
    echo "test counter @ should show no change: " ${sc_data_2['query_scrub_seq']}
    (( ${sc_data_2['dmp_last_duration']} == 0)) || return 1
    (( ${sc_data_2['query_scrub_seq']} == $dbg_counter_at_start)) || return 1

    # unset the 'no deep-scrub'. Deep scrubbing should start now.
    ceph osd unset nodeep-scrub || return 1
    sleep 8
    declare -A expct_qry_duration=( ['query_last_duration']="0" ['query_last_duration_neg']="not0" )
    sc_data_2=()
    extract_published_sch $pgid $now_is $now_is sc_data_2
    echo "test counter @ should be higher than before the unset: " ${sc_data_2['query_scrub_seq']}

    declare -A expct_qry_duration=( ['query_last_duration']="0" ['query_last_duration_neg']="not0" )
    sc_data_2=()
    wait_any_cond $pgid 10 $saved_last_stamp expct_qry_duration "WaitingAfterScrub " sc_data_2 || return 1
    perf_counters $dir ${cluster_conf['osds_num']}
}

function TEST_dump_scrub_schedule() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local objects=90

    TESTDATA="testdata.$$"

    run_mon $dir a --osd_pool_default_size=$OSDS || return 1
    run_mgr $dir x --mgr_stats_period=1 || return 1

    # Set scheduler to "wpq" until there's a reliable way to query scrub states
    # with "--osd-scrub-sleep" set to 0. The "mclock_scheduler" overrides the
    # scrub sleep to 0 and as a result the checks in the test fail.
    local ceph_osd_args="--osd_deep_scrub_randomize_ratio=0 \
            --osd_scrub_interval_randomize_ratio=0 \
            --osd_scrub_backoff_ratio=0.0 \
            --osd_op_queue=wpq \
            --osd_stats_update_period_not_scrubbing=1 \
            --osd_stats_update_period_scrubbing=1 \
            --osd_scrub_retry_after_noscrub=1 \
            --osd_scrub_retry_pg_state=2 \
            --osd_scrub_retry_delay=2 \
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
    #local now_is=`date -I"ns"` # note: uses a comma for the ns part
    local now_is=`date +'%Y-%m-%dT%H:%M:%S.%N%:z'`

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

    #
    # step 2: set noscrub and request a "periodic scrub". Watch for the change in the 'is the scrub
    #         scheduled for the future' value
    #

    ceph osd set noscrub || return 1
    sleep 2
    ceph tell osd.* config set osd_shallow_scrub_chunk_max "3" || return 1
    ceph tell osd.* config set osd_scrub_sleep "2.0" || return 1
    sleep 8
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
    wait_any_cond $pgid 10 $saved_last_stamp cond_active_dmp "WaitingActive " sched_data
    sleep 4
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
    run_mgr $dir x --mgr_stats_period=1 || return 1
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

function wait_initial_scrubs() {
    local -n pg_to_prim_dict=$1
    local extr_dbg=1 # note: 3 and above leave some temp files around

    # set a long schedule for the periodic scrubs. Wait for the
    # initial 'no previous scrub is known' scrubs to finish for all PGs.
    ceph tell osd.* config set osd_scrub_min_interval 7200
    ceph tell osd.* config set osd_deep_scrub_interval 14400
    ceph tell osd.* config set osd_max_scrubs 32
    ceph tell osd.* config set osd_scrub_sleep 1
    ceph tell osd.* config set osd_shallow_scrub_chunk_max 10
    ceph tell osd.* config set osd_scrub_chunk_max 10

    for pg in "${!pg_to_prim_dict[@]}"; do
      (( extr_dbg >= 1 )) && echo "Scheduling initial scrub for $pg"
      ceph tell $pg scrub || return 1
    done

    sleep 1
    (( extr_dbg >= 1 )) && ceph pg dump pgs --format=json-pretty | \
      jq '.pg_stats | map(select(.last_scrub_duration == 0)) | map({pgid: .pgid, last_scrub_duration: .last_scrub_duration})'

    tout=20
    while [ $tout -gt 0 ] ; do
      sleep 1
      (( extr_dbg >= 1 )) && ceph pg dump pgs --format=json-pretty | \
        jq '.pg_stats | map(select(.last_scrub_duration == 0)) | map({pgid: .pgid, last_scrub_duration: .last_scrub_duration})'
      not_done=$(ceph pg dump pgs --format=json-pretty | \
        jq '.pg_stats | map(select(.last_scrub_duration == 0)) | map({pgid: .pgid, last_scrub_duration: .last_scrub_duration})' | wc -l )
      # note that we should ignore a header line
      if [ "$not_done" -le 1 ]; then
        break
      fi
      not_done=$(( (not_done - 2) / 4 ))
      echo "Still waiting for $not_done PGs to finish initial scrubs (timeout $tout)"
      tout=$((tout - 1))
    done
    ceph pg dump pgs --format=json-pretty | \
      jq '.pg_stats | map(select(.last_scrub_duration == 0)) | map({pgid: .pgid, last_scrub_duration: .last_scrub_duration})'
    (( tout == 0 )) && return 1
    return 0
}


# Whenever a PG is being scrubbed at a regular, periodic, urgency, and is queued
# for its replicas:
# if the operator is requesting a scrub of the same PG, the operator's request
# should trigger an abort of the ongoing scrub.
#
# The test process:
# - a periodic scrub is initiated of a PG. That scrub is set to be a very slow one.
# - a second PG, which shares some of its replicas, is intrcuted to be scrubbed. That one
#   should be stuck in replica reservation. We will verify that.
# - now - the operator is requesting that second PG to be scrubbed. The original (pending)
#   scrub should be aborted. We would check for:
#   - the new, operator's scrub to be scheduled
#   - the replicas' reservers to be released
function TEST_abort_periodic_for_operator() {
    local dir=$1
    local -A cluster_conf=(
        ['osds_num']="5"
        ['pgs_in_pool']="16"
        ['pool_name']="test"
    )
    local extr_dbg=1 # note: 3 and above leave some temp files around

    standard_scrub_wpq_cluster "$dir" cluster_conf 2 || return 1
    local poolid=${cluster_conf['pool_id']}
    local poolname=${cluster_conf['pool_name']}
    echo "Pool: $poolname : $poolid"

    #turn off '-x' (but remember previous state)
    local saved_echo_flag=${-//[^x]/}
    set +x

    # fill the pool with some data
    TESTDATA="testdata.$$"
    dd if=/dev/urandom of=$TESTDATA bs=320 count=1
    for i in $( seq 1 256 )
    do
        rados -p "$poolname" put "obj${i}" $TESTDATA 2>/dev/null 1>/dev/null
    done
    rm -f $TESTDATA
    if [[ -n "$saved_echo_flag" ]]; then set -x; fi

    # create the dictionary of the PGs in the pool
    declare -A pg_pr
    declare -A pg_ac
    declare -A pg_po
    build_pg_dicts "$dir" pg_pr pg_ac pg_po "-"
    (( extr_dbg >= 2 )) && echo "PGs table:"
    for pg in "${!pg_pr[@]}"; do
      (( extr_dbg >= 2 )) && echo "Got: $pg: ${pg_pr[$pg]} ( ${pg_ac[$pg]} ) ${pg_po[$pg]}"
    done

    wait_initial_scrubs pg_pr || return 1

    # limit all OSDs to one scrub at a time
    ceph tell osd.* config set osd_max_scrubs 1
    ceph tell osd.* config set osd_stats_update_period_not_scrubbing 1

    # configure for slow scrubs
    ceph tell osd.* config set osd_scrub_sleep 3
    ceph tell osd.* config set osd_shallow_scrub_chunk_max 2
    ceph tell osd.* config set osd_scrub_chunk_max 2
    (( extr_dbg >= 2 )) && ceph tell osd.2 dump_scrub_reservations --format=json-pretty

    # the first PG to work with:
    local pg2=""
    for pg1 in "${!pg_pr[@]}"; do
      for pg in "${!pg_pr[@]}"; do
        if [[ "$pg" == "$pg1" ]]; then
          continue
        fi
        if [[ "${pg_pr[$pg]}" == "${pg_pr[$pg1]}" ]]; then
          local -i common=$(count_common_active $pg $pg1 pg_ac)
          if [[ $common -gt 1 ]]; then
            pg2=$pg
            break 2
          fi
        fi
      done
    done

    if [[ -z "$pg2" ]]; then
      echo "No PG found with the same primary as $pg1"
      return 0 # not an error
    fi

    # the common primary is allowed two concurrent scrubs
    ceph tell osd."${pg_pr[$pg1]}" config set osd_max_scrubs 2
    echo "The two PGs to manipulate are $pg1 and $pg2"

    set_query_debug "$pg1"
    # wait till the information published by pg1 is updated to show it as
    # not being scrubbed
    local is_act
    for i in $( seq 1 3 )
    do
      is_act=$(ceph pg "$pg1" query | jq '.scrubber.active')
      if [[ "$is_act" = "false" ]]; then
          break
      fi
      echo "Still waiting for pg $pg1 to finish scrubbing"
      sleep 0.7
    done
    ceph pg dump pgs
    if [[ "$is_act" != "false" ]]; then
      ceph pg "$pg1" query
      echo "PG $pg1 appears to be still scrubbing"
      return 1
    fi
    sleep 0.5

    echo "Initiating a periodic scrub of $pg1"
    (( extr_dbg >= 2 )) && ceph pg "$pg1" query -f json-pretty | jq '.scrubber'
    ceph tell $pg1 schedule-deep-scrub || return 1
    sleep 1
    (( extr_dbg >= 2 )) && ceph pg "$pg1" query -f json-pretty | jq '.scrubber'

    for i in $( seq 1 14 )
    do
      sleep 0.5
      stt=$(ceph pg "$pg1" query | jq '.scrubber')
      is_active=$(echo $stt | jq '.active')
      is_reserving_replicas=$(echo $stt | jq '.is_reserving_replicas')
      if [[ "$is_active" = "true" && "$is_reserving_replicas" = "false" ]]; then
          break
      fi
      echo "Still waiting for pg $pg1 to start scrubbing: $stt"
    done
    if [[ "$is_active" != "true" || "$is_reserving_replicas" != "false" ]]; then
      ceph pg "$pg1" query -f json-pretty | jq '.scrubber'
      echo "The scrub is not active or is reserving replicas"
      return 1
    fi
    (( extr_dbg >= 2 )) && ceph pg "$pg1" query -f json-pretty | jq '.scrubber'


    # PG 1 is scrubbing, and has reserved the replicas - soem of which are shared
    # by PG 2. As the max-scrubs was set to 1, that should prevent PG 2 from
    # reserving its replicas.

    (( extr_dbg >= 1 )) && ceph tell osd.* dump_scrub_reservations --format=json-pretty

    # now - the 2'nd scrub - which should be blocked on reserving
    set_query_debug "$pg2"
    ceph tell "$pg2" schedule-deep-scrub
    sleep 0.5
    (( extr_dbg >= 2 )) && echo "===================================================================================="
    (( extr_dbg >= 2 )) && ceph pg "$pg2" query -f json-pretty | jq '.scrubber'
    (( extr_dbg >= 2 )) && ceph pg "$pg1" query -f json-pretty | jq '.scrubber'
    sleep 1
    (( extr_dbg >= 2 )) && echo "===================================================================================="
    (( extr_dbg >= 2 )) && ceph pg "$pg2" query -f json-pretty | jq '.scrubber'
    (( extr_dbg >= 2 )) && ceph pg "$pg1" query -f json-pretty | jq '.scrubber'

    # make sure pg2 scrub is stuck in the reserving state
    local stt2=$(ceph pg "$pg2" query | jq '.scrubber')
    local pg2_is_reserving
    pg2_is_reserving=$(echo $stt2 | jq '.is_reserving_replicas')
    if [[ "$pg2_is_reserving" != "true" ]]; then
      echo "The scheduled scrub for $pg2 should have been stuck"
      ceph pg dump pgs
      return 1
    fi

    # now - issue an operator-initiated scrub on pg2.
    # The periodic scrub should be aborted, and the operator-initiated scrub should start.
    echo "Instructing $pg2 to perform a high-priority scrub"
    ceph tell "$pg2" scrub
    for i in $( seq 1 10 )
    do
      sleep 0.5
      stt2=$(ceph pg "$pg2" query | jq '.scrubber')
      pg2_is_active=$(echo $stt2 | jq '.active')
      pg2_is_reserving=$(echo $stt2 | jq '.is_reserving_replicas')
      if [[ "$pg2_is_active" = "true" && "$pg2_is_reserving" != "true" ]]; then
            break
      fi
      echo "Still waiting: $stt2"
    done

    if [[ "$pg2_is_active" != "true" || "$pg2_is_reserving" = "true" ]]; then
      echo "The high-priority scrub for $pg2 is not active or is reserving replicas"
      return 1
    fi
    echo "Done"
}



# Reproduces https://tracker.ceph.com/issues/79134
#
# When a periodic (reservation-requiring) scrub is initiated, the PG's
# scheduling target is wiped (SchedTarget::reset() inside
# start_scrub_session()) *before* the replica reservations are granted.
# If a replica then rejects the reservation, retry_penalized ->
# delay_on_failure() adds the retry delay to the wiped 'not_before' field,
# which holds the utime_t::max() sentinel (2106-02-07T06:28:15.999999).
# As utime_t saturates at 2^32-1 seconds, the addition leaves the target
# at 'never': the PG is never auto-scrubbed again (until an operator
# command or a configuration change recomputes its schedule).
#
# This test forces exactly that sequence:
# - PG 'A' runs a long periodic shallow scrub, holding the reservation of
#   a replica OSD 'R' (osd_max_scrubs=1; reservation queuing disabled, so
#   that a busy replica answers with an immediate REJECT);
# - PG 'B' (sharing R, different primary) is made overdue, so the OSD tick
#   selects its periodic deep scrub while R is still busy;
# - B's deep target gets wiped and its reservation is rejected by R.
#
# The test then verifies that B's deep target carries a real retry time
# (approx. now + osd_scrub_retry_delay) - and not the 2106 sentinel.
# On a cluster with the #79134 defect the test fails, reporting the parked
# target.
function TEST_reservation_reject_parks_target() {
    local dir=$1
    local poolname=test
    local OSDS=4
    local pool_size=3
    local retry_delay=5

    run_mon $dir a --osd_pool_default_size=$pool_size || return 1
    run_mgr $dir x --mgr_stats_period=1 || return 1
    local ceph_osd_args="--osd_max_scrubs=1 --osd_scrub_retry_delay=$retry_delay "
    ceph_osd_args+="--osd_scrub_interval_randomize_ratio=0 --osd_deep_scrub_randomize_ratio=0 "
    ceph_osd_args+="--osd_scrub_backoff_ratio=0 --osd_op_queue=wpq "
    ceph_osd_args+="--osd_scrub_disable_reservation_queuing=true "
    ceph_osd_args+="--osd_scrub_chunk_max=1 --osd_scrub_sleep=3.0 "
    ceph_osd_args+="--debug_osd=20 "
    ceph_osd_args+="--osd_stats_update_period_not_scrubbing=3 --osd_stats_update_period_scrubbing=2"
    for osd in $(seq 0 $(expr $OSDS - 1)); do
        run_osd $dir $osd $ceph_osd_args || return 1
    done

    create_pool $poolname 4 4 || return 1
    wait_for_clean || return 1
    local poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')

    TESTDATA="testdata.$$"
    dd if=/dev/urandom of=$TESTDATA bs=4096 count=1
    for i in $(seq 1 40); do
        rados -p $poolname put obj${i} $TESTDATA || return 1
    done
    rm -f $TESTDATA
    wait_for_clean || return 1

    # let the system-created pools' never-scrubbed PGs drain, so that our
    # PGs are not starved by earlier (1970-stamped) queue entries
    local busy i
    for i in $(seq 1 60); do
        busy=$(ceph pg dump pgs --format json 2>/dev/null | \
            jq '[.pg_stats[] | select(.state | contains("scrubbing"))] | length')
        [ "$busy" = "0" ] && break
        sleep 3
    done

    # deep-scrub our pool's PGs once (operator-initiated - no reservations),
    # so that afterwards only the PG we deliberately make overdue is a
    # deep-scrub candidate
    local pg
    for pg in $(ceph pg dump pgs --format json 2>/dev/null | \
            jq -r ".pg_stats[] | select(.pgid | startswith(\"${poolid}.\")) | .pgid"); do
        ceph tell $pg deep-scrub >/dev/null 2>&1
    done
    sleep 3
    wait_for_clean || return 1

    # find PGs 'A' and 'B': different primaries, intersecting replica sets;
    # 'R' is their shared replica OSD
    local A="" B="" Aprim="" Bprim="" R="" Areps=""
    local pgid prim acting reps cand
    while read -r pgid prim acting; do
        reps=${acting#*,}   # drop the primary
        if [ -z "$A" ]; then
            A=$pgid
            Aprim=$prim
            Areps=$reps
            continue
        fi
        if [ "$prim" = "$Aprim" ]; then
            continue
        fi
        cand=$(comm -12 \
            <(echo $Areps | tr ',' '\n' | sort) \
            <(echo $reps | tr ',' '\n' | sort) | head -1)
        if [ -n "$cand" ]; then
            B=$pgid
            Bprim=$prim
            R=$cand
            break
        fi
    done < <(ceph pg dump pgs --format json 2>/dev/null | \
        jq -r ".pg_stats[] | select(.pgid | startswith(\"${poolid}.\")) | \
               select((.acting | length) == 3) | \
               \"\(.pgid) \(.acting_primary) \(.acting | join(\",\"))\"")
    if [ -z "$B" ]; then
        echo "no suitable (A,B) PG pair with intersecting replicas found"
        return 1
    fi
    echo "A=$A (primary osd.$Aprim), B=$B (primary osd.$Bprim), shared replica R=osd.$R"

    local found_failure=false
    local found_sentinel=false
    local attempt act
    for attempt in 1 2 3 4 5; do
        # arm A: a periodic-urgency shallow scrub that holds R's reservation
        # for a while (40 objects / chunk_max=1 / scrub_sleep=2s)
        ceph tell $A schedule-scrub >/dev/null 2>&1
        act=false
        for i in $(seq 1 30); do
            act=$(ceph pg $A query --format json 2>/dev/null | \
                jq -r '.scrubber.active // false')
            [ "$act" = "true" ] && break
            sleep 2
        done
        if [ "$act" != "true" ]; then
            echo "attempt $attempt: A ($A) did not start scrubbing"
            continue
        fi

        # confirm that A actually holds R's replica reservation before
        # triggering B (otherwise B's request may be granted and scrub)
        local held=""
        for i in $(seq 1 15); do
            held=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${R}) \
                dump_scrub_reservations 2>/dev/null | \
                jq -r "[.remote_scrub_reservations.in_progress[].item] | join(\",\")")
            [[ "$held" == *"$A"* ]] && break
            sleep 2
        done
        if [[ "$held" != *"$A"* ]]; then
            echo "attempt $attempt: A did not take R's reservation (held: '$held')"
            continue
        fi

        # make B's deep scrub overdue, so that the OSD tick selects it while
        # R is still busy with A
        local logmark
        logmark=$(wc -l < $dir/osd.${Bprim}.log 2>/dev/null || echo 0)
        ceph tell $B schedule-deep-scrub >/dev/null 2>&1

        # poll for the rejection for up to ~30s (tick interval is 5s)
        local fresh
        for i in $(seq 1 15); do
            fresh=$(tail -n +$((logmark + 1)) $dir/osd.${Bprim}.log 2>/dev/null | \
                grep "scrub-job:pg\[${B}\]:delay_on_failure: delayed deep scrub due to replicas")
            if [ -n "$fresh" ]; then
                found_failure=true
                echo "$fresh" | grep -q "nb:2106-02-07" && found_sentinel=true
                echo "rejection recorded: $fresh"
                break
            fi
            sleep 2
        done
        [ -n "$fresh" ] && break
        echo "attempt $attempt: B's reservation was not rejected (A released too soon?)"
    done

    # the replica-rejection path must have been exercised for B's deep scrub
    if [ "$found_failure" != "true" ]; then
        echo "could not exercise the replica-rejection path for $B"
        return 1
    fi

    # the regression assertion: the rescheduled target must carry a real
    # retry time - never the 'utime_t::max()' sentinel. Two sources:
    # - the fresh 'delay_on_failure' log line (authoritative);
    # - 'dump_scrubs', if the target is still queued (it may have already
    #   been retried successfully - and dequeued - by now).
    local sched="--"
    sched=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${Bprim}) dump_scrubs 2>/dev/null | \
        jq -r --arg pg "$B" \
            '[.[] | select(.pgid==$pg) | select(.level=="deep")][0].sched_time // "--"')
    echo "B ($B) deep target after the reservation failure: $sched"
    if [ "$found_sentinel" = "true" ] || [[ "$sched" == 2106-02-07* ]]; then
        echo "FAIL: tracker issue #79134 reproduced:"
        echo "  the failed scrub target was parked at the utime_t::max() sentinel"
        echo "  (2106-02-07T06:28:15.999999) instead of being rescheduled for a retry"
        return 1
    fi

    perf_counters $dir $OSDS
}
main osd-scrub-test "$@"

# Local Variables:
# compile-command: "cd build ; make -j4 && \
#    ../qa/run-standalone.sh osd-scrub-test.sh"
# End:
