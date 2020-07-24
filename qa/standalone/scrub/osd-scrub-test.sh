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
        $func $dir || return 1
    done
}

function TEST_scrub_test() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local objects=15

    TESTDATA="testdata.$$"

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=3 || return 1
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

    local primary=$(get_primary $poolname obj1)
    local otherosd=$(get_not_primary $poolname obj1)
    if [ "$otherosd" = "2" ];
    then
      local anotherosd="0"
    else
      local anotherosd="2"
    fi

    objectstore_tool $dir $anotherosd obj1 set-bytes /etc/fstab

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

    teardown $dir || return 1
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

    setup $dir || return 1
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

    teardown $dir || return 1
}

function TEST_scrub_extented_sleep() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local objects=15

    TESTDATA="testdata.$$"

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=3 || return 1
    run_mgr $dir x || return 1
    local scrub_begin_hour=$(date -d '2 hour ago' +"%H" | sed 's/^0//')
    local scrub_end_hour=$(date -d '1 hour ago' +"%H" | sed 's/^0//')
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd --osd_scrub_sleep=0 \
                        --osd_scrub_extended_sleep=10 \
                        --bluestore_cache_autotune=false \
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
    ceph tell $pgid scrub || return 1

    # Due to the long delay, the scrub should not be done within 3 seconds
    for ((i=0; i < 3; i++)); do
        if test "$(get_last_scrub_stamp $pgid)" '>' "$last_scrub" ; then
            return 1
        fi
        sleep 1
    done

    teardown $dir || return 1
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


    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=3 || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd --osd_pool_default_pg_autoscale_mode=off \
	      --osd_deep_scrub_randomize_ratio=0.0 \
	      --osd_scrub_sleep=5.0 \
	      --osd_scrub_interval_randomize_ratio=0  || return 1
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

    ceph tell $pgid $type || return 1
    # deep-scrub won't start without scrub noticing
    if [ "$type" = "deep_scrub" ];
    then
      ceph tell $pgid scrub || return 1
    fi

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
    ceph osd unset noscrub
    TIMEOUT=$(($objects / 2))
    wait_for_scrub $pgid "$last_scrub" || return 1

    teardown $dir || return 1
}

function TEST_scrub_abort() {
    local dir=$1
    _scrub_abort $dir scrub
}

function TEST_deep_scrub_abort() {
    local dir=$1
    _scrub_abort $dir deep_scrub
}

main osd-scrub-test "$@"

# Local Variables:
# compile-command: "cd build ; make -j4 && \
#    ../qa/run-standalone.sh osd-scrub-test.sh"
# End:
