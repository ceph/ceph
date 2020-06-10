#!/usr/bin/env bash
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

    # Fix port????
    export CEPH_MON="127.0.0.1:7114" # git grep '\<7114\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--osd_min_pg_log_entries=5 --osd_max_pg_log_entries=10 "
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

function below_margin() {
    local -i check=$1
    shift
    local -i target=$1

    return $(( $check <= $target && $check >= $target - $margin ? 0 : 1 ))
}

function above_margin() {
    local -i check=$1
    shift
    local -i target=$1

    return $(( $check >= $target && $check <= $target + $margin ? 0 : 1 ))
}

FIND_UPACT='grep "pg[[]${PG}.*backfilling.*update_calc_stats " $log | tail -1 | sed "s/.*[)] \([[][^ p]*\).*$/\1/"'
FIND_FIRST='grep "pg[[]${PG}.*backfilling.*update_calc_stats $which " $log | grep -F " ${UPACT}${addp}" | grep -v est | head -1 | sed "s/.* \([0-9]*\)$/\1/"'
FIND_LAST='grep "pg[[]${PG}.*backfilling.*update_calc_stats $which " $log | tail -1 | sed "s/.* \([0-9]*\)$/\1/"'

function check() {
    local dir=$1
    local PG=$2
    local primary=$3
    local type=$4
    local degraded_start=$5
    local degraded_end=$6
    local misplaced_start=$7
    local misplaced_end=$8
    local primary_start=${9:-}
    local primary_end=${10:-}
    local check_setup=${11:-true}

    local log=$(grep -l +backfilling $dir/osd.$primary.log)
    if [ $check_setup = "true" ];
    then
      local alllogs=$(grep -l +backfilling $dir/osd.*.log)
      if [ "$(echo "$alllogs" | wc -w)" != "1" ];
      then
        echo "Test setup failure, a single OSD should have performed backfill"
        return 1
      fi
    fi

    local addp=" "
    if [ "$type" = "erasure" ];
    then
      addp="p"
    fi

    UPACT=$(eval $FIND_UPACT)
    [ -n "$UPACT" ] || return 1

    # Check 3rd line at start because of false recovery starts
    local which="degraded"
    FIRST=$(eval $FIND_FIRST)
    [ -n "$FIRST" ] || return 1
    below_margin $FIRST $degraded_start || return 1
    LAST=$(eval $FIND_LAST)
    [ -n "$LAST" ] || return 1
    above_margin $LAST $degraded_end || return 1

    # Check 3rd line at start because of false recovery starts
    which="misplaced"
    FIRST=$(eval $FIND_FIRST)
    [ -n "$FIRST" ] || return 1
    below_margin $FIRST $misplaced_start || return 1
    LAST=$(eval $FIND_LAST)
    [ -n "$LAST" ] || return 1
    above_margin $LAST $misplaced_end || return 1

    # This is the value of set into MISSING_ON_PRIMARY
    if [ -n "$primary_start" ];
    then
      which="shard $primary"
      FIRST=$(eval $FIND_FIRST)
      [ -n "$FIRST" ] || return 1
      below_margin $FIRST $primary_start || return 1
      LAST=$(eval $FIND_LAST)
      [ -n "$LAST" ] || return 1
      above_margin $LAST $primary_end || return 1
    fi
}

# [1] -> [1, 0, 2]
# degraded 1000 -> 0
# state: active+undersized+degraded+remapped+backfilling

# PG_STAT OBJECTS MISSING_ON_PRIMARY DEGRADED MISPLACED UNFOUND BYTES LOG DISK_LOG STATE                                           STATE_STAMP                VERSION REPORTED UP      UP_PRIMARY ACTING ACTING_PRIMARY LAST_SCRUB SCRUB_STAMP                LAST_DEEP_SCRUB DEEP_SCRUB_STAMP
# 1.0         500                  0      1000         0       0     0 100      100 active+undersized+degraded+remapped+backfilling 2017-10-27 09:44:23.531466  22'500   26:617 [1,0,2]          1    [1]              1        0'0 2017-10-27 09:43:44.654882             0'0 2017-10-27 09:43:44.654882
function TEST_backfill_sizeup() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    export CEPH_ARGS
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1
    run_osd $dir 5 || return 1

    create_pool $poolname 1 1
    ceph osd pool set $poolname size 1 --yes-i-really-mean-it

    wait_for_clean || return 1

    for i in $(seq 1 $objects)
    do
	rados -p $poolname put obj$i /dev/null
    done

    ceph osd set nobackfill
    ceph osd pool set $poolname size 3
    sleep 2
    ceph osd unset nobackfill

    wait_for_clean || return 1

    local primary=$(get_primary $poolname obj1)
    local PG=$(get_pg $poolname obj1)

    local degraded=$(expr $objects \* 2)
    check $dir $PG $primary replicated $degraded 0 0 0 || return 1

    delete_pool $poolname
    kill_daemons $dir || return 1
}



# [1] -> [0, 2, 4]
# degraded 1000 -> 0
# misplaced 500 -> 0
# state: active+undersized+degraded+remapped+backfilling

# PG_STAT OBJECTS MISSING_ON_PRIMARY DEGRADED MISPLACED UNFOUND BYTES LOG DISK_LOG STATE                                           STATE_STAMP                VERSION REPORTED UP      UP_PRIMARY ACTING ACTING_PRIMARY LAST_SCRUB SCRUB_STAMP                LAST_DEEP_SCRUB DEEP_SCRUB_STAMP
# 1.0         500                  0      1000       500       0     0 100      100 active+undersized+degraded+remapped+backfilling 2017-10-27 09:48:53.326849  22'500   26:603 [0,2,4]          0    [1]              1        0'0 2017-10-27 09:48:13.236253             0'0 2017-10-27 09:48:13.236253
function TEST_backfill_sizeup_out() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1
    run_osd $dir 5 || return 1

    create_pool $poolname 1 1
    ceph osd pool set $poolname size 1 --yes-i-really-mean-it

    wait_for_clean || return 1

    for i in $(seq 1 $objects)
    do
	rados -p $poolname put obj$i /dev/null
    done

    local PG=$(get_pg $poolname obj1)
    # Remember primary during the backfill
    local primary=$(get_primary $poolname obj1)

    ceph osd set nobackfill
    ceph osd out osd.$primary
    ceph osd pool set $poolname size 3
    sleep 2
    ceph osd unset nobackfill

    wait_for_clean || return 1

    local degraded=$(expr $objects \* 2)
    check $dir $PG $primary replicated $degraded 0 $objects 0 || return 1

    delete_pool $poolname
    kill_daemons $dir || return 1
}


# [1 0] -> [1,2]/[1,0]
# misplaced 500 -> 0
# state: active+remapped+backfilling

# PG_STAT OBJECTS MISSING_ON_PRIMARY DEGRADED MISPLACED UNFOUND BYTES LOG DISK_LOG STATE                       STATE_STAMP                VERSION REPORTED UP    UP_PRIMARY ACTING ACTING_PRIMARY LAST_SCRUB SCRUB_STAMP                LAST_DEEP_SCRUB DEEP_SCRUB_STAMP
# 1.0         500                  0        0       500       0     0 100      100 active+remapped+backfilling 2017-10-27 09:51:18.800517  22'500   25:570 [1,2]          1  [1,0]              1        0'0 2017-10-27 09:50:40.441274             0'0 2017-10-27 09:50:40.441274
function TEST_backfill_out() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1
    run_osd $dir 5 || return 1

    create_pool $poolname 1 1
    ceph osd pool set $poolname size 2
    sleep 5

    wait_for_clean || return 1

    for i in $(seq 1 $objects)
    do
	rados -p $poolname put obj$i /dev/null
    done

    local PG=$(get_pg $poolname obj1)
    # Remember primary during the backfill
    local primary=$(get_primary $poolname obj1)

    ceph osd set nobackfill
    ceph osd out osd.$(get_not_primary $poolname obj1)
    sleep 2
    ceph osd unset nobackfill

    wait_for_clean || return 1

    check $dir $PG $primary replicated 0 0 $objects 0 || return 1

    delete_pool $poolname
    kill_daemons $dir || return 1
}


# [0, 1] -> [0, 2]/[0]
# osd 1 down/out
# degraded 500 -> 0
# state: active+undersized+degraded+remapped+backfilling

# PG_STAT OBJECTS MISSING_ON_PRIMARY DEGRADED MISPLACED UNFOUND BYTES LOG DISK_LOG STATE                                           STATE_STAMP                VERSION REPORTED UP    UP_PRIMARY ACTING ACTING_PRIMARY LAST_SCRUB SCRUB_STAMP                LAST_DEEP_SCRUB DEEP_SCRUB_STAMP
# 1.0         500                  0      500         0       0     0 100      100 active+undersized+degraded+remapped+backfilling 2017-10-27 09:53:24.051091  22'500   27:719 [0,2]          0    [0]              0        0'0 2017-10-27 09:52:43.188368             0'0 2017-10-27 09:52:43.188368
function TEST_backfill_down_out() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1
    run_osd $dir 5 || return 1

    create_pool $poolname 1 1
    ceph osd pool set $poolname size 2
    sleep 5

    wait_for_clean || return 1

    for i in $(seq 1 $objects)
    do
	rados -p $poolname put obj$i /dev/null
    done

    local PG=$(get_pg $poolname obj1)
    # Remember primary during the backfill
    local primary=$(get_primary $poolname obj1)
    local otherosd=$(get_not_primary $poolname obj1)

    ceph osd set nobackfill
    kill $(cat $dir/osd.${otherosd}.pid)
    ceph osd down osd.${otherosd}
    ceph osd out osd.${otherosd}
    sleep 2
    ceph osd unset nobackfill

    wait_for_clean || return 1

    check $dir $PG $primary replicated $objects 0 0 0 || return 1

    delete_pool $poolname
    kill_daemons $dir || return 1
}


# [1, 0] -> [2, 3, 4]
# degraded 500 -> 0
# misplaced 1000 -> 0
# state: active+undersized+degraded+remapped+backfilling

# PG_STAT OBJECTS MISSING_ON_PRIMARY DEGRADED MISPLACED UNFOUND BYTES LOG DISK_LOG STATE                                           STATE_STAMP                VERSION REPORTED UP      UP_PRIMARY ACTING ACTING_PRIMARY LAST_SCRUB SCRUB_STAMP                LAST_DEEP_SCRUB DEEP_SCRUB_STAMP
# 1.0         500                  0      500       1000       0     0 100      100 active+undersized+degraded+remapped+backfilling 2017-10-27 09:55:50.375722  23'500   27:553 [2,4,3]          2  [1,0]              1        0'0 2017-10-27 09:55:10.230919             0'0 2017-10-27 09:55:10.230919
function TEST_backfill_out2() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1
    run_osd $dir 5 || return 1

    create_pool $poolname 1 1
    ceph osd pool set $poolname size 2
    sleep 5

    wait_for_clean || return 1

    for i in $(seq 1 $objects)
    do
	rados -p $poolname put obj$i /dev/null
    done

    local PG=$(get_pg $poolname obj1)
    # Remember primary during the backfill
    local primary=$(get_primary $poolname obj1)
    local otherosd=$(get_not_primary $poolname obj1)

    ceph osd set nobackfill
    ceph osd pool set $poolname size 3
    ceph osd out osd.${otherosd}
    ceph osd out osd.${primary}
    # Primary might change before backfill starts
    sleep 2
    primary=$(get_primary $poolname obj1)
    ceph osd unset nobackfill
    ceph tell osd.$primary get_latest_osdmap
    ceph tell osd.$primary debug kick_recovery_wq 0
    sleep 2

    wait_for_clean || return 1

    local misplaced=$(expr $objects \* 2)

    check $dir $PG $primary replicated $objects 0 $misplaced 0 || return 1

    delete_pool $poolname
    kill_daemons $dir || return 1
}


# [0,1] ->  [2,4,3]/[0,1]
# degraded 1000 -> 0
# misplaced 1000 -> 500
# state ends at active+clean+remapped [2,4,3]/[2,4,3,0]
# PG_STAT OBJECTS MISSING_ON_PRIMARY DEGRADED MISPLACED UNFOUND BYTES LOG DISK_LOG STATE                                           STATE_STAMP                VERSION REPORTED UP      UP_PRIMARY ACTING ACTING_PRIMARY LAST_SCRUB SCRUB_STAMP                LAST_DEEP_SCRUB DEEP_SCRUB_STAMP
# 1.0         500                  0     1000       1000       0     0 100      100 active+undersized+degraded+remapped+backfilling 2017-10-30 18:21:45.995149  19'500  23:1817 [2,4,3]          2  [0,1]              0        0'0 2017-10-30 18:21:05.109904             0'0 2017-10-30 18:21:05.109904
# ENDS:
# PG_STAT OBJECTS MISSING_ON_PRIMARY DEGRADED MISPLACED UNFOUND BYTES LOG DISK_LOG STATE                 STATE_STAMP                VERSION REPORTED UP      UP_PRIMARY ACTING    ACTING_PRIMARY LAST_SCRUB SCRUB_STAMP LAST_DEEP_SCRUB DEEP_SCRUB_STAMP
# 1.0         500                  0        0       500       0     0   5        5 active+clean+remapped 2017-10-30 18:22:42.293730  19'500  25:2557 [2,4,3]          2 [2,4,3,0]              2        0'0 2017-10-30 18:21:05.109904             0'0 2017-10-30 18:21:05.109904
function TEST_backfill_sizeup4_allout() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1

    create_pool $poolname 1 1
    ceph osd pool set $poolname size 2

    wait_for_clean || return 1

    for i in $(seq 1 $objects)
    do
	rados -p $poolname put obj$i /dev/null
    done

    local PG=$(get_pg $poolname obj1)
    # Remember primary during the backfill
    local primary=$(get_primary $poolname obj1)
    local otherosd=$(get_not_primary $poolname obj1)

    ceph osd set nobackfill
    ceph osd out osd.$otherosd
    ceph osd out osd.$primary
    ceph osd pool set $poolname size 4
    # Primary might change before backfill starts
    sleep 2
    primary=$(get_primary $poolname obj1)
    ceph osd unset nobackfill
    ceph tell osd.$primary get_latest_osdmap
    ceph tell osd.$primary debug kick_recovery_wq 0
    sleep 2

    wait_for_clean || return 1

    local misdeg=$(expr $objects \* 2)
    check $dir $PG $primary replicated $misdeg 0 $misdeg $objects || return 1

    delete_pool $poolname
    kill_daemons $dir || return 1
}


# [1,2,0] ->  [3]/[1,2]
# misplaced 1000 -> 500
# state ends at active+clean+remapped [3]/[3,1]
# PG_STAT OBJECTS MISSING_ON_PRIMARY DEGRADED MISPLACED UNFOUND BYTES LOG DISK_LOG STATE                       STATE_STAMP                VERSION REPORTED UP  UP_PRIMARY ACTING ACTING_PRIMARY LAST_SCRUB SCRUB_STAMP                LAST_DEEP_SCRUB DEEP_SCRUB_STAMP
# 1.0         500                  0        0       1000       0     0 100      100 active+remapped+backfilling 2017-11-28 19:13:56.092439  21'500   31:790 [3]          3  [1,2]              1        0'0 2017-11-28 19:13:28.698661             0'0 2017-11-28 19:13:28.698661
function TEST_backfill_remapped() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1

    create_pool $poolname 1 1
    ceph osd pool set $poolname size 3
    sleep 5

    wait_for_clean || return 1

    for i in $(seq 1 $objects)
    do
	rados -p $poolname put obj$i /dev/null
    done

    local PG=$(get_pg $poolname obj1)
    # Remember primary during the backfill
    local primary=$(get_primary $poolname obj1)
    local otherosd=$(get_not_primary $poolname obj1)

    ceph osd set nobackfill
    ceph osd out osd.${otherosd}
    for i in $(get_osds $poolname obj1)
    do
        if [ $i = $primary -o $i = $otherosd ];
        then
            continue
        fi
        ceph osd out osd.$i
        break
    done
    ceph osd out osd.${primary}
    ceph osd pool set $poolname size 2
    sleep 2

    # primary may change due to invalidating the old pg_temp, which was [1,2,0],
    # but up_primary (3) chooses [0,1] for acting.
    primary=$(get_primary $poolname obj1)

    ceph osd unset nobackfill
    ceph tell osd.$primary get_latest_osdmap
    ceph tell osd.$primary debug kick_recovery_wq 0

    sleep 2

    wait_for_clean || return 1

    local misplaced=$(expr $objects \* 2)

    check $dir $PG $primary replicated 0 0 $misplaced $objects "" "" false || return 1

    delete_pool $poolname
    kill_daemons $dir || return 1
}

# [1,0,2] -> [4,3,NONE]/[1,0,2]
# misplaced 1500 -> 500
# state ends at active+clean+remapped [4,3,NONE]/[4,3,2]

# PG_STAT OBJECTS MISSING_ON_PRIMARY DEGRADED MISPLACED UNFOUND BYTES LOG DISK_LOG STATE                                STATE_STAMP                VERSION REPORTED UP         UP_PRIMARY ACTING  ACTING_PRIMARY LAST_SCRUB SCRUB_STAMP                LAST_DEEP_SCRUB DEEP_SCRUB_STAMP
# 1.0         500                  0      0      1500       0     0 100      100 active+degraded+remapped+backfilling 2017-10-31 16:53:39.467126  19'500   23:615 [4,3,NONE]          4 [1,0,2]              1        0'0 2017-10-31 16:52:59.624429             0'0 2017-10-31 16:52:59.624429


# ENDS:

# PG_STAT OBJECTS MISSING_ON_PRIMARY DEGRADED MISPLACED UNFOUND BYTES LOG DISK_LOG STATE                 STATE_STAMP                VERSION REPORTED UP         UP_PRIMARY ACTING  ACTING_PRIMARY LAST_SCRUB SCRUB_STAMP LAST_DEEP_SCRUB DEEP_SCRUB_STAMP
# 1.0         500                  0        0       500       0     0   5        5 active+clean+remapped 2017-10-31 16:48:34.414040  19'500  25:2049 [4,3,NONE]          4 [4,3,2]              4        0'0 2017-10-31 16:46:58.203440             0'0 2017-10-31 16:46:58.203440
function TEST_backfill_ec_all_out() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1

    ceph osd erasure-code-profile set myprofile plugin=jerasure technique=reed_sol_van k=2 m=1 crush-failure-domain=osd
    create_pool $poolname 1 1 erasure myprofile

    wait_for_clean || return 1

    for i in $(seq 1 $objects)
    do
	rados -p $poolname put obj$i /dev/null
    done

    local PG=$(get_pg $poolname obj1)
    # Remember primary during the backfill
    local primary=$(get_primary $poolname obj1)

    ceph osd set nobackfill
    for o in $(get_osds $poolname obj1)
    do
        ceph osd out osd.$o
    done
    # Primary might change before backfill starts
    sleep 2
    primary=$(get_primary $poolname obj1)
    ceph osd unset nobackfill
    ceph tell osd.$primary get_latest_osdmap
    ceph tell osd.$primary debug kick_recovery_wq 0
    sleep 2

    wait_for_clean || return 1

    local misplaced=$(expr $objects \* 3)
    check $dir $PG $primary erasure 0 0 $misplaced $objects || return 1

    delete_pool $poolname
    kill_daemons $dir || return 1
}


# [1,0,2] -> [4, 0, 2]
# misplaced 500 -> 0
# active+remapped+backfilling
#
# PG_STAT OBJECTS MISSING_ON_PRIMARY DEGRADED MISPLACED UNFOUND BYTES LOG DISK_LOG STATE                       STATE_STAMP                VERSION REPORTED UP      UP_PRIMARY ACTING  ACTING_PRIMARY LAST_SCRUB SCRUB_STAMP                LAST_DEEP_SCRUB DEEP_SCRUB_STAMP
# 1.0         500                  0        0       500       0     0 100      100 active+remapped+backfilling 2017-11-08 18:05:39.036420  24'500   27:742 [4,0,2]          4 [1,0,2]              1        0'0 2017-11-08 18:04:58.697315             0'0 2017-11-08 18:04:58.697315
function TEST_backfill_ec_prim_out() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1

    ceph osd erasure-code-profile set myprofile plugin=jerasure technique=reed_sol_van k=2 m=1 crush-failure-domain=osd
    create_pool $poolname 1 1 erasure myprofile

    wait_for_clean || return 1

    for i in $(seq 1 $objects)
    do
	rados -p $poolname put obj$i /dev/null
    done

    local PG=$(get_pg $poolname obj1)
    # Remember primary during the backfill
    local primary=$(get_primary $poolname obj1)

    ceph osd set nobackfill
    ceph osd out osd.$primary
    # Primary might change before backfill starts
    sleep 2
    primary=$(get_primary $poolname obj1)
    ceph osd unset nobackfill
    ceph tell osd.$primary get_latest_osdmap
    ceph tell osd.$primary debug kick_recovery_wq 0
    sleep 2

    wait_for_clean || return 1

    local misplaced=$(expr $objects \* 3)
    check $dir $PG $primary erasure 0 0 $objects 0 || return 1

    delete_pool $poolname
    kill_daemons $dir || return 1
}

# [1,0] -> [1,2]
# degraded 500 -> 0
# misplaced 1000 -> 0
#
# PG_STAT OBJECTS MISSING_ON_PRIMARY DEGRADED MISPLACED UNFOUND BYTES LOG DISK_LOG STATE                                           STATE_STAMP                VERSION REPORTED UP      UP_PRIMARY ACTING     ACTING_PRIMARY LAST_SCRUB SCRUB_STAMP                LAST_DEEP_SCRUB DEEP_SCRUB_STAMP
# 1.0         500                  0      500      1000       0     0 100      100 active+undersized+degraded+remapped+backfilling 2017-11-06 14:02:29.439105  24'500  29:1020 [4,3,5]          4 [1,NONE,2]              1        0'0 2017-11-06 14:01:46.509963             0'0 2017-11-06 14:01:46.509963
function TEST_backfill_ec_down_all_out() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1
    run_osd $dir 5 || return 1

    ceph osd erasure-code-profile set myprofile plugin=jerasure technique=reed_sol_van k=2 m=1 crush-failure-domain=osd
    create_pool $poolname 1 1 erasure myprofile
    ceph osd pool set $poolname min_size 2

    wait_for_clean || return 1

    for i in $(seq 1 $objects)
    do
	rados -p $poolname put obj$i /dev/null
    done

    local PG=$(get_pg $poolname obj1)
    # Remember primary during the backfill
    local primary=$(get_primary $poolname obj1)
    local otherosd=$(get_not_primary $poolname obj1)
    local allosds=$(get_osds $poolname obj1)

    ceph osd set nobackfill
    kill $(cat $dir/osd.${otherosd}.pid)
    ceph osd down osd.${otherosd}
    for o in $allosds
    do
        ceph osd out osd.$o
    done
    # Primary might change before backfill starts
    sleep 2
    primary=$(get_primary $poolname obj1)
    ceph osd unset nobackfill
    ceph tell osd.$primary get_latest_osdmap
    ceph tell osd.$primary debug kick_recovery_wq 0
    sleep 2
    flush_pg_stats

    # Wait for recovery to finish
    # Can't use wait_for_clean() because state goes from active+undersized+degraded+remapped+backfilling
    # to  active+undersized+remapped
    while(true)
    do
      if test "$(ceph --format json pg dump pgs |
         jq '.pg_stats | [.[] | .state | select(. == "incomplete")] | length')" -ne "0"
      then
        sleep 2
        continue
      fi
      break
    done
    ceph pg dump pgs
    for i in $(seq 1 60)
    do
      if ceph pg dump pgs | grep ^$PG | grep -qv backfilling
      then
          break
      fi
      if [ $i = "60" ];
      then
          echo "Timeout waiting for recovery to finish"
          return 1
      fi
      sleep 1
    done

    ceph pg dump pgs

    local misplaced=$(expr $objects \* 2)
    check $dir $PG $primary erasure $objects 0 $misplaced 0 || return 1

    delete_pool $poolname
    kill_daemons $dir || return 1
}


# [1,0,2] -> [1,3,2]
# degraded 500 -> 0
# active+backfilling+degraded
#
# PG_STAT OBJECTS MISSING_ON_PRIMARY DEGRADED MISPLACED UNFOUND BYTES LOG DISK_LOG STATE                                           STATE_STAMP                VERSION REPORTED UP      UP_PRIMARY ACTING     ACTING_PRIMARY LAST_SCRUB SCRUB_STAMP                LAST_DEEP_SCRUB DEEP_SCRUB_STAMP
# 1.0         500                  0      500         0       0     0 100      100 active+undersized+degraded+remapped+backfilling 2017-11-06 13:57:25.412322  22'500   28:794 [1,3,2]          1 [1,NONE,2]              1        0'0 2017-11-06 13:54:58.033906             0'0 2017-11-06 13:54:58.033906
function TEST_backfill_ec_down_out() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1
    run_osd $dir 5 || return 1

    ceph osd erasure-code-profile set myprofile plugin=jerasure technique=reed_sol_van k=2 m=1 crush-failure-domain=osd
    create_pool $poolname 1 1 erasure myprofile
    ceph osd pool set $poolname min_size 2

    wait_for_clean || return 1

    for i in $(seq 1 $objects)
    do
	rados -p $poolname put obj$i /dev/null
    done

    local PG=$(get_pg $poolname obj1)
    # Remember primary during the backfill
    local primary=$(get_primary $poolname obj1)
    local otherosd=$(get_not_primary $poolname obj1)

    ceph osd set nobackfill
    kill $(cat $dir/osd.${otherosd}.pid)
    ceph osd down osd.${otherosd}
    ceph osd out osd.${otherosd}
    # Primary might change before backfill starts
    sleep 2
    primary=$(get_primary $poolname obj1)
    ceph osd unset nobackfill
    ceph tell osd.$primary get_latest_osdmap
    ceph tell osd.$primary debug kick_recovery_wq 0
    sleep 2

    wait_for_clean || return 1

    local misplaced=$(expr $objects \* 2)
    check $dir $PG $primary erasure $objects 0 0 0 || return 1

    delete_pool $poolname
    kill_daemons $dir || return 1
}


main osd-backfill-stats "$@"

# Local Variables:
# compile-command: "make -j4 && ../qa/run-standalone.sh osd-backfill-stats.sh"
# End:
