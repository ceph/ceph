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
    export CEPH_MON="127.0.0.1:7115" # git grep '\<7115\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    # so we will not force auth_log_shard to be acting_primary
    CEPH_ARGS+="--osd_force_auth_primary_missing_objects=1000000 "
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

FIND_UPACT='grep "pg[[]${PG}.*recovering.*PeeringState::update_calc_stats " $log | tail -1 | sed "s/.*[)] \([[][^ p]*\).*$/\1/"'
FIND_FIRST='grep "pg[[]${PG}.*recovering.*PeeringState::update_calc_stats $which " $log | grep -F " ${UPACT}${addp}" | grep -v est | head -1 | sed "s/.* \([0-9]*\)$/\1/"'
FIND_LAST='grep "pg[[]${PG}.*recovering.*PeeringState::update_calc_stats $which " $log | tail -1 | sed "s/.* \([0-9]*\)$/\1/"'

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

    local log=$dir/osd.${primary}.log

    local addp=" "
    if [ "$type" = "erasure" ];
    then
      addp="p"
    fi

    UPACT=$(eval $FIND_UPACT)

    # Check 3rd line at start because of false recovery starts
    local which="degraded"
    FIRST=$(eval $FIND_FIRST)
    below_margin $FIRST $degraded_start || return 1
    LAST=$(eval $FIND_LAST)
    above_margin $LAST $degraded_end || return 1

    # Check 3rd line at start because of false recovery starts
    which="misplaced"
    FIRST=$(eval $FIND_FIRST)
    below_margin $FIRST $misplaced_start || return 1
    LAST=$(eval $FIND_LAST)
    above_margin $LAST $misplaced_end || return 1

    # This is the value of set into MISSING_ON_PRIMARY
    if [ -n "$primary_start" ];
    then
      which="shard $primary"
      FIRST=$(eval $FIND_FIRST)
      below_margin $FIRST $primary_start || return 1
      LAST=$(eval $FIND_LAST)
      above_margin $LAST $primary_end || return 1
    fi
}

# [1,0,?] -> [1,2,4]
# degraded 500 -> 0
# active+recovering+degraded

# PG_STAT OBJECTS MISSING_ON_PRIMARY DEGRADED MISPLACED UNFOUND BYTES LOG DISK_LOG STATE                      STATE_STAMP                VERSION REPORTED UP      UP_PRIMARY ACTING  ACTING_PRIMARY LAST_SCRUB SCRUB_STAMP                LAST_DEEP_SCRUB DEEP_SCRUB_STAMP
# 1.0         500                  0      500         0       0     0 500      500 active+recovering+degraded 2017-11-17 19:27:36.493828  28'500   32:603 [1,2,4]          1 [1,2,4]              1        0'0 2017-11-17 19:27:05.915467             0'0 2017-11-17 19:27:05.915467
function do_recovery_out1() {
    local dir=$1
    shift
    local type=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1
    run_osd $dir 5 || return 1

    if [ $type = "erasure" ];
    then
        ceph osd erasure-code-profile set myprofile plugin=jerasure technique=reed_sol_van k=2 m=1 crush-failure-domain=osd
        create_pool $poolname 1 1 $type myprofile
    else
        create_pool $poolname 1 1 $type
    fi

    wait_for_clean || return 1

    for i in $(seq 1 $objects)
    do
	rados -p $poolname put obj$i /dev/null
    done

    local primary=$(get_primary $poolname obj1)
    local PG=$(get_pg $poolname obj1)
    # Only 2 OSDs so only 1 not primary
    local otherosd=$(get_not_primary $poolname obj1)

    ceph osd set norecover
    kill $(cat $dir/osd.${otherosd}.pid)
    ceph osd down osd.${otherosd}
    ceph osd out osd.${otherosd}
    ceph osd unset norecover
    ceph tell osd.$(get_primary $poolname obj1) debug kick_recovery_wq 0
    sleep 2

    wait_for_clean || return 1

    check $dir $PG $primary $type $objects 0 0 0 || return 1

    delete_pool $poolname
    kill_daemons $dir || return 1
}

function TEST_recovery_replicated_out1() {
    local dir=$1

    do_recovery_out1 $dir replicated || return 1
}

function TEST_recovery_erasure_out1() {
    local dir=$1

    do_recovery_out1 $dir erasure || return 1
}

# [0, 1] -> [2,3,4,5]
# degraded 1000 -> 0
# misplaced 1000 -> 0
# missing on primary 500 -> 0

# PG_STAT OBJECTS MISSING_ON_PRIMARY DEGRADED MISPLACED UNFOUND BYTES LOG DISK_LOG STATE                      STATE_STAMP                VERSION REPORTED UP        UP_PRIMARY ACTING    ACTING_PRIMARY LAST_SCRUB SCRUB_STAMP                LAST_DEEP_SCRUB DEEP_SCRUB_STAMP
# 1.0         500                500     1000      1000       0     0 500      500 active+recovering+degraded 2017-10-27 09:38:37.453438  22'500   25:394 [2,4,3,5]          2 [2,4,3,5]              2        0'0 2017-10-27 09:37:58.046748             0'0 2017-10-27 09:37:58.046748
function TEST_recovery_sizeup() {
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

    wait_for_clean || return 1

    for i in $(seq 1 $objects)
    do
	rados -p $poolname put obj$i /dev/null
    done

    local primary=$(get_primary $poolname obj1)
    local PG=$(get_pg $poolname obj1)
    # Only 2 OSDs so only 1 not primary
    local otherosd=$(get_not_primary $poolname obj1)

    ceph osd set norecover
    ceph osd out osd.$primary osd.$otherosd
    ceph osd pool set test size 4
    ceph osd unset norecover
    # Get new primary
    primary=$(get_primary $poolname obj1)

    ceph tell osd.${primary} debug kick_recovery_wq 0
    sleep 2

    wait_for_clean || return 1

    local degraded=$(expr $objects \* 2)
    local misplaced=$(expr $objects \* 2)
    local log=$dir/osd.${primary}.log
    check $dir $PG $primary replicated $degraded 0 $misplaced 0 $objects 0 || return 1

    delete_pool $poolname
    kill_daemons $dir || return 1
}

# [0, 1, 2, 4] -> [3, 5]
# misplaced 1000 -> 0
# missing on primary 500 -> 0
# active+recovering+degraded

# PG_STAT OBJECTS MISSING_ON_PRIMARY DEGRADED MISPLACED UNFOUND BYTES LOG DISK_LOG STATE                      STATE_STAMP                VERSION REPORTED UP    UP_PRIMARY ACTING ACTING_PRIMARY LAST_SCRUB SCRUB_STAMP                LAST_DEEP_SCRUB DEEP_SCRUB_STAMP
# 1.0         500                500         0      1000       0     0 500      500 active+recovering+degraded 2017-10-27 09:34:50.012261  22'500   27:118 [3,5]          3  [3,5]              3        0'0 2017-10-27 09:34:08.617248             0'0 2017-10-27 09:34:08.617248
function TEST_recovery_sizedown() {
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
    ceph osd pool set $poolname size 4

    wait_for_clean || return 1

    for i in $(seq 1 $objects)
    do
	rados -p $poolname put obj$i /dev/null
    done

    local primary=$(get_primary $poolname obj1)
    local PG=$(get_pg $poolname obj1)
    # Only 2 OSDs so only 1 not primary
    local allosds=$(get_osds $poolname obj1)

    ceph osd set norecover
    for osd in $allosds
    do
        ceph osd out osd.$osd
    done

    ceph osd pool set test size 2
    ceph osd unset norecover
    ceph tell osd.$(get_primary $poolname obj1) debug kick_recovery_wq 0
    sleep 2

    wait_for_clean || return 1

    # Get new primary
    primary=$(get_primary $poolname obj1)

    local misplaced=$(expr $objects \* 2)
    local log=$dir/osd.${primary}.log
    check $dir $PG $primary replicated 0 0 $misplaced 0 || return 1

    UPACT=$(grep "pg[[]${PG}.*recovering.*update_calc_stats " $log | tail -1 | sed "s/.*[)] \([[][^ p]*\).*$/\1/")

    # This is the value of set into MISSING_ON_PRIMARY
    FIRST=$(grep "pg[[]${PG}.*recovering.*update_calc_stats shard $primary " $log | grep -F " $UPACT " | head -1 | sed "s/.* \([0-9]*\)$/\1/")
    below_margin $FIRST $objects || return 1
    LAST=$(grep "pg[[]${PG}.*recovering.*update_calc_stats shard $primary " $log | tail -1 | sed "s/.* \([0-9]*\)$/\1/")
    above_margin $LAST 0 || return 1

    delete_pool $poolname
    kill_daemons $dir || return 1
}

# [1] -> [1,2]
# degraded 300 -> 200
# active+recovering+undersized+degraded

# PG_STAT OBJECTS MISSING_ON_PRIMARY DEGRADED MISPLACED UNFOUND BYTES LOG DISK_LOG STATE                                 STATE_STAMP                VERSION REPORTED UP    UP_PRIMARY ACTING ACTING_PRIMARY LAST_SCRUB SCRUB_STAMP                LAST_DEEP_SCRUB DEEP_SCRUB_STAMP
# 1.0         100                  0     300         0       0     0 100      100 active+recovering+undersized+degraded 2017-11-17 17:16:15.302943  13'500   16:643 [1,2]          1  [1,2]              1        0'0 2017-11-17 17:15:34.985563             0'0 2017-11-17 17:15:34.985563
function TEST_recovery_undersized() {
    local dir=$1

    local osds=3
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for i in $(seq 0 $(expr $osds - 1))
    do
      run_osd $dir $i || return 1
    done

    create_pool $poolname 1 1
    ceph osd pool set $poolname size 1 --yes-i-really-mean-it

    wait_for_clean || return 1

    for i in $(seq 1 $objects)
    do
	rados -p $poolname put obj$i /dev/null
    done

    local primary=$(get_primary $poolname obj1)
    local PG=$(get_pg $poolname obj1)

    ceph osd set norecover
    # Mark any osd not the primary (only 1 replica so also has no replica)
    for i in $(seq 0 $(expr $osds - 1))
    do
      if [ $i = $primary ];
      then
        continue
      fi
      ceph osd out osd.$i
      break
    done
    ceph osd pool set test size 4
    ceph osd unset norecover
    ceph tell osd.$(get_primary $poolname obj1) debug kick_recovery_wq 0
    # Give extra sleep time because code below doesn't have the sophistication of wait_for_clean()
    sleep 10
    flush_pg_stats || return 1

    # Wait for recovery to finish
    # Can't use wait_for_clean() because state goes from active+recovering+undersized+degraded
    # to  active+undersized+degraded
    for i in $(seq 1 300)
    do
      if ceph pg dump pgs | grep ^$PG | grep -qv recovering
      then
          break
      fi
      if [ $i = "300" ];
      then
          echo "Timeout waiting for recovery to finish"
          return 1
      fi
      sleep 1
    done

    # Get new primary
    primary=$(get_primary $poolname obj1)
    local log=$dir/osd.${primary}.log

    local first_degraded=$(expr $objects \* 3)
    local last_degraded=$(expr $objects \* 2)
    check $dir $PG $primary replicated $first_degraded $last_degraded 0 0 || return 1

    delete_pool $poolname
    kill_daemons $dir || return 1
}

# [1,0,2] -> [1,3,NONE]/[1,3,2]
# degraded 100 -> 0
# misplaced 100 -> 100
# active+recovering+degraded+remapped

# PG_STAT OBJECTS MISSING_ON_PRIMARY DEGRADED MISPLACED UNFOUND BYTES LOG DISK_LOG STATE                               STATE_STAMP                VERSION REPORTED UP         UP_PRIMARY ACTING  ACTING_PRIMARY LAST_SCRUB SCRUB_STAMP                LAST_DEEP_SCRUB DEEP_SCRUB_STAMP
# 1.0         100                  0      100        100       0     0 100      100 active+recovering+degraded+remapped 2017-11-27 21:24:20.851243  18'500   23:618 [1,3,NONE]          1 [1,3,2]              1        0'0 2017-11-27 21:23:39.395242             0'0 2017-11-27 21:23:39.395242
function TEST_recovery_erasure_remapped() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1

    ceph osd erasure-code-profile set myprofile plugin=jerasure technique=reed_sol_van k=2 m=1 crush-failure-domain=osd
    create_pool $poolname 1 1 erasure myprofile
    ceph osd pool set $poolname min_size 2

    wait_for_clean || return 1

    for i in $(seq 1 $objects)
    do
	rados -p $poolname put obj$i /dev/null
    done

    local primary=$(get_primary $poolname obj1)
    local PG=$(get_pg $poolname obj1)
    local otherosd=$(get_not_primary $poolname obj1)

    ceph osd set norecover
    kill $(cat $dir/osd.${otherosd}.pid)
    ceph osd down osd.${otherosd}
    ceph osd out osd.${otherosd}

    # Mark osd not the primary and not down/out osd as just out
    for i in 0 1 2 3
    do
      if [ $i = $primary ];
      then
	continue
      fi
      if [ $i = $otherosd ];
      then
	continue
      fi
      ceph osd out osd.$i
      break
    done
    ceph osd unset norecover
    ceph tell osd.$(get_primary $poolname obj1) debug kick_recovery_wq 0
    sleep 2

    wait_for_clean || return 1

    local log=$dir/osd.${primary}.log
    check $dir $PG $primary erasure $objects 0 $objects $objects || return 1

    delete_pool $poolname
    kill_daemons $dir || return 1
}

function TEST_recovery_multi() {
    local dir=$1

    local osds=6
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for i in $(seq 0 $(expr $osds - 1))
    do
      run_osd $dir $i || return 1
    done

    create_pool $poolname 1 1
    ceph osd pool set $poolname size 3
    ceph osd pool set $poolname min_size 1

    wait_for_clean || return 1

    rados -p $poolname put obj1 /dev/null

    local primary=$(get_primary $poolname obj1)
    local otherosd=$(get_not_primary $poolname obj1)

    ceph osd set noout
    ceph osd set norecover
    kill $(cat $dir/osd.${otherosd}.pid)
    ceph osd down osd.${otherosd}

    local half=$(expr $objects / 2)
    for i in $(seq 2 $half)
    do
	rados -p $poolname put obj$i /dev/null
    done

    kill $(cat $dir/osd.${primary}.pid)
    ceph osd down osd.${primary}
    activate_osd $dir ${otherosd}
    sleep 3

    for i in $(seq $(expr $half + 1) $objects)
    do
	rados -p $poolname put obj$i /dev/null
    done

    local PG=$(get_pg $poolname obj1)
    local otherosd=$(get_not_primary $poolname obj$objects)

    ceph osd unset noout
    ceph osd out osd.$primary osd.$otherosd
    activate_osd $dir ${primary}
    sleep 3

    ceph osd pool set test size 4
    ceph osd unset norecover
    ceph tell osd.$(get_primary $poolname obj1) debug kick_recovery_wq 0
    sleep 2

    wait_for_clean || return 1

    # Get new primary
    primary=$(get_primary $poolname obj1)

    local log=$dir/osd.${primary}.log
    check $dir $PG $primary replicated 399 0 300 0 99 0 || return 1

    delete_pool $poolname
    kill_daemons $dir || return 1
}

main osd-recovery-stats "$@"

# Local Variables:
# compile-command: "make -j4 && ../qa/run-standalone.sh osd-recovery-stats.sh"
# End:
