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
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
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

function TEST_recovery_last_degraded_latching() {
    local dir=$1
    local osds=6

    # Setup Cluster
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for i in $(seq 0 $(expr $osds - 1)); do
      run_osd $dir $i || return 1
    done

    # Create Pool with specific replica counts
    create_pool $poolname 8 8
    ceph osd pool set $poolname size 3
    ceph osd pool set $poolname min_size 1
    wait_for_clean || return 1

    # Inject data
    local numobjs=100
    for i in $(seq 1 $numobjs); do
      rados -p $poolname put obj$i /dev/null
    done

    # Identify PG and OSDs
    local pgid=$(get_pg $poolname obj1)
    local replicaosds=$(get_osds $poolname obj1 | awk '{print $2, $3}')
    read -r osd_a osd_b <<< "$replicaosds"

    # Capture baseline timestamp
    local last_clean_start=$(ceph pg $pgid query | \
      jq -r '.info.stats.last_clean')

    # --- Step 1: Kill the first non-primary OSD (osd_a) ---
    echo "Setting norecover to freeze PG state..."
    ceph osd set norecover

    echo "Stopping OSD.$osd_a..."
    kill $(cat $dir/osd.${osd_a}.pid)
    ceph osd down osd.${osd_a}
    ceph osd out osd.${osd_a}

    # 1.1 Wait and confirm state moves to degraded or undersized
    local state=""
    for i in $(seq 1 30); do
      state=$(ceph pg $pgid query | jq -r '.info.stats.state')
      echo "Current PG $pgid state: $state"
      if [[ "$state" == *"degraded"* ]] || \
         [[ "$state" == *"undersized"* ]]; then
        break
      fi
      sleep 1
    done

    if [[ "$state" != *"degraded"* ]] && [[ "$state" != *"undersized"* ]]; then
      echo "Error: PG $pgid state ($state) did not become " \
           "degraded/undersized after killing osd.$osd_a."
      return 1
    fi

    # 1.2 Confirm last_degraded updated
    local last_degraded_t1=$(ceph pg $pgid query | \
      jq -r '.info.stats.last_degraded')
    echo "Queried last_degraded (T1): $last_degraded_t1"
    if [[ "$last_degraded_t1" > "$last_clean_start" ]]; then
      echo "Confirmed: last_degraded ($last_degraded_t1) updated on failure."
    else
      echo "Error: last_degraded ($last_degraded_t1) is not newer than " \
           "initial last_clean ($last_clean_start)."
      return 1
    fi

    # --- Step 2: Kill the second non-primary OSD (osd_b) ---
    echo "Stopping OSD.$osd_b..."
    kill $(cat $dir/osd.${osd_b}.pid)
    ceph osd down osd.${osd_b}
    ceph osd out osd.${osd_b}

    # 2.1 Confirm last_degraded remains latched (the same)
    local last_degraded_t2=$(ceph pg $pgid query | \
      jq -r '.info.stats.last_degraded')
    echo "Queried last_degraded (T2): $last_degraded_t2"
    if [[ "$last_degraded_t2" == "$last_degraded_t1" ]]; then
      echo "Test Passed: last_degraded timestamp remained " \
           "stable at $last_degraded_t2."
    else
      echo "Test Failed: last_degraded updated to " \
           "$last_degraded_t2 on second failure."
      return 1
    fi

    # --- Step 3: Recovery ---
    echo "Unsetting norecover and restarting OSDs..."
    ceph osd unset norecover

    echo "Restarting OSDs $osd_a and $osd_b..."
    activate_osd $dir $osd_a
    activate_osd $dir $osd_b
    wait_for_clean || return 1

    # --- Step 4: Final Verification ---
    # After the window closes and is recorded, prepare_stats_for_publish()
    # collapses last_degraded up to last_clean (so the closed window is never
    # re-recorded). The post-recovery resting state is therefore
    # last_degraded == last_clean, not last_degraded < last_clean.
    local final_stats=$(ceph pg $pgid query | \
      jq -r '.info.stats | "\(.last_degraded) \(.last_clean)"')
    read -r last_degraded_final last_clean_final <<< "$final_stats"

    echo "Final Timestamps -> Last Degraded: $last_degraded_final, " \
         "Last Clean: $last_clean_final"
    if [[ ! "$last_degraded_final" > "$last_clean_final" ]]; then
      echo "Test Passed: Recovery successful. last_degraded" \
           "($last_degraded_final) collapsed to <= last_clean" \
           "($last_clean_final)."
    else
      echo "Test Failed: last_degraded ($last_degraded_final) is still ahead" \
           "of last_clean ($last_clean_final) after recovery."
      return 1
    fi

    # Cleanup
    delete_pool $poolname
    kill_daemons $dir || return 1
}

function TEST_recovery_last_degraded_undersized() {
    local dir=$1
    local osds=3

    # 1. Setup Cluster
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for i in $(seq 0 $(expr $osds - 1)); do
      run_osd $dir $i || return 1
    done

    # 2. Create Pool and force size 1
    create_pool $poolname 8 8
    ceph osd pool set $poolname size 1 --yes-i-really-mean-it
    wait_for_clean || return 1

    # Inject data
    for i in $(seq 1 50); do
      rados -p $poolname put obj$i /dev/null
    done

    local pgid=$(get_pg $poolname obj1)
    local primary=$(get_primary $poolname obj1)

    # 3. Select Non-Primary OSD
    local replica_osd=""
    for i in $(seq 0 $(expr $osds - 1)); do
      if [[ "$i" != "$primary" ]]; then
          replica_osd=$i
          break
      fi
    done
    echo "Primary is OSD.$primary, selected OSD.$replica_osd to mark OUT."

    local last_clean_start=$(ceph pg $pgid query | \
      jq -r '.info.stats.last_clean')

    # 4. Mark non-primary OSD out and set norecover
    ceph osd set norecover
    ceph osd out $replica_osd

    # 5. Increase pool size to 4
    echo "Increasing pool size to 4..."
    ceph osd pool set $poolname size 4

    # 6. Unset norecover and kick the recovery queue
    echo "Starting recovery..."
    ceph osd unset norecover
    ceph tell osd.$primary debug kick_recovery_wq 0

    sleep 10
    flush_pg_stats || return 1

    # 7. Custom recovery-wait logic
    echo "Waiting for $pgid to be marked undersized..."
    for i in $(seq 1 300); do
      # Fetch only the stats for the specific PG in JSON format
      local current_state=$(ceph pg $pgid query | jq -r '.info.stats.state')
      echo "Iteration $i: PG $pgid state is [$current_state]"

      # Check if 'recovering' is absent from the state string
      if [[ "$current_state" != *"recovering"* ]]; then
        echo "PG $pgid is marked undersized (current state: $current_state)."
        break
      fi
      if [ "$i" = "300" ]; then
        echo "Timeout waiting for $pgid to become undersized"
        ceph pg $pgid query | jq .
        return 1
      fi
      sleep 1
    done

    # 8. Verification
    local last_degraded_final=$(ceph pg $pgid query | \
      jq -r '.info.stats.last_degraded')
    echo "Initial Clean:  $last_clean_start"
    echo "Final Degraded: $last_degraded_final"

    if [[ "$last_degraded_final" > "$last_clean_start" ]]; then
      echo "Test Passed: last_degraded updated correctly."
    else
      echo "Test Failed: last_degraded ($last_degraded_final) was not updated."
      return 1
    fi

    # Cleanup
    delete_pool $poolname
    kill_daemons $dir || return 1
}

# Verify that the rebuild perf counters on the primary OSD increment after a
# real EC shard recovery, AND that a same-primary peering-interval restart
# occurring mid-rebuild does not truncate or drop the recorded duration.
#
# Sequence:
#  1. Kill one non-primary OSD so the PG goes degraded (1st interval restart)
#  2. Grep primary's log for rebuild latch firing, hold a deliberate gap before
#     the second restart to unambiguously distinguish the duration.
#  3. Mark the non-primary OSD out which results in the 4th OSD added to the
#     acting set and becomes a backfill target (2nd interval restart).
#  4. Assert that "latched failure start" line appears only once in the logs.
#     This confirms that the second restart does not reset the counters.
#  5. Let recovery run to completion. Assert exactly one "recorded rebuild"
#     line, and that the recorded duration covers at least the deliberate gap
#     from step 2 -- proving the full window survived.
function TEST_rebuild_perf_ec_increments() {
    local dir=$1
    local OSDS=4
    local ecpoolname=ectest
    # Deliberate gap between the latch firing and the second interval
    # restart, long enough to be unambiguous against scheduling jitter.
    local gap_secs=5

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      # debug-osd=15 so the "rebuild-stats: latched/recorded" lines emitted
      # by prepare_stats_for_publish() are captured in the OSD log.
      run_osd $dir $osd --osd-mclock-skip-benchmark=true --debug-osd=15 || return 1
    done

    ceph osd erasure-code-profile set ecprofile \
        plugin=jerasure technique=reed_sol_van k=2 m=1 \
        crush-failure-domain=osd || return 1
    ceph osd pool create $ecpoolname 1 1 erasure ecprofile || return 1
    ceph osd pool set $ecpoolname min_size 2 || return 1
    wait_for_clean || return 1

    # Write a few objects so the PG has data that must be recovered.
    for i in $(seq 1 5)
    do
      rados -p $ecpoolname put obj$i /etc/hostname || return 1
    done
    wait_for_clean || return 1

    local primary
    primary=$(get_primary $ecpoolname obj1)
    local PG
    PG=$(get_pg $ecpoolname obj1)
    # Derive the primary's actual shard for a given object (obj1)
    local primary_shard
    primary_shard=$(ceph --format json osd map $ecpoolname obj1 2>/dev/null | \
      jq ".acting | index($primary)")
    local PG_SPG="${PG}s${primary_shard}"
    local replica
    replica=$(get_not_primary $ecpoolname obj1)
    local log=$dir/osd.${primary}.log

    # Pause recovery so the PG stays degraded long enough for the latch to
    # fire inside prepare_stats_for_publish before recovery completes.
    ceph osd set norecover || return 1

    # Kill one non-primary OSD so the PG becomes degraded.
    # ---1st interval restart---
    kill $(cat $dir/osd.${replica}.pid)
    ceph osd down osd.${replica} || return 1

    if [ "$(get_primary $ecpoolname obj1)" != "$primary" ]; then
      echo "FAIL: primary changed after killing a non-primary OSD;" \
           "test topology assumption broken"
      return 1
    fi

    # Wait for the latch to fire.
    local latched=0
    for i in $(seq 1 30)
    do
      flush_pg_stats || return 1
      if grep -q "rebuild-stats: vulnerability window opened for ${PG_SPG} " $log
      then
        latched=1
        break
      fi
      sleep 1
    done
    test "$latched" = 1 || {
      echo "FAIL: rebuild latch never fired after opening the acting-set hole"
      return 1
    }

    # Deliberate gap before the second restart. A duration truncated by a
    # re-latch after that restart would come out well under this.
    sleep $gap_secs

    # --- 2nd interval restart: mark the OSD out to force a remap of a spare
    # OSD into the acting set as a backfill target. Primary is unaffected.
    ceph osd out osd.${replica} || return 1

    if [ "$(get_primary $ecpoolname obj1)" != "$primary" ]; then
      echo "FAIL: primary changed after marking the OSD out;" \
           "test topology assumption broken"
      return 1
    fi

    # Let the new interval settle and force another stats publish so a
    # pre-fix reset-and-relatch would already be visible in the log here.
    sleep 2
    flush_pg_stats || return 1

    local latch_count
    latch_count=$(grep -c "rebuild-stats: vulnerability window opened for ${PG_SPG} " $log)
    test "$latch_count" = 1 || {
      echo "FAIL: expected exactly 1 'latched failure start' for ${PG_SPG}," \
           "got $latch_count -- the same-primary interval restart reset" \
           "the in-progress latch"
      return 1
    }

    # Release the hold and wait for full recovery.
    ceph osd unset norecover || return 1
    wait_for_clean || return 1

    # flush_pg_stats triggers publish_stats_to_osd on every OSD, which calls
    # prepare_stats_for_publish and commits the rebuild counters.
    flush_pg_stats || return 1

    # The primary may be the same OSD we started with (we only killed a
    # replica), but re-query in case CRUSH remapped the primary shard.
    primary=$(get_primary $ecpoolname obj1)
    log=$dir/osd.${primary}.log

    local dump
    dump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) \
           perf dump) || return 1

    local rebuild_avgcount
    rebuild_avgcount=$(\
      jq '.recoverystate_perf.pg_vulnerability_duration.avgcount' <<< "$dump")
    test "$rebuild_avgcount" -ge 1 || {
      echo "FAIL: expected pg_vulnerability_duration.avgcount>=1," \
           "got $rebuild_avgcount"
      return 1
    }

    local rebuild_sum
    rebuild_sum=$(\
      jq '.recoverystate_perf.pg_vulnerability_duration.sum' <<< "$dump")
    echo "$dump" | \
      jq -e '.recoverystate_perf.pg_vulnerability_duration.sum > 0' \
      > /dev/null || {
      echo "FAIL: expected pg_vulnerability_duration.sum>0, got $rebuild_sum"
      return 1
    }

    # Exactly one full rebuild event must have been recorded.
    local record_count
    record_count=$(grep -c "rebuild-stats: recorded vulnerability window for ${PG_SPG} " $log)
    test "$record_count" = 1 || {
      echo "FAIL: expected exactly 1 'recorded rebuild' for ${PG_SPG}," \
           "got $record_count"
      return 1
    }

    # The recorded duration must cover at least the deliberate gap held
    # before the second restart i.e., $gap_secs. pg_vulnerability_duration.sum is
    # reported in fractional seconds.
    echo "$dump" | \
      jq -e ".recoverystate_perf.pg_vulnerability_duration.sum >= ${gap_secs}" \
      > /dev/null || {
      echo "FAIL: expected pg_vulnerability_duration.sum >= ${gap_secs}s" \
           "(the ${gap_secs}s gap held before the second interval restart)," \
           "got ${rebuild_sum}s -- duration looks truncated"
      return 1
    }

    # min/max: exactly one window recorded here, so the longest single
    # window (pg_vulnerability_duration.max_inc, from tinc_with_max) and the
    # shortest (the companion pg_vulnerability_duration_min gauge) both equal
    # the sum, in fractional seconds, and neither is truncated to 0.
    local rebuild_max rebuild_min
    rebuild_max=$(jq '.recoverystate_perf.pg_vulnerability_duration.max_inc' <<< "$dump")
    rebuild_min=$(jq '.recoverystate_perf.pg_vulnerability_duration_min' <<< "$dump")
    echo "INFO: max_inc=${rebuild_max}s pg_vulnerability_duration_min=${rebuild_min}s"
    echo "$dump" | jq -e \
      ".recoverystate_perf.pg_vulnerability_duration.max_inc >= ${gap_secs} and \
       .recoverystate_perf.pg_vulnerability_duration_min > 0 and \
       .recoverystate_perf.pg_vulnerability_duration_min <= \
       .recoverystate_perf.pg_vulnerability_duration.max_inc" > /dev/null || {
      echo "FAIL: pg_vulnerability_duration max_inc / _min not sane" \
           "(max_inc=${rebuild_max}s, min=${rebuild_min}s, gap=${gap_secs}s)"
      return 1
    }

    # pg_rebuild_duration (active recovery/backfill only) -- conservative
    # sanity checks only here, not an exact relationship against
    # pg_vulnerability_duration or gap_secs.
    #
    # pg_rebuild_duration's span is expected to be noticeably shorter than
    # pg_vulnerability_duration's here, not just microseconds apart: for
    # most of the held window the replica is only `down`, not yet `out`,
    # so it's excluded from acting_recovery_backfill and the PG has
    # nothing actionable -- it only starts Recovering/Backfilling once the
    # replica is marked `out` and a spare becomes a real backfill target.
    # Setup validity first: confirm the PG really did enter Recovering or
    # Backfilling at some point (it has real objects to recover, so it
    # should have).
    grep -q "enter Started/Primary/Active/Recovering\|enter Started/Primary/Active/Backfilling" $log || {
      echo "FAIL: ${PG_SPG} never entered Recovering or Backfilling despite" \
           "having real objects to recover -- test setup assumption broken"
      return 1
    }
    local pg_rebuild_avgcount pg_rebuild_sum pg_rebuild_max pg_rebuild_min
    pg_rebuild_avgcount=$(jq '.recoverystate_perf.pg_rebuild_duration.avgcount' <<< "$dump")
    pg_rebuild_sum=$(jq '.recoverystate_perf.pg_rebuild_duration.sum' <<< "$dump")
    pg_rebuild_max=$(jq '.recoverystate_perf.pg_rebuild_duration.max_inc' <<< "$dump")
    pg_rebuild_min=$(jq '.recoverystate_perf.pg_rebuild_duration_min' <<< "$dump")
    echo "INFO: pg_rebuild_duration avgcount=${pg_rebuild_avgcount}" \
         "sum=${pg_rebuild_sum}s max_inc=${pg_rebuild_max}s min=${pg_rebuild_min}s"
    test "$pg_rebuild_avgcount" -ge 1 || {
      echo "FAIL: expected pg_rebuild_duration.avgcount>=1, got" \
           "$pg_rebuild_avgcount"
      return 1
    }
    echo "$dump" | jq -e \
      ".recoverystate_perf.pg_rebuild_duration.sum > 0 and \
       .recoverystate_perf.pg_rebuild_duration.max_inc > 0 and \
       .recoverystate_perf.pg_rebuild_duration_min > 0 and \
       .recoverystate_perf.pg_rebuild_duration_min <= \
       .recoverystate_perf.pg_rebuild_duration.max_inc" > /dev/null || {
      echo "FAIL: pg_rebuild_duration sum/max_inc/_min not sane (sum=" \
           "${pg_rebuild_sum}s, max_inc=${pg_rebuild_max}s, min=${pg_rebuild_min}s)"
      return 1
    }

    delete_pool $ecpoolname
    kill_daemons $dir || return 1
}

# Test to verify the following:
# 1. A forced, deterministic two primary handover chain within ONE continuous
#    vulnerability episode, confirming the departing primary's own segment is
#    recorded on each handover and that this holds across a genuine multi-hop
#    chain, not just a single one.
# 2. The test also observes (without asserting either way) whether the
#    returning OSDs show any activity of their own once brought back.
function TEST_rebuild_perf_multihop_handover() {
    local dir=$1
    local OSDS=4

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd --osd-mclock-skip-benchmark=true --debug-osd=15 || return 1
    done

    create_pool $poolname 1 1 replicated || return 1
    ceph osd pool set $poolname size 4 || return 1
    ceph osd pool set $poolname min_size 2 || return 1
    wait_for_clean || return 1

    for i in $(seq 1 5)
    do
      rados -p $poolname put obj$i /etc/hostname || return 1
    done
    wait_for_clean || return 1

    local PG
    PG=$(get_pg $poolname obj1)
    local primary_a
    primary_a=$(get_primary $poolname obj1)

    # --- Hop 0: kill the original primary A. Primary hands to a survivor B.
    ceph osd set noup || return 1
    ceph osd down osd.${primary_a} || return 1

    local primary_b=""
    for i in $(seq 1 30)
    do
      primary_b=$(get_primary $poolname obj1)
      test "$primary_b" != "$primary_a" && break
      sleep 1
    done
    test "$primary_b" != "$primary_a" -a -n "$primary_b" || {
      echo "FAIL: primary never changed after osd.${primary_a} went down"
      return 1
    }
    local log_b=$dir/osd.${primary_b}.log

    local latched_b=0
    for i in $(seq 1 30)
    do
      flush_pg_stats || return 1
      grep -q "rebuild-stats: vulnerability window opened for ${PG} " $log_b && {
        latched_b=1
        break
      }
      sleep 1
    done
    test "$latched_b" = 1 || {
      echo "FAIL: osd.${primary_b} never latched after taking over primary"
      return 1
    }

    for i in $(seq 6 10)
    do
      rados -p $poolname put obj$i /etc/hostname || return 1
    done

    # --- Hop 1: kill B too, before anything has a chance to recover.
    # osd.${primary_a} is still merely down (not out); so nothing has backfilled
    ceph osd down osd.${primary_b} || return 1

    local primary_c=""
    for i in $(seq 1 30)
    do
      primary_c=$(get_primary $poolname obj1)
      test "$primary_c" != "$primary_a" -a "$primary_c" != "$primary_b" && break
      sleep 1
    done
    test -n "$primary_c" -a "$primary_c" != "$primary_a" -a "$primary_c" != "$primary_b" || {
      echo "FAIL: primary never changed to a third OSD after osd.${primary_b} went down"
      return 1
    }
    local log_c=$dir/osd.${primary_c}.log

    local latched_c=0
    for i in $(seq 1 30)
    do
      flush_pg_stats || return 1
      grep -q "rebuild-stats: vulnerability window opened for ${PG} " $log_c && {
        latched_c=1
        break
      }
      sleep 1
    done
    test "$latched_c" = 1 || {
      echo "FAIL: osd.${primary_c} never opened a window after taking over"
      return 1
    }

    # --- Bring both A and B back and let the episode resolve.
    ceph osd unset noup || return 1
    wait_for_clean || return 1
    flush_pg_stats || return 1

    # test current behavior before peering and persistence of last_degraded
    # are implemented: pg_stat_t.last_degraded is not synced to peers, so
    # each new primary opens a fresh window from its own takeover and the
    # pre-handover exposure is not carried forward. The window is recorded
    # exactly ONCE for this episode -- by whichever OSD is primary when the
    # PG finally reaches clean -- and its duration reflects only that last
    # primary's tenure, not the whole episode.
    #
    # The persistence phase (last_degraded in pg_history_t) changes this:
    # the single record will then span the true onset. When that lands, this
    # test gains an assertion that the recorded duration covers the full
    # wall-clock episode; for now it only pins "exactly one record".
    local total_recorded
    total_recorded=$(grep -h "rebuild-stats: recorded vulnerability window for ${PG} " \
      $dir/osd.*.log | wc -l)
    test "$total_recorded" = 1 || {
      echo "FAIL: expected exactly 1 'recorded vulnerability window' line for" \
           "${PG} across all four OSDs, got $total_recorded"
      return 1
    }

    # Cross-check against the OSD-wide perf counter: summed avgcount across
    # all four OSDs must also be exactly 1.
    local total_avgcount=0
    for osd in 0 1 2 3
    do
      test -S $(get_asok_path osd.${osd}) || continue
      local d
      d=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${osd}) perf dump 2>/dev/null) || continue
      local c
      c=$(jq '.recoverystate_perf.pg_vulnerability_duration.avgcount' <<< "$d")
      total_avgcount=$(expr $total_avgcount + ${c:-0})
    done

    test "$total_avgcount" = 1 || {
      echo "FAIL: expected summed pg_vulnerability_duration.avgcount=1 across" \
           "all OSDs, got $total_avgcount"
      return 1
    }

    delete_pool $poolname
    kill_daemons $dir || return 1
}

# An empty (zero-object) PG that goes undersized/degraded and back to clean
# IS counted by pg_vulnerability_duration under the full solution: the
# counter measures exposure time from the state transition, independent of
# whether any object was ever at risk. (The interim solution deliberately
# discarded these via a delta_recovered/had_redundancy_loss filter; the full
# solution drops that filter.)
function TEST_rebuild_perf_empty_pg_counted() {
    local dir=$1
    local OSDS=4

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd --osd-mclock-skip-benchmark=true --debug-osd=15 || return 1
    done

    create_pool $poolname 1 1 replicated || return 1
    ceph osd pool set $poolname size 3 || return 1
    ceph osd pool set $poolname min_size 2 || return 1
    wait_for_clean || return 1
    # Deliberately no rados put here -- this PG must stay empty throughout.

    local primary
    primary=$(get_primary $poolname dummyname)
    local PG
    PG=$(get_pg $poolname dummyname)
    local otherosd
    otherosd=$(get_not_primary $poolname dummyname)
    local log=$dir/osd.${primary}.log

    ceph osd set noup || return 1
    ceph osd down osd.${otherosd} || return 1

    for i in $(seq 1 10)
    do
      flush_pg_stats || return 1
      sleep 1
    done

    # Confirm the window actually opened (state genuinely went undersized)
    # before checking it was recorded -- a test that just sees "recorded"
    # without this could be picking up unrelated activity.
    grep -q "rebuild-stats: vulnerability window opened for ${PG} " $log || {
      echo "FAIL: the window never opened -- test setup didn't actually" \
           "make the PG undersized, this isn't testing what it claims to"
      return 1
    }

    ceph osd unset noup || return 1
    wait_for_clean || return 1
    flush_pg_stats || return 1

    grep -q "rebuild-stats: recorded vulnerability window for ${PG} " $log || {
      echo "FAIL: an empty PG's undersized/degraded window was NOT recorded" \
           "-- the full solution counts these (exposure time, not data" \
           "movement)"
      return 1
    }

    local dump
    dump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) \
           perf dump) || return 1
    local avgcount
    avgcount=$(jq '.recoverystate_perf.pg_vulnerability_duration.avgcount' \
      <<< "$dump")
    test "$avgcount" -ge 1 || {
      echo "FAIL: expected pg_vulnerability_duration.avgcount>=1 for an" \
           "empty-PG episode, got $avgcount"
      return 1
    }

    # pg_rebuild_duration (active recovery/backfill only) must record NOTHING
    # for this episode -- there were zero objects to recover. Confirm the PG
    # really never entered either state ("enter <state>") trace is unique per
    # state name, so absence here is a direct, not inferred, negative).
    if grep -q "enter Started/Primary/Active/Recovering\|enter Started/Primary/Active/Backfilling" $log
    then
      echo "FAIL: ${PG} entered Recovering or Backfilling despite having" \
           "zero objects -- test setup assumption broken, this isn't" \
           "testing the silent-exposure case it claims to"
      return 1
    fi
    local rebuild_avgcount
    rebuild_avgcount=$(jq '.recoverystate_perf.pg_rebuild_duration.avgcount' \
      <<< "$dump")
    test "$rebuild_avgcount" = 0 || {
      echo "FAIL: expected pg_rebuild_duration.avgcount=0 for a purely" \
           "silent (empty-PG) episode -- no data ever moved, so nothing" \
           "should be recorded here even though pg_vulnerability_duration" \
           "correctly recorded one -- got $rebuild_avgcount"
      return 1
    }

    delete_pool $poolname
    kill_daemons $dir || return 1
}

# A PG split (ceph osd pool set ... pg_num N, triggering
# PeeringState::split_into()/finish_split_stats()) occurring while the parent PG
# is already latched as vulnerable. The test Forces a PG degraded (noup + down
# one replica, size=3/ min_size=2), lets a first recovery cycle complete so
# num_objects_recovered is genuinely nonzero, then re-degrades the same PG,
# holds it vulnerable for a deliberate 8 secs, and grows pg_num from 1 to 2 to
# split it while still degraded. The test finally verifies the following:
#  1. The parent PG's own recorded duration - printed and asserted positive.
#  2. The child must show NO "latched failure start" line of its own anywhere
#     -- the most direct check of all, since it actually inherits the latch.
#  3. The child's recorded duration is also parsed and printed, and asserted
#     >= 5s. Reason: The second-cycle degrade-then-sleep-8-then-split sequence
#     above means an inherited latch's recorded duration must be at least ~8s
#     (it spans that sleep), while a fresh arm at the split moment would show
#     a duration of a few seconds at most.
function TEST_rebuild_perf_pg_split_inherits_latch() {
    local dir=$1
    local OSDS=4

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd --osd-mclock-skip-benchmark=true --debug-osd=15 || return 1
    done

    create_pool $poolname 1 1 replicated || return 1
    ceph osd pool set $poolname size 3 || return 1
    ceph osd pool set $poolname min_size 2 || return 1
    # Deterministic pg_num control for this test -- don't let the
    # autoscaler race the manual pg_num bump below.
    ceph osd pool set $poolname pg_autoscale_mode off || return 1
    wait_for_clean || return 1

    rados -p $poolname bench 5 write -b 4096 --no-cleanup || return 1
    wait_for_clean || return 1

    local primary
    primary=$(get_primary $poolname dummyname)
    local PG
    PG=$(get_pg $poolname dummyname)
    local poolid=${PG%.*}
    local child_pg="${poolid}.1"
    local otherosd
    otherosd=$(get_not_primary $poolname dummyname)
    local log=$dir/osd.${primary}.log

    # --- Priming cycle: force a real, *completed* recovery so
    # num_objects_recovered is genuinely nonzero before the PG is ever
    # re-degraded and split -- see header comment for why this matters.
    ceph osd set noup || return 1
    ceph osd down osd.${otherosd} || return 1
    for i in $(seq 1 10)
    do
      flush_pg_stats || return 1
      sleep 1
    done
    grep -q "rebuild-stats: vulnerability window opened for ${PG} " $log || {
      echo "FAIL: the priming latch never armed -- test setup didn't" \
           "actually make the PG degraded"
      return 1
    }
    # More writes while degraded, so the down OSD has real missing
    # objects to actually recover once it returns (not a no-op catch-up).
    rados -p $poolname bench 5 write -b 4096 --no-cleanup || return 1
    ceph osd unset noup || return 1
    wait_for_clean || return 1
    flush_pg_stats || return 1
    grep -q "rebuild-stats: recorded vulnerability window for ${PG} " $log || {
      echo "FAIL: the priming cycle's recovery was never recorded --" \
           "num_objects_recovered won't be primed, the real test below" \
           "would be vacuous"
      return 1
    }

    # --- Real test: re-degrade the same PG (now with a large, nonzero
    # rebuild_base_recovered baseline once this second latch arms), hold
    # it degraded for a bit, then split while still vulnerable.
    ceph osd set noup || return 1
    ceph osd down osd.${otherosd} || return 1

    local second_armed=false
    for i in $(seq 1 10)
    do
      flush_pg_stats || return 1
      if test "$(grep -c "rebuild-stats: vulnerability window opened for ${PG} " $log)" -ge 2
      then
        second_armed=true
        break
      fi
      sleep 1
    done
    $second_armed || {
      echo "FAIL: the second latch never armed before the split -- test" \
           "setup didn't actually re-degrade the PG"
      return 1
    }
    # Hold the degraded window open for long enough that an inherited
    # duration (spanning this whole sleep) and a freshly-armed one
    # (starting at the split below) are unambiguously distinguishable
    # afterward.
    sleep 8

    # Split the still-degraded parent PG in two
    local before_numpg
    before_numpg=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) \
      perf dump | jq '.osd.numpg')
    ceph osd pool set $poolname pg_num 2 || return 1

    local split_done=false
    for i in $(seq 1 60)
    do
      local numpg
      numpg=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) \
        perf dump | jq '.osd.numpg')
      if test "$numpg" -gt "$before_numpg"
      then
        split_done=true
        break
      fi
      sleep 1
    done
    $split_done || {
      echo "FAIL: split never completed on osd.${primary} (numpg stayed" \
           "at $before_numpg after 60s) -- either the split stalled while" \
           "the PG was degraded (a real, previously unconfirmed risk --" \
           "see this test's header comment), or ${child_pg} isn't where" \
           "expected; check osd logs directly"
      return 1
    }

    ceph osd unset noup || return 1
    wait_for_clean || return 1
    flush_pg_stats || return 1

    # Both the parent and the split-created child must eventually record a
    # genuine, non-discarded rebuild segment with a sane (non-negative)
    # delta_recovered -- checked across all four OSDs since either PG's
    # primary could have landed anywhere post-split.
    for pg in ${PG} ${child_pg}
    do
      local recorded=false
      local discarded=false
      for osd in $(seq 0 $(expr $OSDS - 1))
      do
        grep -q "rebuild-stats: recorded vulnerability window for ${pg} " $dir/osd.${osd}.log \
          && recorded=true
         grep -q "rebuild-stats: discarded vulnerability window for ${pg} " $dir/osd.${osd}.log \
          && discarded=true
      done

      $recorded || {
        echo "FAIL: no 'recorded vulnerability window' line found anywhere" \
             "for ${pg} -- the split-inherited window was lost or never" \
             "resolved"
        return 1
      }
      ! $discarded || {
        echo "FAIL: a 'discarded vulnerability window' line was found for" \
             "${pg} -- the split-inherited window collapsed to a sub-ms" \
             "duration instead of being recorded"
        return 1
      }
    done

    # Sharpest, most direct check of all: the child pg must not emit its
    # own "latched failure start" line BEFORE its first recorded resolution.
    # A correctly inheriting child must produce no "latched" line before the
    # inherited rebuild resolves.
    #
    # The check is deliberately scoped to "before the first recorded line",
    # not "anywhere in the log": growing pg_num can also auto-bump
    # pgp_num_target, and pgp_num's own gradual catch-up is a genuine, unrelated
    # CRUSH remap that can cause the child to legitimately re-latch on its own,
    # later, due to a real misplacement. Therefore, this tests the actual
    # inheritance mechanism directly rather than through inference.
    local child_first_recorded_ts
    child_first_recorded_ts=$(grep -h "rebuild-stats: recorded vulnerability window for ${child_pg} " \
      $dir/osd.*.log | sort | head -1 | awk '{print $1}')
    test -n "$child_first_recorded_ts" || {
      echo "FAIL: could not determine ${child_pg}'s first recorded timestamp"
      return 1
    }
    local child_earliest_latched_ts
    child_earliest_latched_ts=$(grep -h "rebuild-stats: vulnerability window opened for ${child_pg} " \
      $dir/osd.*.log | sort | head -1 | awk '{print $1}')
    if [ -n "$child_earliest_latched_ts" ] \
      && [[ "$child_earliest_latched_ts" < "$child_first_recorded_ts" ]]
    then
      echo "FAIL: ${child_pg} shows its own 'latched failure start' line" \
           "(at $child_earliest_latched_ts) BEFORE its first recorded" \
           "resolution (at $child_first_recorded_ts) -- it independently" \
           "armed a fresh latch instead of inheriting the parent's" \
           "already-armed one at split time"
      return 1
    fi

    # Parent PG sanity check complementing the non-negative delta_recovered
    # check above: its recorded duration from the real (second, post-
    # priming) cycle must be positive. Uses $log specifically (not a glob
    # across all four OSDs) because the primary never changes for this PG
    # throughout the test (only otherosd goes up/down), so both the
    # priming cycle's and the real test's "recorded rebuild for ${PG}"
    # lines land in this single file in true chronological order --
    # `tail -1` reliably picks the real (second) one, not the priming
    # cycle's first one, without relying on cross-file ordering.
    local parent_duration
    parent_duration=$(grep "rebuild-stats: recorded vulnerability window for ${PG} " $log \
      | tail -1 | grep -o "duration=[0-9.]*" | cut -d= -f2)
    test -n "$parent_duration" || {
      echo "FAIL: could not extract ${PG}'s (parent) recorded duration"
      return 1
    }
    echo "INFO: ${PG} (parent) recorded duration=${parent_duration}s"
    awk -v d="$parent_duration" 'BEGIN { exit !(d > 0) }' || {
      echo "FAIL: ${PG}'s (parent) recorded duration (${parent_duration}s)" \
           "is not positive"
      return 1
    }

    # The child PG's  inherited latch's recorded duration must be
    # at least ~8s (it spans that sleep), while a fresh arm at the split
    # moment would show a duration of a few seconds at most (just the
    # post-split recovery time, none of the pre-split sleep). Use >=5 as
    # the threshold -- comfortably below the true ~8s+ floor for a correct
    # inheritance, comfortably above what a fresh split-time arm could
    # plausibly accumulate before this test's own wait_for_clean returns.
    local child_duration
    child_duration=$(grep -h "rebuild-stats: recorded vulnerability window for ${child_pg} " \
      $dir/osd.*.log | grep -o "duration=[0-9.]*" | head -1 | cut -d= -f2)
    test -n "$child_duration" || {
      echo "FAIL: could not extract ${child_pg}'s recorded duration"
      return 1
    }
    echo "INFO: ${child_pg} (child) recorded duration=${child_duration}s"
    awk -v d="$child_duration" 'BEGIN { exit !(d >= 5) }' || {
      echo "FAIL: ${child_pg}'s recorded duration ($child_duration s) is" \
           "too short to have inherited the pre-split latch -- looks like" \
           "the child started a fresh arm at the split moment instead of" \
           "parent-to-child copy taking effect"
      return 1
    }

    # Global reconciliation: pg_vulnerability_duration is an OSD-wide
    # aggregate, not per-PG, so the only meaningful level to verify it at
    # is a TOTAL across all four OSDs. Retries briefly since perf dump can
    # momentarily race a just-written log line's disk flush.
    local total_log_recorded total_log_duration total_avgcount total_sum
    for i in $(seq 1 10)
    do
      local recorded_lines
      recorded_lines=$(grep -h "rebuild-stats: recorded vulnerability window for " \
        $dir/osd.*.log)
      if test -z "$recorded_lines"
      then
        # `wc -l <<< ""` reports 1, not 0 (a here-string always supplies a
        # trailing newline) -- guard the truly-empty case explicitly rather
        # than let that gotcha miscount "no recorded lines yet" as one.
        total_log_recorded=0
        total_log_duration=0
      else
        total_log_recorded=$(wc -l <<< "$recorded_lines")
        total_log_duration=$(grep -o "duration=[0-9.]*" <<< "$recorded_lines" \
          | cut -d= -f2 | awk '{s+=$1} END {print s+0}')
      fi
      total_avgcount=0
      total_sum=0
      for osd in $(seq 0 $(expr $OSDS - 1))
      do
        local dump
        dump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${osd}) \
          perf dump) || return 1
        total_avgcount=$(awk -v a="$total_avgcount" \
          -v b="$(jq '.recoverystate_perf.pg_vulnerability_duration.avgcount' <<< "$dump")" \
          'BEGIN{print a+b}')
        total_sum=$(awk -v a="$total_sum" \
          -v b="$(jq '.recoverystate_perf.pg_vulnerability_duration.sum' <<< "$dump")" \
          'BEGIN{print a+b}')
      done
      test "$total_avgcount" = "$total_log_recorded" && break
      sleep 1
    done

    echo "INFO: total recorded (logs)=${total_log_recorded}," \
         "total avgcount (perf dump)=${total_avgcount}"
    echo "INFO: total duration (logs)=${total_log_duration}s," \
         "total sum (perf dump)=${total_sum}s"

    test "$total_avgcount" = "$total_log_recorded" || {
      echo "FAIL: perf dump's total avgcount (${total_avgcount}) across" \
           "all four OSDs doesn't match the total 'recorded rebuild' line" \
           "count from the logs (${total_log_recorded})"
      return 1
    }
    awk -v a="$total_sum" -v b="$total_log_duration" \
      'BEGIN { d=a-b; if (d<0) d=-d; exit !(d < 0.01) }' || {
      echo "FAIL: perf dump's total sum (${total_sum}s) across all four" \
           "OSDs doesn't match the total recorded duration from the logs" \
           "(${total_log_duration}s)"
      return 1
    }

    delete_pool $poolname
    kill_daemons $dir || return 1
}

# pg_rebuild_duration: the two hooks that arm the active-rebuild
# latch -- Recovering::Recovering() and Backfilling::Backfilling() -- are
# structurally identical but live in separate constructors. The next
# two tests force each path independently and deliberately, so either hook
# silently breaking (e.g. a future refactor) would be caught by exactly one
# of them -- and both check pg_rebuild_duration against
# pg_vulnerability_duration together, for a genuine (non-silent,
# non-throttled) episode.
#
# PG.cc's publish_stats_to_osd() (called at the end of Recovering/
# Backfilling/NotRecovering's own constructors) calls
# prepare_stats_for_publish() synchronously, not deferred -- so for a real,
# un-throttled recovery both counters' onset and close land within the same
# call chain, microseconds apart. pg_rebuild_duration should therefore be
# close to pg_vulnerability_duration here.
function TEST_rebuild_perf_recovering_case() {
    local dir=$1
    local OSDS=4

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd --osd-mclock-skip-benchmark=true --debug-osd=15 || return 1
    done

    create_pool $poolname 1 1 replicated || return 1
    ceph osd pool set $poolname size 3 || return 1
    ceph osd pool set $poolname min_size 2 || return 1
    wait_for_clean || return 1

    for i in $(seq 1 5)
    do
      rados -p $poolname put obj$i /etc/hostname || return 1
    done
    wait_for_clean || return 1

    local primary
    primary=$(get_primary $poolname obj1)
    local PG
    PG=$(get_pg $poolname obj1)
    local otherosd
    otherosd=$(get_not_primary $poolname obj1)
    local log=$dir/osd.${primary}.log

    # Small, log-continuous gap: noup+down, a handful of new writes, revive
    # quickly -- force the log-based "catch up"/Recovering path rather than a
    # full backfill. Default osd_max_pg_log_entries is 10000, so a handful of
    # writes stays comfortably within log-continuity range.
    ceph osd set noup || return 1
    ceph osd down osd.${otherosd} || return 1

    for i in $(seq 6 10)
    do
      rados -p $poolname put obj$i /etc/hostname || return 1
    done

    ceph osd unset noup || return 1
    wait_for_clean || return 1
    flush_pg_stats || return 1

    # Setup validity: this must have been a pure Recovering episode, not a
    # backfill -- otherwise this test isn't independently confirming what
    # it claims to (that would just be redundant with the backfilling test
    # below).
    grep -q "enter Started/Primary/Active/Recovering" $log || {
      echo "FAIL: ${PG} never entered Recovering -- test setup assumption" \
           "broken, this isn't testing the Recovering-path arm hook"
      return 1
    }
    grep -q "enter Started/Primary/Active/Backfilling" $log && {
      echo "FAIL: ${PG} entered Backfilling as well as Recovering -- the" \
           "gap wasn't as log-continuous as this test assumes, so it isn't" \
           "cleanly isolating the Recovering-only path"
      return 1
    }

    local dump
    dump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) \
           perf dump) || return 1

    local vuln_avgcount vuln_sum rebuild_avgcount rebuild_sum
    vuln_avgcount=$(jq '.recoverystate_perf.pg_vulnerability_duration.avgcount' <<< "$dump")
    vuln_sum=$(jq '.recoverystate_perf.pg_vulnerability_duration.sum' <<< "$dump")
    rebuild_avgcount=$(jq '.recoverystate_perf.pg_rebuild_duration.avgcount' <<< "$dump")
    rebuild_sum=$(jq '.recoverystate_perf.pg_rebuild_duration.sum' <<< "$dump")
    echo "INFO: (Recovering case) pg_vulnerability_duration avgcount=${vuln_avgcount}" \
         "sum=${vuln_sum}s, pg_rebuild_duration avgcount=${rebuild_avgcount} sum=${rebuild_sum}s"

    test "$vuln_avgcount" -ge 1 || {
      echo "FAIL: expected pg_vulnerability_duration.avgcount>=1, got $vuln_avgcount"
      return 1
    }
    test "$rebuild_avgcount" -ge 1 || {
      echo "FAIL: expected pg_rebuild_duration.avgcount>=1 for a genuine" \
           "Recovering episode, got $rebuild_avgcount"
      return 1
    }

    # assert the actual structural invariant:
    # pg_rebuild_duration is always a bounded, non-negative subset of
    # pg_vulnerability_duration's span (it can only arm after the
    # vulnerability window is already open, and closes no later than the
    # PG reaches clean), and it must be genuinely nonzero for a real
    # recovery episode.
    test "$(awk -v b="$rebuild_sum" 'BEGIN { print (b > 0) }')" = 1 || {
      echo "FAIL: expected pg_rebuild_duration.sum>0 for a genuine" \
           "Recovering episode, got ${rebuild_sum}s"
      return 1
    }
    test "$(awk -v a="$vuln_sum" -v b="$rebuild_sum" 'BEGIN { print (b <= a) }')" = 1 || {
      echo "FAIL: pg_rebuild_duration.sum (${rebuild_sum}s) exceeds" \
           "pg_vulnerability_duration.sum (${vuln_sum}s) -- the active-" \
           "rebuild span should never be longer than the vulnerability" \
           "window it's a subset of"
      return 1
    }

    delete_pool $poolname
    kill_daemons $dir || return 1
}

function TEST_rebuild_perf_backfilling_case() {
    local dir=$1
    local OSDS=4

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      # Deliberately small osd_min/max_pg_log_entries, same technique
      # qa/standalone/osd/divergent-priors.sh already uses and
      # verification_plan.md's Scenario 4 needed on real hardware: at the
      # default (10000), forcing a genuine full backfill instead of a log
      # catch-up would need an impractical write volume. Passed per-OSD as
      # trailing run_osd args (same mechanism already proven for
      # --debug-osd/--osd-mclock-skip-benchmark just above), not via
      # CEPH_ARGS -- deliberately avoids any risk of a global CEPH_ARGS
      # mutation leaking into other tests in this same file/run.
      run_osd $dir $osd --osd-mclock-skip-benchmark=true --debug-osd=15 \
        --osd_min_pg_log_entries=50 --osd_max_pg_log_entries=100 \
        --osd_pg_log_trim_min=10 \
        --osd_async_recovery_min_cost=1000000 || return 1
    done

    create_pool $poolname 1 1 replicated || return 1
    ceph osd pool set $poolname size 3 || return 1
    ceph osd pool set $poolname min_size 2 || return 1
    wait_for_clean || return 1

    for i in $(seq 1 5)
    do
      rados -p $poolname put obj$i /etc/hostname || return 1
    done
    wait_for_clean || return 1

    local primary
    primary=$(get_primary $poolname obj1)
    local PG
    PG=$(get_pg $poolname obj1)
    local otherosd
    otherosd=$(get_not_primary $poolname obj1)
    local log=$dir/osd.${primary}.log

    # Hold the replica down, with a pile of new objects, then mark it
    # out. Keep otherosd down but not yet out for the ENTIRE write loop,
    # so the PG runs the whole time on the reduced 2-member acting set
    # and genuinely accumulates a 145-150 object backlog neither
    # surviving member has a head start on. Only mark it `out` (never
    # bring it back `in`) once the backlog is fully written, so the
    # spare 4th OSD (blank slate) is pulled in with a large enough
    # missing-object count to force a genuine backfill scan
    ceph osd set noup || return 1
    ceph osd down osd.${otherosd} || return 1

    for i in $(seq 6 150)
    do
      rados -p $poolname put obj$i /etc/hostname || return 1
    done

    ceph osd out osd.${otherosd} || return 1
    ceph osd unset noup || return 1
    wait_for_clean || return 1
    flush_pg_stats || return 1

    # Setup validity: confirm this genuinely took the Backfilling path --
    # if not, neither the out-driven CRUSH remap nor the write volume
    # above forced it, and this isn't testing the Backfilling-path arm
    # hook.
    grep -q "enter Started/Primary/Active/Backfilling" $log || {
      echo "FAIL: ${PG} never entered Backfilling -- test setup assumption" \
           "broken (neither the out-driven remap nor the write volume" \
           "forced a genuine backfill), this isn't testing the" \
           "Backfilling-path arm hook"
      return 1
    }

    local dump
    dump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) \
           perf dump) || return 1

    local vuln_avgcount vuln_sum rebuild_avgcount rebuild_sum
    vuln_avgcount=$(jq '.recoverystate_perf.pg_vulnerability_duration.avgcount' <<< "$dump")
    vuln_sum=$(jq '.recoverystate_perf.pg_vulnerability_duration.sum' <<< "$dump")
    rebuild_avgcount=$(jq '.recoverystate_perf.pg_rebuild_duration.avgcount' <<< "$dump")
    rebuild_sum=$(jq '.recoverystate_perf.pg_rebuild_duration.sum' <<< "$dump")
    echo "INFO: (Backfilling case) pg_vulnerability_duration avgcount=${vuln_avgcount}" \
         "sum=${vuln_sum}s, pg_rebuild_duration avgcount=${rebuild_avgcount} sum=${rebuild_sum}s"

    test "$vuln_avgcount" -ge 1 || {
      echo "FAIL: expected pg_vulnerability_duration.avgcount>=1, got $vuln_avgcount"
      return 1
    }
    test "$rebuild_avgcount" -ge 1 || {
      echo "FAIL: expected pg_rebuild_duration.avgcount>=1 for a genuine" \
           "Backfilling episode, got $rebuild_avgcount"
      return 1
    }

    # assert the actual invariant: pg_rebuild_duration is always a bounded,
    # non-negative subset of pg_vulnerability_duration's span, and must be
    # genuinely nonzero for a real backfill episode.
    test "$(awk -v b="$rebuild_sum" 'BEGIN { print (b > 0) }')" = 1 || {
      echo "FAIL: expected pg_rebuild_duration.sum>0 for a genuine" \
           "Backfilling episode, got ${rebuild_sum}s"
      return 1
    }
    test "$(awk -v a="$vuln_sum" -v b="$rebuild_sum" 'BEGIN { print (b <= a) }')" = 1 || {
      echo "FAIL: pg_rebuild_duration.sum (${rebuild_sum}s) exceeds" \
           "pg_vulnerability_duration.sum (${vuln_sum}s) -- the active-" \
           "rebuild span should never be longer than the vulnerability" \
           "window it's a subset of"
      return 1
    }

    delete_pool $poolname
    kill_daemons $dir || return 1
}

# Small helpers reused verbatim from
# qa/standalone/osd-backfill/osd-backfill-space.sh's TEST_backfill_test_simple
# (not otherwise available in this file) -- needed below because a PG stuck
# backfill_toofull will never reach active+clean, so plain wait_for_clean
# would hang; these instead wait for "no PG is actively backfilling/
# activating right now", which a toofull-stuck PG already satisfies.
function get_num_in_state() {
    local state=$1
    local expression
    expression+="select(contains(\"${state}\"))"
    ceph --format json pg dump pgs 2>/dev/null | \
        jq ".pg_stats | [.[] | .state | $expression] | length"
}

function wait_for_not_state() {
    local state=$1
    local num_in_state=-1
    local cur_in_state
    local -a delays=($(get_timeout_delays $2 5))
    local -i loop=0

    flush_pg_stats || return 1
    while test $(get_num_pgs) == 0 ; do
	sleep 1
    done

    while true ; do
        cur_in_state=$(get_num_in_state ${state})
        test $cur_in_state = "0" && break
        if test $cur_in_state != $num_in_state ; then
            loop=0
            num_in_state=$cur_in_state
        elif (( $loop >= ${#delays[*]} )) ; then
            ceph pg dump pgs
            return 1
        fi
        sleep ${delays[$loop]}
        loop+=1
    done
    return 0
}

function wait_for_not_backfilling() {
    local timeout=$1
    wait_for_not_state backfilling $timeout
}

function wait_for_not_activating() {
    local timeout=$1
    wait_for_not_state activating $timeout
}

# Two pools contend for a shared target's backfill reservation slot;
# one is rejected at grant time (RemoteReservationRejectedTooFull)
# before Backfilling starts. Verifies rebuild_active_start stays
# unarmed through the reject/retry loop while pg_vulnerability_duration
# is already open, then arms once the ratio relaxes.
function TEST_rebuild_perf_backfill_toofull_pause_case() {
    local dir=$1
    local OSDS=3
    local pools=2
    local poolbase=rebuildperftoofull

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      # fake_statfs_for_testing is sized (5300000) for this test's own
      # two-pool topology: with two size-2 replicated pools sharing only
      # 3 OSDs, at least one OSD is always shared between their final acting
      # sets, and that OSD must hold up to one full copy of EACH pool's
      # ~2.46MB data (600 4K objects) for a total data usage of (~4.92MB
      # across the 2 pools. 5300000 (~5.3MB) puts that worst case at ~93%:
      # above the initial .85 ratio (so the toofull contention still
      # triggers) but below the relaxed .99 ratio (so the episode can
      # still complete), with a margin for real bluestore overhead.
      run_osd $dir $osd --osd-mclock-skip-benchmark=true --debug-osd=15 \
        --fake_statfs_for_testing=5300000 --osd_min_pg_log_entries=5 \
        --osd_max_pg_log_entries=10 --osd_max_backfills=10 || return 1
    done

    ceph osd set-backfillfull-ratio .85 || return 1

    for p in $(seq 1 $pools)
    do
      create_pool "${poolbase}${p}" 1 1 replicated || return 1
      ceph osd pool set "${poolbase}${p}" size 1 --yes-i-really-mean-it || return 1
    done
    wait_for_clean || return 1

    # Baseline pg_vulnerability_duration.avgcount and pg_rebuild_duration.
    # avgcount per OSD, captured before either pool is ever degraded --
    # both latches only record on close, so this is safe regardless of
    # which pool ends up toofull. pg_rebuild_duration stays at this
    # baseline until the toofull pool's reservation is actually granted,
    # since its arm hook never runs while the reservation keeps getting
    # rejected.
    local -a vuln_baseline rebuild_baseline
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      local vbdump
      vbdump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${osd}) \
        perf dump) || return 1
      vuln_baseline[$osd]=$(jq '.recoverystate_perf.pg_vulnerability_duration.avgcount' <<< "$vbdump")
      rebuild_baseline[$osd]=$(jq '.recoverystate_perf.pg_rebuild_duration.avgcount' <<< "$vbdump")
    done

    # rados bench, not 600 individual `rados put` invocations per pool --
    # functionally identical (600 real 4KB objects landing in each pool)
    # but 2 CLI invocations total instead of 1200. `obj1` is written
    # explicitly first and separately since get_pg/get_primary below
    # need a known, specific object name to identify the PG/primary --
    # bench then fills in the other 599 real objects per pool.
    dd if=/dev/urandom of=$dir/datafile bs=1024 count=4 2>/dev/null
    for p in $(seq 1 $pools)
    do
      rados -p "${poolbase}${p}" put obj1 $dir/datafile || return 1
      timeout 150 rados -p "${poolbase}${p}" bench 120 write -b 4096 \
        --max-objects 599 --no-cleanup || return 1
    done

    # Bump both pools to size 2 at once, then sleep 30 before polling --
    # gives the reservation/backfill machinery a real settling window
    # rather than relying purely on wait_for_not_backfilling's own
    # polling. Can only fail to reproduce the toofull condition if
    # pool1's and pool2's primary+only OSD happen to coincide (no
    # overlap to contend over) -- the same small risk
    # TEST_backfill_test_simple itself carries.
    for p in $(seq 1 $pools)
    do
      ceph osd pool set "${poolbase}${p}" size 2 || return 1
    done
    sleep 30

    wait_for_not_backfilling 1200 || return 1
    wait_for_not_activating 60 || return 1

    local toofull_count
    toofull_count=$(ceph pg dump pgs | grep -c backfill_toofull)
    test "$toofull_count" = "1" || {
      echo "FAIL: expected exactly 1 pool stuck backfill_toofull, got" \
           "$toofull_count -- test setup assumption broken (see comment" \
           "above this function)"
      return 1
    }

    # Identify which of our 2 known pools is the stuck one by checking each
    # one's own PG state directly -- deliberately not by extracting the
    # numeric pool ID from the toofull pgid and reconstructing a pool name
    # from it (${poolbase}<id>), since pool IDs are cluster-assigned and
    # not guaranteed to start at 1 (e.g. a .mgr pool can already hold id 1),
    # so that reconstruction is not reliable.
    local toofull_pool=""
    local PG=""
    for p in $(seq 1 $pools)
    do
      local pname="${poolbase}${p}"
      local pgid
      pgid=$(get_pg $pname obj1)
      if ceph pg dump pgs --format=json 2>/dev/null | \
           jq -e --arg pgid "$pgid" \
             '.pg_stats[] | select(.pgid==$pgid) | select(.state | contains("backfill_toofull"))' \
           > /dev/null; then
        toofull_pool=$pname
        PG=$pgid
        break
      fi
    done
    test -n "$toofull_pool" || {
      echo "FAIL: couldn't identify which of the 2 pools' PGs is" \
           "backfill_toofull"
      return 1
    }
    local primary
    primary=$(get_primary $toofull_pool obj1)
    local log=$dir/osd.${primary}.log

    flush_pg_stats || return 1
    # "Bump both pools at once" makes the toofull pool's replica lose
    # the reservation race at grant time: WaitLocalBackfillReserved ->
    # WaitRemoteBackfillReserved -> RemoteReservationRejectedTooFull ->
    # NotBackfilling, with the rejection firing before Backfilling is
    # ever entered. The PG keeps retrying every osd_backfill_retry_interval
    # until the ratio is relaxed below, so Backfilling is never entered until
    # then -- hence the assertion here is that it has NOT been entered yet.
    grep -q "enter Started/Primary/Active/Backfilling" $log && {
      echo "FAIL: ${PG} already shows a Backfilling entry before the" \
           "ratio was relaxed -- expected the reservation to be rejected" \
           "at grant time (WaitRemoteBackfillReserved), never reaching" \
           "Backfilling itself; test's toofull-reproduction assumption" \
           "may have changed"
      return 1
    }
    grep -q "enter Started/Primary/Active/NotBackfilling" $log || {
      echo "FAIL: ${PG} never closed via NotBackfilling after going" \
           "toofull -- the RemoteReservationRejectedTooFull -> NotBackfilling" \
           "close path never fired"
      return 1
    }

    # Confirm that the pg_vulnerability_duration window is still
    # open here: is_vulnerable keys off degraded/undersized/misplaced,
    # none of which clear just because backfill itself is paused -- unlike
    # pg_rebuild_duration, this window cannot have closed yet.
    ceph pg dump pgs --format=json 2>/dev/null | \
      jq -e --arg pgid "$PG" \
        '.pg_stats[] | select(.pgid==$pgid) | select(.state | test("degraded|undersized"))' \
      > /dev/null || {
      echo "FAIL: ${PG} is not degraded/undersized while toofull-paused --" \
           "the vulnerability window should structurally still be open" \
           "at this point"
      return 1
    }

    # The reservation was rejected, not revoked, so pg_rebuild_duration's
    # arm hook (Backfilling::Backfilling()) has never run for this
    # episode -- unlike pg_vulnerability_duration's window, which opens
    # independently of whether real backfill work has started.
    # Informational only, not asserted
    local dump rebuild_avgcount_before
    dump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) \
           perf dump) || return 1
    rebuild_avgcount_before=$(jq '.recoverystate_perf.pg_rebuild_duration.avgcount' <<< "$dump")
    echo "INFO: (toofull-pause case) while toofull-rejected," \
         "pg_rebuild_duration.avgcount=${rebuild_avgcount_before}" \
         "(baseline=${rebuild_baseline[$primary]}, osd-wide)"

    # Relax the ratio so the self-scheduled retry (osd_backfill_retry_interval
    # after RemoteReservationRejectedTooFull) finally gets its reservation
    # granted, enters Backfilling for the first time, and completes -- arming
    # and closing the rebuild-duration latch exactly once for this episode.
    ceph osd set-backfillfull-ratio 0.99 || return 1
    wait_for_clean || return 1
    flush_pg_stats || return 1

    local backfilling_count rebuild_avgcount_after
    backfilling_count=$(grep -c "enter Started/Primary/Active/Backfilling" $log)
    dump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) \
           perf dump) || return 1
    rebuild_avgcount_after=$(jq '.recoverystate_perf.pg_rebuild_duration.avgcount' <<< "$dump")
    echo "INFO: (toofull-pause case) Backfilling entries=${backfilling_count}," \
         "pg_rebuild_duration.avgcount=${rebuild_avgcount_after} (osd-wide)"

    test "$backfilling_count" -ge 1 || {
      echo "FAIL: expected >=1 Backfilling entry for ${PG} once the" \
           "reservation was finally granted after relaxing the ratio," \
           "got $backfilling_count"
      return 1
    }
    test "$rebuild_avgcount_after" -ge "$(expr $rebuild_avgcount_before + 1)" || {
      echo "FAIL: expected pg_rebuild_duration.avgcount to grow by at" \
           "least 1 once the toofull-rejected episode's reservation was" \
           "finally granted and completed" \
           "($rebuild_avgcount_before -> $rebuild_avgcount_after)"
      return 1
    }

    local vuln_avgcount_after
    dump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) \
           perf dump) || return 1
    vuln_avgcount_after=$(jq '.recoverystate_perf.pg_vulnerability_duration.avgcount' <<< "$dump")
    echo "INFO: (toofull-pause case) pg_vulnerability_duration.avgcount" \
         "baseline=${vuln_baseline[$primary]} after=${vuln_avgcount_after}" \
         "(osd.${primary}, osd-wide)"
    test "$vuln_avgcount_after" -ge "$(expr ${vuln_baseline[$primary]} + 1)" || {
      echo "FAIL: expected pg_vulnerability_duration.avgcount to grow by" \
           "at least 1 once ${PG}'s single continuous vulnerability window" \
           "(spanning the toofull pause) finally closed" \
           "(${vuln_baseline[$primary]} -> $vuln_avgcount_after)"
      return 1
    }

    for p in $(seq 1 $pools)
    do
      delete_pool "${poolbase}${p}"
    done
    kill_daemons $dir || return 1
}

# Companion to TEST_rebuild_perf_backfill_toofull_pause_case, exercising
# RemoteReservationRevokedTooFull (an already-granted reservation revoked
# mid-flight) rather than that test's RemoteReservationRejectedTooFull
# (rejected before ever being granted)
#
# The backfill pool's target replica is taken down mid-write (kept
# down, never marked out) so it falls behind by a large, log-trimmed
# backlog, then rejoins with some pre-existing data and a genuinely
# bounded (osd_backfill_scan_min/max) multi-round scan for the rest --
# a brand-new, fully empty replica's digest scan completes in a single
# round trip regardless of scan_min/max, which leaves no later scan
# call for BackfillTooFull to ever fire from. A separate, already
# fully-replicated filler pool provides the toofull-crossing bytes in
# two script-timed write rounds: round 1 lands before the reservation
# is requested (so it grants), round 2 lands only once Backfilling is
# confirmed entered (so it revokes an already-granted reservation) --
# producing two Backfilling entries and two recorded
# pg_rebuild_duration samples.
function TEST_rebuild_perf_backfill_toofull_revoke_case() {
    local dir=$1
    local OSDS=3
    local poolbase=rebuildperfrevoke
    local fillerpool=rebuildperfrevokefiller
    # 15MB fake total space -- sized with roughly 5-6% margin at both the
    # .85 (crossing) and .99 (final-completion) thresholds to absorb real
    # bluestore metadata/onode overhead (see the "backfillfull-ratio"
    # skill section) without being so tight a small variance flips the
    # outcome either way.
    local fakespace=15000000

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      # osd_min/max_pg_log_entries + osd_pg_log_trim_min force the
      # down-then-rejoining replica's log to trim past its own last
      # update, so it's classified for genuine backfill (not log-based
      # recovery) on rejoin. osd_async_recovery_min_cost raised well
      # above the write volume so its missing-object count doesn't
      # divert it into async recovery instead.
      #
      # For this test, the custom mClock profile is employed to
      # hard-cap the background_recovery class to a small fraction of a
      # fixed, known capacity instead, leaving the client class (the
      # filler pool's writes below) unconstrained -- stretching the
      # backfill's own duration so there's a real window for filler
      # round 2 and osd_heartbeat_interval to land mid-flight.
      # background_best_effort is deliberately left at its
      # own unconstrained default (res=0/lim=0) rather than also
      # throttled.      #
      #
      # osd_op_num_shards/osd_op_num_threads_per_shard pinned explicitly
      # (rather than left to the hdd/ssd-detected defaults) -- pinning
      # removes that dependency on whatever media type gets detected.
      run_osd $dir $osd --osd-mclock-skip-benchmark=true --debug-osd=15 \
        --fake_statfs_for_testing=$fakespace \
        --osd_min_pg_log_entries=5 --osd_max_pg_log_entries=10 \
        --osd_pg_log_trim_min=10 \
        --osd_async_recovery_min_cost=1000000 \
        --osd_max_backfills=10 \
        --osd_backfill_scan_min=8 --osd_backfill_scan_max=16 \
        --osd_op_num_shards=8 --osd_op_num_threads_per_shard=2 \
        --osd_mclock_profile=custom \
        --osd_mclock_max_capacity_iops_hdd=1000 \
        --osd_mclock_max_capacity_iops_ssd=1000 \
        --osd_mclock_max_sequential_bandwidth_hdd=4096000 \
        --osd_mclock_max_sequential_bandwidth_ssd=4096000 \
        --osd_mclock_scheduler_background_recovery_res=0.015 \
        --osd_mclock_scheduler_background_recovery_lim=0.03 || return 1
    done

    ceph osd set-backfillfull-ratio .85 || return 1

    # Filler pool: created directly at size=$OSDS (all 3 OSDs, guaranteed
    # to include whichever OSD ends up as the backfill pool's new
    # replica/target below, without needing to predict CRUSH's choice) --
    # never resized, so it never goes degraded/undersized and never
    # touches recoverystate_perf itself. Its writes are ordinary client
    # I/O the whole time, giving this script full, deterministic control
    # over exactly when the target's real on-disk usage grows, instead of
    # depending on a second reservation request's own timing.
    create_pool $fillerpool 1 1 replicated || return 1
    ceph osd pool set $fillerpool size $OSDS || return 1
    wait_for_clean || return 1

    # Backfill pool: starts at size 2 with a small initial write (both
    # members fully in sync), giving the "down one member" step below a
    # real starting point to fall behind from.
    create_pool ${poolbase} 1 1 replicated || return 1
    ceph osd pool set ${poolbase} size 2 || return 1
    wait_for_clean || return 1

    dd if=/dev/urandom of=$dir/datafile bs=1024 count=4 2>/dev/null
    rados -p ${poolbase} put obj1 $dir/datafile || return 1
    for i in $(seq 2 30)
    do
      rados -p ${poolbase} put obj$i $dir/datafile || return 1
    done
    wait_for_clean || return 1

    local PG primary otherosd log
    PG=$(get_pg ${poolbase} obj1)
    primary=$(get_primary ${poolbase} obj1)
    otherosd=$(get_not_primary ${poolbase} obj1)
    log=$dir/osd.${primary}.log

    # Baselines, same rationale as TEST_rebuild_perf_backfill_toofull_pause_case
    # -- captured before the backfill pool is ever degraded. Unlike that
    # test, there's no same-primary-coincidence confound to worry about
    # here: the filler pool never goes degraded on its own, so it can
    # never itself contribute to either counter.
    local -a vuln_baseline rebuild_baseline
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      local vbdump
      vbdump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${osd}) \
        perf dump) || return 1
      vuln_baseline[$osd]=$(jq '.recoverystate_perf.pg_vulnerability_duration.avgcount' <<< "$vbdump")
      rebuild_baseline[$osd]=$(jq '.recoverystate_perf.pg_rebuild_duration.avgcount' <<< "$vbdump")
    done

    # Filler round 1 (~1700 objects, ~6.96MB): lands now, while otherosd
    # is still up, so it reaches all 3 OSDs (including otherosd) as
    # ordinary fast client I/O -- well under .85*15MB=12.75MB. Landing
    # this BEFORE otherosd goes down matters: otherosd is filler's own
    # primary too (a single-PG pool spanning all 3 OSDs), so if this
    # write happened while otherosd were down, otherosd would ALSO need
    # to backfill this data in on rejoin, through the exact same
    # throttled background_recovery class as the backfill pool's own
    # episode below -- starving the crossing this test depends on
    # instead of producing it.
    timeout 150 rados -p $fillerpool bench 120 write -b 4096 \
      --max-objects 1700 --no-cleanup || return 1
    wait_for_clean || return 1

    # Hold otherosd down (excluded from acting, never marked out) while
    # writing the rest of the backlog, so it falls behind by a large
    # enough margin to need genuine backfill, not a log catch-up, once
    # it rejoins -- same down/write-more ordering
    # TEST_rebuild_perf_backfilling_case's own proven recipe uses.
    ceph osd set noup || return 1
    ceph osd down osd.${otherosd} || return 1

    for i in $(seq 31 150)
    do
      rados -p ${poolbase} put obj$i $dir/datafile || return 1
    done

    # otherosd rejoins now that noup is cleared. Its filler-pool copy is
    # already complete (round 1 landed before it went down, and nothing
    # wrote to the filler pool while it was down), so only the backfill
    # pool's own reservation is requested here, for the ~120 objects
    # actually missing.
    ceph osd unset noup || return 1

    # Poll (bounded) for a genuine Backfilling entry, confirming the
    # reservation was actually GRANTED -- not rejected outright the way
    # TEST_rebuild_perf_backfill_toofull_pause_case's setup produces --
    # before round 2 lands.
    local entered=0
    for i in $(seq 1 30)
    do
      grep -q "enter Started/Primary/Active/Backfilling" $log && {
        entered=1
        break
      }
      sleep 1
    done
    test "$entered" = 1 || {
      echo "FAIL: ${PG} never entered Backfilling -- either its" \
           "reservation was rejected outright instead of granted (filler" \
           "round 1 may be sized too close to the .85 ratio already), or" \
           "otherosd's rejoin was classified as log-based recovery" \
           "instead of backfill (see osd_pg_log_trim_min/" \
           "osd_async_recovery_min_cost above)"
      return 1
    }

    # Filler round 2 (~1600 objects, ~6.55MB): pushes real target usage to
    # ~13.5MB alone (before this PG's own backfill contributes anything),
    # decisively past .85*15MB=12.75MB -- deliberately timed to land only
    # after Backfilling was confirmed entered above.
    timeout 150 rados -p $fillerpool bench 120 write -b 4096 \
      --max-objects 1600 --no-cleanup || return 1

    # Poll (not a blind sleep) for the close, since the exact timing
    # between the mclock recovery/best-effort throttling above,
    # osd_backfill_scan_max, and osd_heartbeat_interval isn't fully
    # deterministic.
    local closed=0
    for i in $(seq 1 100)
    do
      grep -q "enter Started/Primary/Active/NotBackfilling" $log && {
        closed=1
        break
      }
      sleep 3
    done
    test "$closed" = 1 || {
      echo "FAIL: ${PG} never closed via NotBackfilling within the poll" \
           "window -- either the backfill pool finished on its own before" \
           "filler round 2's crossing was detected (the recovery/best-" \
           "effort mclock caps may need to be lower, or the object count" \
           "higher, to widen the window), or round 2 didn't push the" \
           "target far enough past the ratio; see this function's own" \
           "header comment"
      return 1
    }
    grep -q "RemoteReservationRevokedTooFull" $log || {
      echo "FAIL: ${PG} closed via NotBackfilling but not via" \
           "RemoteReservationRevokedTooFull specifically -- got a" \
           "different close reason than this test is designed to exercise"
      return 1
    }

    # Structural proxy for "the pg_vulnerability_duration window is still
    # open here" -- same rationale as the toofull_pause_case sibling test.
    ceph pg dump pgs --format=json 2>/dev/null | \
      jq -e --arg pgid "$PG" \
        '.pg_stats[] | select(.pgid==$pgid) | select(.state | test("degraded|undersized"))' \
      > /dev/null || {
      echo "FAIL: ${PG} is not degraded/undersized while toofull-paused --" \
           "the vulnerability window should structurally still be open" \
           "at this point"
      return 1
    }

    # Unlike the toofull_pause_case sibling (reject path -- latch never
    # armed at all), this IS the revoke path: Backfilling was genuinely
    # entered and then closed via NotBackfilling, so exactly one
    # (truncated) sample should already be recorded here -- no same-
    # primary confound to soften this into an informational-only check
    # (see this function's own baseline-capture comment above).
    local dump rebuild_avgcount_before
    dump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) \
           perf dump) || return 1
    rebuild_avgcount_before=$(jq '.recoverystate_perf.pg_rebuild_duration.avgcount' <<< "$dump")
    echo "INFO: (toofull-revoke case) after the first truncated span," \
         "pg_rebuild_duration.avgcount=${rebuild_avgcount_before}" \
         "(baseline=${rebuild_baseline[$primary]}, osd-wide)"
    test "$rebuild_avgcount_before" -ge "$(expr ${rebuild_baseline[$primary]} + 1)" || {
      echo "FAIL: expected pg_rebuild_duration.avgcount to have grown by" \
           "at least 1 already, for the truncated pre-revoke span" \
           "(baseline ${rebuild_baseline[$primary]} -> $rebuild_avgcount_before)"
      return 1
    }

    # Relax the ratio so the self-scheduled retry (osd_backfill_retry_interval
    # after RemoteReservationRevokedTooFull, see Backfilling::react() in
    # PeeringState.cc) succeeds once it fires, re-arming and recording a
    # SECOND, separate sample once this episode finally completes.
    ceph osd set-backfillfull-ratio 0.99 || return 1
    wait_for_clean || return 1
    flush_pg_stats || return 1

    local backfilling_count rebuild_avgcount_after
    backfilling_count=$(grep -c "enter Started/Primary/Active/Backfilling" $log)
    dump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) \
           perf dump) || return 1
    rebuild_avgcount_after=$(jq '.recoverystate_perf.pg_rebuild_duration.avgcount' <<< "$dump")
    echo "INFO: (toofull-revoke case) Backfilling entries=${backfilling_count}," \
         "pg_rebuild_duration.avgcount=${rebuild_avgcount_after} (osd-wide)"

    test "$backfilling_count" -ge 2 || {
      echo "FAIL: expected >=2 Backfilling entries for ${PG} (once before" \
           "the revoke, once after resuming), got $backfilling_count"
      return 1
    }
    test "$rebuild_avgcount_after" -ge "$(expr $rebuild_avgcount_before + 1)" || {
      echo "FAIL: expected pg_rebuild_duration.avgcount to grow by at" \
           "least 1 more after the revoked episode resumed and completed" \
           "($rebuild_avgcount_before -> $rebuild_avgcount_after)"
      return 1
    }

    local vuln_avgcount_after
    dump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) \
           perf dump) || return 1
    vuln_avgcount_after=$(jq '.recoverystate_perf.pg_vulnerability_duration.avgcount' <<< "$dump")
    echo "INFO: (toofull-revoke case) pg_vulnerability_duration.avgcount" \
         "baseline=${vuln_baseline[$primary]} after=${vuln_avgcount_after}" \
         "(osd.${primary}, osd-wide)"
    test "$vuln_avgcount_after" -ge "$(expr ${vuln_baseline[$primary]} + 1)" || {
      echo "FAIL: expected pg_vulnerability_duration.avgcount to grow by" \
           "at least 1 once ${PG}'s single continuous vulnerability window" \
           "(spanning both the pre-revoke and post-resume backfill spans)" \
           "finally closed (${vuln_baseline[$primary]} -> $vuln_avgcount_after)"
      return 1
    }

    delete_pool ${poolbase}
    delete_pool $fillerpool
    kill_daemons $dir || return 1
}

# Another small helper reused verbatim from
# qa/standalone/osd/osd-rep-recov-eio.sh (not otherwise available in this
# file) -- polls a PG's state string.
function get_state() {
    local pgid=$1
    local sname=state
    ceph --format json pg dump pgs 2>/dev/null | \
        jq -r ".pg_stats | .[] | select(.pgid==\"$pgid\") | .$sname"
}

# Unfound-family case: an object EIO-poisoned on both of its 2 surviving
# replicas while the 3rd (down) OSD rejoins and recovers everything else,
# so the PG runs out of any way to satisfy that one object and posts
# UnfoundRecovery -> NotRecovering (closes the latch mid-progress, with
# ~99 other objects' worth of real work already done), and stays there.
function TEST_rebuild_perf_recovery_unfound_case() {
    local dir=$1
    local lastobj=100
    local testobj=obj75

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 2)
    do
      run_osd $dir $osd --osd-mclock-skip-benchmark=true --debug-osd=15 || return 1
    done

    create_pool $poolname 1 1 replicated || return 1
    ceph osd pool set $poolname size 3 || return 1
    ceph osd pool set $poolname min_size 2 || return 1
    wait_for_clean || return 1

    rados -p $poolname put myobject /etc/hostname || return 1

    local -a initial_osds=($(get_osds $poolname myobject))
    local last_osd=${initial_osds[-1]}
    local primary
    primary=$(get_primary $poolname myobject)
    local PG
    PG=$(get_pg $poolname myobject)
    local log=$dir/osd.${primary}.log

    # Baseline pg_vulnerability_duration.avgcount for the primary, captured
    # before last_osd is killed and the PG ever becomes degraded.
    local vuln_baseline vbdump
    vbdump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) \
      perf dump) || return 1
    vuln_baseline=$(jq '.recoverystate_perf.pg_vulnerability_duration.avgcount' <<< "$vbdump")

    kill_daemons $dir TERM osd.${last_osd} 2>&2 < /dev/null || return 1
    ceph osd down ${last_osd} || return 1
    ceph osd out ${last_osd} || return 1

    dd if=/dev/urandom of=${dir}/ORIGINAL bs=1024 count=4
    for i in $(seq 1 $lastobj)
    do
      rados --pool $poolname put obj${i} $dir/ORIGINAL || return 1
    done

    # Poison the object on BOTH surviving replicas -- once last_osd rejoins
    # and needs this one object recovered, there is no good copy anywhere.
    inject_eio rep data $poolname $testobj $dir 0 || return 1
    inject_eio rep data $poolname $testobj $dir 1 || return 1

    activate_osd $dir ${last_osd} || return 1
    ceph osd in ${last_osd} || return 1

    sleep 15

    for tmp in $(seq 1 100); do
      state=$(get_state ${PG})
      echo $state | grep -v recovering
      if [ "$?" = "0" ]; then
        break
      fi
      echo "$state "
      sleep 1
    done

    ceph pg dump pgs
    ceph pg ${PG} list_unfound | grep -q $testobj || return 1

    # Command should hang because the object is genuinely unfound.
    timeout 5 rados -p $poolname get $testobj $dir/CHECK
    test $? = "124" || return 1

    flush_pg_stats || return 1
    grep -q "enter Started/Primary/Active/Recovering" $log || {
      echo "FAIL: ${PG} never entered Recovering -- test setup assumption" \
           "broken, this isn't testing the arm hook"
      return 1
    }
    grep -q "enter Started/Primary/Active/NotRecovering" $log || {
      echo "FAIL: ${PG} never closed via NotRecovering after going" \
           "unfound -- the UnfoundRecovery -> NotRecovering close path" \
           "never fired"
      return 1
    }

    # Confirm that the pg_vulnerability_duration window is still open here.
    # The unfound object is still genuinely missing (num_objects_degraded>0),
    # so is_vulnerable must still be true even though recovery itself has
    # stalled with nothing self-scheduling a retry.
    ceph pg dump pgs --format=json 2>/dev/null | \
      jq -e --arg pgid "$PG" \
        '.pg_stats[] | select(.pgid==$pgid) | select(.state | test("degraded"))' \
      > /dev/null || {
      echo "FAIL: ${PG} is not degraded while unfound-stalled -- the" \
           "vulnerability window should structurally still be open" \
           "at this point"
      return 1
    }

    local dump rebuild_avgcount_before
    dump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) \
           perf dump) || return 1
    rebuild_avgcount_before=$(jq '.recoverystate_perf.pg_rebuild_duration.avgcount' <<< "$dump")
    echo "INFO: (unfound case) after going unfound," \
         "pg_rebuild_duration.avgcount=${rebuild_avgcount_before} (osd-wide)"
    test "$rebuild_avgcount_before" -ge 1 || {
      echo "FAIL: expected pg_rebuild_duration.avgcount>=1 already recorded" \
           "for the ~$(expr $lastobj - 1) objects genuinely recovered" \
           "before the unfound object blocked further progress, got" \
           "$rebuild_avgcount_before"
      return 1
    }

    # Trigger a retry by issuing mark_unfound_lost which re-posts DoRecovery().
    ceph pg ${PG} mark_unfound_lost delete || return 1
    wait_for_clean || return 1
    flush_pg_stats || return 1

    local recovering_count rebuild_avgcount_after
    recovering_count=$(grep -c "enter Started/Primary/Active/Recovering" $log)
    dump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) \
           perf dump) || return 1
    rebuild_avgcount_after=$(jq '.recoverystate_perf.pg_rebuild_duration.avgcount' <<< "$dump")
    echo "INFO: (unfound case) Recovering entries=${recovering_count}," \
         "pg_rebuild_duration.avgcount=${rebuild_avgcount_after} (osd-wide)"

    test "$recovering_count" -ge 2 || {
      echo "FAIL: expected >=2 Recovering entries for ${PG} (once before" \
           "going unfound, once after mark_unfound_lost re-armed it), got" \
           "$recovering_count"
      return 1
    }
    test "$rebuild_avgcount_after" -ge "$(expr $rebuild_avgcount_before + 1)" || {
      echo "FAIL: expected pg_rebuild_duration.avgcount to grow by at" \
           "least 1 after mark_unfound_lost re-armed and completed the" \
           "episode ($rebuild_avgcount_before -> $rebuild_avgcount_after)"
      return 1
    }

    local vuln_avgcount_after
    dump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) \
           perf dump) || return 1
    vuln_avgcount_after=$(jq '.recoverystate_perf.pg_vulnerability_duration.avgcount' <<< "$dump")
    echo "INFO: (unfound case) pg_vulnerability_duration.avgcount" \
         "baseline=${vuln_baseline} after=${vuln_avgcount_after}" \
         "(osd.${primary}, osd-wide)"
    test "$vuln_avgcount_after" -ge "$(expr $vuln_baseline + 1)" || {
      echo "FAIL: expected pg_vulnerability_duration.avgcount to grow by" \
           "at least 1 once ${PG}'s single continuous vulnerability window" \
           "(spanning the unfound stall) finally closed" \
           "($vuln_baseline -> $vuln_avgcount_after)"
      return 1
    }

    for i in $(seq 1 $lastobj)
    do
      if [ obj${i} = "$testobj" ]; then
        ! rados -p $poolname get $testobj $dir/CHECK || return 1
      else
        rados --pool $poolname get obj${i} $dir/CHECK || return 1
        diff -q $dir/ORIGINAL $dir/CHECK || return 1
      fi
    done

    rm -f ${dir}/ORIGINAL ${dir}/CHECK

    delete_pool $poolname
    kill_daemons $dir || return 1
}

# RemoteReservationRevoked resume case: Of Backfilling::suspend_backfill()'s
# 4 call sites, 3 (Defer/Unfound/TooFull, all covered above) close via
# NotBackfilling; the 4th, Backfilling::react(RemoteReservationRevoked)
# (still needs backfill), instead transits to WaitLocalBackfillReserved and
# must NOT close the latch -- a mid-flight resume, not a genuine pause.
# This test exercises it via genuine priority contention on a backfill TARGET's
# remote_reserver: 2 different pools/PGs both need the SAME target OSD back
# after a down/revive cycle; whichever gets granted first is force-backfill'd
# out of the way by the other, sending MBackfillReserve::REVOKE
# (RepRecovering::react(RemoteBackfillPreempted) on the target) to the
# "victim" PG's primary.
#
# Here the contention is *remote*, on a shared TARGET, so instead both pools
# are simply sized 3 on an exactly-3-OSD cluster: acting is then
# deterministically {0,1,2} for both. Primary (acting[0], a separate per-pgid
# CRUSH decision) is# forced to a distinct OSD per pool via
# `ceph osd pg-upmap-primary` rather than left to CRUSH's natural per-pool hash.
# The OSD taken down/revived to create the remote contention is derived from
# whichever of the 3 OSDs isn't primary for either pool.
function TEST_rebuild_perf_backfill_remote_revoke_resumes_case() {
    local dir=$1
    local OSDS=3

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      # osd_async_recovery_min_cost set well above the 150-object-per-pool
      # write volume so a rejoining replica is classified for genuine
      # synchronous Backfilling (PeeringState::choose_async_recovery_
      # replicated), not diverted into async recovery -- this test's
      # whole premise (RemoteReservationRevoked preempting an already-
      # established Backfilling reservation) requires reaching
      # Backfilling in the first place.
      #
      # osd_pg_log_trim_min lowered below the log excess (150-100=50) so
      # the log genuinely trims past down_osd's last_update on rejoin,
      # landing it on the backfill/RemoteReservationRevoked path instead
      # of ordinary log-based recovery.
      run_osd $dir $osd --osd-mclock-skip-benchmark=true --debug-osd=15 \
        --debug_reserver=20 --osd_max_backfills=1 \
        --osd_min_pg_log_entries=50 --osd_max_pg_log_entries=100 \
        --osd_pg_log_trim_min=10 \
        --osd_async_recovery_min_cost=1000000 || return 1
    done

    local pool1=rebuildperfrevoke1
    local pool2=rebuildperfrevoke2
    create_pool $pool1 1 1 replicated || return 1
    ceph osd pool set $pool1 size 3 || return 1
    ceph osd pool set $pool1 min_size 2 || return 1
    create_pool $pool2 1 1 replicated || return 1
    ceph osd pool set $pool2 size 3 || return 1
    ceph osd pool set $pool2 min_size 2 || return 1

    # Force distinct primaries for the two pools instead of relying on
    # CRUSH's natural per-pool hash: with only 3 OSDs, that hash collides
    # on the same primary for both pools often enough (~1 in 3) that a
    # "coincidence, rerun" guard would make this test genuinely flaky in
    # CI, where there's no one to rerun it. `ceph osd pg-upmap-primary`
    # (a normal, already-up-to-date pgid, no data movement needed since
    # all 3 OSDs are acting members either way) makes the assignment
    # deterministic on every run. It requires min_compat_client >= reef.
    # It also errors if the target is already primary, which just means
    # the desired assignment already held -- not a real failure, so its
    # own exit status isn't checked; the get_primary reads below are the
    # actual verification.
    ceph osd set-require-min-compat-client reef || return 1
    local PG1 PG2 primary1 primary2
    PG1=$(get_pg $pool1 obj1)
    PG2=$(get_pg $pool2 obj1)
    ceph osd pg-upmap-primary $PG1 0
    ceph osd pg-upmap-primary $PG2 1
    wait_for_clean || return 1

    for i in $(seq 1 5)
    do
      rados -p $pool1 put obj$i /etc/hostname || return 1
      rados -p $pool2 put obj$i /etc/hostname || return 1
    done
    wait_for_clean || return 1

    primary1=$(get_primary $pool1 obj1)
    primary2=$(get_primary $pool2 obj1)
    test "$primary1" = 0 || {
      echo "FAIL: ${pool1}'s primary is osd.${primary1}, expected osd.0 --" \
           "pg-upmap-primary didn't take effect as expected"
      return 1
    }
    test "$primary2" = 1 || {
      echo "FAIL: ${pool2}'s primary is osd.${primary2}, expected osd.1 --" \
           "pg-upmap-primary didn't take effect as expected"
      return 1
    }

    # The OSD to take down/revive below must be a member of both pools
    # (guaranteed: size=3=OSDS) but primary for neither -- otherwise
    # marking it down would force a genuine primary handover for that
    # pool instead of a remote-reservation preemption, a different
    # mechanism (the departing primary's own latch is lost or fragmented
    # on handover) that would silently query the wrong daemon afterward
    # and contaminate this test's specific "remote revoke never closes"
    # check. Derived from whichever of the 3 OSDs isn't primary1 or
    # primary2 (always osd.2, given the forced assignment above, but
    # derived rather than hardcoded to keep this self-consistent if the
    # forced primaries above ever change).
    local down_osd=""
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      if [ "$osd" != "$primary1" ] && [ "$osd" != "$primary2" ]; then
        down_osd=$osd
        break
      fi
    done
    test -n "$down_osd" || {
      echo "FAIL: couldn't find an OSD that's a member of both pools but" \
           "primary for neither -- test setup assumption broken"
      return 1
    }

    # Baseline pg_vulnerability_duration.avgcount per OSD, captured before
    # down_osd goes down and either pool becomes degraded.
    local -a vuln_baseline
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      local vbdump
      vbdump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${osd}) \
        perf dump) || return 1
      vuln_baseline[$osd]=$(jq '.recoverystate_perf.pg_vulnerability_duration.avgcount' <<< "$vbdump")
    done

    ceph osd set noup || return 1
    ceph osd down osd.${down_osd} || return 1

    # Backlog on BOTH pools, enough to exceed the lowered log-retention
    # threshold so each PG genuinely backfills (not a log catch-up) once
    # the victim returns -- this matters for pool2 too, not just pool1: the
    # replica-side reservation request for recovery
    # (RepNotRecovering::react(RequestRecoveryPrio)) and for backfill
    # (RepNotRecovering::react(RequestBackfillPrio)) both call the exact
    # same pl->request_remote_recovery_reservation() (PG.cc), i.e.
    # the *same* remote_reserver capacity either way -- but a log-catch-up
    # (recovery) PG being preempted fires RemoteRecoveryPreempted ->
    # MRecoveryReserve::REVOKE -> DeferRecovery on its primary, the
    # *other*, closes-the-latch asymmetric path documented above, not the
    # one this test exists to check. Without this, pool2 (only 5 objects)
    # would stay well under even the lowered 50-entry threshold and take
    # the recovery path instead, silently testing the wrong mechanism (or
    # just making `force-backfill` inapplicable to it, since it wouldn't
    # need backfilling at all).
    for i in $(seq 6 150)
    do
      rados -p $pool1 put obj$i /etc/hostname || return 1
      rados -p $pool2 put obj$i /etc/hostname || return 1
    done

    # Freeze actual data movement so the reservation dance (which proceeds
    # regardless of nobackfill -- see
    # TEST_rebuild_perf_backfill_toofull_pause_case's own comment on this)
    # is inspectable without racing to completion.
    ceph osd set nobackfill || return 1
    ceph osd unset noup || return 1

    # Both PG1 and PG2 now want down_osd back; with osd_max_backfills=1
    # on down_osd, only one can hold its remote_reserver slot at a
    # time -- whichever wins the race becomes PG_VICTIM below, the other
    # PG_PREEMPTOR once force-backfilled. Don't assume which wins.
    local victim_item=""
    for i in $(seq 1 60)
    do
      victim_item=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${down_osd}) \
        dump_recovery_reservations 2>/dev/null | \
        jq -r '.remote_reservations.in_progress[0].item // empty')
      test -n "$victim_item" && break
      sleep 2
    done
    test -n "$victim_item" || {
      echo "FAIL: neither ${PG1} nor ${PG2} ever showed up as an" \
           "in-progress remote reservation on osd.${down_osd}"
      return 1
    }

    local PG_VICTIM PG_PREEMPTOR victim_primary victim_log
    if [ "$victim_item" = "$PG1" ]; then
      PG_VICTIM=$PG1; PG_PREEMPTOR=$PG2; victim_primary=$primary1
    else
      PG_VICTIM=$PG2; PG_PREEMPTOR=$PG1; victim_primary=$primary2
    fi
    victim_log=$dir/osd.${victim_primary}.log

    flush_pg_stats || return 1
    grep -q "enter Started/Primary/Active/Backfilling" $victim_log || {
      echo "FAIL: ${PG_VICTIM} never entered Backfilling before being" \
           "preempted -- test setup assumption broken"
      return 1
    }

    # Structural proxy for "the pg_vulnerability_duration window is still
    # open here": PG_VICTIM is still missing down_osd's copy, so
    # is_vulnerable must still be true regardless of the
    # reservation-preemption dance.
    ceph pg dump pgs --format=json 2>/dev/null | \
      jq -e --arg pgid "$PG_VICTIM" \
        '.pg_stats[] | select(.pgid==$pgid) | select(.state | test("degraded|undersized"))' \
      > /dev/null || {
      echo "FAIL: ${PG_VICTIM} is not degraded/undersized before being" \
           "preempted -- the vulnerability window should structurally" \
           "still be open at this point"
      return 1
    }

    local dump rebuild_avgcount_before
    dump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${victim_primary}) \
           perf dump) || return 1
    rebuild_avgcount_before=$(jq '.recoverystate_perf.pg_rebuild_duration.avgcount' <<< "$dump")

    # Force the OTHER pg's priority up -- this should preempt PG_VICTIM's
    # existing lower-priority grant on down_osd's remote_reserver, sending
    # MBackfillReserve::REVOKE to ${victim_primary} --
    # Backfilling::react(RemoteReservationRevoked) there must resume
    # through WaitLocalBackfillReserved WITHOUT an intervening
    # NotBackfilling.
    local max_tries=10
    for i in $(seq 1 $max_tries)
    do
      if ! ceph pg force-backfill $PG_PREEMPTOR 2>&1 | \
           grep -q "doesn't require backfilling"; then
        break
      fi
      test "$i" = "$max_tries" && {
        echo "FAIL: couldn't force-backfill ${PG_PREEMPTOR}"
        return 1
      }
      sleep 2
    done

    # force-backfill only updates PG_PREEMPTOR's local reservation
    # priority (PeeringState::set_force_backfill(), PG.cc:1389-1394) --
    # it never reaches down_osd's remote_reserver, so the request PG_
    # PREEMPTOR already sent stays queued at its original, tied-with-
    # PG_VICTIM priority. `ceph pg repeer` forces a fresh peering
    # interval for just this PG (no OSD marked down, no daemon
    # touched), which makes it re-send its remote reservation request
    # -- this time with the forced priority already set, so it can
    # actually preempt PG_VICTIM's grant.
    ceph pg repeer $PG_PREEMPTOR || return 1

    local preempted=0
    for i in $(seq 1 60)
    do
      local cur
      cur=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${down_osd}) \
        dump_recovery_reservations 2>/dev/null | \
        jq -r '.remote_reservations.in_progress[0].item // empty')
      if [ "$cur" = "$PG_PREEMPTOR" ]; then
        preempted=1
        break
      fi
      sleep 2
    done
    test "$preempted" = 1 || {
      echo "FAIL: ${PG_PREEMPTOR} never preempted ${PG_VICTIM} on" \
           "down_osd's remote_reserver -- test setup assumption broken" \
           "(priority preemption didn't happen as expected)"
      return 1
    }

    flush_pg_stats || return 1
    grep -q "enter Started/Primary/Active/NotBackfilling" $victim_log && {
      echo "FAIL: ${PG_VICTIM} closed via NotBackfilling after being" \
           "remotely preempted -- Backfilling::react(RemoteReservationRevoked)" \
           "should resume through WaitLocalBackfillReserved WITHOUT closing" \
           "the latch, but it fragmented the span instead"
      return 1
    }

    # Let things settle back down and actually finish.
    ceph pg cancel-force-backfill $PG_PREEMPTOR || return 1
    ceph osd unset nobackfill || return 1
    wait_for_clean || return 1
    flush_pg_stats || return 1

    grep -q "enter Started/Primary/Active/NotBackfilling" $victim_log && {
      echo "FAIL: ${PG_VICTIM} closed via NotBackfilling at some point" \
           "during this episode -- the RemoteReservationRevoked resume" \
           "should never have gone through NotBackfilling at all"
      return 1
    }

    local rebuild_avgcount_after
    dump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${victim_primary}) \
           perf dump) || return 1
    rebuild_avgcount_after=$(jq '.recoverystate_perf.pg_rebuild_duration.avgcount' <<< "$dump")
    echo "INFO: (remote-revoke-resume case) pg_rebuild_duration.avgcount" \
         "before=${rebuild_avgcount_before} after=${rebuild_avgcount_after}" \
         "(osd.${victim_primary}, osd-wide)"

    test "$rebuild_avgcount_after" -ge "$(expr $rebuild_avgcount_before + 1)" || {
      echo "FAIL: expected pg_rebuild_duration.avgcount to grow by at" \
           "least 1 once ${PG_VICTIM} finally completed" \
           "($rebuild_avgcount_before -> $rebuild_avgcount_after)"
      return 1
    }

    local vuln_avgcount_after
    dump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${victim_primary}) \
           perf dump) || return 1
    vuln_avgcount_after=$(jq '.recoverystate_perf.pg_vulnerability_duration.avgcount' <<< "$dump")
    echo "INFO: (remote-revoke-resume case) pg_vulnerability_duration.avgcount" \
         "baseline=${vuln_baseline[$victim_primary]} after=${vuln_avgcount_after}" \
         "(osd.${victim_primary}, osd-wide)"
    test "$vuln_avgcount_after" -ge "$(expr ${vuln_baseline[$victim_primary]} + 1)" || {
      echo "FAIL: expected pg_vulnerability_duration.avgcount to grow by" \
           "at least 1 once ${PG_VICTIM}'s single continuous vulnerability" \
           "window finally closed" \
           "(${vuln_baseline[$victim_primary]} -> $vuln_avgcount_after)"
      return 1
    }

    delete_pool $pool1
    delete_pool $pool2
    kill_daemons $dir || return 1
}


main osd-recovery-stats "$@"

# Local Variables:
# compile-command: "make -j4 && ../qa/run-standalone.sh osd-recovery-stats.sh"
# End:
