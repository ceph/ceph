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
    local final_stats=$(ceph pg $pgid query | \
      jq -r '.info.stats | "\(.last_degraded) \(.last_clean)"')
    read -r last_degraded_final last_clean_final <<< "$final_stats"

    echo "Final Timestamps -> Last Degraded: $last_degraded_final, " \
         "Last Clean: $last_clean_final"
    if [[ "$last_clean_final" > "$last_degraded_final" ]]; then
      echo "Test Passed: Recovery successful. last_clean ($last_clean_final) " \
           "is newer than last_degraded ($last_degraded_final)."
    else
      echo "Test Failed: last_clean ($last_clean_final) was not updated " \
           "correctly after recovery."
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
      if grep -q "rebuild-stats: latched failure start for ${PG_SPG} " $log
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
    latch_count=$(grep -c "rebuild-stats: latched failure start for ${PG_SPG} " $log)
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
    record_count=$(grep -c "rebuild-stats: recorded rebuild for ${PG_SPG} " $log)
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
      grep -q "rebuild-stats: latched failure start for ${PG} " $log_b && {
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

    local recorded_b=0
    for i in $(seq 1 30)
    do
      flush_pg_stats || return 1
      grep -q "rebuild-stats: recorded rebuild for ${PG} .*reason=handover-away" $log_b && {
        recorded_b=1
        break
      }
      sleep 1
    done
    test "$recorded_b" = 1 || {
      echo "FAIL: osd.${primary_b}'s segment was not recorded on its own handover-away"
      return 1
    }

    local latched_c=0
    for i in $(seq 1 30)
    do
      grep -q "rebuild-stats: latched failure start for ${PG} " $log_c && {
        latched_c=1
        break
      }
      sleep 1
    done
    test "$latched_c" = 1 || {
      echo "FAIL: osd.${primary_c} never latched after taking over primary" \
           "(the multi-hop chain, requires a fresh arming of the latch here)"
      return 1
    }

    # --- Bring both A and B back and let the episode resolve. Which OSD
    # ends up recording the eventual "reached-clean" segment, and how many
    # more handovers happen in between, is deliberately not predicted here.
    ceph osd unset noup || return 1
    wait_for_clean || return 1
    flush_pg_stats || return 1

    local final_primary
    final_primary=$(get_primary $poolname obj1)
    local log_final=$dir/osd.${final_primary}.log
    grep -q "rebuild-stats: recorded rebuild for ${PG} .*reason=reached-clean" $log_final || {
      echo "FAIL: no OSD recorded a reached-clean segment once the PG went clean" \
           "(checked the final primary, osd.${final_primary})"
      return 1
    }

    # Soft observation (not asserted either way): did osd.a or osd.b show any
    # of their own rebuild-stats activity after coming back up? Either outcome
    # is informative and is just logged here. A genuinely open, likely
    # timing-dependent question with no settled answer (whether the
    # async-recovery quick-promotion ever arms a spurious segment that gets
    # recorded or discarded before being demoted again). The following 2 checks
    # will confirm this observation:
    #
    # 1. A "discarded rebuild" line for this PG on ANY of the four OSDs is
    #    unambiguous: A discarded line can only be residue of an UNPLANNED
    #    arm-then-filtered-out cycle, exactly the quick-promotion signature,
    #    regardless of which OSD ends up primary or how many real handovers occur.
    for osd in 0 1 2 3
    do
      if grep -q "rebuild-stats: discarded rebuild for ${PG} " $dir/osd.${osd}.log
      then
        echo "OBSERVATION: osd.${osd} shows a DISCARDED rebuild-stats segment" \
             "for ${PG}; likely evidence of the async-recovery quick-promotion" \
             "arming and then being filtered out at record time"
      fi
    done

    # 2. Total "latched failure start" lines for this PG, across all four
    # OSDs, as a coarser but still useful signal: exactly 2 are
    # structurally guaranteed by this test's design (primary_b's and
    # primary_c's own arms), plus one more if whichever OSD ends up as
    # final_primary needed a fresh arm of its own (i.e. a third real
    # handover happened beyond the two this test forces). A count higher than
    # that baseline would mean an extra, unplanned arm occurred somewhere --
    # whether or not it went on to be recorded or discarded.
    local total_latches
    total_latches=$(grep -h "rebuild-stats: latched failure start for ${PG} " \
      $dir/osd.*.log | wc -l)
    echo "OBSERVATION: ${total_latches} total 'latched' lines for ${PG} across" \
         "all four OSDs this run (2 expected structurally, 3 if the final" \
         "primary needed its own fresh arm; more than that would mean an" \
         "extra, unplanned arm occurred)"

    delete_pool $poolname
    kill_daemons $dir || return 1
}

# Test that verifies that an empty PG (no objects ever written) going
# undersized+degraded and back to active+clean must not be recorded at all --
# the interim solution's documented, deliberately-unfixed limitation.
function TEST_rebuild_perf_empty_pg_not_counted() {
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

    # Confirm the latch actually armed (state genuinely went
    # undersized) before checking it was correctly discarded -- a test
    # that just sees "nothing recorded" without this is equally
    # consistent with the mechanism never engaging at all.
    grep -q "rebuild-stats: latched failure start for ${PG} " $log || {
      echo "FAIL: the latch never armed at all -- test setup didn't" \
           "actually make the PG undersized, this isn't testing what" \
           "it claims to"
      return 1
    }

    ceph osd unset noup || return 1
    wait_for_clean || return 1
    flush_pg_stats || return 1

    if grep -q "rebuild-stats: recorded rebuild for ${PG} " $log
    then
      echo "FAIL: an empty PG's undersized/degraded window was recorded --" \
           "this is supposed to remain a known, unfixed limitation"
      return 1
    fi

    grep -q "rebuild-stats: discarded rebuild for ${PG} .*delta_recovered=0 had_redundancy_loss=0" $log || {
      echo "FAIL: expected a 'discarded' line confirming the filter" \
           "correctly rejected this empty-PG episode, found none"
      return 1
    }

    local dump
    dump=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) \
           perf dump) || return 1
    local avgcount
    avgcount=$(jq '.recoverystate_perf.pg_vulnerability_duration.avgcount' \
      <<< "$dump")
    test "$avgcount" = "0" || {
      echo "FAIL: expected pg_vulnerability_duration.avgcount=0 for an" \
           "empty-PG episode, got $avgcount"
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
    grep -q "rebuild-stats: latched failure start for ${PG} " $log || {
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
    grep -q "rebuild-stats: recorded rebuild for ${PG} " $log || {
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
      if test "$(grep -c "rebuild-stats: latched failure start for ${PG} " $log)" -ge 2
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
        grep -q "rebuild-stats: recorded rebuild for ${pg} " $dir/osd.${osd}.log \
          && recorded=true
        grep -q "rebuild-stats: discarded rebuild for ${pg} " $dir/osd.${osd}.log \
          && discarded=true
      done

      $recorded || {
        echo "FAIL: no 'recorded rebuild' line found anywhere for ${pg} --" \
             "the split-inherited latch was lost or never resolved"
        return 1
      }
      ! $discarded || {
        echo "FAIL: a 'discarded rebuild' line was found for ${pg} -- likely" \
             "delta_recovered went negative after the split, discarding a" \
             "genuine rebuild instead of recording it"
        return 1
      }

      # Directly confirm delta_recovered isn't negative in the recorded line
      # itself, not just that it recorded instead of being discarded -- the
      # two filters overlap but aren't identical.
      grep -h "rebuild-stats: recorded rebuild for ${pg} " $dir/osd.*.log \
        | grep -q "delta_recovered=-" && {
        echo "FAIL: ${pg} recorded a NEGATIVE delta_recovered -- the" \
             "split-time num_objects_recovered redistribution bug is" \
             "present"
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
   child_first_recorded_ts=$(grep -h "rebuild-stats: recorded rebuild for ${child_pg} " \
      $dir/osd.*.log | sort | head -1 | awk '{print $1}')
    test -n "$child_first_recorded_ts" || {
      echo "FAIL: could not determine ${child_pg}'s first recorded timestamp"
      return 1
    }
    local child_earliest_latched_ts
    child_earliest_latched_ts=$(grep -h "rebuild-stats: latched failure start for ${child_pg} " \
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
    parent_duration=$(grep "rebuild-stats: recorded rebuild for ${PG} " $log \
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
    child_duration=$(grep -h "rebuild-stats: recorded rebuild for ${child_pg} " \
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
      total_log_recorded=$(grep -h "rebuild-stats: recorded rebuild for " \
        $dir/osd.*.log | wc -l)
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
    total_log_duration=$(grep -h "rebuild-stats: recorded rebuild for " $dir/osd.*.log \
      | grep -o "duration=[0-9.]*" | cut -d= -f2 | awk '{s+=$1} END {print s+0}')

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

main osd-recovery-stats "$@"

# Local Variables:
# compile-command: "make -j4 && ../qa/run-standalone.sh osd-recovery-stats.sh"
# End:
