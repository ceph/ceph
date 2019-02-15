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
    export CEPH_MON="127.0.0.1:7114" # git grep '\<7114\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON --osd_max_backfills=1 --debug_reserver=20"
    export objects=200
    export poolprefix=test
    export FORCE_PRIO="255"    # See OSD_RECOVERY_PRIORITY_FORCED
    export NORMAL_PRIO="180"   # See OSD_RECOVERY_PRIORITY_BASE

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}


function TEST_recovery_priority() {
    local dir=$1
    local pools=10
    local OSDS=5
    local max_tries=10

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    export CEPH_ARGS

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd || return 1
    done

    for p in $(seq 1 $pools)
    do
      create_pool "${poolprefix}$p" 1 1
      ceph osd pool set "${poolprefix}$p" size 2
    done
    sleep 5

    wait_for_clean || return 1

    ceph pg dump pgs

    # Find 3 pools with a pg with the same primaries but second
    # replica on another osd.
    local PG1
    local POOLNUM1
    local pool1
    local chk_osd1_1
    local chk_osd1_2

    local PG2
    local POOLNUM2
    local pool2
    local chk_osd2

    local PG3
    local POOLNUM3
    local pool3

    for p in $(seq 1 $pools)
    do
      ceph pg map ${p}.0 --format=json | jq '.acting[]' > $dir/acting
      local test_osd1=$(head -1 $dir/acting)
      local test_osd2=$(tail -1 $dir/acting)
      if [ -z "$PG1" ];
      then
        PG1="${p}.0"
        POOLNUM1=$p
        pool1="${poolprefix}$p"
        chk_osd1_1=$test_osd1
        chk_osd1_2=$test_osd2
      elif [ -z "$PG2" -a $chk_osd1_1 = $test_osd1 -a $chk_osd1_2 != $test_osd2 ];
      then
        PG2="${p}.0"
        POOLNUM2=$p
        pool2="${poolprefix}$p"
        chk_osd2=$test_osd2
      elif [ -n "$PG2" -a $chk_osd1_1 = $test_osd1 -a $chk_osd1_2 != $test_osd2 -a "$chk_osd2" != $test_osd2 ];
      then
        PG3="${p}.0"
        POOLNUM3=$p
        pool3="${poolprefix}$p"
        break
      fi
    done
    rm -f $dir/acting

    if [ "$pool2" = "" -o "pool3" = "" ];
    then
      echo "Failure to find appropirate PGs"
      return 1
    fi

    for p in $(seq 1 $pools)
    do
      if [ $p != $POOLNUM1 -a $p != $POOLNUM2 -a $p != $POOLNUM3 ];
      then
        delete_pool ${poolprefix}$p
      fi
    done

    ceph osd pool set $pool2 size 1
    ceph osd pool set $pool3 size 1
    wait_for_clean || return 1

    dd if=/dev/urandom of=$dir/data bs=1M count=10
    p=1
    for pname in $pool1 $pool2 $pool3
    do
      for i in $(seq 1 $objects)
      do
	rados -p ${pname} put obj${i}-p${p} $dir/data
      done
      p=$(expr $p + 1)
    done

    local otherosd=$(get_not_primary $pool1 obj1-p1)

    ceph pg dump pgs
    ERRORS=0

    ceph osd set norecover
    ceph osd set noout

    # Get a pg to want to recover and quickly force it
    # to be preempted.
    ceph osd pool set $pool3 size 2
    sleep 2
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${chk_osd1_1}) dump_reservations || return 1

    # 3. Item is in progress, adjust priority with no higher priority waiting
    for i in $(seq 1 $max_tries)
    do
      if ! ceph pg force-recovery $PG3 2>&1 | grep -q "doesn't require recovery"; then
        break
      fi
      if [ "$i" = "$max_tries" ]; then
        echo "ERROR: Didn't appear to be able to force-recovery"
        ERRORS=$(expr $ERRORS + 1)
      fi
      sleep 2
    done
    flush_pg_stats || return 1
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${chk_osd1_1}) dump_reservations || return 1

    ceph osd out osd.$chk_osd1_2
    sleep 2
    flush_pg_stats || return 1
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${chk_osd1_1}) dump_reservations || return 1
    ceph pg dump pgs

    ceph osd pool set $pool2 size 2
    sleep 2
    flush_pg_stats || return 1
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${chk_osd1_1}) dump_reservations > $dir/out || return 1
    cat $dir/out
    ceph pg dump pgs

    PRIO=$(cat $dir/out | jq "(.local_reservations.queues[].items[] | select(.item == \"${PG1}\")).prio")
    if [ "$PRIO" != "$NORMAL_PRIO" ];
    then
      echo "The normal PG ${PG1} doesn't have prio $NORMAL_PRIO queued waiting"
      ERRORS=$(expr $ERRORS + 1)
    fi

    # Using eval will strip double-quotes from item
    eval ITEM=$(cat $dir/out | jq '.local_reservations.in_progress[0].item')
    if [ "$ITEM" != ${PG3} ];
    then
      echo "The first force-recovery PG $PG3 didn't become the in progress item"
      ERRORS=$(expr $ERRORS + 1)
    else
      PRIO=$(cat $dir/out | jq '.local_reservations.in_progress[0].prio')
      if [ "$PRIO" != $FORCE_PRIO ];
      then
        echo "The first force-recovery PG ${PG3} doesn't have prio $FORCE_PRIO"
        ERRORS=$(expr $ERRORS + 1)
      fi
    fi

    # 1. Item is queued, re-queue with new priority
    for i in $(seq 1 $max_tries)
    do
      if ! ceph pg force-recovery $PG2 2>&1 | grep -q "doesn't require recovery"; then
        break
      fi
      if [ "$i" = "$max_tries" ]; then
        echo "ERROR: Didn't appear to be able to force-recovery"
        ERRORS=$(expr $ERRORS + 1)
      fi
      sleep 2
    done
    sleep 2
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${chk_osd1_1}) dump_reservations > $dir/out || return 1
    cat $dir/out
    PRIO=$(cat $dir/out | jq "(.local_reservations.queues[].items[] | select(.item == \"${PG2}\")).prio")
    if [ "$PRIO" != "$FORCE_PRIO" ];
    then
      echo "The second force-recovery PG ${PG2} doesn't have prio $FORCE_PRIO"
      ERRORS=$(expr $ERRORS + 1)
    fi
    flush_pg_stats || return 1

    # 4. Item is in progress, if higher priority items waiting prempt item
    #ceph osd unset norecover
    ceph pg cancel-force-recovery $PG3 || return 1
    sleep 2
    #ceph osd set norecover
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${chk_osd1_1}) dump_reservations > $dir/out || return 1
    cat $dir/out
    PRIO=$(cat $dir/out | jq "(.local_reservations.queues[].items[] | select(.item == \"${PG3}\")).prio")
    if [ "$PRIO" != "$NORMAL_PRIO" ];
    then
      echo "After cancel-recovery PG ${PG3} doesn't have prio $NORMAL_PRIO"
      ERRORS=$(expr $ERRORS + 1)
    fi

    eval ITEM=$(cat $dir/out | jq '.local_reservations.in_progress[0].item')
    if [ "$ITEM" != ${PG2} ];
    then
      echo "The force-recovery PG $PG2 didn't become the in progress item"
      ERRORS=$(expr $ERRORS + 1)
    else
      PRIO=$(cat $dir/out | jq '.local_reservations.in_progress[0].prio')
      if [ "$PRIO" != $FORCE_PRIO ];
      then
        echo "The first force-recovery PG ${PG2} doesn't have prio $FORCE_PRIO"
        ERRORS=$(expr $ERRORS + 1)
      fi
    fi

    ceph pg cancel-force-recovery $PG2 || return 1
    sleep 5
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${chk_osd1_1}) dump_reservations || return 1

    # 2. Item is queued, re-queue and preempt because new priority higher than an in progress item
    flush_pg_stats || return 1
    ceph pg force-recovery $PG3 || return 1
    sleep 2

    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${chk_osd1_1}) dump_reservations > $dir/out || return 1
    cat $dir/out
    PRIO=$(cat $dir/out | jq "(.local_reservations.queues[].items[] | select(.item == \"${PG2}\")).prio")
    if [ "$PRIO" != "$NORMAL_PRIO" ];
    then
      echo "After cancel-force-recovery PG ${PG3} doesn't have prio $NORMAL_PRIO"
      ERRORS=$(expr $ERRORS + 1)
    fi

    eval ITEM=$(cat $dir/out | jq '.local_reservations.in_progress[0].item')
    if [ "$ITEM" != ${PG3} ];
    then
      echo "The force-recovery PG $PG3 didn't get promoted to an in progress item"
      ERRORS=$(expr $ERRORS + 1)
    else
      PRIO=$(cat $dir/out | jq '.local_reservations.in_progress[0].prio')
      if [ "$PRIO" != $FORCE_PRIO ];
      then
        echo "The force-recovery PG ${PG2} doesn't have prio $FORCE_PRIO"
        ERRORS=$(expr $ERRORS + 1)
      fi
    fi

    ceph osd unset noout
    ceph osd unset norecover

    wait_for_clean "CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${chk_osd1_1}) dump_reservations" || return 1

    ceph pg dump pgs

    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${chk_osd1_1}) dump_pgstate_history

    if [ $ERRORS != "0" ];
    then
      echo "$ERRORS error(s) found"
    else
      echo TEST PASSED
    fi

    delete_pool $pool1
    delete_pool $pool2
    delete_pool $pool3
    kill_daemons $dir || return 1
    return $ERRORS
}

#
# Show that pool recovery_priority is added to recovery priority
#
# Create 2 pools with 2 OSDs with different primarys
# pool 1 with recovery_priority 1
# pool 2 with recovery_priority 2
#
# Start recovery by changing the pool sizes from 1 to 2
# Use dump_reservations to verify priorities
function TEST_recovery_pool_priority() {
    local dir=$1
    local pools=3 # Don't assume the first 2 pools are exact what we want
    local OSDS=2

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    export CEPH_ARGS

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd || return 1
    done

    for p in $(seq 1 $pools)
    do
      create_pool "${poolprefix}$p" 1 1
      ceph osd pool set "${poolprefix}$p" size 2
    done
    sleep 5

    wait_for_clean || return 1

    ceph pg dump pgs

    # Find 2 pools with different primaries which
    # means the replica must be on another osd.
    local PG1
    local POOLNUM1
    local pool1
    local chk_osd1_1
    local chk_osd1_2

    local PG2
    local POOLNUM2
    local pool2
    local chk_osd2_1
    local chk_osd2_2

    for p in $(seq 1 $pools)
    do
      ceph pg map ${p}.0 --format=json | jq '.acting[]' > $dir/acting
      local test_osd1=$(head -1 $dir/acting)
      local test_osd2=$(tail -1 $dir/acting)
      if [ -z "$PG1" ];
      then
        PG1="${p}.0"
        POOLNUM1=$p
        pool1="${poolprefix}$p"
        chk_osd1_1=$test_osd1
        chk_osd1_2=$test_osd2
      elif [ $chk_osd1_1 != $test_osd1 ];
      then
        PG2="${p}.0"
        POOLNUM2=$p
        pool2="${poolprefix}$p"
        chk_osd2_1=$test_osd1
        chk_osd2_2=$test_osd2
        break
      fi
    done
    rm -f $dir/acting

    if [ "$pool2" = "" ];
    then
      echo "Failure to find appropirate PGs"
      return 1
    fi

    for p in $(seq 1 $pools)
    do
      if [ $p != $POOLNUM1 -a $p != $POOLNUM2 ];
      then
        delete_pool ${poolprefix}$p
      fi
    done

    pool1_extra_prio=1
    pool2_extra_prio=2
    pool1_prio=$(expr $NORMAL_PRIO + $pool1_extra_prio)
    pool2_prio=$(expr $NORMAL_PRIO + $pool2_extra_prio)

    ceph osd pool set $pool1 size 1
    ceph osd pool set $pool1 recovery_priority $pool1_extra_prio
    ceph osd pool set $pool2 size 1
    ceph osd pool set $pool2 recovery_priority $pool2_extra_prio
    wait_for_clean || return 1

    dd if=/dev/urandom of=$dir/data bs=1M count=10
    p=1
    for pname in $pool1 $pool2
    do
      for i in $(seq 1 $objects)
      do
	rados -p ${pname} put obj${i}-p${p} $dir/data
      done
      p=$(expr $p + 1)
    done

    local otherosd=$(get_not_primary $pool1 obj1-p1)

    ceph pg dump pgs
    ERRORS=0

    ceph osd pool set $pool1 size 2
    ceph osd pool set $pool2 size 2
    sleep 10
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${chk_osd1_1}) dump_reservations > $dir/dump.${chk_osd1_1}.out
    echo osd.${chk_osd1_1}
    cat $dir/dump.${chk_osd1_1}.out
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${chk_osd1_2}) dump_reservations > $dir/dump.${chk_osd1_2}.out
    echo osd.${chk_osd1_2}
    cat $dir/dump.${chk_osd1_2}.out

    # Using eval will strip double-quotes from item
    eval ITEM=$(cat $dir/dump.${chk_osd1_1}.out | jq '.local_reservations.in_progress[0].item')
    if [ "$ITEM" != ${PG1} ];
    then
      echo "The primary PG for $pool1 didn't become the in progress item"
      ERRORS=$(expr $ERRORS + 1)
    else
      PRIO=$(cat $dir/dump.${chk_osd1_1}.out | jq '.local_reservations.in_progress[0].prio')
      if [ "$PRIO" != $pool1_prio ];
      then
        echo "The primary PG ${PG1} doesn't have prio $pool1_prio"
        ERRORS=$(expr $ERRORS + 1)
      fi
    fi

    # Using eval will strip double-quotes from item
    eval ITEM=$(cat $dir/dump.${chk_osd1_2}.out | jq '.remote_reservations.in_progress[0].item')
    if [ "$ITEM" != ${PG1} ];
    then
      echo "The primary PG for $pool1 didn't become the in progress item on remote"
      ERRORS=$(expr $ERRORS + 1)
    else
      PRIO=$(cat $dir/dump.${chk_osd1_2}.out | jq '.remote_reservations.in_progress[0].prio')
      if [ "$PRIO" != $pool1_prio ];
      then
        echo "The primary PG ${PG1} doesn't have prio $pool1_prio on remote"
        ERRORS=$(expr $ERRORS + 1)
      fi
    fi

    # Using eval will strip double-quotes from item
    eval ITEM=$(cat $dir/dump.${chk_osd2_1}.out | jq '.local_reservations.in_progress[0].item')
    if [ "$ITEM" != ${PG2} ];
    then
      echo "The primary PG for $pool2 didn't become the in progress item"
      ERRORS=$(expr $ERRORS + 1)
    else
      PRIO=$(cat $dir/dump.${chk_osd2_1}.out | jq '.local_reservations.in_progress[0].prio')
      if [ "$PRIO" != $pool2_prio ];
      then
        echo "The primary PG ${PG2} doesn't have prio $pool2_prio"
        ERRORS=$(expr $ERRORS + 1)
      fi
    fi

    # Using eval will strip double-quotes from item
    eval ITEM=$(cat $dir/dump.${chk_osd2_2}.out | jq '.remote_reservations.in_progress[0].item')
    if [ "$ITEM" != ${PG2} ];
    then
      echo "The primary PG $PG2 didn't become the in progress item on remote"
      ERRORS=$(expr $ERRORS + 1)
    else
      PRIO=$(cat $dir/dump.${chk_osd2_2}.out | jq '.remote_reservations.in_progress[0].prio')
      if [ "$PRIO" != $pool2_prio ];
      then
        echo "The primary PG ${PG2} doesn't have prio $pool2_prio on remote"
        ERRORS=$(expr $ERRORS + 1)
      fi
    fi

    wait_for_clean || return 1

    if [ $ERRORS != "0" ];
    then
      echo "$ERRORS error(s) found"
    else
      echo TEST PASSED
    fi

    delete_pool $pool1
    delete_pool $pool2
    kill_daemons $dir || return 1
    return $ERRORS
}

main osd-recovery-prio "$@"

# Local Variables:
# compile-command: "make -j4 && ../qa/run-standalone.sh osd-recovery-prio.sh"
# End:
