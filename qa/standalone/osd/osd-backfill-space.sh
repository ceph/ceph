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

    export CEPH_MON="127.0.0.1:7180" # git grep '\<7180\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--osd_min_pg_log_entries=5 --osd_max_pg_log_entries=10 "
    CEPH_ARGS+="--fake_statfs_for_testing=3686400 "
    CEPH_ARGS+="--osd_max_backfills=10 "
    export objects=600
    export poolprefix=test

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}


function get_num_in_state() {
    local state=$1
    local expression
    expression+="select(contains(\"${state}\"))"
    ceph --format json pg dump pgs 2>/dev/null | \
        jq ".pg_stats | [.[] | .state | $expression] | length"
}


function wait_for_state() {
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


function wait_for_backfill() {
    local timeout=$1
    wait_for_state backfilling $timeout
}


function wait_for_active() {
    local timeout=$1
    wait_for_state activating $timeout
}

# All tests are created in an environment which has fake total space
# of 3600K (3686400) which can hold 600 6K replicated objects or
# 200 18K shards of erasure coded objects.  For a k=3, m=2 EC pool
# we have a theoretical 54K object but with the chunk size of 4K
# and a rounding of 4K to account for the chunks is 36K max object
# which is ((36K / 3) + 4K) * 200  = 3200K which is 88% of
# 3600K for a shard.

# Create 2 pools with size 1
# Write enough data that only 1 pool pg can fit per osd
# Incresase the pool size to 2
# On 3 OSDs this should result in 1 OSD with overlapping replicas,
# so both pools can't fit.  We assume pgid 1.0 and 2.0 won't
# map to the same 2 OSDs.
# At least 1 pool shouldn't have room to backfill
# All other pools should go active+clean
function TEST_backfill_test_simple() {
    local dir=$1
    local pools=2
    local OSDS=3

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    export CEPH_ARGS

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd || return 1
    done

    ceph osd set-backfillfull-ratio .85

    for p in $(seq 1 $pools)
    do
      create_pool "${poolprefix}$p" 1 1
      ceph osd pool set "${poolprefix}$p" size 1
    done

    wait_for_clean || return 1

    # This won't work is if the 2 pools primary and only osds
    # are the same.

    dd if=/dev/urandom of=$dir/datafile bs=1024 count=4
    for o in $(seq 1 $objects)
    do
      for p in $(seq 1 $pools)
      do
	rados -p "${poolprefix}$p" put obj$o $dir/datafile
      done
    done

    ceph pg dump pgs

    for p in $(seq 1 $pools)
    do
      ceph osd pool set "${poolprefix}$p" size 2
    done
    sleep 5

    wait_for_backfill 240 || return 1
    wait_for_active 60 || return 1

    ERRORS=0
    if [ "$(ceph pg dump pgs | grep +backfill_toofull | wc -l)" != "1" ];
    then
      echo "One pool should have been in backfill_toofull"
      ERRORS="$(expr $ERRORS + 1)"
    fi

    expected="$(expr $pools - 1)"
    if [ "$(ceph pg dump pgs | grep active+clean | wc -l)" != "$expected" ];
    then
      echo "$expected didn't finish backfill"
      ERRORS="$(expr $ERRORS + 1)"
    fi

    ceph pg dump pgs

    if [ $ERRORS != "0" ];
    then
      return 1
    fi

    for i in $(seq 1 $pools)
    do
      delete_pool "${poolprefix}$i"
    done
    kill_daemons $dir || return 1
    ! grep -q "num_bytes mismatch" $dir/osd.*.log || return 1
}


# Create 8 pools of size 1 on 20 OSDs
# Write 4K * 600 objects (only 1 pool pg can fit on any given osd)
# Increase pool size to 2
# At least 1 pool shouldn't have room to backfill
# All other pools should go active+clean
function TEST_backfill_test_multi() {
    local dir=$1
    local pools=8
    local OSDS=20

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    export CEPH_ARGS

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd || return 1
    done

    ceph osd set-backfillfull-ratio .85

    for p in $(seq 1 $pools)
    do
      create_pool "${poolprefix}$p" 1 1
      ceph osd pool set "${poolprefix}$p" size 1
    done

    wait_for_clean || return 1

    dd if=/dev/urandom of=$dir/datafile bs=1024 count=4
    for o in $(seq 1 $objects)
    do
      for p in $(seq 1 $pools)
      do
	rados -p "${poolprefix}$p" put obj$o $dir/datafile
      done
    done

    ceph pg dump pgs

    for p in $(seq 1 $pools)
    do
      ceph osd pool set "${poolprefix}$p" size 2
    done
    sleep 5

    wait_for_backfill 240 || return 1
    wait_for_active 60 || return 1

    ERRORS=0
    full="$(ceph pg dump pgs | grep +backfill_toofull | wc -l)"
    if [ "$full" -lt "1" ];
    then
      echo "At least one pool should have been in backfill_toofull"
      ERRORS="$(expr $ERRORS + 1)"
    fi

    expected="$(expr $pools - $full)"
    if [ "$(ceph pg dump pgs | grep active+clean | wc -l)" != "$expected" ];
    then
      echo "$expected didn't finish backfill"
      ERRORS="$(expr $ERRORS + 1)"
    fi

    ceph pg dump pgs

    if [ $ERRORS != "0" ];
    then
      return 1
    fi

    for i in $(seq 1 $pools)
    do
      delete_pool "${poolprefix}$i"
    done
    # Work around for http://tracker.ceph.com/issues/38195
    kill_daemons $dir #|| return 1
    ! grep -q "num_bytes mismatch" $dir/osd.*.log || return 1
}


# To make sure that when 2 pg try to backfill at the same time to
# the same target.  This might be covered by the simple test above
# but this makes sure we get it.
#
# Create 10 pools of size 2 and identify 2 that have the same
# non-primary osd.
# Delete all other pools
# Set size to 1 and write 4K * 600 to each pool
# Set size back to 2
# The 2 pools should race to backfill.
# One pool goes active+clean
# The other goes acitve+...+backfill_toofull
function TEST_backfill_test_sametarget() {
    local dir=$1
    local pools=10
    local OSDS=5

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    export CEPH_ARGS

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd || return 1
    done

    ceph osd set-backfillfull-ratio .85

    for p in $(seq 1 $pools)
    do
      create_pool "${poolprefix}$p" 1 1
      ceph osd pool set "${poolprefix}$p" size 2
    done
    sleep 5

    wait_for_clean || return 1

    ceph pg dump pgs

    # Find 2 pools with a pg that distinct primaries but second
    # replica on the same osd.
    local PG1
    local POOLNUM1
    local pool1
    local chk_osd1
    local chk_osd2

    local PG2
    local POOLNUM2
    local pool2
    for p in $(seq 1 $pools)
    do
      ceph pg map ${p}.0 --format=json | jq '.acting[]' > $dir/acting
      local test_osd1=$(head -1 $dir/acting)
      local test_osd2=$(tail -1 $dir/acting)
      if [ $p = "1" ];
      then
        PG1="${p}.0"
        POOLNUM1=$p
        pool1="${poolprefix}$p"
        chk_osd1=$test_osd1
        chk_osd2=$test_osd2
      elif [ $chk_osd1 != $test_osd1 -a $chk_osd2 = $test_osd2 ];
      then
        PG2="${p}.0"
        POOLNUM2=$p
        pool2="${poolprefix}$p"
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

    ceph osd pool set $pool1 size 1
    ceph osd pool set $pool2 size 1

    wait_for_clean || return 1

    dd if=/dev/urandom of=$dir/datafile bs=1024 count=4
    for i in $(seq 1 $objects)
    do
	rados -p $pool1 put obj$i $dir/datafile
        rados -p $pool2 put obj$i $dir/datafile
    done

    ceph osd pool set $pool1 size 2
    ceph osd pool set $pool2 size 2
    sleep 5

    wait_for_backfill 240 || return 1
    wait_for_active 60 || return 1

    ERRORS=0
    if [ "$(ceph pg dump pgs | grep +backfill_toofull | wc -l)" != "1" ];
    then
      echo "One pool should have been in backfill_toofull"
      ERRORS="$(expr $ERRORS + 1)"
    fi

    if [ "$(ceph pg dump pgs | grep active+clean | wc -l)" != "1" ];
    then
      echo "One didn't finish backfill"
      ERRORS="$(expr $ERRORS + 1)"
    fi

    ceph pg dump pgs

    if [ $ERRORS != "0" ];
    then
      return 1
    fi

    delete_pool $pool1
    delete_pool $pool2
    kill_daemons $dir || return 1
    ! grep -q "num_bytes mismatch" $dir/osd.*.log || return 1
}

# 2 pools can't both backfill to a target which has other data
# 1 of the pools has objects that increase from 1024 to 2611 bytes
#
# Write to fill pool which is size 1
# Take fill pool osd down (other 2 pools must go to the remaining OSDs
# Save an export of data on fill OSD and restart it
# Write an intial 1K to pool1 which has pg 2.0
# Export 2.0 from non-fillpool OSD don't wait for it to start-up
# Take down fillpool OSD
# Put 1K object version of 2.0 on fillpool OSD
# Put back fillpool data on fillpool OSD
# With fillpool down write 2611 byte objects 
# Take down $osd and bring back $fillosd simultaneously
# Wait for backfilling
# PG 2.0 will be able to backfill its remaining data
# PG 3.0 must get backfill_toofull
function TEST_backfill_multi_partial() {
    local dir=$1
    local EC=$2
    local pools=2
    local OSDS=3

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    export CEPH_ARGS

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd || return 1
    done

    ceph osd set-backfillfull-ratio .85

    ceph osd set-require-min-compat-client luminous
    create_pool fillpool 1 1
    ceph osd pool set fillpool size 1
    for p in $(seq 1 $pools)
    do
      create_pool "${poolprefix}$p" 1 1
      ceph osd pool set "${poolprefix}$p" size 2
    done

    wait_for_clean || return 1

    # Partially fill an osd
    # We have room for 600 6K replicated objects, if we create 2611 byte objects
    # there is 3600K - (2611 * 600) = 2070K, so the fill pool and one
    # replica from the other 2 is 85% of 3600K

    dd if=/dev/urandom of=$dir/datafile bs=2611 count=1
    for o in $(seq 1 $objects)
    do
      rados -p fillpool put obj-fill-${o} $dir/datafile
    done

    local fillosd=$(get_primary fillpool obj-fill-1)
    osd=$(expr $fillosd + 1)
    if [ "$osd" = "$OSDS" ]; then
      osd="0"
    fi

    sleep 5
    kill $(cat $dir/osd.$fillosd.pid)
    ceph osd out osd.$fillosd
    sleep 2

    _objectstore_tool_nodown $dir $fillosd --op export-remove --pgid 1.0 --file $dir/fillexport.out || return 1
    activate_osd $dir $fillosd || return 1

    ceph pg dump pgs

    dd if=/dev/urandom of=$dir/datafile bs=1024 count=1
    for o in $(seq 1 $objects)
    do
      rados -p "${poolprefix}1" put obj-1-${o} $dir/datafile
    done

    ceph pg dump pgs
    # The $osd OSD is started, but we don't wait so we can kill $fillosd at the same time
    _objectstore_tool_nowait $dir $osd --op export --pgid 2.0 --file $dir/export.out
    kill $(cat $dir/osd.$fillosd.pid)
    sleep 5
    _objectstore_tool_nodown $dir $fillosd --force --op remove --pgid 2.0
    _objectstore_tool_nodown $dir $fillosd --op import --pgid 2.0 --file $dir/export.out || return 1
    _objectstore_tool_nodown $dir $fillosd --op import --pgid 1.0 --file $dir/fillexport.out || return 1
    ceph pg dump pgs
    sleep 20
    ceph pg dump pgs

    # re-write everything
    dd if=/dev/urandom of=$dir/datafile bs=2611 count=1
    for o in $(seq 1 $objects)
    do
      for p in $(seq 1 $pools)
      do
	rados -p "${poolprefix}$p" put obj-${p}-${o} $dir/datafile
      done
    done

    kill $(cat $dir/osd.$osd.pid)
    ceph osd out osd.$osd

    activate_osd $dir $fillosd || return 1
    ceph osd in osd.$fillosd
    sleep 15

    wait_for_backfill 240 || return 1
    wait_for_active 60 || return 1

    flush_pg_stats || return 1
    ceph pg dump pgs

    ERRORS=0
    if [ "$(ceph pg dump pgs | grep "^3.0" | grep +backfill_toofull | wc -l)" != "1" ];
    then
      echo "PG 3.0 should be in backfill_toofull"
      ERRORS="$(expr $ERRORS + 1)"
    fi

    if [ "$(ceph pg dump pgs | grep "^2.0" | grep active+clean | wc -l)" != "1" ];
    then
      echo "PG 2.0 should have completed backfill"
      ERRORS="$(expr $ERRORS + 1)"
    fi

    if [ $ERRORS != "0" ];
    then
      return 1
    fi

    delete_pool fillpool
    for i in $(seq 1 $pools)
    do
      delete_pool "${poolprefix}$i"
    done
    kill_daemons $dir || return 1
    ! grep -q "num_bytes mismatch" $dir/osd.*.log || return 1
}

# Make sure that the amount of bytes already on the replica doesn't
# cause an out of space condition
#
# Create 1 pool and write 4K * 600 objects
# Remove 25% (150) of the objects with one OSD down (noout set)
# Increase the size of the remaining 75% (450) of the objects to 6K
# Bring back down OSD
# The pool should go active+clean
function TEST_backfill_grow() {
    local dir=$1
    local poolname="test"
    local OSDS=3

    run_mon $dir a || return 1
    run_mgr $dir x || return 1

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd || return 1
    done

    ceph osd set-backfillfull-ratio .85

    create_pool $poolname 1 1
    ceph osd pool set $poolname size 3
    sleep 5

    wait_for_clean || return 1

    dd if=/dev/urandom of=${dir}/4kdata bs=1k count=4
    for i in $(seq 1 $objects)
    do
	rados -p $poolname put obj$i $dir/4kdata
    done

    local PG=$(get_pg $poolname obj1)
    # Remember primary during the backfill
    local primary=$(get_primary $poolname obj1)
    local otherosd=$(get_not_primary $poolname obj1)

    ceph osd set noout
    kill_daemons $dir TERM $otherosd || return 1

    rmobjects=$(expr $objects / 4)
    for i in $(seq 1 $rmobjects)
    do
        rados -p $poolname rm obj$i
    done

    dd if=/dev/urandom of=${dir}/6kdata bs=6k count=1
    for i in $(seq $(expr $rmobjects + 1) $objects)
    do
	rados -p $poolname put obj$i $dir/6kdata
    done

    activate_osd $dir $otherosd || return 1

    ceph tell osd.$primary debug kick_recovery_wq 0

    sleep 2

    wait_for_clean || return 1

    delete_pool $poolname
    kill_daemons $dir || return 1
    ! grep -q "num_bytes mismatch" $dir/osd.*.log || return 1
}

# Create a 5 shard EC pool on 6 OSD cluster
# Fill 1 OSD with 2600K of data take that osd down.
# Write the EC pool on 5 OSDs
# Take down 1 (must contain an EC shard)
# Bring up OSD with fill data
# Not enought room to backfill to partially full OSD
function TEST_ec_backfill_simple() {
    local dir=$1
    local EC=$2
    local pools=1
    local OSDS=6
    local k=3
    local m=2
    local ecobjects=$(expr $objects / $k)

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    export CEPH_ARGS

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd || return 1
    done

    ceph osd set-backfillfull-ratio .85
    create_pool fillpool 1 1
    ceph osd pool set fillpool size 1

    # Partially fill an osd
    # We have room for 200 18K replicated objects, if we create 13K objects
    # there is only 3600K - (13K * 200) = 1000K which won't hold
    # a k=3 shard below ((18K / 3) + 4K) * 200 = 2000K
    # Actual usage per shard is 8K * 200 = 1600K because 18K/3 is 6K which
    # rounds to 8K.  The 2000K is the ceiling on the 18K * 200 = 3600K logical
    # bytes in the pool.
    dd if=/dev/urandom of=$dir/datafile bs=1024 count=13
    for o in $(seq 1 $ecobjects)
    do
      rados -p fillpool put obj$o $dir/datafile
    done

    local fillosd=$(get_primary fillpool obj1)
    osd=$(expr $fillosd + 1)
    if [ "$osd" = "$OSDS" ]; then
      osd="0"
    fi

    sleep 5
    kill $(cat $dir/osd.$fillosd.pid)
    ceph osd out osd.$fillosd
    sleep 2
    ceph osd erasure-code-profile set ec-profile k=$k m=$m crush-failure-domain=osd technique=reed_sol_van plugin=jerasure || return 1

    for p in $(seq 1 $pools)
    do
        ceph osd pool create "${poolprefix}$p" 1 1 erasure ec-profile
    done

    # Can't wait for clean here because we created a stale pg
    #wait_for_clean || return 1
    sleep 5

    ceph pg dump pgs

    dd if=/dev/urandom of=$dir/datafile bs=1024 count=18
    for o in $(seq 1 $ecobjects)
    do
      for p in $(seq 1 $pools)
      do
	rados -p "${poolprefix}$p" put obj$o $dir/datafile
      done
    done

    kill $(cat $dir/osd.$osd.pid)
    ceph osd out osd.$osd

    activate_osd $dir $fillosd || return 1
    ceph osd in osd.$fillosd
    sleep 30

    ceph pg dump pgs

    wait_for_backfill 240 || return 1
    wait_for_active 60 || return 1

    ceph pg dump pgs

    ERRORS=0
    if [ "$(ceph pg dump pgs | grep -v "^1.0" | grep +backfill_toofull | wc -l)" != "1" ]; then
      echo "One pool should have been in backfill_toofull"
      ERRORS="$(expr $ERRORS + 1)"
    fi

    if [ $ERRORS != "0" ];
    then
      return 1
    fi

    delete_pool fillpool
    for i in $(seq 1 $pools)
    do
      delete_pool "${poolprefix}$i"
    done
    kill_daemons $dir || return 1
}

function osdlist() {
    local OSDS=$1
    local excludeosd=$2

    osds=""
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      if [ $osd = $excludeosd ];
      then
        continue
      fi
      if [ -n "$osds" ]; then
        osds="${osds} "
      fi
      osds="${osds}${osd}"
    done
    echo $osds
}

# Create a pool with size 1 and fill with data so that only 1 EC shard can fit.
# Write data to 2 EC pools mapped to the same OSDs (excluding filled one)
# Remap the last OSD to partially full OSD on both pools
# The 2 pools should race to backfill.
# One pool goes active+clean
# The other goes acitve+...+backfill_toofull
function TEST_ec_backfill_multi() {
    local dir=$1
    local EC=$2
    local pools=2
    local OSDS=6
    local k=3
    local m=2
    local ecobjects=$(expr $objects / $k)

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    export CEPH_ARGS

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd || return 1
    done

    # This test requires that shards from 2 different pools
    # fit on a given OSD, but both will not fix.  I'm using
    # making the fillosd plus 1 shard use 75% of the space,
    # leaving not enough to be under the 85% set here.
    ceph osd set-backfillfull-ratio .85

    ceph osd set-require-min-compat-client luminous
    create_pool fillpool 1 1
    ceph osd pool set fillpool size 1

    # Partially fill an osd
    # We have room for 200 18K replicated objects, if we create 9K objects
    # there is only 3600K - (9K * 200) = 1800K which will only hold
    # one k=3 shard below ((12K / 3) + 4K) * 200 = 1600K
    # The actual data will be (12K / 3) * 200 = 800K because the extra
    # is the reservation padding for chunking.
    dd if=/dev/urandom of=$dir/datafile bs=1024 count=9
    for o in $(seq 1 $ecobjects)
    do
      rados -p fillpool put obj$o $dir/datafile
    done

    local fillosd=$(get_primary fillpool obj1)
    ceph osd erasure-code-profile set ec-profile k=3 m=2 crush-failure-domain=osd technique=reed_sol_van plugin=jerasure || return 1

    nonfillosds="$(osdlist $OSDS $fillosd)"

    for p in $(seq 1 $pools)
    do
        ceph osd pool create "${poolprefix}$p" 1 1 erasure ec-profile
        ceph osd pg-upmap "$(expr $p + 1).0" $nonfillosds
    done

    # Can't wait for clean here because we created a stale pg
    #wait_for_clean || return 1
    sleep 15

    ceph pg dump pgs

    dd if=/dev/urandom of=$dir/datafile bs=1024 count=12
    for o in $(seq 1 $ecobjects)
    do
      for p in $(seq 1 $pools)
      do
	rados -p "${poolprefix}$p" put obj$o-$p $dir/datafile
      done
    done

    ceph pg dump pgs

    for p in $(seq 1 $pools)
    do
      ceph osd pg-upmap $(expr $p + 1).0 ${nonfillosds% *} $fillosd
    done

    sleep 10

    wait_for_backfill 240 || return 1
    wait_for_active 60 || return 1

    ceph pg dump pgs

    ERRORS=0
    if [ "$(ceph pg dump pgs | grep -v "^1.0" | grep +backfill_toofull | wc -l)" != "1" ];
    then
      echo "One pool should have been in backfill_toofull"
      ERRORS="$(expr $ERRORS + 1)"
    fi

    if [ "$(ceph pg dump pgs | grep -v "^1.0" | grep active+clean | wc -l)" != "1" ];
    then
      echo "One didn't finish backfill"
      ERRORS="$(expr $ERRORS + 1)"
    fi

    if [ $ERRORS != "0" ];
    then
      return 1
    fi

    delete_pool fillpool
    for i in $(seq 1 $pools)
    do
      delete_pool "${poolprefix}$i"
    done
    kill_daemons $dir || return 1
}

# Similar to TEST_ec_backfill_multi but one of the ec pools
# already had some data on the target OSD

# Create a pool with size 1 and fill with data so that only 1 EC shard can fit.
# Write a small amount of data to 1 EC pool that still includes the filled one
# Take down fillosd with noout set
# Write data to 2 EC pools mapped to the same OSDs (excluding filled one)
# Remap the last OSD to partially full OSD on both pools
# The 2 pools should race to backfill.
# One pool goes active+clean
# The other goes acitve+...+backfill_toofull
function SKIP_TEST_ec_backfill_multi_partial() {
    local dir=$1
    local EC=$2
    local pools=2
    local OSDS=5
    local k=3
    local m=2
    local ecobjects=$(expr $objects / $k)
    local lastosd=$(expr $OSDS - 1)

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    export CEPH_ARGS

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd || return 1
    done

    # This test requires that shards from 2 different pools
    # fit on a given OSD, but both will not fix.  I'm using
    # making the fillosd plus 1 shard use 75% of the space,
    # leaving not enough to be under the 85% set here.
    ceph osd set-backfillfull-ratio .85

    ceph osd set-require-min-compat-client luminous
    create_pool fillpool 1 1
    ceph osd pool set fillpool size 1
    # last osd
    ceph osd pg-upmap 1.0 $lastosd

    # Partially fill an osd
    # We have room for 200 18K replicated objects, if we create 9K objects
    # there is only 3600K - (9K * 200) = 1800K which will only hold
    # one k=3 shard below ((12K / 3) + 4K) * 200 = 1600K
    # The actual data will be (12K / 3) * 200 = 800K because the extra
    # is the reservation padding for chunking.
    dd if=/dev/urandom of=$dir/datafile bs=1024 count=9
    for o in $(seq 1 $ecobjects)
    do
      rados -p fillpool put obj$o $dir/datafile
    done

    local fillosd=$(get_primary fillpool obj1)
    ceph osd erasure-code-profile set ec-profile k=3 m=2 crush-failure-domain=osd technique=reed_sol_van plugin=jerasure || return 1

    nonfillosds="$(osdlist $OSDS $fillosd)"

    for p in $(seq 1 $pools)
    do
        ceph osd pool create "${poolprefix}$p" 1 1 erasure ec-profile
        ceph osd pg-upmap "$(expr $p + 1).0" $(seq 0 $lastosd)
    done

    # Can't wait for clean here because we created a stale pg
    #wait_for_clean || return 1
    sleep 15

    ceph pg dump pgs

    dd if=/dev/urandom of=$dir/datafile bs=1024 count=1
    for o in $(seq 1 $ecobjects)
    do
      rados -p "${poolprefix}1" put obj$o-1 $dir/datafile
    done

    for p in $(seq 1 $pools)
    do
        ceph osd pg-upmap "$(expr $p + 1).0" $(seq 0 $(expr $lastosd - 1))
    done
    ceph pg dump pgs

    #ceph osd set noout
    #kill_daemons $dir TERM osd.$lastosd || return 1

    dd if=/dev/urandom of=$dir/datafile bs=1024 count=12
    for o in $(seq 1 $ecobjects)
    do
      for p in $(seq 1 $pools)
      do
	rados -p "${poolprefix}$p" put obj$o-$p $dir/datafile
      done
    done

    ceph pg dump pgs

    # Now backfill lastosd by adding back into the upmap
    for p in $(seq 1 $pools)
    do
        ceph osd pg-upmap "$(expr $p + 1).0" $(seq 0 $lastosd)
    done
    #activate_osd $dir $lastosd || return 1
    #ceph tell osd.0 debug kick_recovery_wq 0

    sleep 10
    ceph pg dump pgs

    wait_for_backfill 240 || return 1
    wait_for_active 60 || return 1

    ceph pg dump pgs

    ERRORS=0
    if [ "$(ceph pg dump pgs | grep -v "^1.0" | grep +backfill_toofull | wc -l)" != "1" ];
    then
      echo "One pool should have been in backfill_toofull"
      ERRORS="$(expr $ERRORS + 1)"
    fi

    if [ "$(ceph pg dump pgs | grep -v "^1.0" | grep active+clean | wc -l)" != "1" ];
    then
      echo "One didn't finish backfill"
      ERRORS="$(expr $ERRORS + 1)"
    fi

    if [ $ERRORS != "0" ];
    then
      return 1
    fi

    delete_pool fillpool
    for i in $(seq 1 $pools)
    do
      delete_pool "${poolprefix}$i"
    done
    kill_daemons $dir || return 1
}

function SKIP_TEST_ec_backfill_multi_partial() {
    local dir=$1
    local EC=$2
    local pools=2
    local OSDS=6

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    export CEPH_ARGS

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd || return 1
    done

    # Below we need to fit 3200K in 3600K which is 88%
    # so set to 90%
    ceph osd set-backfillfull-ratio .90

    ceph osd set-require-min-compat-client luminous
    create_pool fillpool 1 1
    ceph osd pool set fillpool size 1

    # Partially fill an osd
    # We have room for 200 48K ec objects, if we create 4k replicated objects
    # there is 3600K - (4K * 200) = 2800K which won't hold 2 k=3 shard
    # of 200 12K objects which takes ((12K / 3) + 4K) * 200 = 1600K each.
    # On the other OSDs 2 * 1600K = 3200K which is 88% of 3600K.
    dd if=/dev/urandom of=$dir/datafile bs=1024 count=4
    for o in $(seq 1 $objects)
    do
      rados -p fillpool put obj$o $dir/datafile
    done

    local fillosd=$(get_primary fillpool obj1)
    osd=$(expr $fillosd + 1)
    if [ "$osd" = "$OSDS" ]; then
      osd="0"
    fi

    sleep 5
    kill $(cat $dir/osd.$fillosd.pid)
    ceph osd out osd.$fillosd
    sleep 2
    ceph osd erasure-code-profile set ec-profile k=3 m=2 crush-failure-domain=osd technique=reed_sol_van plugin=jerasure || return 1

    for p in $(seq 1 $pools)
    do
        ceph osd pool create "${poolprefix}$p" 1 1 erasure ec-profile
    done

    # Can't wait for clean here because we created a stale pg
    #wait_for_clean || return 1
    sleep 5

    ceph pg dump pgs

    dd if=/dev/urandom of=$dir/datafile bs=1024 count=12
    for o in $(seq 1 $objects)
    do
      for p in $(seq 1 $pools)
      do
	rados -p "${poolprefix}$p" put obj$o $dir/datafile
      done
    done

    #ceph pg map 2.0 --format=json | jq '.'
    kill $(cat $dir/osd.$osd.pid)
    ceph osd out osd.$osd

    _objectstore_tool_nodown $dir $osd --op export --pgid 2.0 --file $dir/export.out
    _objectstore_tool_nodown $dir $fillosd --op import --pgid 2.0 --file $dir/export.out

    activate_osd $dir $fillosd || return 1
    ceph osd in osd.$fillosd
    sleep 15

    wait_for_backfill 240 || return 1
    wait_for_active 60 || return 1

    ERRORS=0
    if [ "$(ceph pg dump pgs | grep -v "^1.0" | grep +backfill_toofull | wc -l)" != "1" ];
    then
      echo "One pool should have been in backfill_toofull"
      ERRORS="$(expr $ERRORS + 1)"
    fi

    if [ "$(ceph pg dump pgs | grep -v "^1.0" | grep active+clean | wc -l)" != "1" ];
    then
      echo "One didn't finish backfill"
      ERRORS="$(expr $ERRORS + 1)"
    fi

    ceph pg dump pgs

    if [ $ERRORS != "0" ];
    then
      return 1
    fi

    delete_pool fillpool
    for i in $(seq 1 $pools)
    do
      delete_pool "${poolprefix}$i"
    done
    kill_daemons $dir || return 1
}

# Create 1 EC pool
# Write 200 12K objects ((12K / 3) + 4K) *200) = 1600K
# Take 1 shard's OSD down (with noout set)
# Remove 50 objects ((12K / 3) + 4k) * 50) = 400K
# Write 150 36K objects (grow 150 objects) 2400K
# 	But there is already 1600K usage so backfill
# 	would be too full if it didn't account for existing data
# Bring back down OSD so it must backfill
# It should go active+clean taking into account data already there
function TEST_ec_backfill_grow() {
    local dir=$1
    local poolname="test"
    local OSDS=6
    local k=3
    local m=2
    local ecobjects=$(expr $objects / $k)

    run_mon $dir a || return 1
    run_mgr $dir x || return 1

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd || return 1
    done

    ceph osd set-backfillfull-ratio .85

    ceph osd set-require-min-compat-client luminous
    ceph osd erasure-code-profile set ec-profile k=$k m=$m crush-failure-domain=osd technique=reed_sol_van plugin=jerasure || return 1
    ceph osd pool create $poolname 1 1 erasure ec-profile

    wait_for_clean || return 1

    dd if=/dev/urandom of=${dir}/12kdata bs=1k count=12
    for i in $(seq 1 $ecobjects)
    do
	rados -p $poolname put obj$i $dir/12kdata
    done

    local PG=$(get_pg $poolname obj1)
    # Remember primary during the backfill
    local primary=$(get_primary $poolname obj1)
    local otherosd=$(get_not_primary $poolname obj1)

    ceph osd set noout
    kill_daemons $dir TERM $otherosd || return 1

    rmobjects=$(expr $ecobjects / 4)
    for i in $(seq 1 $rmobjects)
    do
        rados -p $poolname rm obj$i
    done

    dd if=/dev/urandom of=${dir}/36kdata bs=1k count=36
    for i in $(seq $(expr $rmobjects + 1) $ecobjects)
    do
	rados -p $poolname put obj$i $dir/36kdata
    done

    activate_osd $dir $otherosd || return 1

    ceph tell osd.$primary debug kick_recovery_wq 0

    sleep 2

    wait_for_clean || return 1

    delete_pool $poolname
    kill_daemons $dir || return 1
}

main osd-backfill-space "$@"

# Local Variables:
# compile-command: "make -j4 && ../qa/run-standalone.sh osd-backfill-space.sh"
# End:
