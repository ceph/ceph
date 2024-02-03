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

    # This should multiple of 6
    export loglen=12
    export divisor=3
    export trim=$(expr $loglen / 2)
    export DIVERGENT_WRITE=$(expr $trim / $divisor)
    export DIVERGENT_REMOVE=$(expr $trim / $divisor)
    export DIVERGENT_CREATE=$(expr $trim / $divisor)
    export poolname=test
    export testobjects=100
    # Fix port????
    export CEPH_MON="127.0.0.1:7115" # git grep '\<7115\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    # so we will not force auth_log_shard to be acting_primary
    CEPH_ARGS+="--osd_force_auth_primary_missing_objects=1000000 "
    CEPH_ARGS+="--osd_debug_pg_log_writeout=true "
    CEPH_ARGS+="--osd_min_pg_log_entries=$loglen --osd_max_pg_log_entries=$loglen --osd_pg_log_trim_min=$trim "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}


# Special case divergence test
#	Test handling of divergent entries with prior_version
#	prior to log_tail
# 	based on qa/tasks/divergent_prior.py
function TEST_divergent() {
    local dir=$1

    # something that is always there
    local dummyfile='/etc/fstab'
    local dummyfile2='/etc/resolv.conf'

    local num_osds=3
    local osds="$(seq 0 $(expr $num_osds - 1))"
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for i in $osds
    do
      run_osd $dir $i || return 1
    done

    ceph osd set noout
    ceph osd set noin
    ceph osd set nodown
    create_pool $poolname 1 1
    ceph osd pool set $poolname size 3
    ceph osd pool set $poolname min_size 2

    flush_pg_stats || return 1
    wait_for_clean || return 1

    # determine primary
    local divergent="$(ceph pg dump pgs --format=json | jq '.pg_stats[0].up_primary')"
    echo "primary and soon to be divergent is $divergent"
    ceph pg dump pgs
    local non_divergent=""
    for i in $osds
    do
      if [ "$i" = "$divergent" ]; then
	  continue
      fi
      non_divergent="$non_divergent $i"
    done

    echo "writing initial objects"
    # write a bunch of objects
    for i in $(seq 1 $testobjects)
    do
      rados -p $poolname put existing_$i $dummyfile
    done

    WAIT_FOR_CLEAN_TIMEOUT=20 wait_for_clean

    local pgid=$(get_pg $poolname existing_1)

    # blackhole non_divergent
    echo "blackholing osds $non_divergent"
    ceph pg dump pgs
    for i in $non_divergent
    do
      CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${i}) config set objectstore_blackhole 1
    done

    local case5=$testobjects
    local case3=$(expr $testobjects - 1)
    # Write some soon to be divergent
    echo 'writing divergent object'
    rados -p $poolname put existing_$case5 $dummyfile &
    echo 'create missing divergent object'
    inject_eio rep data $poolname existing_$case3 $dir 0 || return 1
    rados -p $poolname get existing_$case3 $dir/existing &
    sleep 10
    killall -9 rados

    # kill all the osds but leave divergent in
    echo 'killing all the osds'
    ceph pg dump pgs
    kill_daemons $dir KILL osd || return 1
    for i in $osds
    do
      ceph osd down osd.$i
    done
    for i in $non_divergent
    do
      ceph osd out osd.$i
    done

    # bring up non-divergent
    echo "bringing up non_divergent $non_divergent"
    ceph pg dump pgs
    for i in $non_divergent
    do
      activate_osd $dir $i || return 1
    done
    for i in $non_divergent
    do
      ceph osd in osd.$i
    done

    WAIT_FOR_CLEAN_TIMEOUT=20 wait_for_clean

    # write 1 non-divergent object (ensure that old divergent one is divergent)
    objname="existing_$(expr $DIVERGENT_WRITE + $DIVERGENT_REMOVE)"
    echo "writing non-divergent object $objname"
    ceph pg dump pgs
    rados -p $poolname put $objname $dummyfile2

    # ensure no recovery of up osds first
    echo 'delay recovery'
    ceph pg dump pgs
    for i in $non_divergent
    do
      CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${i}) set_recovery_delay 100000
    done

    # bring in our divergent friend
    echo "revive divergent $divergent"
    ceph pg dump pgs
    ceph osd set noup
    activate_osd $dir $divergent
    sleep 5

    echo 'delay recovery divergent'
    ceph pg dump pgs
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${divergent}) set_recovery_delay 100000

    ceph osd unset noup

    wait_for_osd up 0
    wait_for_osd up 1
    wait_for_osd up 2

    ceph pg dump pgs
    echo 'wait for peering'
    ceph pg dump pgs
    rados -p $poolname put foo $dummyfile

    echo "killing divergent $divergent"
    ceph pg dump pgs
    kill_daemons $dir KILL osd.$divergent
    #_objectstore_tool_nodown $dir $divergent --op log --pgid $pgid
    echo "reviving divergent $divergent"
    ceph pg dump pgs
    activate_osd $dir $divergent

    sleep 20

    echo "allowing recovery"
    ceph pg dump pgs
    # Set osd_recovery_delay_start back to 0 and kick the queue
    for i in $osds
    do
	 ceph tell osd.$i debug kick_recovery_wq 0
    done

    echo 'reading divergent objects'
    ceph pg dump pgs
    for i in $(seq 1 $(expr $DIVERGENT_WRITE + $DIVERGENT_REMOVE))
    do
      rados -p $poolname get existing_$i $dir/existing || return 1
    done
    rm -f $dir/existing

    grep _merge_object_divergent_entries $(find $dir -name '*osd*log')
    # Check for _merge_object_divergent_entries for case #5
    if ! grep -q "_merge_object_divergent_entries.*cannot roll back, removing and adding to missing" $(find $dir -name '*osd*log')
    then
	    echo failure
	    return 1
    fi
    echo "success"

    delete_pool $poolname
    kill_daemons $dir || return 1
}

function TEST_divergent_ec() {
    local dir=$1

    # something that is always there
    local dummyfile='/etc/fstab'
    local dummyfile2='/etc/resolv.conf'

    local num_osds=3
    local osds="$(seq 0 $(expr $num_osds - 1))"
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for i in $osds
    do
      run_osd $dir $i || return 1
    done

    ceph osd set noout
    ceph osd set noin
    ceph osd set nodown
    create_ec_pool $poolname true k=2 m=1 || return 1

    flush_pg_stats || return 1
    wait_for_clean || return 1

    # determine primary
    local divergent="$(ceph pg dump pgs --format=json | jq '.pg_stats[0].up_primary')"
    echo "primary and soon to be divergent is $divergent"
    ceph pg dump pgs
    local non_divergent=""
    for i in $osds
    do
      if [ "$i" = "$divergent" ]; then
	  continue
      fi
      non_divergent="$non_divergent $i"
    done

    echo "writing initial objects"
    # write a bunch of objects
    for i in $(seq 1 $testobjects)
    do
      rados -p $poolname put existing_$i $dummyfile
    done

    WAIT_FOR_CLEAN_TIMEOUT=20 wait_for_clean

    local pgid=$(get_pg $poolname existing_1)

    # blackhole non_divergent
    echo "blackholing osds $non_divergent"
    ceph pg dump pgs
    for i in $non_divergent
    do
      CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${i}) config set objectstore_blackhole 1
    done

    # Write some soon to be divergent
    echo 'writing divergent object'
    rados -p $poolname put existing_$testobjects $dummyfile2 &
    sleep 1
    rados -p $poolname put existing_$testobjects $dummyfile &
    rados -p $poolname mksnap snap1
    rados -p $poolname put existing_$(expr $testobjects - 1) $dummyfile &
    sleep 10
    killall -9 rados

    # kill all the osds but leave divergent in
    echo 'killing all the osds'
    ceph pg dump pgs
    kill_daemons $dir KILL osd || return 1
    for i in $osds
    do
      ceph osd down osd.$i
    done
    for i in $non_divergent
    do
      ceph osd out osd.$i
    done

    # bring up non-divergent
    echo "bringing up non_divergent $non_divergent"
    ceph pg dump pgs
    for i in $non_divergent
    do
      activate_osd $dir $i || return 1
    done
    for i in $non_divergent
    do
      ceph osd in osd.$i
    done

    sleep 5
    #WAIT_FOR_CLEAN_TIMEOUT=20 wait_for_clean

    # write 1 non-divergent object (ensure that old divergent one is divergent)
    objname="existing_$(expr $DIVERGENT_WRITE + $DIVERGENT_REMOVE)"
    echo "writing non-divergent object $objname"
    ceph pg dump pgs
    rados -p $poolname put $objname $dummyfile2

    WAIT_FOR_CLEAN_TIMEOUT=20 wait_for_clean

    # Dump logs
    for i in $non_divergent
    do
      kill_daemons $dir KILL osd.$i || return 1
      _objectstore_tool_nodown $dir $i --op log --pgid $pgid
      activate_osd $dir $i || return 1
    done
    _objectstore_tool_nodown $dir $divergent --op log --pgid $pgid

    WAIT_FOR_CLEAN_TIMEOUT=20 wait_for_clean

    # ensure no recovery of up osds first
    echo 'delay recovery'
    ceph pg dump pgs
    for i in $non_divergent
    do
      CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${i}) set_recovery_delay 100000
    done

    # bring in our divergent friend
    echo "revive divergent $divergent"
    ceph pg dump pgs
    ceph osd set noup
    activate_osd $dir $divergent
    sleep 5

    echo 'delay recovery divergent'
    ceph pg dump pgs
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${divergent}) set_recovery_delay 100000

    ceph osd unset noup

    wait_for_osd up 0
    wait_for_osd up 1
    wait_for_osd up 2

    ceph pg dump pgs
    echo 'wait for peering'
    ceph pg dump pgs
    rados -p $poolname put foo $dummyfile

    echo "killing divergent $divergent"
    ceph pg dump pgs
    kill_daemons $dir KILL osd.$divergent
    #_objectstore_tool_nodown $dir $divergent --op log --pgid $pgid
    echo "reviving divergent $divergent"
    ceph pg dump pgs
    activate_osd $dir $divergent

    sleep 20

    echo "allowing recovery"
    ceph pg dump pgs
    # Set osd_recovery_delay_start back to 0 and kick the queue
    for i in $osds
    do
	 ceph tell osd.$i debug kick_recovery_wq 0
    done

    echo 'reading divergent objects'
    ceph pg dump pgs
    for i in $(seq 1 $(expr $DIVERGENT_WRITE + $DIVERGENT_REMOVE))
    do
      rados -p $poolname get existing_$i $dir/existing || return 1
    done
    rm -f $dir/existing

    grep _merge_object_divergent_entries $(find $dir -name '*osd*log')
    # Check for _merge_object_divergent_entries for case #3
    # XXX: Not reproducing this case
#    if ! grep -q "_merge_object_divergent_entries.* missing, .* adjusting" $(find $dir -name '*osd*log')
#    then
#	echo failure
#	return 1
#    fi
    # Check for _merge_object_divergent_entries for case #4
    if ! grep -q "_merge_object_divergent_entries.*rolled back" $(find $dir -name '*osd*log')
    then
	echo failure
	return 1
    fi
    echo "success"

    delete_pool $poolname
    kill_daemons $dir || return 1
}

# Special case divergence test with ceph-objectstore-tool export/remove/import
# 	Test handling of divergent entries with prior_version
# 	prior to log_tail and a ceph-objectstore-tool export/import
# 	based on qa/tasks/divergent_prior2.py
function TEST_divergent_2() {
    local dir=$1

    # something that is always there
    local dummyfile='/etc/fstab'
    local dummyfile2='/etc/resolv.conf'

    local num_osds=3
    local osds="$(seq 0 $(expr $num_osds - 1))"
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for i in $osds
    do
      run_osd $dir $i || return 1
    done

    ceph osd set noout
    ceph osd set noin
    ceph osd set nodown
    create_pool $poolname 1 1
    ceph osd pool set $poolname size 3
    ceph osd pool set $poolname min_size 2

    flush_pg_stats || return 1
    wait_for_clean || return 1

    # determine primary
    local divergent="$(ceph pg dump pgs --format=json | jq '.pg_stats[0].up_primary')"
    echo "primary and soon to be divergent is $divergent"
    ceph pg dump pgs
    local non_divergent=""
    for i in $osds
    do
      if [ "$i" = "$divergent" ]; then
	  continue
      fi
      non_divergent="$non_divergent $i"
    done

    echo "writing initial objects"
    # write a bunch of objects
    for i in $(seq 1 $testobjects)
    do
      rados -p $poolname put existing_$i $dummyfile
    done

    WAIT_FOR_CLEAN_TIMEOUT=20 wait_for_clean

    local pgid=$(get_pg $poolname existing_1)

    # blackhole non_divergent
    echo "blackholing osds $non_divergent"
    ceph pg dump pgs
    for i in $non_divergent
    do
      CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${i}) config set objectstore_blackhole 1
    done

    # Do some creates to hit case 2
    echo 'create new divergent objects'
    for i in $(seq 1 $DIVERGENT_CREATE)
    do
      rados -p $poolname create newobject_$i &
    done
    # Write some soon to be divergent
    echo 'writing divergent objects'
    for i in $(seq 1 $DIVERGENT_WRITE)
    do
      rados -p $poolname put existing_$i $dummyfile2 &
    done
    # Remove some soon to be divergent
    echo 'remove divergent objects'
    for i in $(seq 1 $DIVERGENT_REMOVE)
    do
      rmi=$(expr $i + $DIVERGENT_WRITE)
      rados -p $poolname rm existing_$rmi &
    done
    sleep 10
    killall -9 rados

    # kill all the osds but leave divergent in
    echo 'killing all the osds'
    ceph pg dump pgs
    kill_daemons $dir KILL osd || return 1
    for i in $osds
    do
      ceph osd down osd.$i
    done
    for i in $non_divergent
    do
      ceph osd out osd.$i
    done

    # bring up non-divergent
    echo "bringing up non_divergent $non_divergent"
    ceph pg dump pgs
    for i in $non_divergent
    do
      activate_osd $dir $i || return 1
    done
    for i in $non_divergent
    do
      ceph osd in osd.$i
    done

    WAIT_FOR_CLEAN_TIMEOUT=20 wait_for_clean

    # write 1 non-divergent object (ensure that old divergent one is divergent)
    objname="existing_$(expr $DIVERGENT_WRITE + $DIVERGENT_REMOVE)"
    echo "writing non-divergent object $objname"
    ceph pg dump pgs
    rados -p $poolname put $objname $dummyfile2

    WAIT_FOR_CLEAN_TIMEOUT=20 wait_for_clean

    # ensure no recovery of up osds first
    echo 'delay recovery'
    ceph pg dump pgs
    for i in $non_divergent
    do
      CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${i}) set_recovery_delay 100000
    done

    # bring in our divergent friend
    echo "revive divergent $divergent"
    ceph pg dump pgs
    ceph osd set noup
    activate_osd $dir $divergent
    sleep 5

    echo 'delay recovery divergent'
    ceph pg dump pgs
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${divergent}) set_recovery_delay 100000

    ceph osd unset noup

    wait_for_osd up 0
    wait_for_osd up 1
    wait_for_osd up 2

    ceph pg dump pgs
    echo 'wait for peering'
    ceph pg dump pgs
    rados -p $poolname put foo $dummyfile

    # At this point the divergent_priors should have been detected

    echo "killing divergent $divergent"
    ceph pg dump pgs
    kill_daemons $dir KILL osd.$divergent

    # export a pg
    expfile=$dir/exp.$$.out
    _objectstore_tool_nodown $dir $divergent --op export-remove --pgid $pgid --file $expfile
    _objectstore_tool_nodown $dir $divergent --op import --file $expfile

    echo "reviving divergent $divergent"
    ceph pg dump pgs
    activate_osd $dir $divergent
    wait_for_osd up $divergent

    sleep 20
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${divergent}) dump_ops_in_flight

    echo "allowing recovery"
    ceph pg dump pgs
    # Set osd_recovery_delay_start back to 0 and kick the queue
    for i in $osds
    do
	 ceph tell osd.$i debug kick_recovery_wq 0
    done

    echo 'reading divergent objects'
    ceph pg dump pgs
    for i in $(seq 1 $(expr $DIVERGENT_WRITE + $DIVERGENT_REMOVE))
    do
      rados -p $poolname get existing_$i $dir/existing || return 1
    done
    for i in $(seq 1 $DIVERGENT_CREATE)
    do
      rados -p $poolname get newobject_$i $dir/existing
    done
    rm -f $dir/existing

    grep _merge_object_divergent_entries $(find $dir -name '*osd*log')
    # Check for _merge_object_divergent_entries for case #1
    if ! grep -q "_merge_object_divergent_entries: more recent entry found:" $(find $dir -name '*osd*log')
    then
	    echo failure
	    return 1
    fi
    # Check for _merge_object_divergent_entries for case #2
    if ! grep -q "_merge_object_divergent_entries.*prior_version or op type indicates creation" $(find $dir -name '*osd*log')
    then
	    echo failure
	    return 1
    fi
    echo "success"

    rm $dir/$expfile

    delete_pool $poolname
    kill_daemons $dir || return 1
}

# this is the same as case _2 above, except we enable pg autoscaling in order
# to reproduce https://tracker.ceph.com/issues/41816
function TEST_divergent_3() {
    local dir=$1

    # something that is always there
    local dummyfile='/etc/fstab'
    local dummyfile2='/etc/resolv.conf'

    local num_osds=3
    local osds="$(seq 0 $(expr $num_osds - 1))"
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for i in $osds
    do
      run_osd $dir $i || return 1
    done

    ceph osd set noout
    ceph osd set noin
    ceph osd set nodown
    create_pool $poolname 1 1
    ceph osd pool set $poolname size 3
    ceph osd pool set $poolname min_size 2

    # reproduce https://tracker.ceph.com/issues/41816
    ceph osd pool set $poolname pg_autoscale_mode on

    divergent=-1
    start_time=$(date +%s)
    max_duration=300

    while [ "$divergent" -le -1 ]
      do
        flush_pg_stats || return 1
        wait_for_clean || return 1

        # determine primary
        divergent="$(ceph pg dump pgs --format=json | jq '.pg_stats[0].up_primary')"
        echo "primary and soon to be divergent is $divergent"
        ceph pg dump pgs

        current_time=$(date +%s)
        elapsed_time=$(expr $current_time - $start_time)
        if [ "$elapsed_time" -gt "$max_duration" ]; then
          echo "timed out waiting for divergent"
          return 1
        fi
    done

    local non_divergent=""
    for i in $osds
    do
      if [ "$i" = "$divergent" ]; then
	  continue
      fi
      non_divergent="$non_divergent $i"
    done

    echo "writing initial objects"
    # write a bunch of objects
    for i in $(seq 1 $testobjects)
    do
      rados -p $poolname put existing_$i $dummyfile
    done

    WAIT_FOR_CLEAN_TIMEOUT=20 wait_for_clean

    local pgid=$(get_pg $poolname existing_1)

    # blackhole non_divergent
    echo "blackholing osds $non_divergent"
    ceph pg dump pgs
    for i in $non_divergent
    do
      CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${i}) config set objectstore_blackhole 1
    done

    # Do some creates to hit case 2
    echo 'create new divergent objects'
    for i in $(seq 1 $DIVERGENT_CREATE)
    do
      rados -p $poolname create newobject_$i &
    done
    # Write some soon to be divergent
    echo 'writing divergent objects'
    for i in $(seq 1 $DIVERGENT_WRITE)
    do
      rados -p $poolname put existing_$i $dummyfile2 &
    done
    # Remove some soon to be divergent
    echo 'remove divergent objects'
    for i in $(seq 1 $DIVERGENT_REMOVE)
    do
      rmi=$(expr $i + $DIVERGENT_WRITE)
      rados -p $poolname rm existing_$rmi &
    done
    sleep 10
    killall -9 rados

    # kill all the osds but leave divergent in
    echo 'killing all the osds'
    ceph pg dump pgs
    kill_daemons $dir KILL osd || return 1
    for i in $osds
    do
      ceph osd down osd.$i
    done
    for i in $non_divergent
    do
      ceph osd out osd.$i
    done

    # bring up non-divergent
    echo "bringing up non_divergent $non_divergent"
    ceph pg dump pgs
    for i in $non_divergent
    do
      activate_osd $dir $i || return 1
    done
    for i in $non_divergent
    do
      ceph osd in osd.$i
    done

    WAIT_FOR_CLEAN_TIMEOUT=20 wait_for_clean

    # write 1 non-divergent object (ensure that old divergent one is divergent)
    objname="existing_$(expr $DIVERGENT_WRITE + $DIVERGENT_REMOVE)"
    echo "writing non-divergent object $objname"
    ceph pg dump pgs
    rados -p $poolname put $objname $dummyfile2

    WAIT_FOR_CLEAN_TIMEOUT=20 wait_for_clean

    # ensure no recovery of up osds first
    echo 'delay recovery'
    ceph pg dump pgs
    for i in $non_divergent
    do
      CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${i}) set_recovery_delay 100000
    done

    # bring in our divergent friend
    echo "revive divergent $divergent"
    ceph pg dump pgs
    ceph osd set noup
    activate_osd $dir $divergent
    sleep 5

    echo 'delay recovery divergent'
    ceph pg dump pgs
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${divergent}) set_recovery_delay 100000

    ceph osd unset noup

    wait_for_osd up 0
    wait_for_osd up 1
    wait_for_osd up 2

    ceph pg dump pgs
    echo 'wait for peering'
    ceph pg dump pgs
    rados -p $poolname put foo $dummyfile

    # At this point the divergent_priors should have been detected

    echo "killing divergent $divergent"
    ceph pg dump pgs
    kill_daemons $dir KILL osd.$divergent

    # export a pg
    expfile=$dir/exp.$$.out
    _objectstore_tool_nodown $dir $divergent --op export-remove --pgid $pgid --file $expfile
    _objectstore_tool_nodown $dir $divergent --op import --file $expfile

    echo "reviving divergent $divergent"
    ceph pg dump pgs
    activate_osd $dir $divergent
    wait_for_osd up $divergent

    sleep 20
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${divergent}) dump_ops_in_flight

    echo "allowing recovery"
    ceph pg dump pgs
    # Set osd_recovery_delay_start back to 0 and kick the queue
    for i in $osds
    do
	 ceph tell osd.$i debug kick_recovery_wq 0
    done

    echo 'reading divergent objects'
    ceph pg dump pgs
    for i in $(seq 1 $(expr $DIVERGENT_WRITE + $DIVERGENT_REMOVE))
    do
      rados -p $poolname get existing_$i $dir/existing || return 1
    done
    for i in $(seq 1 $DIVERGENT_CREATE)
    do
      rados -p $poolname get newobject_$i $dir/existing
    done
    rm -f $dir/existing

    grep _merge_object_divergent_entries $(find $dir -name '*osd*log')
    # Check for _merge_object_divergent_entries for case #1
    if ! grep -q "_merge_object_divergent_entries: more recent entry found:" $(find $dir -name '*osd*log')
    then
	    echo failure
	    return 1
    fi
    # Check for _merge_object_divergent_entries for case #2
    if ! grep -q "_merge_object_divergent_entries.*prior_version or op type indicates creation" $(find $dir -name '*osd*log')
    then
	    echo failure
	    return 1
    fi
    echo "success"

    rm $dir/$expfile

    delete_pool $poolname
    kill_daemons $dir || return 1
}


main divergent-priors "$@"

# Local Variables:
# compile-command: "make -j4 && ../qa/run-standalone.sh divergent-priors.sh"
# End:
