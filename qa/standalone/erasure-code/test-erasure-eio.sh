#!/usr/bin/env bash
#
# Copyright (C) 2015 Red Hat <contact@redhat.com>
#
#
# Author: Kefu Chai <kchai@redhat.com>
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

    export CEPH_MON="127.0.0.1:7112" # git grep '\<7112\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--osd-objectstore=filestore "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        run_mon $dir a || return 1
	run_mgr $dir x || return 1
	create_pool rbd 4 || return 1

        # check that erasure code plugins are preloaded
        CEPH_ARGS='' ceph --admin-daemon $(get_asok_path mon.a) log flush || return 1
        grep 'load: jerasure.*lrc' $dir/mon.a.log || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function setup_osds() {
    local count=$1
    shift

    for id in $(seq 0 $(expr $count - 1)) ; do
        run_osd $dir $id || return 1
    done

    # check that erasure code plugins are preloaded
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.0) log flush || return 1
    grep 'load: jerasure.*lrc' $dir/osd.0.log || return 1
}

function get_state() {
    local pgid=$1
    local sname=state
    ceph --format json pg dump pgs 2>/dev/null | \
        jq -r ".pg_stats | .[] | select(.pgid==\"$pgid\") | .$sname"
}

function create_erasure_coded_pool() {
    local poolname=$1
    shift
    local k=$1
    shift
    local m=$1
    shift

    ceph osd erasure-code-profile set myprofile \
        plugin=jerasure \
        k=$k m=$m \
        crush-failure-domain=osd || return 1
    create_pool $poolname 1 1 erasure myprofile \
        || return 1
    wait_for_clean || return 1
}

function delete_erasure_coded_pool() {
    local poolname=$1
    ceph osd pool delete $poolname $poolname --yes-i-really-really-mean-it
    ceph osd erasure-code-profile rm myprofile
}

function rados_put() {
    local dir=$1
    local poolname=$2
    local objname=${3:-SOMETHING}

    for marker in AAA BBB CCCC DDDD ; do
        printf "%*s" 1024 $marker
    done > $dir/ORIGINAL
    #
    # get and put an object, compare they are equal
    #
    rados --pool $poolname put $objname $dir/ORIGINAL || return 1
}

function rados_get() {
    local dir=$1
    local poolname=$2
    local objname=${3:-SOMETHING}
    local expect=${4:-ok}

    #
    # Expect a failure to get object
    #
    if [ $expect = "fail" ];
    then
        ! rados --pool $poolname get $objname $dir/COPY
        return
    fi
    #
    # get an object, compare with $dir/ORIGINAL
    #
    rados --pool $poolname get $objname $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
    rm $dir/COPY
}


function inject_remove() {
    local pooltype=$1
    shift
    local which=$1
    shift
    local poolname=$1
    shift
    local objname=$1
    shift
    local dir=$1
    shift
    local shard_id=$1
    shift

    local -a initial_osds=($(get_osds $poolname $objname))
    local osd_id=${initial_osds[$shard_id]}
    objectstore_tool $dir $osd_id $objname remove || return 1
}

# Test with an inject error
function rados_put_get_data() {
    local inject=$1
    shift
    local dir=$1
    shift
    local shard_id=$1
    shift
    local arg=$1

    # inject eio to speificied shard
    #
    local poolname=pool-jerasure
    local objname=obj-$inject-$$-$shard_id
    rados_put $dir $poolname $objname || return 1
    inject_$inject ec data $poolname $objname $dir $shard_id || return 1
    rados_get $dir $poolname $objname || return 1

    if [ "$arg" = "recovery" ];
    then
        #
        # take out the last OSD used to store the object,
        # bring it back, and check for clean PGs which means
        # recovery didn't crash the primary.
        #
        local -a initial_osds=($(get_osds $poolname $objname))
        local last_osd=${initial_osds[-1]}
        # Kill OSD
        kill_daemons $dir TERM osd.${last_osd} >&2 < /dev/null || return 1
        ceph osd out ${last_osd} || return 1
        ! get_osds $poolname $objname | grep '\<'${last_osd}'\>' || return 1
        ceph osd in ${last_osd} || return 1
        run_osd $dir ${last_osd} || return 1
        wait_for_clean || return 1
    fi

    shard_id=$(expr $shard_id + 1)
    inject_$inject ec data $poolname $objname $dir $shard_id || return 1
    # Now 2 out of 3 shards get an error, so should fail
    rados_get $dir $poolname $objname fail || return 1
    rm $dir/ORIGINAL
}

# Change the size of speificied shard
#
function set_size() {
    local objname=$1
    shift
    local dir=$1
    shift
    local shard_id=$1
    shift
    local bytes=$1
    shift
    local mode=${1}

    local poolname=pool-jerasure
    local -a initial_osds=($(get_osds $poolname $objname))
    local osd_id=${initial_osds[$shard_id]}
    ceph osd set noout
    if [ "$mode" = "add" ];
    then
      objectstore_tool $dir $osd_id $objname get-bytes $dir/CORRUPT || return 1
      dd if=/dev/urandom bs=$bytes count=1 >> $dir/CORRUPT
    elif [ "$bytes" = "0" ];
    then
      touch $dir/CORRUPT
    else
      dd if=/dev/urandom bs=$bytes count=1 of=$dir/CORRUPT
    fi
    objectstore_tool $dir $osd_id $objname set-bytes $dir/CORRUPT || return 1
    rm -f $dir/CORRUPT
    ceph osd unset noout
}

function rados_get_data_bad_size() {
    local dir=$1
    shift
    local shard_id=$1
    shift
    local bytes=$1
    shift
    local mode=${1:-set}

    local poolname=pool-jerasure
    local objname=obj-size-$$-$shard_id-$bytes
    rados_put $dir $poolname $objname || return 1

    # Change the size of speificied shard
    #
    set_size $objname $dir $shard_id $bytes $mode || return 1

    rados_get $dir $poolname $objname || return 1

    # Leave objname and modify another shard
    shard_id=$(expr $shard_id + 1)
    set_size $objname $dir $shard_id $bytes $mode || return 1
    rados_get $dir $poolname $objname fail || return 1
    rm $dir/ORIGINAL
}

#
# These two test cases try to validate the following behavior:
#  For object on EC pool, if there is one shard having read error (
#  either primary or replica), client can still read object.
#
# If 2 shards have read errors the client will get an error.
#
function TEST_rados_get_subread_eio_shard_0() {
    local dir=$1
    setup_osds 4 || return 1

    local poolname=pool-jerasure
    create_erasure_coded_pool $poolname 2 1 || return 1
    # inject eio on primary OSD (0) and replica OSD (1)
    local shard_id=0
    rados_put_get_data eio $dir $shard_id || return 1
    delete_erasure_coded_pool $poolname
}

function TEST_rados_get_subread_eio_shard_1() {
    local dir=$1
    setup_osds 4 || return 1

    local poolname=pool-jerasure
    create_erasure_coded_pool $poolname 2 1 || return 1
    # inject eio into replicas OSD (1) and OSD (2)
    local shard_id=1
    rados_put_get_data eio $dir $shard_id || return 1
    delete_erasure_coded_pool $poolname
}

# We don't remove the object from the primary because
# that just causes it to appear to be missing

function TEST_rados_get_subread_missing() {
    local dir=$1
    setup_osds 4 || return 1

    local poolname=pool-jerasure
    create_erasure_coded_pool $poolname 2 1 || return 1
    # inject remove into replicas OSD (1) and OSD (2)
    local shard_id=1
    rados_put_get_data remove $dir $shard_id || return 1
    delete_erasure_coded_pool $poolname
}

#
#
# These two test cases try to validate that following behavior:
#  For object on EC pool, if there is one shard which an incorrect
# size this will cause an internal read error, client can still read object.
#
# If 2 shards have incorrect size the client will get an error.
#
function TEST_rados_get_bad_size_shard_0() {
    local dir=$1
    setup_osds 4 || return 1

    local poolname=pool-jerasure
    create_erasure_coded_pool $poolname 2 1 || return 1
    # Set incorrect size into primary OSD (0) and replica OSD (1)
    local shard_id=0
    rados_get_data_bad_size $dir $shard_id 10 || return 1
    rados_get_data_bad_size $dir $shard_id 0 || return 1
    rados_get_data_bad_size $dir $shard_id 256 add || return 1
    delete_erasure_coded_pool $poolname
}

function TEST_rados_get_bad_size_shard_1() {
    local dir=$1
    setup_osds 4 || return 1

    local poolname=pool-jerasure
    create_erasure_coded_pool $poolname 2 1 || return 1
    # Set incorrect size into replicas OSD (1) and OSD (2)
    local shard_id=1
    rados_get_data_bad_size $dir $shard_id 10 || return 1
    rados_get_data_bad_size $dir $shard_id 0 || return 1
    rados_get_data_bad_size $dir $shard_id 256 add || return 1
    delete_erasure_coded_pool $poolname
}

function TEST_rados_get_with_subreadall_eio_shard_0() {
    local dir=$1
    local shard_id=0

    setup_osds 4 || return 1

    local poolname=pool-jerasure
    create_erasure_coded_pool $poolname 2 1 || return 1
    # inject eio on primary OSD (0)
    rados_put_get_data eio $dir $shard_id recovery || return 1

    delete_erasure_coded_pool $poolname
}

function TEST_rados_get_with_subreadall_eio_shard_1() {
    local dir=$1
    local shard_id=1

    setup_osds 4 || return 1

    local poolname=pool-jerasure
    create_erasure_coded_pool $poolname 2 1 || return 1
    # inject eio on replica OSD (1)
    rados_put_get_data eio $dir $shard_id recovery || return 1

    delete_erasure_coded_pool $poolname
}

# Test recovery the object attr read error
function TEST_ec_object_attr_read_error() {
    local dir=$1
    local objname=myobject

    setup_osds 7 || return 1

    local poolname=pool-jerasure
    create_erasure_coded_pool $poolname 3 2 || return 1

    local primary_osd=$(get_primary $poolname $objname)
    # Kill primary OSD
    kill_daemons $dir TERM osd.${primary_osd} >&2 < /dev/null || return 1

    # Write data
    rados_put $dir $poolname $objname || return 1

    # Inject eio, shard 1 is the one read attr
    inject_eio ec mdata $poolname $objname $dir 1 || return 1

    # Restart OSD
    run_osd $dir ${primary_osd} || return 1

    # Cluster should recover this object
    wait_for_clean || return 1

    rados_get $dir $poolname myobject || return 1

    delete_erasure_coded_pool $poolname
}

# Test recovery the first k copies aren't all available
function TEST_ec_single_recovery_error() {
    local dir=$1
    local objname=myobject

    setup_osds 7 || return 1

    local poolname=pool-jerasure
    create_erasure_coded_pool $poolname 3 2 || return 1

    rados_put $dir $poolname $objname || return 1
    inject_eio ec data $poolname $objname $dir 0 || return 1

    local -a initial_osds=($(get_osds $poolname $objname))
    local last_osd=${initial_osds[-1]}
    # Kill OSD
    kill_daemons $dir TERM osd.${last_osd} >&2 < /dev/null || return 1
    ceph osd down ${last_osd} || return 1
    ceph osd out ${last_osd} || return 1

    # Cluster should recover this object
    wait_for_clean || return 1

    rados_get $dir $poolname myobject || return 1

    delete_erasure_coded_pool $poolname
}

# Test recovery when repeated reads are needed due to EIO
function TEST_ec_recovery_multiple_errors() {
    local dir=$1
    local objname=myobject

    setup_osds 9 || return 1

    local poolname=pool-jerasure
    create_erasure_coded_pool $poolname 4 4 || return 1

    rados_put $dir $poolname $objname || return 1
    inject_eio ec data $poolname $objname $dir 0 || return 1
    # first read will try shards 0,1,2 when 0 gets EIO, shard 3 gets
    # tried as well. Make that fail to test multiple-EIO handling.
    inject_eio ec data $poolname $objname $dir 3 || return 1
    inject_eio ec data $poolname $objname $dir 4 || return 1

    local -a initial_osds=($(get_osds $poolname $objname))
    local last_osd=${initial_osds[-1]}
    # Kill OSD
    kill_daemons $dir TERM osd.${last_osd} >&2 < /dev/null || return 1
    ceph osd down ${last_osd} || return 1
    ceph osd out ${last_osd} || return 1

    # Cluster should recover this object
    wait_for_clean || return 1

    rados_get $dir $poolname myobject || return 1

    delete_erasure_coded_pool $poolname
}

# Test recovery when there's only one shard to recover, but multiple
# objects recovering in one RecoveryOp
function TEST_ec_recovery_multiple_objects() {
    local dir=$1
    local objname=myobject

    ORIG_ARGS=$CEPH_ARGS
    CEPH_ARGS+=' --osd-recovery-max-single-start 3 --osd-recovery-max-active 3 '
    setup_osds 7 || return 1
    CEPH_ARGS=$ORIG_ARGS

    local poolname=pool-jerasure
    create_erasure_coded_pool $poolname 3 2 || return 1

    rados_put $dir $poolname test1
    rados_put $dir $poolname test2
    rados_put $dir $poolname test3

    ceph osd out 0 || return 1

    # Cluster should recover these objects all at once
    wait_for_clean || return 1

    rados_get $dir $poolname test1
    rados_get $dir $poolname test2
    rados_get $dir $poolname test3

    delete_erasure_coded_pool $poolname
}

# test multi-object recovery when the one missing shard gets EIO
function TEST_ec_recovery_multiple_objects_eio() {
    local dir=$1
    local objname=myobject

    ORIG_ARGS=$CEPH_ARGS
    CEPH_ARGS+=' --osd-recovery-max-single-start 3 --osd-recovery-max-active 3 '
    setup_osds 7 || return 1
    CEPH_ARGS=$ORIG_ARGS

    local poolname=pool-jerasure
    create_erasure_coded_pool $poolname 3 2 || return 1

    rados_put $dir $poolname test1
    rados_put $dir $poolname test2
    rados_put $dir $poolname test3

    # can't read from this shard anymore
    inject_eio ec data $poolname $objname $dir 0 || return 1
    ceph osd out 0 || return 1

    # Cluster should recover these objects all at once
    wait_for_clean || return 1

    rados_get $dir $poolname test1
    rados_get $dir $poolname test2
    rados_get $dir $poolname test3

    delete_erasure_coded_pool $poolname
}

# Test backfill with unfound object
function TEST_ec_backfill_unfound() {
    local dir=$1
    local objname=myobject
    local lastobj=300
    # Must be between 1 and $lastobj
    local testobj=obj250

    ORIG_ARGS=$CEPH_ARGS
    CEPH_ARGS+=' --osd_min_pg_log_entries=5 --osd_max_pg_log_entries=10'
    setup_osds 5 || return 1
    CEPH_ARGS=$ORIG_ARGS

    local poolname=pool-jerasure
    create_erasure_coded_pool $poolname 3 2 || return 1

    ceph pg dump pgs

    rados_put $dir $poolname $objname || return 1

    local -a initial_osds=($(get_osds $poolname $objname))
    local last_osd=${initial_osds[-1]}
    kill_daemons $dir TERM osd.${last_osd} 2>&2 < /dev/null || return 1
    ceph osd down ${last_osd} || return 1
    ceph osd out ${last_osd} || return 1

    ceph pg dump pgs

    dd if=/dev/urandom of=${dir}/ORIGINAL bs=1024 count=4
    for i in $(seq 1 $lastobj)
    do
      rados --pool $poolname put obj${i} $dir/ORIGINAL || return 1
    done

    inject_eio ec data $poolname $testobj $dir 0 || return 1
    inject_eio ec data $poolname $testobj $dir 1 || return 1

    run_osd $dir ${last_osd} || return 1
    ceph osd in ${last_osd} || return 1

    sleep 15

    for tmp in $(seq 1 100); do
      state=$(get_state 2.0)
      echo $state | grep backfill_unfound
      if [ "$?" = "0" ]; then
        break
      fi
      echo $state
      sleep 1
    done

    ceph pg dump pgs
    ceph pg 2.0 list_unfound | grep -q $testobj || return 1

    # Command should hang because object is unfound
    timeout 5 rados -p $poolname get $testobj $dir/CHECK
    test $? = "124" || return 1

    ceph pg 2.0 mark_unfound_lost delete

    wait_for_clean || return 1

    for i in $(seq 1 $lastobj)
    do
      if [ obj${i} = "$testobj" ]; then
        # Doesn't exist anymore
        ! rados -p $poolname get $testobj $dir/CHECK || return 1
      else
        rados --pool $poolname get obj${i} $dir/CHECK || return 1
        diff -q $dir/ORIGINAL $dir/CHECK || return 1
      fi
    done

    rm -f ${dir}/ORIGINAL ${dir}/CHECK

    delete_erasure_coded_pool $poolname
}

# Test recovery with unfound object
function TEST_ec_recovery_unfound() {
    local dir=$1
    local objname=myobject
    local lastobj=100
    # Must be between 1 and $lastobj
    local testobj=obj75

    ORIG_ARGS=$CEPH_ARGS
    CEPH_ARGS+=' --osd-recovery-max-single-start 3 --osd-recovery-max-active 3 '
    CEPH_ARGS+=' --osd_min_pg_log_entries=5 --osd_max_pg_log_entries=10'
    setup_osds 5 || return 1
    CEPH_ARGS=$ORIG_ARGS

    local poolname=pool-jerasure
    create_erasure_coded_pool $poolname 3 2 || return 1

    ceph pg dump pgs

    rados_put $dir $poolname $objname || return 1

    local -a initial_osds=($(get_osds $poolname $objname))
    local last_osd=${initial_osds[-1]}
    kill_daemons $dir TERM osd.${last_osd} 2>&2 < /dev/null || return 1
    ceph osd down ${last_osd} || return 1
    ceph osd out ${last_osd} || return 1

    ceph pg dump pgs

    dd if=/dev/urandom of=${dir}/ORIGINAL bs=1024 count=4
    for i in $(seq 1 $lastobj)
    do
      rados --pool $poolname put obj${i} $dir/ORIGINAL || return 1
    done

    inject_eio ec data $poolname $testobj $dir 0 || return 1
    inject_eio ec data $poolname $testobj $dir 1 || return 1

    run_osd $dir ${last_osd} || return 1
    ceph osd in ${last_osd} || return 1

    sleep 15

    for tmp in $(seq 1 100); do
      state=$(get_state 2.0)
      echo $state | grep recovery_unfound
      if [ "$?" = "0" ]; then
        break
      fi
      echo "$state "
      sleep 1
    done

    ceph pg dump pgs
    ceph pg 2.0 list_unfound | grep -q $testobj || return 1

    # Command should hang because object is unfound
    timeout 5 rados -p $poolname get $testobj $dir/CHECK
    test $? = "124" || return 1

    ceph pg 2.0 mark_unfound_lost delete

    wait_for_clean || return 1

    for i in $(seq 1 $lastobj)
    do
      if [ obj${i} = "$testobj" ]; then
        # Doesn't exist anymore
        ! rados -p $poolname get $testobj $dir/CHECK || return 1
      else
        rados --pool $poolname get obj${i} $dir/CHECK || return 1
        diff -q $dir/ORIGINAL $dir/CHECK || return 1
      fi
    done

    rm -f ${dir}/ORIGINAL ${dir}/CHECK

    delete_erasure_coded_pool $poolname
}

main test-erasure-eio "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/erasure-code/test-erasure-eio.sh"
# End:
