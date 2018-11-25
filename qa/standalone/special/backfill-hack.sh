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

function create_ec_pool() {
    local pool_name=$1
    shift
    local allow_overwrites=$1
    shift

    ceph osd erasure-code-profile set myprofile crush-failure-domain=osd "$@" || return 1

    create_pool "$poolname" 1 1 erasure myprofile || return 1

    if [ "$allow_overwrites" = "true" ]; then
        ceph osd pool set "$poolname" allow_ec_overwrites true || return 1
    fi

    wait_for_clean || return 1
    return 0
}

function TEST_scrub_erasure_decode_error() {
    local dir=$1
    local allow_overwrites=$2
    local poolname=ecpool
    local total_objs=100
    local test_objs=50
    local allow_overwrites=true
    local OSDS=4

    setup $dir || return 1
    CEPH_ARGS+="--osd_ignore_recover_bad_ec_objects=1 "
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in $(seq 0 $(expr $OSDS - 1)) ; do
        if [ "$allow_overwrites" = "true" ]; then
            run_osd_bluestore $dir $id || return 1
        else
            run_osd $dir $id || return 1
        fi
    done

    create_ec_pool $poolname $allow_overwrites k=2 m=1 stripe_unit=2K --force || return 1
    wait_for_clean || return 1
    ceph osd pool set $poolname min_size 2

    ceph osd set noout
    ceph osd set nobackfill
    ceph osd set noscrub || return 1
    ceph osd set nodeep-scrub || return 1
    dd if=/dev/urandom bs=2k count=4 of=$dir/ORIGINAL

    for i in $(seq 1 $total_objs) ; do
        objname=EOBJ${i}
        rados --pool $poolname put $objname $dir/ORIGINAL || return 1
    done

    testobj=EOBJ1
    local primary=$(get_primary $poolname $testobj)
    local corruptosd=$(ceph --format json osd map $poolname $testobj 2>/dev/null | jq ".acting | map(select (. != $primary)) | .[0]")
    local downosd=$(ceph --format json osd map $poolname $testobj 2>/dev/null | jq ".acting | map(select (. != $primary)) | .[1]")

    # Get hinfo_key for ORIGINAL data
    #objectstore_tool $dir $downosd $testobj dump
    objectstore_tool $dir $downosd $testobj get-attr _ > $dir/attr
    objectstore_tool $dir $downosd $testobj get-attr hinfo_key > $dir/hinfo

    dd if=/dev/urandom bs=2k count=4 of=$dir/OTHER
    for i in $(seq 1 $total_objs) ; do
        objname=EOBJ${i}
        rados --pool $poolname put $objname $dir/OTHER || return 1
    done
    for id in $(seq 0 $(expr $OSDS - 1)) ; do
      kill $(cat $dir/osd.${id}.pid)
    done
    sleep 5
    for i in $(seq 1 $test_objs) ; do
      testobj=EOBJ${i}
      local osd_data=$dir/$corruptosd

      #ceph-objectstore-tool --data-path $dir/$downosd $testobj dump
      ceph-objectstore-tool --data-path $osd_data $testobj set-bytes $dir/ORIGINAL || return 1
      ceph-objectstore-tool --data-path $osd_data $testobj set-attr _ $dir/attr || return 1
      ceph-objectstore-tool --data-path $osd_data $testobj set-attr hinfo_key $dir/hinfo || return 1
      #ceph-objectstore-tool --data-path $osd_data $testobj dump
    done
    rm -f $dir/hinfo $dir/attr
    for id in $(seq 0 $(expr $OSDS - 1)) ; do
      activate_osd $dir $id
    done
    wait_for_clean || return 1

    ceph osd unset noout
    kill $(cat $dir/osd.${downosd}.pid)
    ceph osd down osd.$downosd
    ceph osd out osd.$downosd
    ceph osd unset nobackfill
    ceph tell osd.$(get_primary $poolname $testobj) debug kick_recovery_wq 0

    sleep 10
    ceph pg dump pgs
    wait_for_clean || return 1
    ceph pg dump pgs
    grep "Ignoring decode" $dir/osd.${primary}.log

    for i in $(seq 1 $test_objs) ; do
      testobj=EOBJ${i}
      rados -p $poolname rm $testobj
    done

    activate_osd $dir $downosd
    ceph osd in osd.$downosd
    wait_for_clean || return 1

    local PG=$(get_pg $poolname EOBJ5)
    pg_scrub $PG || return 1
    ceph pg dump pgs
    rados list-inconsistent-obj $PG | jq '.'
    pg_deep_scrub $PG || return 1
    ceph pg dump pgs
    rados list-inconsistent-obj $PG | jq '.'
    repair $PG
    sleep 15
    ceph pg dump pgs
    rados list-inconsistent-obj $PG | jq '.'

    ceph pg dump pgs
    return 0
}

main backfill-hack "$@"

# Local Variables:
# compile-command: "make -j4 && ../qa/run-standalone.sh backfill-hack.sh"
# End:
