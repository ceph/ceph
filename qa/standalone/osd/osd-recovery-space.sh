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

    export CEPH_MON="127.0.0.1:7221" # git grep '\<7221\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
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


function wait_for_recovery_toofull() {
    local timeout=$1
    wait_for_state recovery_toofull $timeout
}


# Create 1 pools with size 1
# set ful-ratio to 50%
# Write data 600 5K (3000K)
# Inject fake_statfs_for_testing to 3600K (83% full)
# Incresase the pool size to 2
# The pool shouldn't have room to recovery
function TEST_recovery_test_simple() {
    local dir=$1
    local pools=1
    local OSDS=2

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    export CEPH_ARGS

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd || return 1
    done

    ceph osd set-nearfull-ratio .40
    ceph osd set-backfillfull-ratio .45
    ceph osd set-full-ratio .50

    for p in $(seq 1 $pools)
    do
      create_pool "${poolprefix}$p" 1 1
      ceph osd pool set "${poolprefix}$p" size 1
    done

    wait_for_clean || return 1

    dd if=/dev/urandom of=$dir/datafile bs=1024 count=5
    for o in $(seq 1 $objects)
    do
      rados -p "${poolprefix}$p" put obj$o $dir/datafile
    done

    for o in $(seq 0 $(expr $OSDS - 1))
    do
      ceph tell osd.$o injectargs '--fake_statfs_for_testing 3686400' || return 1
    done
    sleep 5

    ceph pg dump pgs

    for p in $(seq 1 $pools)
    do
      ceph osd pool set "${poolprefix}$p" size 2
    done

    # If this times out, we'll detected errors below
    wait_for_recovery_toofull 30

    ERRORS=0
    if [ "$(ceph pg dump pgs | grep +recovery_toofull | wc -l)" != "1" ];
    then
      echo "One pool should have been in recovery_toofull"
      ERRORS="$(expr $ERRORS + 1)"
    fi

    ceph pg dump pgs
    ceph status
    ceph status --format=json-pretty > $dir/stat.json

    eval SEV=$(jq '.health.checks.PG_RECOVERY_FULL.severity' $dir/stat.json)
    if [ "$SEV" != "HEALTH_ERR" ]; then
      echo "PG_RECOVERY_FULL severity $SEV not HEALTH_ERR"
      ERRORS="$(expr $ERRORS + 1)"
    fi
    eval MSG=$(jq '.health.checks.PG_RECOVERY_FULL.summary.message' $dir/stat.json)
    if [ "$MSG" != "Full OSDs blocking recovery: 1 pg recovery_toofull" ]; then
      echo "PG_RECOVERY_FULL message '$MSG' mismatched"
      ERRORS="$(expr $ERRORS + 1)"
    fi
    rm -f $dir/stat.json

    if [ $ERRORS != "0" ];
    then
      return 1
    fi

    for i in $(seq 1 $pools)
    do
      delete_pool "${poolprefix}$i"
    done
    kill_daemons $dir || return 1
}


main osd-recovery-space "$@"

# Local Variables:
# compile-command: "make -j4 && ../qa/run-standalone.sh osd-recovery-space.sh"
# End:
