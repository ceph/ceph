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

MAX_SCRUBS=4
SCRUB_SLEEP=2
POOL_SIZE=3

function run() {
    local dir=$1
    shift
    local SLEEP=0
    local CHUNK_MAX=5

    export CEPH_MON="127.0.0.1:7184" # git grep '\<7184\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--osd_max_scrubs=$MAX_SCRUBS "
    CEPH_ARGS+="--osd_scrub_sleep=$SLEEP "
    CEPH_ARGS+="--osd_scrub_chunk_max=$CHUNK_MAX "
    CEPH_ARGS+="--osd_scrub_sleep=$SCRUB_SLEEP "
    CEPH_ARGS+="--osd_pool_default_size=$POOL_SIZE "

    export -n CEPH_CLI_TEST_DUP_COMMAND
    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_recover_unexpected() {
    local dir=$1
    shift
    local OSDS=6
    local PGS=16
    local POOLS=3
    local OBJS=1000

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for o in $(seq 0 $(expr $OSDS - 1))
    do
        run_osd $dir $o
    done

    for i in $(seq 1 $POOLS)
    do
        create_pool test$i $PGS $PGS
    done

    wait_for_clean || return 1

    dd if=/dev/urandom of=datafile bs=4k count=2
    for i in $(seq 1 $POOLS)
    do
       for j in $(seq 1 $OBJS)
       do
	       rados -p test$i put obj$j datafile
       done
    done
    rm datafile

    ceph osd set noscrub
    ceph osd set nodeep-scrub

    for qpg in $(ceph pg dump pgs --format=json-pretty | jq '.pg_stats[].pgid')
    do
	primary=$(ceph pg dump pgs --format=json | jq ".pg_stats[] | select(.pgid == $qpg) | .acting_primary")
	eval pg=$qpg   # strip quotes around qpg
	CEPH_ARGS='' ceph daemon $(get_asok_path osd.$primary) trigger_scrub $pg
    done

    ceph pg dump pgs

    max=$(CEPH_ARGS='' ceph daemon $(get_asok_path osd.0) dump_scrub_reservations | jq '.osd_max_scrubs')
    if [ $max != $MAX_SCRUBS];
    then
	echo "ERROR: Incorrect osd_max_scrubs from dump_scrub_reservations"
	return 1
    fi

    ceph osd unset noscrub

    ok=false
    for i in $(seq 0 300)
    do
	ceph pg dump pgs
	if ceph pg dump pgs | grep scrubbing; then
	    ok=true
	    break
	fi
	sleep 1
    done
    if test $ok = "false"; then
	echo "ERROR: Test set-up failed no scrubbing"
	return 1
    fi

    local total=0
    local zerocount=0
    local maxzerocount=3
    while(true)
    do
	pass=0
	for o in $(seq 0 $(expr $OSDS - 1))
	do
		CEPH_ARGS='' ceph daemon $(get_asok_path osd.$o) dump_scrub_reservations
		scrubs=$(CEPH_ARGS='' ceph daemon $(get_asok_path osd.$o) dump_scrub_reservations | jq '.scrubs_local + .scrubs_remote')
		if [ $scrubs -gt $MAX_SCRUBS ]; then
		    echo "ERROR: More than $MAX_SCRUBS currently reserved"
		    return 1
	        fi
		pass=$(expr $pass + $scrubs)
        done
	if [ $pass = "0" ]; then
	    zerocount=$(expr $zerocount + 1)
	fi
	if [ $zerocount -gt $maxzerocount ]; then
	    break
	fi
	total=$(expr $total + $pass)
	sleep $(expr $SCRUB_SLEEP \* 2)
    done

    # Check that there are no more scrubs
    for i in $(seq 0 5)
    do
        if ceph pg dump pgs | grep scrubbing; then
	    echo "ERROR: Extra scrubs after test completion...not expected"
	    return 1
        fi
	sleep $SCRUB_SLEEP
    done

    echo $total total reservations seen

    # Sort of arbitraty number based on PGS * POOLS * POOL_SIZE as the number of total scrub
    # reservations that must occur.  However, the loop above might see the same reservation more
    # than once.
    actual_reservations=$(expr $PGS \* $POOLS \* $POOL_SIZE)
    if [ $total -lt $actual_reservations ]; then
	echo "ERROR: Unexpectedly low amount of scrub reservations seen during test"
	return 1
    fi

    return 0
}


main osd-scrub-dump "$@"

# Local Variables:
# compile-command: "cd build ; make check && \
#    ../qa/run-standalone.sh osd-scrub-dump.sh"
# End:
