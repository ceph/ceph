#! /usr/bin/env bash
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

    export CEPH_MON="127.0.0.1:7124" # git grep '\<7124\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    export -n CEPH_CLI_TEST_DUP_COMMAND
    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done
}

function TEST_recovery_scrub() {
    local dir=$1
    local poolname=test

    TESTDATA="testdata.$$"
    OSDS=8
    PGS=32
    OBJECTS=4

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
        run_osd $dir $osd || return 1
    done

    # Create a pool with $PGS pgs
    create_pool $poolname $PGS $PGS
    wait_for_clean || return 1
    poolid=$(ceph osd dump | grep "^pool.*[']test[']" | awk '{ print $2 }')

    dd if=/dev/urandom of=$TESTDATA bs=1M count=50
    for i in $(seq 1 $OBJECTS)
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    ceph osd pool set $poolname size 4

    pids=""
    for pg in $(seq 0 $(expr $PGS - 1))
    do
        run_in_background pids pg_scrub $poolid.$(printf "%x" $pg)
    done
    ceph pg dump pgs
    wait_background pids
    return_code=$?
    if [ $return_code -ne 0 ]; then return $return_code; fi

    ERRORS=0
    pidfile=$(find $dir 2>/dev/null | grep $name_prefix'[^/]*\.pid')
    pid=$(cat $pidfile)
    if ! kill -0 $pid
    then
        echo "OSD crash occurred"
        tail -100 $dir/osd.0.log
        ERRORS=$(expr $ERRORS + 1)
    fi

    kill_daemons $dir || return 1

    declare -a err_strings
    err_strings[0]="not scheduling scrubs due to active recovery"
    # Test with these two strings after disabled check in OSD::sched_scrub()
    #err_strings[0]="handle_scrub_reserve_request: failed to reserve remotely"
    #err_strings[1]="sched_scrub: failed to reserve locally"

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
        grep "failed to reserve\|not scheduling scrubs" $dir/osd.${osd}.log
    done
    for err_string in "${err_strings[@]}"
    do
        found=false
        for osd in $(seq 0 $(expr $OSDS - 1))
        do
            if grep "$err_string" $dir/osd.${osd}.log > /dev/null;
            then
                found=true
            fi
        done
        if [ "$found" = "false" ]; then
            echo "Missing log message '$err_string'"
	    ERRORS=$(expr $ERRORS + 1)
        fi
    done

    teardown $dir || return 1

    if [ $ERRORS != "0" ];
    then
        echo "TEST FAILED WITH $ERRORS ERRORS"
        return 1
    fi

    echo "TEST PASSED"
    return 0
}

main osd-recovery-scrub "$@"

# Local Variables:
# compile-command: "cd build ; make -j4 && \
#    ../qa/run-standalone.sh osd-recovery-scrub.sh"
# End:
