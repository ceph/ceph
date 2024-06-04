#!/usr/bin/env bash
#
# Copyright (C) 2022 Red Hat <contact@redhat.com>
#
# Author: Sridhar Seshasayee <sseshasa@redhat.com>
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
    CEPH_ARGS+="--debug-bluestore 20 "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_snaptrim_stats() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local PGNUM=8
    local PGPNUM=8
    local objects=10
    local WAIT_FOR_UPDATE=10

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=$OSDS || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd --osd_pool_default_pg_autoscale_mode=off || return 1
    done

    # disable scrubs
    ceph osd set noscrub || return 1
    ceph osd set nodeep-scrub || return 1

    # Create a pool
    create_pool $poolname $PGNUM $PGPNUM
    wait_for_clean || return 1
    poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')

    # write a few objects
    TESTDATA="testdata.1"
    dd if=/dev/urandom of=$TESTDATA bs=4096 count=1
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    # create a snapshot, clones
    SNAP=1
    rados -p $poolname mksnap snap${SNAP}
    TESTDATA="testdata.2"
    dd if=/dev/urandom of=$TESTDATA  bs=4096 count=1
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    # remove the snapshot, should trigger snaptrim
    rados -p $poolname rmsnap snap${SNAP}

    # check for snaptrim stats
    wait_for_clean || return 1
    sleep $WAIT_FOR_UPDATE
    local objects_trimmed=0
    local snaptrim_duration_total=0.0
    for i in $(seq 0 $(expr $PGNUM - 1))
    do
        local pgid="${poolid}.${i}"
        objects_trimmed=$(expr $objects_trimmed + $(ceph pg $pgid query | \
            jq '.info.stats.objects_trimmed'))
        snaptrim_duration_total=`echo $snaptrim_duration_total + $(ceph pg \
            $pgid query | jq '.info.stats.snaptrim_duration') | bc`
    done
    test $objects_trimmed -eq $objects || return 1
    echo "$snaptrim_duration_total > 0.0" | bc || return 1

    teardown $dir || return 1
}

function TEST_snaptrim_stats_multiple_snaps() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local PGNUM=8
    local PGPNUM=8
    local objects=10
    local WAIT_FOR_UPDATE=10

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=$OSDS || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd --osd_pool_default_pg_autoscale_mode=off || return 1
    done

    # disable scrubs
    ceph osd set noscrub || return 1
    ceph osd set nodeep-scrub || return 1

    # Create a pool
    create_pool $poolname $PGNUM $PGPNUM
    wait_for_clean || return 1
    poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')

    # write a few objects
    local TESTDATA="testdata.0"
    dd if=/dev/urandom of=$TESTDATA bs=4096 count=1
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    # create snapshots, clones
    NUMSNAPS=2
    for i in `seq 1 $NUMSNAPS`
    do
        rados -p $poolname mksnap snap${i}
        TESTDATA="testdata".${i}
        dd if=/dev/urandom of=$TESTDATA  bs=4096 count=1
        for i in `seq 1 $objects`
        do
            rados -p $poolname put obj${i} $TESTDATA
        done
        rm -f $TESTDATA
    done

    # remove the snapshots, should trigger snaptrim
    local total_objects_trimmed=0
    for i in `seq 1 $NUMSNAPS`
    do
        rados -p $poolname rmsnap snap${i}

        # check for snaptrim stats
        wait_for_clean || return 1
        sleep $WAIT_FOR_UPDATE
        local objects_trimmed=0
        local snaptrim_duration_total=0.0
        for i in $(seq 0 $(expr $PGNUM - 1))
        do
            local pgid="${poolid}.${i}"
            objects_trimmed=$(expr $objects_trimmed + $(ceph pg $pgid query | \
                jq '.info.stats.objects_trimmed'))
            snaptrim_duration_total=`echo $snaptrim_duration_total + $(ceph pg \
                $pgid query | jq '.info.stats.snaptrim_duration') | bc`
        done
        test $objects_trimmed -eq $objects || return 1
        echo "$snaptrim_duration_total > 0.0" | bc || return 1
        total_objects_trimmed=$(expr $total_objects_trimmed + $objects_trimmed)
    done

    test $total_objects_trimmed -eq $((objects * NUMSNAPS)) || return 1

    teardown $dir || return 1
}
main test-snaptrim-stats "$@"

# Local Variables:
# compile-command: "cd build ; make -j4 && \
#   ../qa/run-standalone.sh test-snaptrim-stats.sh"
# End:
