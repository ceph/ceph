#!/bin/bash
#
# Copyright (C) 2014 Red Hat <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
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

source test/mon/mon-test-helpers.sh
source test/osd/osd-test-helpers.sh

function run() {
    local dir=$1

    export CEPH_MON="127.0.0.1:7107"
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    setup $dir || return 1
    run_mon $dir a --public-addr $CEPH_MON || return 1
    for id in $(seq 0 3) ; do
        run_osd $dir $id || return 1
    done
    FUNCTIONS=${FUNCTIONS:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for TEST_function in $FUNCTIONS ; do
        if ! $TEST_function $dir ; then
            cat $dir/a/log
            return 1
        fi
    done
    teardown $dir || return 1
}

function wait_for_repair() {
    local dir=$1
    local primary=$2
    local pg=$3
    local -i tries=0
    while [ $tries -lt 100 ] ; do
        CEPH_ARGS='' ./ceph --admin-daemon $dir/ceph-osd.$primary.asok log flush || return 1
        if grep --quiet "$pg repair ok" $dir/osd-$primary.log ; then
            return 0
        fi
        let tries++
        sleep 1
    done
    grep --quiet "$pg repair ok" $dir/osd-$primary.log || return 1
}

function TEST_log_repair() {
    local dir=$1
    local poolname=rbd
    local pg=$(get_pg $poolname SOMETHING)
    local -a osds=($(get_osds $poolname SOMETHING))
    local primary=${osds[$first]}

    ./ceph pg repair $pg
    wait_for_repair $dir $primary $pg 
    grep --quiet "$pg repair starts" $dir/osd-$primary.log || return 1
}

# 
# 1) add an object 
# 2) remove the corresponding file from the primary OSD
# 3) repair the PG
#
# Reproduces http://tracker.ceph.com/issues/8914
#
function TEST_bug_8914() {
    local dir=$1
    local poolname=rbd
    local payload=ABCDEF

    ./ceph osd set noscrub || return 1
    ./ceph osd set nodeep-scrub || return 1

    echo $payload > $dir/ORIGINAL
    #
    # 1) add an object 
    #
    ./rados --pool $poolname put SOMETHING $dir/ORIGINAL || return 1
    local -a osds=($(get_osds $poolname SOMETHING))
    local file=$(find $dir/${osds[$first]} -name '*SOMETHING*')
    local -i tries=0
    while [ ! -f $file -a $tries -lt 100 ] ; do
        let tries++
        sleep 1
    done
    grep --quiet --recursive --text $payload $file || return 1
    #
    # 2) remove the corresponding file from the primary OSD
    #
    rm $file
    #
    # 3) repair the PG
    #
    local pg=$(get_pg $poolname SOMETHING)
    ./ceph pg repair $pg
    local -i tries=0
    while [ ! -f $file -a $tries -lt 100 ] ; do
        let tries++
        sleep 1
    done
    #
    # The file must be back
    #
    test -f $file || return 1
}

main osd-scrub-repair

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/osd/osd-scrub-repair.sh"
# End:
