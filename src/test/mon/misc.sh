#!/bin/bash
#
# Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
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

function run() {
    local dir=$1

    export CEPH_MON="127.0.0.1:7102"
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    setup $dir || return 1
    run_mon $dir a --public-addr $CEPH_MON
    FUNCTIONS=${FUNCTIONS:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for TEST_function in $FUNCTIONS ; do
        if ! $TEST_function $dir ; then
            cat $dir/a/log
            return 1
        fi
    done
    teardown $dir || return 1
}

TEST_POOL=rbd

function TEST_osd_pool_get_set() {
    local dir=$1
    ./ceph osd dump | grep 'pool 0' | grep hashpspool || return 1
    ./ceph osd pool set $TEST_POOL hashpspool 0 || return 1
    ! ./ceph osd dump | grep 'pool 0' | grep hashpspool || return 1
    ./ceph osd pool set $TEST_POOL hashpspool 1 || return 1
    ./ceph osd dump | grep 'pool 0' | grep hashpspool || return 1
    ./ceph osd pool set $TEST_POOL hashpspool false || return 1
    ! ./ceph osd dump | grep 'pool 0' | grep hashpspool || return 1
    ./ceph osd pool set $TEST_POOL hashpspool false || return 1
    # check that setting false twice does not toggle to true (bug)
    ! ./ceph osd dump | grep 'pool 0' | grep hashpspool || return 1
    ./ceph osd pool set $TEST_POOL hashpspool true || return 1
    ./ceph osd dump | grep 'pool 0' | grep hashpspool || return 1
}

main misc

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/mon/misc.sh"
# End:
