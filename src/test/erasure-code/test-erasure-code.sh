#!/bin/bash
#
# Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
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

    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=127.0.0.1 "

    setup $dir || return 1
    run_mon $dir a --public-addr 127.0.0.1 || return 1
    for id in $(seq 0 4) ; do
        run_osd $dir $id || return 1
    done
    create_erasure_coded_pool || return 1
    FUNCTIONS=${FUNCTIONS:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for TEST_function in $FUNCTIONS ; do
        if ! $TEST_function $dir ; then
            cat $dir/a/log
            return 1
        fi
    done
    teardown $dir || return 1
}

function create_erasure_coded_pool() {
    ./ceph osd erasure-code-profile set myprofile \
        ruleset-failure-domain=osd || return 1
    ./ceph osd pool create ecpool 12 12 erasure myprofile \
        || return 1
}

function delete_pool() {
    local poolname=$1

    ./ceph osd pool delete $poolname $poolname --yes-i-really-really-mean-it
}

function rados_put_get() {
    local dir=$1
    local poolname=$2

    local payload=ABC
    echo "$payload" > $dir/ORIGINAL

    ./rados --pool $poolname put SOMETHING $dir/ORIGINAL || return 1
    ./rados --pool $poolname get SOMETHING $dir/COPY || return 1

    diff $dir/ORIGINAL $dir/COPY || return 1

    rm $dir/ORIGINAL $dir/COPY
}

function plugin_exists() {
    local plugin=$1

    local status
    ./ceph osd erasure-code-profile set TESTPROFILE plugin=$plugin
    if ./ceph osd crush rule create-erasure TESTRULE TESTPROFILE 2>&1 |
        grep "$plugin.*No such file" ; then
        status=1
    else
        ./ceph osd crush rule rm TESTRULE
        status=0
    fi
    ./ceph osd erasure-code-profile rm TESTPROFILE 
    return $status
}

function TEST_rados_put_get_isa() {
    if ! plugin_exists isa ; then
        echo "SKIP because plugin isa has not been built"
        return 0
    fi
    local dir=$1
    local poolname=pool-isa

    ./ceph osd erasure-code-profile set profile-isa \
        plugin=isa \
        ruleset-failure-domain=osd || return 1
    ./ceph osd pool create $poolname 12 12 erasure profile-isa \
        || return 1

    rados_put_get $dir $poolname || return 1

    delete_pool $poolname
}

function TEST_rados_put_get_jerasure() {
    local dir=$1

    rados_put_get $dir ecpool || return 1
}

function TEST_alignment_constraints() {
    local payload=ABC
    echo "$payload" > $dir/ORIGINAL
    # 
    # Verify that the rados command enforces alignment constraints
    # imposed by the stripe width
    # See http://tracker.ceph.com/issues/8622
    #
    local stripe_width=$(./ceph-conf --show-config-value osd_pool_erasure_code_stripe_width)
    local block_size=$((stripe_width - 1))
    dd if=/dev/zero of=$dir/ORIGINAL bs=$block_size count=2
    ./rados --block-size=$block_size \
        --pool ecpool put UNALIGNED $dir/ORIGINAL || return 1
    rm $dir/ORIGINAL
}

main test-erasure-code

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/erasure-code/test-erasure-code.sh"
# End:
