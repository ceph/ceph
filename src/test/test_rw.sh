#!/usr/bin/env bash
set -x

#
# Generic read/write from object store test
#

# Includes
source "`dirname $0`/test_common.sh"

TEST_POOL=rbd

# Functions
my_write_objects() {
        write_objects $1 $2 10 1000000 $TEST_POOL
}

setup() {
        export CEPH_NUM_OSD=$1

        # Start ceph
        ./stop.sh

        ./vstart.sh -d -n || die "vstart.sh failed"
}

read_write_1_impl() {
        write_objects 1 2 100 8192 $TEST_POOL
        read_objects 2 100 8192 

        write_objects 3 3 10 81920 $TEST_POOL
        read_objects 3 10 81920 

        write_objects 4 4 100 4 $TEST_POOL
        read_objects 4 100 4

        write_objects 1 2 100 8192 $TEST_POOL
        read_objects 2 100 8192

        # success
        return 0
}

read_write_1() {
        setup 3
        read_write_1_impl
}

run() {
        read_write_1 || die "test failed"
}

$@
