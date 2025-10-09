#!/usr/bin/env bash
set -x

#
# Test pools
#

# Includes
source "`dirname $0`/test_common.sh"

# Functions
setup() {
        export CEPH_NUM_OSD=$1

        # Start ceph
        ./stop.sh

        ./vstart.sh -d -n || die "vstart failed"
}

test629_impl() {
        # create the pool
        ./ceph -c ./ceph.conf osd pool create foo 8 || die "pool create failed"

        # Write lots and lots of objects
        write_objects 1 1 10 1000000 foo

        # Take down first osd
        stop_osd 0

        # Now degraded PGs should exist
        poll_cmd "./ceph pg debug degraded_pgs_exist" TRUE 3 120

        # delete the pool
        ./ceph -c ./ceph.conf osd pool rm foo foo --yes-i-really-really-mean-it || die "pool rm failed"

        # make sure the system is stable
        sleep 10
}

test629(){
        setup 3
        test629_impl
}

run() {
        test629 || die "test failed"
}

$@
