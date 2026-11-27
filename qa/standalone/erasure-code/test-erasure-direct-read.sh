#!/usr/bin/env bash
#
# EC direct reads that the OSD trims to zero length.
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

    export CEPH_MON="127.0.0.1:7189" # git grep '\<7189\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

# An OSD that crashed while serving the client's ops, as told by its log
function osd_crashes() {
    local dir=$1
    grep -E 'FAILED ceph_assert|Caught signal' $dir/osd.*.log
}

# A direct sparse read of an empty object, or one that starts at the end of
# the object, is trimmed to zero length on the OSD. The OSD used to assert
# in interval_set::range_start() under do_sparse_read()
# (https://tracker.ceph.com/issues/83048).
#
# The reads are sent by the LibRadosSplitOpECPP.SparseRead* tests. They
# cannot tell that the OSD crashed: the client resends the read once the OSD
# is marked down, and gets the right (empty) answer from another OSD. Hence
# the OSD logs and the OSD map are checked here.
function TEST_direct_sparse_read_trimmed_to_zero() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in 0 1 2 ; do
        run_osd $dir $id || return 1
    done
    wait_for_osd up 0 || return 1
    wait_for_osd up 1 || return 1
    wait_for_osd up 2 || return 1

    # The test connects with CEPH_ARGS, but it must read a config file
    touch $dir/client.conf
    CEPH_CONF=$dir/client.conf timeout 600 ceph_test_rados_api_split_op_pp \
        --gtest_filter='*LibRadosSplitOpECPP.SparseRead*' || return 1

    if osd_crashes $dir ; then
        echo "FAIL: an OSD crashed serving a direct sparse read trimmed to zero length"
        grep -B2 -A12 -E 'FAILED ceph_assert|Caught signal' $dir/osd.*.log | head -60
        return 1
    fi
    test "$(ceph osd stat -f json | jq '.num_up_osds')" = 3 || return 1
}

main test-erasure-direct-read "$@"

# Local Variables:
# compile-command: "cd ../../../build ; make -j4 && ../qa/run-standalone.sh test-erasure-direct-read.sh"
# End:
