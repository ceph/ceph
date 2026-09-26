#!/usr/bin/env bash
#
# Copyright (C) 2026 Clyso Technologies Inc. <https://www.clyso.com>
#
# Author: Dan van der Ster <dan.vanderster@clyso.com>
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

    export CEPH_MON="127.0.0.1:7155" # git grep '\<7155\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none "
    CEPH_ARGS+="--auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--ms_connection_stall_timeout=10 "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

# A cluster connection that stops moving data while both ends keep it open
# must be reset, so the writes it carries complete instead of waiting for
# ms_connection_idle_timeout.
function TEST_stalled_cluster_connection_is_reset() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1

    # min_size 2: every write needs the osd.0 <-> osd.1 cluster connection
    create_pool test 8 8 || return 1
    ceph osd pool set test size 2 || return 1
    ceph osd pool set test min_size 2 || return 1
    wait_for_clean || return 1

    local i
    for i in $(seq 1 8) ; do
        timeout 30 rados -p test put warm-$i /etc/group || return 1
    done

    ceph tell osd.0 injectargs '--ms_inject_blackhole_lossless=true' || return 1

    # these stall until the frozen connection is reset
    for i in $(seq 1 8) ; do
        timeout 60 rados -p test put obj-$i /etc/group || return 1
    done

    # otherwise the writes above prove nothing
    grep -q "freezing connection" $dir/osd.0.log || return 1
    grep -q "no message acknowledged for more than" $dir/osd.*.log || return 1

    ceph tell osd.0 injectargs '--ms_inject_blackhole_lossless=false' || return 1
    for i in $(seq 1 8) ; do
        timeout 30 rados -p test get obj-$i $dir/obj || return 1
        cmp /etc/group $dir/obj || return 1
    done
}

main osd-msgr-stall "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && \
#   test/osd/osd-msgr-stall.sh"
# End:
