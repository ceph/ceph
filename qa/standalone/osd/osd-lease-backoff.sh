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

# Writers to the wedged pool, reaped on exit so a failed run can't leak them.
STUCK_PIDS=""
function reap_stuck_writers() {
    if [ -n "$STUCK_PIDS" ] ; then
        kill $STUCK_PIDS 2>/dev/null
        wait $STUCK_PIDS 2>/dev/null
        STUCK_PIDS=""
    fi
}
trap reap_stuck_writers EXIT

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7149" # git grep '\<7149\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none "
    CEPH_ARGS+="--auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    # short read lease: grace 5 * ratio 0.8 => 4s, renewed every 2s
    CEPH_ARGS+="--osd-heartbeat-grace=5 "
    CEPH_ARGS+="--osd-pool-default-read-lease-ratio=0.8 "
    # small enough for a dozen 4MiB writes to fill
    CEPH_ARGS+="--osd-client-message-size-cap=16777216 "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

# A laggy PG must not stall client I/O to the other PGs on its primary, which
# happens if its queued ops fill the client throttle.
# osd_debug_drop_pg_lease_acks_pool wedges one pool in LAGGY, as in #79893.
function TEST_laggy_pg_does_not_starve_other_pgs() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    create_pool laggypool 1 1 || return 1
    ceph osd pool set laggypool size 3 || return 1
    create_pool healthypool 8 8 || return 1
    ceph osd pool set healthypool size 3 || return 1
    wait_for_clean || return 1

    # the throttle is per-OSD, so the pools must share a primary
    local victim
    victim=$(ceph osd map laggypool SOMETHING --format=json | jq -r '.acting_primary')
    echo "laggypool primary is osd.$victim"

    local healthy_obj=""
    local i
    for i in $(seq 1 200) ; do
        local p
        p=$(ceph osd map healthypool "healthy-$i" --format=json | jq -r '.acting_primary')
        if [ "$p" = "$victim" ] ; then
            healthy_obj="healthy-$i"
            break
        fi
    done
    if [ -z "$healthy_obj" ] ; then
        echo "could not find a healthypool object primaried on osd.$victim"
        return 1
    fi
    echo "healthypool object $healthy_obj shares primary osd.$victim"

    dd if=/dev/urandom of=$dir/4m bs=1048576 count=4 2>/dev/null || return 1

    # baseline: both pools serve
    timeout 60 rados -p laggypool put warmup $dir/4m || return 1
    timeout 60 rados -p healthypool put $healthy_obj $dir/4m || return 1

    # replicas stop acking leases, so laggypool's PG stays laggy
    local laggypool_id
    laggypool_id=$(ceph osd dump --format=json | \
        jq -r '.pools[] | select(.pool_name=="laggypool") | .pool') || return 1
    echo "laggypool is pool id $laggypool_id"
    ceph tell 'osd.*' injectargs \
        "--osd_debug_drop_pg_lease_acks_pool=$laggypool_id" || return 1
    sleep 10

    # fill the primary's client throttle; these writes hang
    for i in $(seq 1 12) ; do
        rados -p laggypool put "stuck-$i" $dir/4m >/dev/null 2>&1 &
        STUCK_PIDS="$STUCK_PIDS $!"
    done
    sleep 20

    # otherwise the rest of the test proves nothing
    local pgid
    pgid=$(get_pg laggypool SOMETHING) || return 1
    if ! ceph pg $pgid query | jq -r '.state' | grep -q laggy ; then
        echo "laggypool pg $pgid never went laggy -- test is not exercising the bug"
        ceph pg $pgid query | jq -r '.state'
        return 1
    fi

    # without the backoff the OSD has stopped reading clients, and this hangs
    timeout 60 rados -p healthypool put $healthy_obj $dir/4m || return 1
    timeout 60 rados -p healthypool get $healthy_obj $dir/readback || return 1
    cmp $dir/4m $dir/readback || return 1

    # laggypool must serve again, i.e. its backoffs were released
    ceph tell 'osd.*' injectargs '--osd_debug_drop_pg_lease_acks_pool=-1' || return 1
    reap_stuck_writers
    timeout 120 rados -p laggypool put after-recovery $dir/4m || return 1

    wait_for_clean || return 1
}

main osd-lease-backoff "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && \
#   test/osd/osd-lease-backoff.sh"
# End:
