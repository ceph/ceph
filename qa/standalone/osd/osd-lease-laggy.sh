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

    export CEPH_MON="127.0.0.1:7148" # git grep '\<7148\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none "
    CEPH_ARGS+="--auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    # Keep the read lease short so the test does not have to wait out the
    # 16s default.  grace 5 * ratio 0.8 => a 4s lease, renewed every 2s.
    CEPH_ARGS+="--osd-heartbeat-grace=5 "
    CEPH_ARGS+="--osd-pool-default-read-lease-ratio=0.8 "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

# A pg that latches PG_STATE_LAGGY must be able to recover on its own.
#
# PG_STATE_LAGGY is set from PrimaryLogPG::check_laggy() and cleared only from
# PrimaryLogPG::recheck_readable().  For an acting set larger than one the only
# caller of recheck_readable() used to be PeeringState::proc_lease_ack(), so if
# lease renewal stalled -- no more leases sent, therefore no more acks -- the pg
# stayed laggy and every client op sat in waiting_for_readable until the next
# interval change, i.e. until an OSD was restarted.
#
# osd_debug_drop_pg_lease_renewals simulates that stall by discarding the
# RenewLease timer events instead of arming them.
function TEST_laggy_pg_recovers_when_lease_renewal_stalls() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    create_pool test 1 1 || return 1
    ceph osd pool set test size 3 || return 1
    ceph osd pool set test min_size 2 || return 1
    wait_for_clean || return 1

    local pgid
    pgid=$(get_pg test SOMETHING) || return 1

    # baseline: writes work
    timeout 30 rados -p test put obj-before /etc/group || return 1

    # Break the renewal chain everywhere.  The already-armed event fires once
    # more, re-arms into the void, and from then on no lease is sent.
    ceph tell 'osd.*' injectargs '--osd_debug_drop_pg_lease_renewals=true' || return 1

    # Let the outstanding lease expire (4s lease, renewed every 2s).
    sleep 10

    # This write finds mnow > readable_until, so check_laggy() latches
    # PG_STATE_LAGGY and parks the op in waiting_for_readable.  With the
    # safety net in place the pg schedules its own CheckReadable, notices the
    # renewal chain has stalled, restarts it and requeues the op.  Without it
    # the op never completes and this times out.
    timeout 120 rados -p test put obj-after /etc/group || return 1

    # And the pg must not be left permanently marked laggy.
    local tries=0
    while ceph pg $pgid query | jq -r '.state' | grep -q laggy ; do
        tries=$((tries + 1))
        if [ $tries -gt 60 ] ; then
            echo "pg $pgid still laggy after recovering"
            ceph pg $pgid query
            return 1
        fi
        sleep 1
    done

    # Reads must work too, and the object we wrote while laggy must be intact.
    timeout 30 rados -p test get obj-after $dir/obj-after || return 1
    cmp /etc/group $dir/obj-after || return 1

    ceph tell 'osd.*' injectargs '--osd_debug_drop_pg_lease_renewals=false' || return 1
    wait_for_clean || return 1
}

main osd-lease-laggy "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && \
#   test/osd/osd-lease-laggy.sh"
# End:
