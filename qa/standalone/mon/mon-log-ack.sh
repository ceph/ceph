#!/usr/bin/env bash
#
# Daemons whose monitor session is on a peon must get MLogAcks for their
# cluster log entries, or their log queue grows for as long as they run and is
# sent again, and committed again, on every monitor session reset.
# https://tracker.ceph.com/issues/81604
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

    export MONA=127.0.0.1:7316
    export MONB=127.0.0.1:7317
    export FSID=$(uuidgen)
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$FSID --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$MONA,$MONB "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

# Print a counter of the "clog" perf counters of a daemon.
function clog_counter() {
    local daemon=$1
    local counter=$2
    CEPH_ARGS='' ceph --format json --admin-daemon $(get_asok_path $daemon) \
        perf dump clog | jq ".clog.$counter"
}

# Wait until the daemon has no unacknowledged cluster log entries left.
function wait_for_clog_acked() {
    local daemon=$1
    for ((i=0; i < 60; i++)); do
        if [ "$(clog_counter $daemon queue_len)" = "0" ] &&
           [ "$(clog_counter $daemon acked)" -gt 0 ]; then
            return 0
        fi
        sleep 1
    done
    echo "$daemon: queue_len $(clog_counter $daemon queue_len)" \
        "sent $(clog_counter $daemon sent) acked $(clog_counter $daemon acked)"
    return 1
}

# Print the name of the monitor that has a session with the given daemon.
function mon_of() {
    local daemon=$1
    local mon
    for mon in a b ; do
        if CEPH_ARGS='' ceph --format json --admin-daemon $(get_asok_path mon.$mon) \
            sessions | jq -e ".[] | select(.name == \"$daemon\")" >/dev/null ; then
            echo $mon
            return 0
        fi
    done
    return 1
}

function TEST_log_ack_through_peon() {
    local dir=$1

    run_mon $dir a --public-addr $MONA \
        --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    run_mon $dir b --public-addr $MONB \
        --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    timeout 360 ceph mon stat || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    create_pool rbd 4 || return 1
    wait_for_clean || return 1

    # Whichever monitor the OSD is connected to, make it a peon: the OSD keeps
    # its session through the election.
    local osdmon=$(mon_of osd.0)
    test -n "$osdmon" || return 1
    ceph mon set election_strategy disallow || return 1
    ceph mon add disallowed_leader $osdmon || return 1
    for ((i=0; i < 60; i++)); do
        CEPH_ARGS='' ceph --admin-daemon $(get_asok_path mon.$osdmon) mon_status |
            grep -q '"peon"' && break
        sleep 1
    done
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path mon.$osdmon) mon_status |
        grep '"peon"' || return 1
    test "$(mon_of osd.0)" = "$osdmon" || return 1

    # Each scrub logs "<pgid> scrub starts" and "scrub ok" to the cluster log.
    local sent=$(clog_counter osd.0 sent)
    local pgid
    for pgid in $(get_pg_list rbd) ; do
        ceph pg scrub $pgid || return 1
    done
    for ((i=0; i < 60; i++)); do
        test "$(clog_counter osd.0 sent)" -gt "$sent" && break
        sleep 1
    done
    test "$(clog_counter osd.0 sent)" -gt "$sent" || return 1

    wait_for_clog_acked osd.0 || return 1
    # The peon's own cluster log entries are forwarded to the leader as well.
    wait_for_clog_acked mon.$osdmon || return 1
}

function get_pg_list() {
    local pool=$1
    ceph pg ls-by-pool $pool --format json | jq -r '.pg_stats[].pgid'
}

main mon-log-ack "$@"

# Local Variables:
# compile-command: "cd ../../../build ; ../qa/run-standalone.sh mon-log-ack.sh"
# End:
