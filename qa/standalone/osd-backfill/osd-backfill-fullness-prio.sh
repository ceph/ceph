#!/usr/bin/env bash
#
# Copyright (C) 2026 Clyso GmbH
#
# Author: Frédéric Nass <frederic.nass@clyso.com>
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
# Test osd_backfill_drain_full_priority_boost.
#
# Layout (6 OSDs, osd_max_backfills=1, nobackfill so nothing completes):
#   pool A/B/C, 1 PG each, size 2, all pinned with pg-upmap to the same
#   primary P:  A=[P,R1]  B=[P,R2]  C=[P,R3]
# Then each PG is remapped away from its replica (R1/R2/R3 become "outgoing",
# kept in acting through pg_temp) while R2 is injected backfillfull.
#
# Expected on P's local reserver (dump_recovery_reservations):
#   1. B (drains backfillfull R2) gets 110+boost (131) and preempts A (110)
#   2. R3 becomes backfillfull -> queued C is raised in place (AdvMap hook)
#   3. R2 clears -> running B keeps its boost
#   4. R3 clears -> queued C keeps its boost (the hook only raises)
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7280" # git grep '\<7280\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON --osd_max_backfills=1 --debug_reserver=20 "
    # keep the PG log short, so that a remap needs backfill, not log recovery
    CEPH_ARGS+="--osd_min_pg_log_entries=5 --osd_max_pg_log_entries=10 --osd_pg_log_trim_min=5 "
    CEPH_ARGS+="--osd_backfill_drain_full_priority_boost=21 "
    # backfill priorities are only honoured by the reservers, but keep wpq
    # like osd-backfill-prio.sh so that mclock does not reset osd_max_backfills
    CEPH_ARGS+="--osd-op-queue=wpq "
    export NORMAL_PRIO=110      # OSD_BACKFILL_PRIORITY_BASE + 10
    export BOOSTED_PRIO=131     # NORMAL_PRIO + osd_backfill_drain_full_priority_boost

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function pool_pgid() {
    local id=$(ceph osd pool ls detail -f json | jq ".[] | select(.pool_name == \"$1\") | .pool_id")
    echo "${id}.0"
}

# wait until the OSDMap shows (want=1) or no longer shows (want=0) a state
function wait_osd_state() {
    local osd=$1 state=$2 want=$3
    for i in $(seq 1 60) ; do
        if ceph osd dump -f json | jq -e ".osds[] | select(.osd == $osd) | .state | index(\"$state\")" > /dev/null ; then
            [ "$want" = "1" ] && return 0
        else
            [ "$want" = "0" ] && return 0
        fi
        sleep 1
    done
    echo "osd.$osd: timeout waiting for $state=$want"
    return 1
}

# print "in_progress:PRIO" or "queued:PRIO" for a PG on an OSD's local reserver
function local_prio() {
    local osd=$1 pgid=$2
    local out=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$osd) dump_recovery_reservations)
    local p=$(echo "$out" | jq -r "[.local_reservations.in_progress[] | select(.item == \"$pgid\") | .prio] | first // empty")
    local q=$(echo "$out" | jq -r "[.local_reservations.queues[].items[] | select(.item == \"$pgid\") | .prio] | first // empty")
    if [ -n "$p" ] ; then echo "in_progress:$p" ; elif [ -n "$q" ] ; then echo "queued:$q" ; else echo "none" ; fi
}

# wait until "ceph pg <pgid> query" reports a state containing $2
function wait_pg_state() {
    local pgid=$1 want=$2
    for i in $(seq 1 60) ; do
        ceph pg $pgid query 2>/dev/null | jq -r .state | grep -q "$want" && return 0
        sleep 1
    done
    echo "timeout waiting for $pgid to be $want: $(ceph pg $pgid query | jq -r .state)"
    return 1
}

function expect_prio() {
    local osd=$1 pgid=$2 want=$3 what=$4
    local got
    for i in $(seq 1 30) ; do
        got=$(local_prio $osd $pgid)
        [ "$got" = "$want" ] && { echo "OK: $what: $pgid $got" ; return 0 ; }
        sleep 1
    done
    echo "FAIL: $what: $pgid is $got, expected $want"
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$osd) dump_recovery_reservations
    return 1
}

function TEST_backfill_fullness_boost() {
    local dir=$1
    local P=0 R1=1 R2=2 R3=3 X=4 Y=5
    local ERRORS=0

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 5) ; do
        run_osd $dir $osd || return 1
    done
    ceph osd set-require-min-compat-client luminous || return 1

    for pool in A B C ; do
        create_pool $pool 1 1
        ceph osd pool set $pool pg_autoscale_mode off
        ceph osd pool set $pool size 2 --yes-i-really-mean-it
    done
    local PGA=$(pool_pgid A) PGB=$(pool_pgid B) PGC=$(pool_pgid C)
    ceph osd pg-upmap $PGA $P $R1 || return 1
    ceph osd pg-upmap $PGB $P $R2 || return 1
    ceph osd pg-upmap $PGC $P $R3 || return 1
    wait_for_clean || return 1

    dd if=/dev/urandom of=$dir/data bs=1M count=4
    for pool in A B C ; do
        for i in $(seq 1 20) ; do
            rados -p $pool put obj$i $dir/data || return 1
        done
    done
    wait_for_clean || return 1
    ceph pg dump pgs

    ceph osd set noout
    ceph osd set nobackfill

    # A: remapped away from a non-full OSD, takes P's only local slot
    ceph osd pg-upmap $PGA $P $X || return 1
    # wait for Backfilling (not just WaitRemoteBackfillReserved): a local
    # preemption is only handled in Backfilling
    wait_pg_state $PGA backfilling || return 1
    expect_prio $P $PGA "in_progress:$NORMAL_PRIO" "A holds the slot" || ERRORS=$((ERRORS + 1))

    # R2 becomes backfillfull, then B is remapped away from it
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$R2) injectfull backfillfull || return 1
    wait_osd_state $R2 backfillfull 1 || return 1
    ceph osd pg-upmap $PGB $P $Y || return 1
    expect_prio $P $PGB "in_progress:$BOOSTED_PRIO" "B is boosted and preempts A" || ERRORS=$((ERRORS + 1))
    wait_pg_state $PGB backfilling || ERRORS=$((ERRORS + 1))
    expect_prio $P $PGA "queued:$NORMAL_PRIO" "A was preempted and re-queued" || ERRORS=$((ERRORS + 1))

    # C: remapped away from a non-full OSD, waits
    ceph osd pg-upmap $PGC $P $X || return 1
    expect_prio $P $PGC "queued:$NORMAL_PRIO" "C queued unboosted" || ERRORS=$((ERRORS + 1))

    # R3 becomes backfillfull: the AdvMap hook raises queued C in place
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$R3) injectfull backfillfull || return 1
    wait_osd_state $R3 backfillfull 1 || return 1
    expect_prio $P $PGC "queued:$BOOSTED_PRIO" "C raised by the AdvMap hook" || ERRORS=$((ERRORS + 1))

    # R2 clears: running B keeps its priority (and is therefore not preempted by C)
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$R2) injectfull none || return 1
    wait_osd_state $R2 backfillfull 0 || return 1
    sleep 5
    expect_prio $P $PGB "in_progress:$BOOSTED_PRIO" "running B not lowered" || ERRORS=$((ERRORS + 1))

    # R3 clears: queued C keeps its boost, the hook never lowers a priority
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$R3) injectfull none || return 1
    wait_osd_state $R3 backfillfull 0 || return 1
    sleep 5
    expect_prio $P $PGC "queued:$BOOSTED_PRIO" "queued C keeps its boost" || ERRORS=$((ERRORS + 1))

    # the hook should have logged its updates on the primary
    grep -q "drains a full osd" $dir/osd.$P.log || {
        echo "FAIL: no priority update logged on osd.$P" ; ERRORS=$((ERRORS + 1)) ; }

    ceph osd unset nobackfill
    ceph osd unset noout
    wait_for_clean || return 1

    if [ $ERRORS != "0" ] ; then
        echo "$ERRORS error(s) found"
        return 1
    fi
    delete_pool A ; delete_pool B ; delete_pool C
    kill_daemons $dir || return 1
    return 0
}

main osd-backfill-fullness-prio "$@"

# Local Variables:
# compile-command: "make -j4 && ../qa/run-standalone.sh osd-backfill-fullness-prio.sh"
# End:
