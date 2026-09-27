#!/usr/bin/env bash
#
# Copyright (C) 2026 IBM
#
# Author: Kyle Bader <kbader@ibm.com>
#
# An OSD at the PG hard limit (mon_max_pg_per_osd *
# osd_max_pg_per_osd_hard_ratio) withholds creating a PG, and once it has
# room again it forces a pg_temp change so the PG peers again
# (OSD::resume_creating_pg). For a backfill target, which is in the PG's up
# set but not in its acting set:
# - the OSD must remember the withheld PG across new OSDMaps;
# - the pg_temp change must change the acting set, also when that is the
#   primary alone.
# Otherwise the primary waits in activating+remapped for good.
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

    export CEPH_MON="127.0.0.1:7307" # git grep '\<7307\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--osd_pool_default_size=2 --osd_pool_default_min_size=1 "
    # the hard limit is mon_max_pg_per_osd itself
    CEPH_ARGS+="--osd_max_pg_per_osd_hard_ratio=1 "
    # a short log: a new OSD is backfilled, not recovered from the log
    CEPH_ARGS+="--osd_min_pg_log_entries=1 --osd_max_pg_log_entries=2 --osd_pg_log_trim_min=1 "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function up_set() {
    ceph pg map $1 -f json | jq -r '.up | map(tostring) | join(" ")'
}

function num_pgs() {
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$1) status | jq -r '.num_pgs'
}

function osd_newest_map() {
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$1) status | jq -r '.newest_map'
}

# a cluster of 4 OSDs; pool a has one PG, with more writes than its log
# keeps, on $primary and $replica; $target holds two PGs of pools b and c,
# as many as it may; $other is the fourth OSD
function setup_target_at_limit() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for osd in 0 1 2 3 ; do
        run_osd $dir $osd || return 1
    done
    ceph osd set-require-min-compat-client luminous --yes-i-really-mean-it || return 1
    # only this test moves PGs
    ceph balancer off || return 1

    create_pool a 1 1 || return 1
    wait_for_clean || return 1
    for i in $(seq 1 20) ; do
        rados -p a put obj$i /etc/passwd || return 1
    done
    wait_for_clean || return 1
    pgid=$(ceph osd pool ls detail -f json | jq -r '.[] | select(.pool_name == "a") | .pool_id').0
    local up=($(up_set $pgid))
    primary=${up[0]}
    replica=${up[1]}
    target=""
    other=""
    for osd in 0 1 2 3 ; do
        if [ $osd != $primary -a $osd != $replica ] ; then
            if [ -z "$target" ] ; then
                target=$osd
            else
                other=$osd
            fi
        fi
    done

    create_pool b 1 1 || return 1
    create_pool c 1 1 || return 1
    for pool in b c ; do
        local id=$(ceph osd pool ls detail -f json | jq -r ".[] | select(.pool_name == \"$pool\") | .pool_id")
        ceph osd pg-upmap $id.0 $target $other || return 1
    done
    wait_for_clean || return 1
    ceph tell osd.$target config set mon_max_pg_per_osd $(num_pgs $target) || return 1
}

function wait_for_withhold() {
    local dir=$1
    local i
    for i in $(seq 1 60) ; do
        grep -q "withhold creation of pg $pgid" $dir/osd.$target.log && return 0
        sleep 1
    done
    return 1
}

function TEST_withheld_backfill_target_survives_new_map() {
    local dir=$1

    setup_target_at_limit $dir || return 1

    # move the PG from the replica to the target: the target is a backfill
    # target (pg_temp keeps the replica acting), and withholds creating it
    ceph osd pg-upmap-items $pgid $replica $target || return 1
    wait_for_withhold $dir || return 1

    # any new OSDMap
    ceph osd set noout || return 1
    local epoch=$(ceph osd dump -f json | jq '.epoch')
    local i
    for i in $(seq 1 60) ; do
        test $(osd_newest_map $target) -ge $epoch && break
        sleep 1
    done
    test $(osd_newest_map $target) -ge $epoch || return 1

    # room again: the PG must peer again and backfill the target
    ceph tell osd.$target config set mon_max_pg_per_osd 1000 || return 1
    wait_for_clean || return 1
    test "$(up_set $pgid)" = "$primary $target" || return 1
    ceph osd unset noout || return 1
}

function TEST_withheld_backfill_target_single_acting() {
    local dir=$1

    setup_target_at_limit $dir || return 1

    # the replica dies and is marked out, and the target replaces it: the
    # primary alone is acting while it backfills the target, which
    # withholds creating the PG
    kill_daemons $dir TERM osd.$replica || return 1
    ceph osd down $replica || return 1
    ceph osd out $replica || return 1
    ceph osd pg-upmap $pgid $primary $target || return 1
    wait_for_withhold $dir || return 1

    # room again: the pg_temp change must change the acting set [primary]
    ceph tell osd.$target config set mon_max_pg_per_osd 1000 || return 1
    wait_for_clean || return 1
    test "$(up_set $pgid)" = "$primary $target" || return 1
}

main osd-max-pg-backfill-target "$@"

# Local Variables:
# compile-command: "make -j4 && ../qa/run-standalone.sh osd-max-pg-backfill-target.sh"
# End:
