#!/usr/bin/env bash
#
# Copyright (C) 2026 IBM <contact@ibm.com>
#
# Author: Kyle Bader <kbader@ibm.com>
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
# Losing OSDs shrinks the PG budget, which in legacy autoscaling could
# trigger an immediate merge storm on top of recovery. Under simple
# autoscaling nothing may move on its own: warn pools hold their plan
# pending, and even a mode 'on' pool is held when the plan is a large
# merge on a pool holding data (mon_osd_pool_pg_merge_confirm_bytes).

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7103" # git grep '\<7103\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done
}

NUM_OSDS=8
# budget = T x OSDs = 768 PG replicas: ratio 0.5 -> pg_num 128 and
# 0.25 -> 64 at size 3. Keeping 2 of 8 OSDs quarters the budget so the
# plans stay exact powers of two: 128 -> 32 and 64 -> 16.
MON_TARGET_PG_PER_OSD=96

function wait_for() {
    local sec=$1
    local cmd=$2

    while true ; do
        if bash -c "$cmd" ; then
            break
        fi
        sec=$(( $sec - 1 ))
        if [ $sec -le 0 ]; then
            echo failed
            return 1
        fi
        sleep 1
    done
    return 0
}

function pg_num_target() {
    ceph osd dump -f json | jq ".pools[] | select(.pool_name == \"$1\") | .pg_num_target"
}

function TEST_simple_autoscale_osd_loss() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    local i
    for i in $(seq 0 $(( NUM_OSDS - 1 ))); do
        run_osd $dir $i || return 1
    done

    ceph config set global mon_target_pg_per_osd $MON_TARGET_PG_PER_OSD
    ceph config set mgr mgr/pg_autoscaler/sleep_interval 5
    ceph osd pool set simpleautoscale

    # born at their planned pg_num, plan already current
    ceph osd pool create pin-warn --effective-ratio 0.5 --autoscale-mode warn || return 1
    ceph osd pool create pin-on --effective-ratio 0.25 --autoscale-mode on || return 1
    ceph osd pool get pin-warn pg_num | grep -w 128 || return 1
    ceph osd pool get pin-on pg_num | grep -w 64 || return 1
    ceph osd pool get pin-warn pg_autoscale_plan | grep -w current || return 1
    # pg_num_min 16 keeps pin-on's post-loss plan (16) below half of its
    # pg_num, so the merge-confirm guardrail is exercised
    ceph osd pool set pin-on pg_num_min 16 || return 1
    wait_for_clean || return 1

    # pin-on holds data, so its large merge must be held for confirmation
    ceph config set mon mon_osd_pool_pg_merge_confirm_bytes 4096
    rados -p pin-on bench 2 write -b 65536 --no-cleanup || return 1
    wait_for 120 "ceph df | grep -w pin-on | grep -Eq 'KiB|MiB'" || return 1

    # lose three quarters of the cluster: kill and purge 6 of 8 OSDs. The
    # budget drops to 192 PG replicas: pin-warn plans 32, pin-on plans 16.
    # (wait_for_osd clobbers a variable named i, so use another)
    local osd_id
    for osd_id in 2 3 4 5 6 7; do
        kill_daemons $dir KILL osd.$osd_id || return 1
        wait_for_osd down $osd_id || return 1
        ceph osd purge $osd_id --yes-i-really-mean-it || return 1
    done

    # both pools go pending with their new plans stamped...
    wait_for 120 "ceph osd pool get pin-warn pg_autoscale_plan | grep -w pending" || return 1
    wait_for 120 "ceph osd pool get pin-on pg_autoscale_plan | grep -w pending" || return 1
    ceph osd pool get pin-warn planned_pg_num | grep -w 32 || return 1
    ceph osd pool get pin-on planned_pg_num | grep -w 16 || return 1
    wait_for 120 "ceph health detail | grep POOL_AUTOSCALE_PENDING" || return 1
    ceph health detail | grep "held for manual acceptance" || return 1

    # ... and nothing moves on its own: not the warn pool (never acts),
    # not the 'on' pool (its merge is held). Watch several planner passes.
    for i in $(seq 1 6); do
        sleep 5
        test "$(pg_num_target pin-warn)" = "128" || return 1
        test "$(pg_num_target pin-on)" = "64" || return 1
    done

    # acceptance executes exactly the stamped plans: the empty warn pool
    # accepts plainly, the data-bearing 'on' pool requires confirmation
    ceph osd pool autoscale-accept pin-warn || return 1
    wait_for 60 "test \$(ceph osd dump -f json | jq '.pools[] | select(.pool_name == \"pin-warn\") | .pg_num_target') = 32" || return 1
    wait_for 120 "ceph osd pool get pin-warn pg_autoscale_plan | grep -w current" || return 1
    ! ceph osd pool get pin-warn planned_pg_num || return 1

    ! ceph osd pool autoscale-accept pin-on || return 1
    ceph osd pool autoscale-accept pin-on --yes-i-really-mean-it || return 1
    wait_for 60 "test \$(ceph osd dump -f json | jq '.pools[] | select(.pool_name == \"pin-on\") | .pg_num_target') = 16" || return 1
    wait_for 120 "ceph osd pool get pin-on pg_autoscale_plan | grep -w current" || return 1
    wait_for_health_gone POOL_AUTOSCALE_PENDING || return 1

    ceph config rm mon mon_osd_pool_pg_merge_confirm_bytes
    teardown $dir || return 1
}

main pg-autoscaler-osd-loss "$@"

# Local Variables:
# compile-command: "cd build ; make -j4 && \
#    ../qa/run-standalone.sh pg-autoscaler-osd-loss.sh"
# End:
