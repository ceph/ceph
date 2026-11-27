#!/usr/bin/env bash
#
# Copyright (C) 2026 IBM
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

MONA=127.0.0.1:7360 # git grep '\<7360\>' : there must be only one
MONB=127.0.0.1:7361 # git grep '\<7361\>' : there must be only one
MONC=127.0.0.1:7362 # git grep '\<7362\>' : there must be only one

function run() {
    local dir=$1
    shift

    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS_BASE=$CEPH_ARGS
    export CEPH_ARGS

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function start_migration() {
    local pool=$1
    ceph config set global enable_experimental_unrecoverable_data_corrupting_features poolmigration || return 1
    ceph osd set-require-min-compat-client vampire || return 1
    ceph osd pool set noautoscale || return 1
    # to twice the PGs; the target takes the pool's name and the source
    # becomes .migrate-1
    ceph osd pool migrate $pool 16 || return 1
}

function wait_for_migration() {
    for i in $(seq 1 120) ; do
        ceph osd pool ls detail -f json | \
            jq -e 'map(select(.migration_src != null or .migration_target != null)) | length == 0' > /dev/null && return 0
        sleep 5
    done
    ceph osd pool ls detail
    ceph pg dump pgs_brief
    return 1
}

function TEST_migrate_keeps_applications() {
    local dir=$1

    CEPH_ARGS="$CEPH_ARGS_BASE--mon-host=$MONA "
    run_mon $dir a --public-addr $MONA || return 1
    run_mgr $dir x || return 1
    for id in 0 1 2 ; do
        run_osd $dir $id || return 1
    done
    ceph osd pool create data 8 || return 1
    ceph osd pool application enable data rbd || return 1
    wait_for_clean || return 1

    start_migration data || return 1
    ceph osd pool application get data > $dir/apps.json || return 1
    grep rbd $dir/apps.json || return 1
    ceph osd pool application get .migrate-1 | grep rbd || return 1
}

function TEST_migrate_answers_peons() {
    local dir=$1

    CEPH_ARGS="$CEPH_ARGS_BASE--mon-host=$MONA,$MONB,$MONC "
    run_mon $dir a --public-addr $MONA || return 1
    run_mon $dir b --public-addr $MONB || return 1
    run_mon $dir c --public-addr $MONC || return 1
    run_mgr $dir x || return 1
    local leader=$(timeout 60 ceph quorum_status -f json | jq -r .quorum_leader_name)
    local peon_addr=$(ceph mon dump -f json | \
        jq -r "[.mons[] | select(.name != \"$leader\")] | first | .public_addrs.addrvec[0] | \"\\(.type):\\(.addr)\"")
    test -n "$peon_addr" || return 1
    # every OSD only knows a peon, which forwards its messages to the leader
    for id in 0 1 2 ; do
        CEPH_ARGS="$CEPH_ARGS_BASE--mon-host=$peon_addr " run_osd $dir $id || return 1
    done
    ceph osd pool create data 8 || return 1
    wait_for_clean || return 1
    echo migrated > $dir/obj
    for i in $(seq 0 15) ; do
        rados -p data put obj$i $dir/obj || return 1
    done

    start_migration data || return 1
    wait_for_migration || return 1
    wait_for_clean || return 1
    for mon in a b c ; do
        CEPH_ARGS='' ceph --admin-daemon $(get_asok_path mon.$mon) ops > $dir/ops.$mon.json || return 1
        local stuck=$(jq '[.ops[] | select(.description | startswith("osd_pg_migrate_pool"))] | length' $dir/ops.$mon.json)
        if test "$stuck" != 0 ; then
            echo "mon.$mon still tracks $stuck osd_pg_migrate_pool request(s)"
            jq '.ops[] | select(.description | startswith("osd_pg_migrate_pool"))' $dir/ops.$mon.json | head -40
            return 1
        fi
    done
}

main osd-pool-migrate "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/mon/osd-pool-migrate.sh"
# End:
