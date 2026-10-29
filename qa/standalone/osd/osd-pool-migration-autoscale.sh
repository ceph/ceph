#!/usr/bin/env bash
#
# A migration leaves the target pool with the autoscale mode it was created
# with, and no PG count changes while it migrates.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7160" # git grep '\<7160\>' : there must be only one
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

function wait_for_migration() {
    local pool=$1
    for i in $(seq 1 120) ; do
        ceph osd dump --format=json | \
            jq -e ".pools[] | select(.pool_name == \"$pool\") | has(\"migration_src\") | not" > /dev/null && return 0
        sleep 1
    done
    return 1
}

function TEST_migrated_pool_keeps_autoscale_mode() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in 0 1 2 ; do
        run_osd $dir $id || return 1
    done
    ceph config set global enable_experimental_unrecoverable_data_corrupting_features poolmigration || return 1
    ceph osd set-require-min-compat-client umbrella || return 1
    ceph osd pool create keep 4 4 replicated || return 1
    wait_for_clean || return 1

    # no PG can migrate until recovery is allowed again
    ceph osd set norecover || return 1
    ceph osd set nobackfill || return 1
    ceph osd pool migrate keep --pool_type replicated --pg_num 4 --autoscale_mode warn || return 1
    ! ceph osd pool set keep pg_num 8 || return 1
    ! ceph osd pool set keep pgp_num 2 || return 1
    ceph osd unset norecover || return 1
    ceph osd unset nobackfill || return 1

    wait_for_migration keep || return 1
    test "$(ceph osd pool get keep pg_autoscale_mode --format=json | jq -r '.pg_autoscale_mode')" = warn || return 1
    test "$(ceph osd pool get keep nopgchange --format=json | jq -r '.nopgchange')" = false || return 1
    ceph osd pool set keep pg_num 8 || return 1
}

main osd-pool-migration-autoscale "$@"
