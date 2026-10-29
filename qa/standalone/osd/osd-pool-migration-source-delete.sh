#!/usr/bin/env bash
#
# The migration of an object finishes when the lock that delayed deleting it
# from the source pool is released, even though the object still holds the
# PG's only recovery slot.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7162" # git grep '\<7162\>' : there must be only one
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

function TEST_source_delete_waits_for_lock() {
    local dir=$1
    local osd_args="--osd_mclock_override_recovery_settings=true --osd_recovery_max_active=1"

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in 0 1 2 ; do
        run_osd $dir $id $osd_args || return 1
    done
    ceph config set global enable_experimental_unrecoverable_data_corrupting_features poolmigration || return 1
    ceph osd set-require-min-compat-client umbrella || return 1
    ceph osd pool create held 1 1 replicated || return 1
    wait_for_clean || return 1

    echo hello > $dir/obj
    for i in $(seq 1 4) ; do
        rados -p held put obj$i $dir/obj || return 1
    done

    # the first object copied is read-locked for 2 seconds when its copy
    # completes
    ceph tell osd.\* config set osd_debug_pool_migration_hold_source_delete 2 || return 1
    ceph osd pool migrate held --pool_type replicated || return 1
    wait_for_migration held || return 1
    grep -q "injecting a read" $dir/osd.*.log || return 1
    for i in $(seq 1 4) ; do
        rados -p held get obj$i $dir/got || return 1
        cmp $dir/obj $dir/got || return 1
    done
}

main osd-pool-migration-source-delete "$@"
