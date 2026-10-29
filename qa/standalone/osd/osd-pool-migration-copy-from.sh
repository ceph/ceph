#!/usr/bin/env bash
#
# A copy_from whose source is in a pool being migrated finds the object where
# it is, whether or not its PG has migrated yet.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7159" # git grep '\<7159\>' : there must be only one
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

# A client that opens the pool by name once the migration has started addresses
# the source of a copy to the target pool
function TEST_copy_from_unmigrated_object() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in 0 1 2 ; do
        run_osd $dir $id || return 1
    done
    ceph config set global enable_experimental_unrecoverable_data_corrupting_features poolmigration || return 1
    ceph osd set-require-min-compat-client umbrella || return 1
    ceph osd pool create cfpool 8 8 replicated || return 1
    wait_for_clean || return 1

    echo hello > $dir/src
    rados -p cfpool put src $dir/src || return 1
    rados -p cfpool cp src before || return 1

    # no PG can migrate, so src stays in the source pool
    ceph osd set norecover || return 1
    ceph osd set nobackfill || return 1
    ceph osd pool migrate cfpool --pool_type replicated || return 1

    rados -p cfpool get src $dir/got || return 1
    cmp $dir/src $dir/got || return 1
    rados -p cfpool cp src after || return 1
    rados -p cfpool get after $dir/copied || return 1
    cmp $dir/src $dir/copied || return 1
}

main osd-pool-migration-copy-from "$@"
