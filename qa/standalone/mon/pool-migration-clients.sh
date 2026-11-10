#!/usr/bin/env bash
#
# Pool migration with connected clients that can't follow a migration.
# mon_debug_hide_client_features makes the monitor see clients as older:
# 1 << 54 (POOL_MIGRATION) for an upstream umbrella client, 1 << 16
# (SERVER_OCTOPUS) for a luminous to nautilus one, which still sends bit 54.
# It stays set while the old client runs, because a client may reopen its
# monitor session; the test's own ceph commands look old meanwhile too.
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

POOL_MIGRATION=$((1 << 54))
SERVER_OCTOPUS=$((1 << 16))

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7174" # git grep '\<7174\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir
        local ret=$?
        stop_old_client
        [ $ret = 0 ] || return 1
        teardown $dir || return 1
    done
}

function start_cluster() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in 0 1 2 ; do
        run_osd $dir $id || return 1
    done
    create_pool test 8 || return 1
    wait_for_clean || return 1
    for i in $(seq 1 64) ; do
        rados -p test put obj$i /etc/hosts || return 1
    done
    ceph osd set-require-min-compat-client umbrella || return 1
    ceph osd set-require-min-compat-client pool_migration || return 1
}

function enable_pool_migration() {
    ceph config set global \
        enable_experimental_unrecoverable_data_corrupting_features \
        poolmigration || return 1
}

# Leave a client connected that the monitor sees without the given features
function start_old_client() {
    local dir=$1
    local hide=$2

    ceph tell mon.a config set mon_debug_hide_client_features $hide || return 1
    PYTHONUNBUFFERED=1 ceph -w > $dir/old-client.log 2>&1 &
    OLD_CLIENT_PID=$!
    local -i loop=0
    while ! grep -q 'cluster:' $dir/old-client.log ; do
        (( loop++ < 60 )) || return 1
        sleep 1
    done
}

function stop_old_client() {
    if [ -n "$OLD_CLIENT_PID" ] ; then
        kill $OLD_CLIENT_PID
        wait $OLD_CLIENT_PID
        OLD_CLIENT_PID=
        ceph tell mon.a config set mon_debug_hide_client_features 0
    fi
}

function TEST_require_pool_migration_clients() {
    local dir=$1

    start_cluster $dir || return 1
    enable_pool_migration || return 1
    ceph osd unset-require-min-compat-client pool_migration || return 1
    ! ceph osd dump | grep -w require_pool_migration || return 1
    ceph osd unset-require-min-compat-client pool_migration 2>&1 | \
        grep "is not set already" || return 1

    # a migration needs the requirement
    ! ceph osd pool migrate test 16 2> $dir/migrate.out || return 1
    grep "set-require-min-compat-client pool_migration" $dir/migrate.out || return 1

    # which a connected client that lacks the feature blocks
    start_old_client $dir $POOL_MIGRATION || return 1
    ! ceph osd set-require-min-compat-client pool_migration 2> $dir/set.out || return 1
    cat $dir/set.out
    grep "cannot set require_min_compat_client pool_migration: [0-9]* connected client(s)" \
        $dir/set.out || return 1
    ceph osd set-require-min-compat-client pool_migration --yes-i-really-mean-it || return 1
    ceph osd dump | grep -w flags | grep -w require_pool_migration || return 1
    ceph osd unset-require-min-compat-client pool_migration || return 1
    ! ceph osd dump | grep -w require_pool_migration || return 1
    stop_old_client
    wait_for_health_gone POOL_MIGRATION_INCOMPATIBLE_CLIENTS || return 1
    ceph osd set-require-min-compat-client pool_migration || return 1
    ceph osd set-require-min-compat-client pool_migration 2>&1 | \
        grep "is set already" || return 1

    # and which a migration keeps
    ceph osd pause || return 1
    ceph osd pool migrate test 16 || return 1
    ! ceph osd unset-require-min-compat-client pool_migration 2> $dir/unset.out || return 1
    grep "is being migrated" $dir/unset.out || return 1
    ceph osd dump | grep -w flags | grep -w require_pool_migration || return 1
    ceph osd unpause || return 1
}

function TEST_old_clients_block_migration() {
    local dir=$1

    start_cluster $dir || return 1

    # no warning until pool migration is enabled
    start_old_client $dir $POOL_MIGRATION || return 1
    sleep 15
    ! ceph health detail | grep POOL_MIGRATION_INCOMPATIBLE_CLIENTS || return 1
    enable_pool_migration || return 1
    wait_for_health POOL_MIGRATION_INCOMPATIBLE_CLIENTS || return 1
    ceph health detail | grep "client.admin at .* can't follow a pool migration" || return 1

    ! ceph osd pool migrate test 16 2> $dir/migrate.out || return 1
    cat $dir/migrate.out
    grep 'pool migration is not supported by [0-9]* connected client(s)' $dir/migrate.out || return 1
    ! ceph osd pool ls --show-all | grep -x '\.migrate-1' || return 1

    stop_old_client
    wait_for_health_gone POOL_MIGRATION_INCOMPATIBLE_CLIENTS || return 1

    start_old_client $dir $SERVER_OCTOPUS || return 1
    wait_for_health POOL_MIGRATION_INCOMPATIBLE_CLIENTS || return 1
    ! ceph osd pool migrate test 16 || return 1

    stop_old_client
    wait_for_health_gone POOL_MIGRATION_INCOMPATIBLE_CLIENTS || return 1
    ceph osd pool migrate test 16 || return 1
    ceph osd pool ls --show-all | grep -x '\.migrate-1' || return 1
}

function TEST_old_client_during_migration() {
    local dir=$1

    start_cluster $dir || return 1
    enable_pool_migration || return 1

    # keep the migration going
    ceph osd pause || return 1
    ceph osd pool migrate test 16 || return 1
    ceph osd pool ls detail --format=json | \
        jq -e '.[] | select(.pool_name == "test") | .migration_src' || return 1

    start_old_client $dir $POOL_MIGRATION || return 1
    wait_for_health POOL_MIGRATING_WITH_INCOMPATIBLE_CLIENTS || return 1
    ceph health | grep HEALTH_ERR || return 1
    ! ceph health detail | grep POOL_MIGRATION_INCOMPATIBLE_CLIENTS || return 1

    stop_old_client
    wait_for_health_gone POOL_MIGRATING_WITH_INCOMPATIBLE_CLIENTS || return 1
    ceph osd unpause || return 1
}

main pool-migration-clients "$@"

# Local Variables:
# compile-command: "cd ../../../build ; make -j4 && ../qa/run-standalone.sh pool-migration-clients.sh"
# End:
