#!/usr/bin/env bash
#
# A snap removed while an earlier migration of the pool was the target stays
# removed: removing it again, as a client does when it resends the request,
# changes nothing.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7161" # git grep '\<7161\>' : there must be only one
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

function pool_id() {
    local pool=$1
    ceph osd dump --format=json | jq ".pools[] | select(.pool_name == \"$pool\") | .pool"
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

# Removes a self-managed snap of a pool, a new one unless a snap id is given,
# and prints its id
function remove_self_managed_snap() {
    python3 - "$@" <<'EOF'
import sys
import rados
cluster = rados.Rados()
cluster.conf_parse_env()
cluster.connect()
ioctx = cluster.open_ioctx(sys.argv[1])
snap = int(sys.argv[2]) if len(sys.argv) > 2 else ioctx.create_self_managed_snap()
ioctx.remove_self_managed_snap(snap)
print(snap)
ioctx.close()
cluster.shutdown()
EOF
}

# Lists the removed snaps that the OSDs of a pool are still to trim
function snaps_to_trim() {
    local pool_id=$1
    ceph osd dump --format=json | jq -c "[.removed_snaps_queue[] | select(.pool == $pool_id) | .snaps[]]"
}

function TEST_snap_removed_again_after_migration() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in 0 1 2 ; do
        run_osd $dir $id || return 1
    done
    ceph config set global enable_experimental_unrecoverable_data_corrupting_features poolmigration || return 1
    ceph osd set-require-min-compat-client umbrella || return 1
    ceph osd pool create snaprm 4 4 replicated || return 1
    wait_for_clean || return 1

    ceph osd pool migrate snaprm --pool_type replicated || return 1
    wait_for_migration snaprm || return 1
    local first_target=$(pool_id snaprm)

    local snap
    snap=$(remove_self_managed_snap snaprm) || return 1
    for i in $(seq 1 60) ; do
        test "$(snaps_to_trim $first_target)" = "[]" && break
        sleep 1
    done
    test "$(snaps_to_trim $first_target)" = "[]" || return 1

    ceph osd pool migrate snaprm --pool_type replicated || return 1
    remove_self_managed_snap snaprm $snap || return 1
    test "$(snaps_to_trim $first_target)" = "[]" || return 1
    test "$(snaps_to_trim $(pool_id snaprm))" = "[]" || return 1
}

main osd-pool-migration-snap-remove "$@"
