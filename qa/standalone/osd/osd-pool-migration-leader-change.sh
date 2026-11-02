#!/usr/bin/env bash
#
# A pool migration finishes when the OSDs report migrated PGs to a monitor
# that lost the leadership.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7171" # git grep '\<7171\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7172" # git grep '\<7172\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7173" # git grep '\<7173\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
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
    ceph osd pool ls detail --show-all
    return 1
}

function leader() {
    timeout 30 ceph quorum_status --format=json | jq -r .quorum_leader_name
}

function wait_for_leader() {
    local name=$1
    for i in $(seq 1 120) ; do
        test "$(leader)" = $name && return 0
        sleep 1
    done
    return 1
}

# mon.a, the leader, takes a change to the source pool into its pending map,
# cannot commit it because mon.b and mon.c are stopped, and loses the
# leadership to mon.b
function strand_pending_source_pool_on_mon_a() {
    local dir=$1
    local ok=0

    kill -STOP $(cat $dir/mon.b.pid) $(cat $dir/mon.c.pid)
    CEPH_ARGS="${CEPH_ARGS/--mon-host=$CEPH_MON /--mon-host=$CEPH_MON_A }" \
        timeout 30 ceph osd pool application enable .migrate-1 pending --yes-i-really-mean-it &
    local cli=$!
    for i in $(seq 1 20) ; do
        if grep -q 'prepare_update mon_command.*osd pool application enable' $dir/mon.a.log ; then
            ok=1
            break
        fi
        sleep 1
    done
    kill -STOP $(cat $dir/mon.a.pid)
    kill -CONT $(cat $dir/mon.b.pid) $(cat $dir/mon.c.pid)
    wait_for_leader b && ceph mon add disallowed_leader a || ok=0
    kill -CONT $(cat $dir/mon.a.pid)
    wait $cli
    test $ok = 1
}

function migrate_reporting_to_mon_a() {
    local dir=$1
    local trigger=$2

    run_mon $dir a --public-addr $CEPH_MON_A || return 1
    run_mon $dir b --public-addr $CEPH_MON_B || return 1
    run_mon $dir c --public-addr $CEPH_MON_C || return 1
    wait_for_quorum 300 3 || return 1
    ceph mon set election_strategy disallow || return 1
    run_mgr $dir x || return 1
    for id in 0 1 2 ; do
        run_osd $dir $id || return 1
    done
    ceph config set global enable_experimental_unrecoverable_data_corrupting_features poolmigration || return 1
    ceph osd set-require-min-compat-client umbrella || return 1
    ceph config set mon mon_pool_migration_max_pg_ratio 0.01 || return 1
    ceph osd pool create src 4 4 replicated || return 1
    wait_for_clean || return 1
    for i in $(seq 1 8) ; do
        rados -p src put obj$i /etc/hosts || return 1
    done

    # no PG can migrate until recovery is allowed again
    ceph osd set norecover || return 1
    ceph osd set nobackfill || return 1
    ceph osd pool migrate src --pool_type replicated --pg_num 4 || return 1
    wait_for_leader a || return 1
    if [ $trigger = yes ] ; then
        strand_pending_source_pool_on_mon_a $dir || return 1
    else
        ceph mon add disallowed_leader a || return 1
    fi
    wait_for_quorum 300 3 || return 1
    wait_for_leader b || return 1

    for id in 0 1 2 ; do
        kill_daemons $dir TERM osd.$id || return 1
        activate_osd $dir $id --mon-host-override=$CEPH_MON_A || return 1
    done
    ceph osd unset norecover || return 1
    ceph osd unset nobackfill || return 1
    wait_for_migration src || return 1
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path mon.a) log flush || return 1
    test $(grep -c 'preprocess_pg_migrated_pool osd_pg_migrate_pool' $dir/mon.a.log) -ge 4 || return 1
    for i in $(seq 1 8) ; do
        rados -p src get obj$i $dir/obj || return 1
        cmp $dir/obj /etc/hosts || return 1
    done
}

function TEST_migration_reporting_to_former_leader() {
    migrate_reporting_to_mon_a $1 no
}

function TEST_migration_reporting_to_former_leader_with_pending_source() {
    migrate_reporting_to_mon_a $1 yes
}

main osd-pool-migration-leader-change "$@"
