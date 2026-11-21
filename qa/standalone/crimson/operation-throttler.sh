#!/usr/bin/env bash
# Exercise throttle dependencies through real client writes and backfill.
# Run from build/:
#   ../qa/run-standalone.sh crimson/operation-throttler.sh

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7197" # git grep '\<7197\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-TEST_wpq TEST_mclock}
    local bench_pid
    for func in $funcs; do
        setup $dir || return 1
        if ! $func $dir; then
            if [ -n "$bench_pid" ]; then
                kill "$bench_pid" 2>/dev/null || true
                wait "$bench_pid" 2>/dev/null || true
            fi
            timeout 10 ceph pg dump pgs || true
            local id
            for id in 0 1 2 3; do
                CEPH_ARGS= timeout 10 ceph --admin-daemon "$(get_asok_path osd.$id)" \
                    dump_ops_in_flight > "$dir/osd.$id-ops.log" 2>&1 || true
            done
            return 1
        fi
        teardown $dir || return 1
    done
}

function wait_for_throttle_pgs() {
    local placement=${1:-initial}
    local deadline=$((SECONDS + 120))
    local pgs
    while [ $SECONDS -lt $deadline ]; do
        pgs=$(set -o pipefail
            timeout 10 ceph pg dump pgs --format=json |
                jq -c '[.pg_stats[] | {state, acting}]') || return 1
        if jq -e --arg placement "$placement" '
            length == 8 and
            all(.[]; .state | contains("active")) and
            all(.[]; .state | contains("clean")) and
            (if $placement == "out" then
                all(.[]; .acting | index(0) == null)
             elif $placement == "in" then
                any(.[]; .acting | index(0) != null)
             else
                true
             end)' <<< "$pgs" >/dev/null; then
            return 0
        fi
        sleep 1
    done
    echo "Timed out waiting for clean PGs after $placement placement"
    return 1
}

function write_during_backfill() {
    local dir=$1
    local action=$2

    # Start writes before changing placement, so requests can be requeued
    # and compete with recovery while the throttle is saturated.
    timeout 90 rados -p throttle bench 10 write -t 32 -b 4096 \
        --run-name "$action" --no-cleanup \
        > "$dir/write-$action.log" 2>&1 &
    bench_pid=$!
    sleep 1
    kill -0 "$bench_pid" || return 1
    ceph osd "$action" 0 || return 1
    kill -0 "$bench_pid" || return 1
    local result=0
    wait "$bench_pid" || result=$?
    bench_pid=
    cat "$dir/write-$action.log"
    test "$result" = 0 || return 1
    wait_for_throttle_pgs "$action" || return 1
}

function test_throttle_backfill() {
    local dir=$1
    local scheduler=$2

    run_mon $dir a --osd_pool_default_size=3 \
        --osd_pool_default_min_size=2 --osd_pool_default_crimson=true || return 1
    run_mgr $dir x || return 1
    local id
    for id in 0 1 2 3; do
        run_crimson_osd $dir $id --osd-objectstore=cyanstore \
            --crimson-memory=512M \
            --crimson-reactor-backend=epoll --crimson-osd-scheduler-concurrency=3 \
            --osd-op-queue="$scheduler" --osd-op-queue-cut-off=high \
            --osd-mclock-profile=high_recovery_ops || return 1
    done
    create_pool throttle 8 8 || return 1
    wait_for_throttle_pgs || return 1

    # Populate every replica before changing placement. The workload's
    # concurrency exceeds the shared throttle limit on each OSD.
    timeout 90 rados -p throttle bench 1 write -t 32 -b 65536 \
        --max-objects 128 --run-name seed --no-cleanup || return 1
    write_during_backfill $dir out || return 1
    write_during_backfill $dir in || return 1

    # Sequential bench reads validate the contents, including data written
    # before and during both placement changes.
    local name
    for name in seed out in; do
        timeout 90 rados -p throttle bench 10 seq --run-name "$name" || return 1
    done
}

function TEST_wpq() {
    test_throttle_backfill "$1" wpq
}

function TEST_mclock() {
    test_throttle_backfill "$1" mclock_scheduler
}

main operation-throttler "$@"
