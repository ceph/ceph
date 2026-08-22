#!/bin/bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7114" # git grep '\<7114\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_osd_perf() {
    local dir=$1
    setup $dir || return 1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1

    # Drive a mix of read and write IO so every per-OSD KPI
    # (op_latency / op_r_latency / op_w_latency / bluestore_w_latency /
    # bluestore_r_latency / kv_sync) has at least one sample by the time
    # we query the command.
    ceph osd pool create testpool 8 || return 1
    rados bench -p testpool 10 write --no-cleanup >/dev/null 2>&1
    rados bench -p testpool 5 rand >/dev/null 2>&1
    sleep 10

    # Plain text output: the legacy commit/apply columns were dropped;
    # prometheus and the dashboard read pg_map.osd_stat directly.
    local out
    out=$(ceph osd perf) || return 1
    echo "$out" | grep -q "commit_latency(ms)" && return 1
    echo "$out" | grep -q "apply_latency(ms)" && return 1
    echo "$out" | grep -q "op_latency(ms)" || return 1
    echo "$out" | grep -q "op_r_latency(ms)" || return 1
    echo "$out" | grep -q "op_w_latency(ms)" || return 1
    echo "$out" | grep -q "bluestore_w_latency(ms)" || return 1
    echo "$out" | grep -q "bluestore_r_latency(ms)" || return 1
    echo "$out" | grep -q "kv_sync(ms)" || return 1

    # JSON output: same shape, legacy keys removed.
    local json
    json=$(ceph osd perf -f json) || return 1
    echo "$json" | grep -q '"commit_latency_ms"' && return 1
    echo "$json" | grep -q '"apply_latency_ms"' && return 1
    echo "$json" | grep -q '"osd_perf_infos"' || return 1
    echo "$json" | grep -q '"op_latency_ms"' || return 1

    teardown $dir || return 1
}

main osd-perf "$@"
