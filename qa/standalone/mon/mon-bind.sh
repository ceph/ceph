#!/usr/bin/env bash
#
# Copyright (C) 2017 Quantum Corp.
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

SOCAT_PIDS=()

function port_forward() {
    local source_port=$1
    local target_port=$2

    socat TCP-LISTEN:${source_port},fork,reuseaddr TCP:localhost:${target_port} &
    SOCAT_PIDS+=( $! )
}

function cleanup() {
    for p in "${SOCAT_PIDS[@]}"; do
        kill $p
    done
    SOCAT_PIDS=()
}

trap cleanup SIGTERM SIGKILL SIGQUIT SIGINT

function run() {
    local dir=$1
    shift

    export MON_IP=127.0.0.1
    export MONA_PUBLIC=7132 # git grep '\<7132\>' ; there must be only one
    export MONB_PUBLIC=7133 # git grep '\<7133\>' ; there must be only one
    export MONC_PUBLIC=7134 # git grep '\<7134\>' ; there must be only one
    export MONA_BIND=7135   # git grep '\<7135\>' ; there must be only one
    export MONB_BIND=7136   # git grep '\<7136\>' ; there must be only one
    export MONC_BIND=7137   # git grep '\<7137\>' ; there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir && cleanup || { cleanup; return 1; }
        teardown $dir
    done
}

function TEST_mon_client_connect_fails() {
    local dir=$1

    # start the mon with a public-bind-addr that is different
    # from the public-addr.
    CEPH_ARGS+="--mon-initial-members=a "
    CEPH_ARGS+="--mon-host=${MON_IP}:${MONA_PUBLIC} "
    run_mon $dir a --mon-host=${MON_IP}:${MONA_PUBLIC} --public-bind-addr=${MON_IP}:${MONA_BIND} || return 1

    # now attempt to ping it that should fail.
    timeout 3 ceph ping mon.a || return 0
    return 1
}

function TEST_mon_client_connect() {
    local dir=$1

    # start the mon with a public-bind-addr that is different
    # from the public-addr.
    CEPH_ARGS+="--mon-initial-members=a "
    CEPH_ARGS+="--mon-host=${MON_IP}:${MONA_PUBLIC} "
    run_mon $dir a --mon-host=${MON_IP}:${MONA_PUBLIC} --public-bind-addr=${MON_IP}:${MONA_BIND} || return 1

    # now forward the public port to the bind port.
    port_forward ${MONA_PUBLIC} ${MONA_BIND}

    # attempt to connect. we expect that to work
    ceph ping mon.a || return 1
}

function TEST_mon_quorum() {
    local dir=$1

    # start the mon with a public-bind-addr that is different
    # from the public-addr.
    CEPH_ARGS+="--mon-initial-members=a,b,c "
    CEPH_ARGS+="--mon-host=${MON_IP}:${MONA_PUBLIC},${MON_IP}:${MONB_PUBLIC},${MON_IP}:${MONC_PUBLIC} "
    run_mon $dir a --public-addr=${MON_IP}:${MONA_PUBLIC} --public-bind-addr=${MON_IP}:${MONA_BIND} || return 1
    run_mon $dir b --public-addr=${MON_IP}:${MONB_PUBLIC} --public-bind-addr=${MON_IP}:${MONB_BIND} || return 1
    run_mon $dir c --public-addr=${MON_IP}:${MONC_PUBLIC} --public-bind-addr=${MON_IP}:${MONC_BIND} || return 1

    # now forward the public port to the bind port.
    port_forward ${MONA_PUBLIC} ${MONA_BIND}
    port_forward ${MONB_PUBLIC} ${MONB_BIND}
    port_forward ${MONC_PUBLIC} ${MONC_BIND}

    # expect monmap to contain 3 monitors (a, b, and c)
    jqinput="$(ceph quorum_status --format=json 2>/dev/null)"
    jq_success "$jqinput" '.monmap.mons | length == 3' || return 1

    # quorum should form
    wait_for_quorum 300 3 || return 1
    # expect quorum to have all three monitors
    jqfilter='.quorum | length == 3'
    jq_success "$jqinput" "$jqfilter" || return 1
}

function TEST_put_get() {
    local dir=$1

    # start the mon with a public-bind-addr that is different
    # from the public-addr.
    CEPH_ARGS+="--mon-initial-members=a,b,c "
    CEPH_ARGS+="--mon-host=${MON_IP}:${MONA_PUBLIC},${MON_IP}:${MONB_PUBLIC},${MON_IP}:${MONC_PUBLIC} "
    run_mon $dir a --public-addr=${MON_IP}:${MONA_PUBLIC} --public-bind-addr=${MON_IP}:${MONA_BIND} || return 1
    run_mon $dir b --public-addr=${MON_IP}:${MONB_PUBLIC} --public-bind-addr=${MON_IP}:${MONB_BIND} || return 1
    run_mon $dir c --public-addr=${MON_IP}:${MONC_PUBLIC} --public-bind-addr=${MON_IP}:${MONC_BIND} || return 1

    # now forward the public port to the bind port.
    port_forward ${MONA_PUBLIC} ${MONA_BIND}
    port_forward ${MONB_PUBLIC} ${MONB_BIND}
    port_forward ${MONC_PUBLIC} ${MONC_BIND}

    # quorum should form
    wait_for_quorum 300 3 || return 1

    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    create_pool hello 8 || return 1

    echo "hello world" > $dir/hello
    rados --pool hello put foo $dir/hello || return 1
    rados --pool hello get foo $dir/hello2 || return 1
    diff $dir/hello $dir/hello2 || return 1
}

main mon-bind "$@"
