#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

mon_port=$(get_unused_port)

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:$mon_port"
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    set -e

    setup $dir || return 1
    TEST_tox $dir || return 1
    teardown $dir || return 1
}

function TEST_tox() {
    local dir=$1

    run_mon $dir a
    run_mgr $dir x
    pushd ../python-common
    CEPH_DIR=$PWD/$dir ../script/run_tox.sh
}


main python-common "$@"
