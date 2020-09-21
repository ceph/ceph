#!/usr/bin/env bash
set -ex

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7202" # git grep '\<7202\>' : there must be only one
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

function TEST_failure_log() {
    local dir=$1

    cat > $dir/test_failure.log << EOF
This is a fake log file
*
*
*
*
*
This ends the fake log file
EOF

    # Test fails
    return 1
}

function TEST_failure_core_only() {
    local dir=$1

    run_mon $dir a || return 1
    kill_daemons $dir SEGV mon 5
    return 0
}

main test_failure "$@"
