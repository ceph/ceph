#!/usr/bin/env bash
set -ex

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

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

main test_failure "$@"
