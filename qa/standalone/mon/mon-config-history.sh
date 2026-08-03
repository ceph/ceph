#!/usr/bin/env bash
#
# Author: Steven Zhang <yzhan298@gmail.com>
#
# This test verifies that the mon config-history trimming logic works as expected.  It
# does so by writing a number of distinct values to a config key, then waiting for the
# mon to trim the history down to the configured retention count.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7157"
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

function history_keys() {
    ceph config-key ls --format=json | jq -r '.[]' | grep '^config-history/' || true
}

function count_history_entries() {
    local sign=$1
    local key=$2
    history_keys | grep -c -E "^config-history/[0-9]+/[${sign}]${key}\$" || true
}

function wait_for_history_entries() {
    local sign=$1
    local key=$2
    local expected=$3
    local -a delays=($(get_timeout_delays $TIMEOUT .1))
    local -i loop=0
    local n

    while true ; do
        n=$(count_history_entries "$sign" "$key")
        if [ "$n" -eq "$expected" ] ; then
            return 0
        fi
        if (( loop >= ${#delays[*]} )) ; then
            echo "timed out: '${sign}${key}' has $n versions, expected $expected"
            history_keys
            return 1
        fi
        sleep ${delays[$loop]}
        loop+=1
    done
}

function TEST_config_history_trim() {
    local dir=$1

    run_mon $dir a --mon-tick-interval=1 --mon-config-history-size=3 || return 1

    local i
    for i in $(seq 1 8) ; do
        ceph config set osd osd_memory_target $((4294967296 + i * 1048576)) || return 1
    done

    local last=$((4294967296 + 8 * 1048576))
    test "$(ceph config-key get config/osd/osd_memory_target)" = "$last" || return 1

    wait_for_history_entries + osd/osd_memory_target 3 || return 1
    wait_for_history_entries - osd/osd_memory_target 3 || return 1

    test "$(ceph config-key get config/osd/osd_memory_target)" = "$last" || return 1
    test "$(ceph config get osd.0 osd_memory_target)" = "$last" || return 1
}

main mon-config-history "$@"