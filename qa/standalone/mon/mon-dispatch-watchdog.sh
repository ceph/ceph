#!/usr/bin/env bash
#
# Validate that the monitor dispatch watchdog aborts a monitor whose messenger
# dispatch thread stops making progress after the watchdog has been armed.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7213" # git grep '\<7213\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        if ! $func $dir ; then
            teardown $dir 1
            return 1
        fi
        teardown $dir || return 1
    done
}

function wait_for_log() {
    local logfile=$1
    local pattern=$2
    local timeout=${3:-30}

    for ((i=0; i < timeout; i++)); do
        if grep -q "$pattern" "$logfile" 2>/dev/null ; then
            return 0
        fi
        sleep 1
    done

    echo "timed out waiting for '$pattern' in $logfile"
    tail -n 80 "$logfile" || true
    return 1
}

function wait_for_pid_exit() {
    local pidfile=$1
    local timeout=${2:-30}
    local pid

    test -s "$pidfile" || {
        echo "missing pid file: $pidfile"
        return 1
    }
    pid=$(cat "$pidfile")

    for ((i=0; i < timeout; i++)); do
        if ! kill -0 "$pid" 2>/dev/null ; then
            return 0
        fi
        sleep 1
    done

    echo "process $pid from $pidfile did not exit"
    return 1
}

function TEST_mon_dispatch_watchdog_aborts_stalled_dispatch() {
    local dir=$1
    local mon_log="$dir/mon.a.log"
    local mon_pidfile="$dir/mon.a.pid"

    # This test expects ceph-mon to abort. Avoid turning the expected abort into
    # a standalone failure caused only by a generated core file.
    ulimit -c 0 || true

    run_mon $dir a \
        --heartbeat-interval=1 \
        --mon-dispatch-watchdog-timeout=2 || return 1

    ceph ping mon.a || return 1
    ceph tell mon.a injectargs -- --mon-inject-dispatch-delay=10 || return 1

    if timeout 12 ceph ping mon.a ; then
        echo "ceph ping unexpectedly succeeded while dispatch delay was injected"
        return 1
    fi

    wait_for_log "$mon_log" "injecting monitor dispatch delay" 15 || return 1
    wait_for_log "$mon_log" "had suicide timed out" 15 || return 1
    wait_for_pid_exit "$mon_pidfile" 15 || return 1
}

main mon-dispatch-watchdog "$@"
