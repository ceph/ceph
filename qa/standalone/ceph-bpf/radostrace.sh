#!/usr/bin/env bash
#
# Author: Dongdong Tao <tdd21151186@gmail.com>
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

# Wait for radostrace to be ready (polling started)
# Args: $1 = trace output file, $2 = timeout in seconds
function wait_for_radostrace_ready() {
    local trace_file=$1
    local timeout=${2:-60}
    local elapsed=0

    echo "Waiting for radostrace to start polling..."
    while [ $elapsed -lt $timeout ]; do
        if grep -q "Started polling from ring buffer" "$trace_file" 2>/dev/null; then
            echo "radostrace is ready (took ${elapsed}s)"
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    echo "ERROR: Timeout waiting for radostrace to be ready"
    return 1
}

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7220" # git grep '\<7220\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    # Check if radostrace binary exists
    if [ ! -x ./bin/radostrace ]; then
        echo "radostrace binary not found, skipping tests"
        return 0
    fi

    # Check if we can run eBPF (requires root or CAP_SYS_ADMIN)
    # Try using sudo if not already root
    if [ $(id -u) -ne 0 ]; then
        if ! sudo -n true 2>/dev/null; then
            echo "radostrace requires root privileges and passwordless sudo is not available, skipping tests"
            echo "Run with: sudo ../qa/run-standalone.sh tracing/radostrace.sh"
            return 0
        fi
        echo "Using sudo for radostrace (passwordless sudo available)"
        SUDO="sudo"
    else
        SUDO=""
    fi

    # Check kernel version >= 5.8 (required for BPF ring buffer support)
    local kernel_version=$(uname -r | cut -d. -f1-2)
    local kernel_major=$(echo $kernel_version | cut -d. -f1)
    local kernel_minor=$(echo $kernel_version | cut -d. -f2)
    if [ "$kernel_major" -lt 5 ] || ([ "$kernel_major" -eq 5 ] && [ "$kernel_minor" -lt 8 ]); then
        echo "Kernel version $kernel_version is less than 5.8, skipping tests"
        echo "radostrace requires Linux kernel 5.8 or later for BPF ring buffer support"
        return 0
    fi

    local funcs=${@:-$(set | ${SED} -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        kill_daemons $dir KILL || return 1
        teardown $dir || return 1
    done
}

function TEST_radostrace_help() {
    local dir=$1

    # Test that radostrace --help works (doesn't need sudo)
    ./bin/radostrace --help || return 1
    ./bin/radostrace --help | grep -q "Usage" || return 1
    ./bin/radostrace --help | grep -q "\-p, \-\-pid" || return 1
    ./bin/radostrace --help | grep -q "\-t, \-\-timeout" || return 1
}

function TEST_radostrace_basic() {
    local dir=$1
    local trace_output=$dir/trace.out
    local trace_pid

    # Start a minimal cluster
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    # Create a pool and wait for it to be ready
    ceph osd pool create testpool 8 || return 1
    sleep 5  # Give time for pool to be ready

    # Prepare test file
    echo "test data for radostrace" > $dir/testfile

    # Start radostrace in background with debug mode (long timeout, we'll kill it after)
    # Use stdbuf -o0 for unbuffered output so grep can detect messages immediately
    echo "Starting radostrace with debug mode..."
    $SUDO stdbuf -o0 ./bin/radostrace -d -t 120 > $trace_output 2>&1 &
    trace_pid=$!
    echo "radostrace started with PID $trace_pid"

    # Wait for radostrace to be ready (polling started)
    if ! wait_for_radostrace_ready "$trace_output" 60; then
        echo "Output was:"
        cat $trace_output
        $SUDO kill $trace_pid 2>/dev/null || true
        return 1
    fi

    # Generate RADOS I/O - run multiple times to ensure capture
    echo "Generating RADOS I/O..."
    for i in 1 2 3; do
        rados -p testpool put testobj$i $dir/testfile || return 1
        rados -p testpool get testobj$i $dir/testfile.out || return 1
    done
    echo "RADOS I/O completed"

    sleep 2

    # Kill radostrace now that we have our data
    echo "Stopping radostrace..."
    $SUDO kill $trace_pid 2>/dev/null || true
    wait $trace_pid 2>/dev/null || true
    echo "radostrace stopped"

    # Debug: show file info
    echo "Trace output file info:"
    ls -la $trace_output
    echo "Trace output content:"
    cat $trace_output
    echo "--- end of trace output ---"

    # Verify output contains expected data
    if ! grep -q "testobj" $trace_output; then
        echo "ERROR: radostrace output does not contain 'testobj'"
        return 1
    fi

    # Verify output format has expected columns
    if ! grep -q "pid" $trace_output; then
        echo "ERROR: radostrace output missing header"
        return 1
    fi

    echo "radostrace basic test passed"
}

function TEST_radostrace_pid_filter() {
    local dir=$1
    local trace_output=$dir/trace.out
    local trace_pid

    # Start a minimal cluster
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    # Create a pool
    ceph osd pool create testpool 8 || return 1
    sleep 5

    # Start a rados bench in background to get its PID (long duration)
    rados -p testpool bench 120 write --no-cleanup > $dir/bench.out 2>&1 &
    local bench_pid=$!
    echo "rados bench started with PID $bench_pid"
    sleep 1

    # Start radostrace filtering on bench PID with debug mode
    # Use stdbuf -o0 for unbuffered output so grep can detect messages immediately
    $SUDO stdbuf -o0 ./bin/radostrace -d -p $bench_pid -t 120 > $trace_output 2>&1 &
    trace_pid=$!
    echo "radostrace started with PID $trace_pid"

    # Wait for radostrace to be ready
    if ! wait_for_radostrace_ready "$trace_output" 60; then
        echo "Output was:"
        cat $trace_output
        $SUDO kill $trace_pid 2>/dev/null || true
        kill $bench_pid 2>/dev/null || true
        return 1
    fi

    # Let bench run for a few seconds to generate I/O
    sleep 5

    # Stop both
    echo "Stopping radostrace and bench..."
    $SUDO kill $trace_pid 2>/dev/null || true
    kill $bench_pid 2>/dev/null || true
    wait $trace_pid 2>/dev/null || true
    wait $bench_pid 2>/dev/null || true

    # Debug output
    echo "Trace output:"
    cat $trace_output

    # Verify we captured operations
    if [ ! -s $trace_output ]; then
        echo "WARNING: radostrace output is empty"
        return 1
    fi

    # Verify we see benchmark objects
    if grep -q "benchmark" $trace_output; then
        echo "radostrace pid filter test passed"
    else
        echo "ERROR: radostrace did not capture benchmark objects"
        return 1
    fi
}

function TEST_radostrace_rbd() {
    local dir=$1
    local trace_output=$dir/trace.out
    local trace_pid

    # Start a minimal cluster
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    # Create rbd pool and image
    ceph osd pool create rbd 8 || return 1
    rbd pool init rbd || return 1
    rbd create testimg --size 128 --pool rbd || return 1
    sleep 5

    # Start radostrace in background with debug mode
    # Use stdbuf -o0 for unbuffered output so grep can detect messages immediately
    echo "Starting radostrace..."
    $SUDO stdbuf -o0 ./bin/radostrace -d -t 120 > $trace_output 2>&1 &
    trace_pid=$!
    echo "radostrace started with PID $trace_pid"

    # Wait for radostrace to be ready
    if ! wait_for_radostrace_ready "$trace_output" 60; then
        echo "Output was:"
        cat $trace_output
        $SUDO kill $trace_pid 2>/dev/null || true
        return 1
    fi

    # Run rbd bench to generate I/O
    echo "Running rbd bench..."
    rbd bench --io-type write --io-size 4096 --io-total 4M rbd/testimg || return 1
    echo "rbd bench completed"

    sleep 5

    # Stop radostrace
    echo "Stopping radostrace..."
    $SUDO kill $trace_pid 2>/dev/null || true
    wait $trace_pid 2>/dev/null || true

    # Debug output
    echo "Trace output:"
    cat $trace_output

    # Verify we captured rbd_data objects
    if ! grep -q "rbd_data" $trace_output; then
        echo "ERROR: radostrace did not capture rbd_data objects"
        return 1
    fi

    # Verify we see write operations
    if ! grep -q "write" $trace_output; then
        echo "ERROR: radostrace did not capture write operations"
        return 1
    fi

    echo "radostrace rbd test passed"
}

main radostrace "$@"
