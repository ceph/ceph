#!/usr/bin/env bash
source $(dirname $0)/../detect-build-env-vars.sh
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

# BlueStore requires libaio with a working io_getevents(2).  On some
# systems (e.g. Ubuntu 24.04 with kernel 6.8+ inside a restricted
# AppArmor/Landlock environment) io_getevents with a non-zero timeout
# returns -EPERM.  Detect this before starting the cluster and skip the
# test (CTest exit code 77 == SKIP) rather than crashing ceph-osd.
if ! python3 - <<'EOF'
import ctypes, sys
from ctypes import Structure, c_long

class io_event(Structure):
    _fields_ = [('data', c_long), ('obj', c_long), ('res', c_long), ('res2', c_long)]

class timespec(Structure):
    _fields_ = [('tv_sec', c_long), ('tv_nsec', c_long)]

try:
    libaio = ctypes.CDLL('libaio.so.1', use_errno=True)
except OSError:
    sys.exit(0)  # libaio not present; let the test fail with a clear message

ctx = c_long(0)
if libaio.io_setup(128, ctypes.byref(ctx)) < 0:
    sys.exit(0)  # io_setup failed; not an AppArmor issue

dummy = io_event()
ts = timespec(0, 1 * 1000 * 1000)  # 1 ms – same blocking path as bluestore AIO poll
r = libaio.io_getevents(ctx, 1, 1, ctypes.byref(dummy), ctypes.byref(ts))
libaio.io_destroy(ctx)
sys.exit(0 if r >= 0 else 1)
EOF
then
    echo "SKIP: libaio io_getevents is not permitted on this system (AppArmor/Landlock restriction)"
    exit 77
fi

set -e

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:$(get_unused_port)"
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    set -e

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
	$func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_safe_to_destroy() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    flush_pg_stats

    ceph osd safe-to-destroy 0
    ceph osd safe-to-destroy 1
    ceph osd safe-to-destroy 2
    ceph osd safe-to-destroy 3

    ceph osd pool create foo 128
    sleep 2
    flush_pg_stats
    wait_for_clean

    expect_failure $dir 'pgs currently' ceph osd safe-to-destroy 0
    expect_failure $dir 'pgs currently' ceph osd safe-to-destroy 1
    expect_failure $dir 'pgs currently' ceph osd safe-to-destroy 2
    expect_failure $dir 'pgs currently' ceph osd safe-to-destroy 3

    ceph osd out 0
    sleep 2
    flush_pg_stats
    wait_for_clean

    ceph osd safe-to-destroy 0

    # even osds without osd_stat are ok if all pgs are active+clean
    id=`ceph osd create`
    ceph osd safe-to-destroy $id
}

function TEST_ok_to_stop() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1

    ceph osd pool create foo 128
    ceph osd pool set foo size 3
    ceph osd pool set foo min_size 2
    sleep 1
    flush_pg_stats
    wait_for_clean

    ceph osd ok-to-stop 0
    ceph osd ok-to-stop 1
    ceph osd ok-to-stop 2
    ceph osd ok-to-stop 3
    expect_failure $dir bad_become_inactive ceph osd ok-to-stop 0 1

    ceph osd pool set foo min_size 1
    sleep 1
    flush_pg_stats
    wait_for_clean
    ceph osd ok-to-stop 0 1
    ceph osd ok-to-stop 1 2
    ceph osd ok-to-stop 2 3
    ceph osd ok-to-stop 3 4
    expect_failure $dir bad_become_inactive ceph osd ok-to-stop 0 1 2
    expect_failure $dir bad_become_inactive ceph osd ok-to-stop 0 1 2 3
}

main safe-to-destroy "$@"
