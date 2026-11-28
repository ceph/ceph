#!/usr/bin/env bash
#
# Apply and revert a libpulp live patch on a running ceph-mon, see
# doc/dev/livepatch.rst. Skipped unless this is a WITH_LIVEPATCH build,
# libpulp is installed and the test may ptrace the monitor.
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function libpulp_so() {
    local d
    for d in /usr/lib64 /usr/lib/x86_64-linux-gnu /usr/lib /usr/local/lib64 \
             /usr/local/lib ; do
        if [ -e $d/libpulp.so.0 ]; then
            echo $d/libpulp.so.0
            return
        fi
    done
}

function skip_reason() {
    local build=$1

    if ! grep -q '^WITH_LIVEPATCH:BOOL=ON' $build/CMakeCache.txt 2>/dev/null; then
        echo "not a WITH_LIVEPATCH build"
    elif ! type ulp > /dev/null 2>&1 || [ -z "$(libpulp_so)" ]; then
        echo "libpulp is not installed"
    elif [ $(id -u) != 0 ] &&
         [ "$(cat /proc/sys/kernel/yama/ptrace_scope 2>/dev/null)" != 0 ]; then
        echo "not allowed to ptrace (not root, and kernel.yama.ptrace_scope is not 0)"
    fi
}

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7164" # git grep '\<7164\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    BUILD=$(dirname $(dirname $(command -v ceph-mon)))
    local reason=$(skip_reason $BUILD)
    if [ -n "$reason" ]; then
        echo "SKIP: $reason"
        return 0
    fi

    local funcs=${@:-$(set | ${SED} -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function livepatch_count() {
    ceph tell mon.a version | jq '.livepatches | length'
}

# ulp trigger refuses with EAGAIN while a thread is inside malloc or dlopen
function trigger() {
    ulp trigger --timeout 30 -r 1000 "$@"
}

function TEST_mon_apply_revert() {
    local dir=$1
    local patch=$(readlink -f $dir)/lp_mon_block_pool_rm.so

    # `ulp post` rewrites the binary, and the patch is bound to the path of
    # its target: run a copy
    mkdir -p $dir/bin
    cp $BUILD/bin/ceph-mon $dir/bin/ceph-mon || return 1
    ulp post $dir/bin/ceph-mon || return 1
    ulp livepatchable $dir/bin/ceph-mon || return 1
    $CEPH_ROOT/src/script/livepatch/build-patch.sh $BUILD \
        $CEPH_ROOT/src/script/livepatch/example/lp_mon_block_pool_rm.cc \
        $dir/bin/ceph-mon mon/OSDMonitor.cc \
        '^_ZN10OSDMonitor18_check_remove_pool' \
        lp_OSDMonitor_check_remove_pool $dir || return 1

    PATH=$dir/bin:$PATH LD_PRELOAD=$(libpulp_so) \
        run_mon $dir a --mon-allow-pool-delete=true || return 1
    local pid=$(cat $dir/mon.a.pid)
    grep -q libpulp /proc/$pid/maps || return 1

    ceph osd pool create one 8 || return 1
    ceph osd pool create two 8 || return 1
    ceph tell mon.a version | jq -e '.livepatch_enabled' || return 1
    test "$(livepatch_count)" = 0 || return 1
    ceph osd pool rm one one --yes-i-really-really-mean-it || return 1

    trigger -p $pid $patch || return 1
    test "$(livepatch_count)" = 1 || return 1
    ceph tell mon.a version | jq -r '.livepatches[0].file' | grep -q "^$patch\$" || return 1
    ! ceph osd pool rm two two --yes-i-really-really-mean-it 2> $dir/rm.err || return 1
    grep -q 'disabled by a live patch' $dir/rm.err || return 1
    ceph osd pool ls | grep -q '^two$' || return 1

    trigger -p $pid --revert $patch || return 1
    test "$(livepatch_count)" = 0 || return 1
    ceph osd pool rm two two --yes-i-really-really-mean-it || return 1
    ! ceph osd pool ls | grep -q '^two$' || return 1

    # the same process all along
    test "$(cat $dir/mon.a.pid)" = $pid || return 1
    kill -0 $pid || return 1
}

main livepatch "$@"

# Local Variables:
# compile-command: "cd ../../../build ; make -j4 && ../qa/run-standalone.sh livepatch.sh"
# End:
