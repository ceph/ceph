#!/usr/bin/env bash
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7145" # git grep '\<7145\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done
}

function TEST_reuse_id() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    ceph osd pool create foo 50 || return 1
    wait_for_clean || return 1

    kill_daemons $dir TERM osd.0
    kill_daemons $dir TERM osd.1
    kill_daemons $dir TERM osd.2
    ceph-objectstore-tool --data-path $dir/0 --op remove --pgid 1.0  --force
    ceph-objectstore-tool --data-path $dir/1 --op remove --pgid 1.0  --force
    ceph-objectstore-tool --data-path $dir/2 --op remove --pgid 1.0  --force
    activate_osd $dir 0 || return 1
    activate_osd $dir 1 || return 1
    activate_osd $dir 2 || return 1
    sleep 10
    ceph pg ls | grep 1.0 | grep stale || return 1

    ceph osd force-create-pg 1.0 --yes-i-really-mean-it || return 1
    wait_for_clean || return 1
}

main osd-force-create-pg "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/osd/osd-force-create-pg.sh"
# End:
