#!/usr/bin/env bash

#
# test pidfile here 
#

# Includes
source $(dirname $0)/detect-build-env-vars.sh
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7124" # git grep '\<7124\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done
}

function TEST_without_pidfile() {
    local dir=$1
    setup $dir
    local data=$dir/osd1
    local id=1
    ceph-mon \
        --id $id \
        --mkfs \
        --mon-data=$data \
        --run-dir=$dir || return 1
    expect_failure $dir "ignore empty --pid-file" ceph-mon \
        -f \
        --log-to-stderr \
	--log_flush_on_exit \
        --pid-file= \
        --id $id \
        --mon-data=$data \
        --run-dir=$dir || return 1
    teardown $dir
}

function TEST_pidfile() {
    local dir=$1
    setup $dir 

    # no daemon can use a pidfile that is owned by another daemon
    run_mon $dir a || return 1
    sleep 5
    run_mon $dir a --log-to-stderr -f 2>&1 | grep "failed to lock pidfile" || return 1

    run_osd $dir 0 || return 1
    sleep 5
    activate_osd $dir 0 --log-to-stderr -f 2>&1 | grep "failed to lock pidfile" || return 1

    # when a daemon shutdown, it will not unlink a path different from
    # the one it owns
    mv $dir/osd.0.pid $dir/osd.0.pid.old || return 1
    cp $dir/osd.0.pid.old $dir/osd.0.pid || return 1
    kill_daemons $dir TERM osd.0 || return 1
    test -f $dir/osd.0.pid || return 1

    # when a daemon starts, it re-uses the pid file if no other daemon
    # has it locked
    run_osd $dir 0 || return 1
    ! cmp $dir/osd.0.pid $dir/osd.0.pid.old || return 1

    # if the pid in the file is different from the pid of the daemon
    # the file is not removed because it is assumed to be owned by
    # another daemon
    mkdir $dir/old
    cp $dir/osd.0.pid $dir/old/osd.0.pid  # so that kill_daemon finds the pid
    echo 123 > $dir/osd.0.pid
    kill_daemons $dir/old TERM osd.0 || return 1
    test -f $dir/osd.0.pid || return 1

    # when the daemon shutdown, it removes its own pid file
    test -f $dir/mon.a.pid || return 1
    kill_daemons $dir TERM mon.a || return 1
    ! test -f $dir/mon.a.pid || return 1

    teardown $dir || return 1
}

main pidfile "$@"

