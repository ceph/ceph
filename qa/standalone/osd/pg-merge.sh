#!/usr/bin/env bash
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7147" # git grep '\<7147\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON --mon_min_osdmap_epochs=50 --paxos_service_trim_min=10"

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done
}

function TEST_import_after_merge_and_gap() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=2 || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1

    ceph osd pool create foo 2 || return 1
    wait_for_clean || return 1
    rados -p foo bench 3 write -b 1024 --no-cleanup || return 1

    kill_daemons $dir TERM osd.0
    ceph-objectstore-tool --data-path $dir/0 --op export --pgid 1.1 --file $dir/1.1  --force
    ceph-objectstore-tool --data-path $dir/0 --op export --pgid 1.0 --file $dir/1.0  --force
    activate_osd $dir 0 || return 1

    bin/ceph osd pool set foo pg_num 1
    sleep 5
    wait_for_clean || return 1

    #
    kill_daemons $dir TERM osd.0
    ceph-objectstore-tool --data-path $dir/0 --op remove --pgid 1.0 --force || return 1
    # this will import half the original pg
    ceph-objectstore-tool --data-path $dir/0 --op import --pgid 1.0 --file $dir/1.0 || return 1
    # we disallow import of 1.1 since it doesn't logically exist anymore.
    # ceph-objectstore-tool --data-path $dir/0 --op import --pgid 1.1 --file $dir/1.1 || return 1
    activate_osd $dir 0 || return 1

    wait_for_clean || return 1

    # make a map gap
    for f in `seq 1 50` ; do
	ceph osd set nodown
	ceph osd unset nodown
    done
    wait_for_clean || return 1
    ceph osd osd down 0
    sleep 3
    wait_for_clean || return 1

    kill_daemons $dir TERM osd.0

    # this should fail.. 1.1 still doesn't exist
    ceph-objectstore-tool --data-path $dir/0 --op import --pgid 1.1 --file $dir/1.1 && echo expected failure && return 1

    # this should not
    ceph-objectstore-tool --data-path $dir/0 --op remove --pgid 1.0 --force || return 1
    ceph-objectstore-tool --data-path $dir/0 --op import --pgid 1.0 --file $dir/1.0 || return 1

    activate_osd $dir 0 || return 1

    wait_for_clean || return 1
}

main pg-merge "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/osd/pg-merge.sh"
# End:
