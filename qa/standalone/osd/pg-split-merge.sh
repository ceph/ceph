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
    run_mon $dir a --osd_pool_default_size=1 || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1

    ceph osd pool create foo 2 || return 1
    wait_for_clean || return 1
    rados -p foo bench 3 write -b 1024 --no-cleanup || return 1

    kill_daemons $dir TERM osd.0 || return 1
    ceph-objectstore-tool --data-path $dir/0 --op export --pgid 1.1 --file $dir/1.1  --force || return 1
    ceph-objectstore-tool --data-path $dir/0 --op export --pgid 1.0 --file $dir/1.0  --force || return 1
    activate_osd $dir 0 || return 1

    ceph osd pool set foo pg_num 1
    sleep 5
    while ceph daemon osd.0 perf dump | jq '.osd.numpg' | grep 2 ; do sleep 1 ; done
    wait_for_clean || return 1

    #
    kill_daemons $dir TERM osd.0 || return 1
    ceph-objectstore-tool --data-path $dir/0 --op remove --pgid 1.0 --force || return 1
    # this will import both halves the original pg
    ceph-objectstore-tool --data-path $dir/0 --op import --pgid 1.1 --file $dir/1.1 || return 1
    ceph-objectstore-tool --data-path $dir/0 --op import --pgid 1.0 --file $dir/1.0 || return 1
    activate_osd $dir 0 || return 1

    wait_for_clean || return 1

    # make a map gap
    for f in `seq 1 50` ; do
	ceph osd set nodown
	ceph osd unset nodown
    done

    # poke and prod to ensure last_epech_clean is big, reported to mon, and
    # the osd is able to trim old maps
    rados -p foo bench 1 write -b 1024 --no-cleanup || return 1
    wait_for_clean || return 1
    ceph tell osd.0 send_beacon
    sleep 5
    ceph osd set nodown
    ceph osd unset nodown
    sleep 5

    kill_daemons $dir TERM osd.0 || return 1

    # this should fail.. 1.1 still doesn't exist
    ! ceph-objectstore-tool --data-path $dir/0 --op import --pgid 1.1 --file $dir/1.1 || return 1

    ceph-objectstore-tool --data-path $dir/0 --op export-remove --pgid 1.0 --force --file $dir/1.0.later || return 1

    # this should fail too because of the gap
    ! ceph-objectstore-tool --data-path $dir/0 --op import --pgid 1.1 --file $dir/1.1 || return 1
    ! ceph-objectstore-tool --data-path $dir/0 --op import --pgid 1.0 --file $dir/1.0 || return 1

    # we can force it...
    ceph-objectstore-tool --data-path $dir/0 --op import --pgid 1.1 --file $dir/1.1 --force || return 1
    ceph-objectstore-tool --data-path $dir/0 --op import --pgid 1.0 --file $dir/1.0 --force || return 1

    # ...but the osd won't start, so remove it again.
    ceph-objectstore-tool --data-path $dir/0 --op remove --pgid 1.0 --force || return 1
    ceph-objectstore-tool --data-path $dir/0 --op remove --pgid 1.1 --force || return 1

    ceph-objectstore-tool --data-path $dir/0 --op import --pgid 1.0 --file $dir/1.0.later --force || return 1


    activate_osd $dir 0 || return 1

    wait_for_clean || return 1
}

function TEST_import_after_split() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1

    ceph osd pool create foo 1 || return 1
    wait_for_clean || return 1
    rados -p foo bench 3 write -b 1024 --no-cleanup || return 1

    kill_daemons $dir TERM osd.0 || return 1
    ceph-objectstore-tool --data-path $dir/0 --op export --pgid 1.0 --file $dir/1.0  --force || return 1
    activate_osd $dir 0 || return 1

    ceph osd pool set foo pg_num 2
    sleep 5
    while ceph daemon osd.0 perf dump | jq '.osd.numpg' | grep 1 ; do sleep 1 ; done
    wait_for_clean || return 1

    kill_daemons $dir TERM osd.0 || return 1

    ceph-objectstore-tool --data-path $dir/0 --op remove --pgid 1.0 --force || return 1

    # this should fail because 1.1 (split child) is there
    ! ceph-objectstore-tool --data-path $dir/0 --op import --pgid 1.0 --file $dir/1.0 || return 1

    ceph-objectstore-tool --data-path $dir/0 --op remove --pgid 1.1 --force || return 1
    # now it will work (1.1. is gone)
    ceph-objectstore-tool --data-path $dir/0 --op import --pgid 1.0 --file $dir/1.0 || return 1

    activate_osd $dir 0 || return 1

    wait_for_clean || return 1
}


main pg-split-merge "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/osd/pg-split-merge.sh"
# End:
