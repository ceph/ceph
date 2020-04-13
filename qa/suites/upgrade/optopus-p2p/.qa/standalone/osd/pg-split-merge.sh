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

function TEST_a_merge_empty() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=3 || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    ceph osd pool create foo 2 || return 1
    ceph osd pool set foo pgp_num 1 || return 1

    wait_for_clean || return 1

    # note: we need 1.0 to have the same or more objects than 1.1
    #  1.1
    rados -p foo put foo1 /etc/passwd
    rados -p foo put foo2 /etc/passwd
    rados -p foo put foo3 /etc/passwd
    rados -p foo put foo4 /etc/passwd
    #  1.0
    rados -p foo put foo5 /etc/passwd
    rados -p foo put foo6 /etc/passwd
    rados -p foo put foo8 /etc/passwd
    rados -p foo put foo10 /etc/passwd
    rados -p foo put foo11 /etc/passwd
    rados -p foo put foo12 /etc/passwd
    rados -p foo put foo16 /etc/passwd

    wait_for_clean || return 1

    ceph tell osd.1 config set osd_debug_no_purge_strays true
    ceph osd pool set foo size 2 || return 1
    wait_for_clean || return 1

    kill_daemons $dir TERM osd.2 || return 1
    ceph-objectstore-tool --data-path $dir/2 --op remove --pgid 1.1 --force || return 1
    activate_osd $dir 2 || return 1

    wait_for_clean || return 1

    # osd.2: now 1.0 is there but 1.1 is not

    # instantiate 1.1 on osd.2 with last_update=0'0 ('empty'), which is
    # the problematic state... then let it merge with 1.0
    ceph tell osd.2 config set osd_debug_no_acting_change true
    ceph osd out 0 1
    ceph osd pool set foo pg_num 1
    sleep 5
    ceph tell osd.2 config set osd_debug_no_acting_change false

    # go back to osd.1 being primary, and 3x so the osd.2 copy doesn't get
    # removed
    ceph osd in 0 1
    ceph osd pool set foo size 3

    wait_for_clean || return 1

    # scrub to ensure the osd.3 copy of 1.0 was incomplete (vs missing
    # half of its objects).
    ceph pg scrub 1.0
    sleep 10
    ceph log last debug
    ceph pg ls
    ceph pg ls | grep ' active.clean ' || return 1
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
