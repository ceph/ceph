#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

mon_port=$(get_unused_port)

CHECKPOINT_ARGS="--osd-map-full-checkpoint-interval=5"

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:$mon_port"
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

function churn_osdmap_epochs() {
    local rounds=$1
    for i in $(seq 1 $rounds); do
        ceph osd pool set foo min_size 1
        ceph osd pool set foo min_size 2
    done
}

function TEST_map_checkpoint() {
    local dir=$1

    run_mon $dir a
    run_mgr $dir x
    run_osd $dir 0 $CHECKPOINT_ARGS
    run_osd $dir 1 $CHECKPOINT_ARGS
    run_osd $dir 2 $CHECKPOINT_ARGS

    create_pool foo 8
    wait_for_clean || return 1
    rados -p foo put obj1 /etc/group || return 1

    churn_osdmap_epochs 10
    wait_for_clean || return 1

    local newest=$(ceph osd dump -f json | jq .epoch)
    test "$newest" -gt 15 || return 1

    grep "enabling FULLMAP_CHECKPOINTS" $dir/osd.0.log || return 1

    local stored=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.0) \
        perf dump | jq .osd.full_map_stored)
    local consumed=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.0) \
        perf dump | jq .osd.map_message_epochs)
    test "$stored" -lt "$consumed" || return 1

    # restart: boot has to rebuild the current map when it is not a checkpoint
    kill_daemons $dir TERM osd.0 || return 1
    activate_osd $dir 0 $CHECKPOINT_ARGS || return 1
    TIMEOUT=60 wait_for_osd up 0 || return 1
    wait_for_clean || return 1
    rados -p foo put obj2 /etc/group || return 1
    rados -p foo get obj1 $dir/obj1.out || return 1
    diff $dir/obj1.out /etc/group || return 1

    # more churn after restart, then read/write again
    churn_osdmap_epochs 5
    wait_for_clean || return 1
    rados -p foo put obj3 /etc/group || return 1

    # offline access must work for every epoch, checkpoint or not
    kill_daemons $dir TERM osd.0 || return 1

    ceph-objectstore-tool --data-path $dir/0 --no-mon-config \
        --op dump-super | grep "periodic full osdmap checkpoints" || return 1

    # the cluster keeps advancing epochs after osd.0 stops, so probe the
    # newest map present in osd.0's own store
    local maps=$(ceph-objectstore-tool --data-path $dir/0 --no-mon-config \
        --op dump-super | jq -r .maps)
    local last_start=$(echo $maps | sed 's/.*[[,]\([0-9]*\)~[0-9]*\]$/\1/')
    local last_len=$(echo $maps | sed 's/.*~\([0-9]*\)\]$/\1/')
    newest=$((last_start + last_len - 1))
    test "$newest" -gt 8 || return 1

    for e in $(seq $((newest - 7)) $newest); do
        ceph-objectstore-tool --data-path $dir/0 --no-mon-config \
            --op get-osdmap --epoch $e --file $dir/osdmap.$e || return 1
        osdmaptool --print $dir/osdmap.$e | grep "epoch $e" || return 1
    done

    # disabling the option must backfill full maps and clear the feature
    activate_osd $dir 0 || return 1
    TIMEOUT=60 wait_for_osd up 0 || return 1
    wait_for_clean || return 1
    grep "cleared FULLMAP_CHECKPOINTS feature" $dir/osd.0.log || return 1

    kill_daemons $dir TERM osd.0 || return 1
    if ceph-objectstore-tool --data-path $dir/0 --no-mon-config \
        --op dump-super | grep "periodic full osdmap checkpoints" ; then
        return 1
    fi
    activate_osd $dir 0 || return 1
    TIMEOUT=60 wait_for_osd up 0 || return 1
    wait_for_clean || return 1
    rados -p foo put obj4 /etc/group || return 1

    echo success

    delete_pool foo
    kill_daemons $dir || return 1
}

function TEST_map_checkpoint_crash_and_reconfigure() {
    local dir=$1

    run_mon $dir a
    run_mgr $dir x
    run_osd $dir 0 $CHECKPOINT_ARGS
    run_osd $dir 1 $CHECKPOINT_ARGS
    run_osd $dir 2 $CHECKPOINT_ARGS

    create_pool foo 8
    wait_for_clean || return 1
    rados -p foo put obj1 /etc/group || return 1
    churn_osdmap_epochs 8
    wait_for_clean || return 1

    # hard crash, then boot must replay from the last checkpoint
    kill_daemons $dir KILL osd.0 || return 1
    activate_osd $dir 0 $CHECKPOINT_ARGS || return 1
    TIMEOUT=60 wait_for_osd up 0 || return 1
    wait_for_clean || return 1
    rados -p foo put obj2 /etc/group || return 1

    # restart with a different interval; placement of old checkpoints
    # must not matter
    churn_osdmap_epochs 4
    kill_daemons $dir TERM osd.0 || return 1
    activate_osd $dir 0 --osd-map-full-checkpoint-interval=3 || return 1
    TIMEOUT=60 wait_for_osd up 0 || return 1
    wait_for_clean || return 1
    churn_osdmap_epochs 4
    rados -p foo put obj3 /etc/group || return 1
    rados -p foo get obj1 $dir/obj1.out || return 1
    diff $dir/obj1.out /etc/group || return 1

    echo success

    delete_pool foo
    kill_daemons $dir || return 1
}

function TEST_map_checkpoint_disabled_unaffected() {
    local dir=$1

    run_mon $dir a
    run_mgr $dir x
    run_osd $dir 0
    run_osd $dir 1
    run_osd $dir 2

    create_pool foo 8
    wait_for_clean || return 1
    rados -p foo put obj1 /etc/group || return 1

    churn_osdmap_epochs 5
    wait_for_clean || return 1

    if grep "enabling FULLMAP_CHECKPOINTS" $dir/osd.0.log ; then
        return 1
    fi

    kill_daemons $dir TERM osd.0 || return 1
    if ceph-objectstore-tool --data-path $dir/0 --no-mon-config \
        --op dump-super | grep "periodic full osdmap checkpoints" ; then
        return 1
    fi
    activate_osd $dir 0 || return 1
    TIMEOUT=60 wait_for_osd up 0 || return 1
    wait_for_clean || return 1

    echo success

    delete_pool foo
    kill_daemons $dir || return 1
}

main osd-map-checkpoint "$@"

# Local Variables:
# compile-command: "make -j4 && ../qa/run-standalone.sh osd-map-checkpoint.sh"
# End:
