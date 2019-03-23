#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7150" # git grep '\<7150\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7151" # git grep '\<7151\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7152" # git grep '\<7152\>' : there must be only one
    export CEPH_MON_D="127.0.0.1:7153" # git grep '\<7153\>' : there must be only one
    export CEPH_MON_E="127.0.0.1:7154" # git grep '\<7154\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    export ORIG_CEPH_ARGS="$CEPH_ARGS"

    local funcs=${@:-$(set | ${SED} -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        kill_daemons $dir KILL || return 1
        teardown $dir || return 1
    done
}

function TEST_1_mon_checks() {
    local dir=$1

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON_A "

    run_mon $dir a --public-addr=$CEPH_MON_A || return 1

    ceph mon ok-to-stop dne || return 1
    ! ceph mon ok-to-stop a || return 1

    ! ceph mon ok-to-add-offline || return 1

    ! ceph mon ok-to-rm a || return 1
    ceph mon ok-to-rm dne || return 1
}

function TEST_2_mons_checks() {
    local dir=$1

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B "

    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1

    ceph mon ok-to-stop dne || return 1
    ! ceph mon ok-to-stop a || return 1
    ! ceph mon ok-to-stop b || return 1
    ! ceph mon ok-to-stop a b || return 1

    ceph mon ok-to-add-offline || return 1

    ceph mon ok-to-rm a || return 1
    ceph mon ok-to-rm b || return 1
    ceph mon ok-to-rm dne || return 1
}

function TEST_3_mons_checks() {
    local dir=$1

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C "

    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1
    run_mon $dir c --public-addr=$CEPH_MON_C || return 1
    wait_for_quorum 60 3

    ceph mon ok-to-stop dne || return 1
    ceph mon ok-to-stop a || return 1
    ceph mon ok-to-stop b || return 1
    ceph mon ok-to-stop c || return 1
    ! ceph mon ok-to-stop a b || return 1
    ! ceph mon ok-to-stop b c || return 1
    ! ceph mon ok-to-stop a b c || return 1

    ceph mon ok-to-add-offline || return 1

    ceph mon ok-to-rm a || return 1
    ceph mon ok-to-rm b || return 1
    ceph mon ok-to-rm c || return 1

    kill_daemons $dir KILL mon.b
    wait_for_quorum 60 2

    ! ceph mon ok-to-stop a || return 1
    ceph mon ok-to-stop b || return 1
    ! ceph mon ok-to-stop c || return 1

    ! ceph mon ok-to-add-offline || return 1

    ! ceph mon ok-to-rm a || return 1
    ceph mon ok-to-rm b || return 1
    ! ceph mon ok-to-rm c || return 1
}

function TEST_4_mons_checks() {
    local dir=$1

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C,$CEPH_MON_D "

    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1
    run_mon $dir c --public-addr=$CEPH_MON_C || return 1
    run_mon $dir d --public-addr=$CEPH_MON_D || return 1
    wait_for_quorum 60 4

    ceph mon ok-to-stop dne || return 1
    ceph mon ok-to-stop a || return 1
    ceph mon ok-to-stop b || return 1
    ceph mon ok-to-stop c || return 1
    ceph mon ok-to-stop d || return 1
    ! ceph mon ok-to-stop a b || return 1
    ! ceph mon ok-to-stop c d || return 1

    ceph mon ok-to-add-offline || return 1

    ceph mon ok-to-rm a || return 1
    ceph mon ok-to-rm b || return 1
    ceph mon ok-to-rm c || return 1

    kill_daemons $dir KILL mon.a
    wait_for_quorum 60 3

    ceph mon ok-to-stop a || return 1
    ! ceph mon ok-to-stop b || return 1
    ! ceph mon ok-to-stop c || return 1
    ! ceph mon ok-to-stop d || return 1

    ceph mon ok-to-add-offline || return 1

    ceph mon ok-to-rm a || return 1
    ceph mon ok-to-rm b || return 1
    ceph mon ok-to-rm c || return 1
    ceph mon ok-to-rm d || return 1
}

function TEST_5_mons_checks() {
    local dir=$1

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C,$CEPH_MON_D,$CEPH_MON_E "

    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1
    run_mon $dir c --public-addr=$CEPH_MON_C || return 1
    run_mon $dir d --public-addr=$CEPH_MON_D || return 1
    run_mon $dir e --public-addr=$CEPH_MON_E || return 1
    wait_for_quorum 60 5

    ceph mon ok-to-stop dne || return 1
    ceph mon ok-to-stop a || return 1
    ceph mon ok-to-stop b || return 1
    ceph mon ok-to-stop c || return 1
    ceph mon ok-to-stop d || return 1
    ceph mon ok-to-stop e || return 1
    ceph mon ok-to-stop a b || return 1
    ceph mon ok-to-stop c d || return 1
    ! ceph mon ok-to-stop a b c || return 1

    ceph mon ok-to-add-offline || return 1

    ceph mon ok-to-rm a || return 1
    ceph mon ok-to-rm b || return 1
    ceph mon ok-to-rm c || return 1
    ceph mon ok-to-rm d || return 1
    ceph mon ok-to-rm e || return 1

    kill_daemons $dir KILL mon.a
    wait_for_quorum 60 4

    ceph mon ok-to-stop a || return 1
    ceph mon ok-to-stop b || return 1
    ceph mon ok-to-stop c || return 1
    ceph mon ok-to-stop d || return 1
    ceph mon ok-to-stop e || return 1

    ceph mon ok-to-add-offline || return 1

    ceph mon ok-to-rm a || return 1
    ceph mon ok-to-rm b || return 1
    ceph mon ok-to-rm c || return 1
    ceph mon ok-to-rm d || return 1
    ceph mon ok-to-rm e || return 1

    kill_daemons $dir KILL mon.e
    wait_for_quorum 60 3

    ceph mon ok-to-stop a || return 1
    ! ceph mon ok-to-stop b || return 1
    ! ceph mon ok-to-stop c || return 1
    ! ceph mon ok-to-stop d || return 1
    ceph mon ok-to-stop e || return 1

    ! ceph mon ok-to-add-offline || return 1

    ceph mon ok-to-rm a || return 1
    ! ceph mon ok-to-rm b || return 1
    ! ceph mon ok-to-rm c || return 1
    ! ceph mon ok-to-rm d || return 1
    ceph mon ok-to-rm e || return 1
}

function TEST_0_mds() {
    local dir=$1

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON_A "

    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_mds $dir a || return 1

    ceph osd pool create meta 1 || return 1
    ceph osd pool create data 1 || return 1
    ceph fs new myfs meta data || return 1
    sleep 5

    ! ceph mds ok-to-stop a || return 1
    ! ceph mds ok-to-stop a dne || return 1
    ceph mds ok-to-stop dne || return 1

    run_mds $dir b || return 1
    sleep 5

    ceph mds ok-to-stop a || return 1
    ceph mds ok-to-stop b || return 1
    ! ceph mds ok-to-stop a b || return 1
    ceph mds ok-to-stop a dne1 dne2 || return 1
    ceph mds ok-to-stop b dne || return 1
    ! ceph mds ok-to-stop a b dne || return 1
    ceph mds ok-to-stop dne1 dne2 || return 1

    kill_daemons $dir KILL mds.a
}


main ok-to-stop "$@"
