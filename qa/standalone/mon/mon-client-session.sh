#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7171" # git grep '\<7171\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7172" # git grep '\<7172\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7173" # git grep '\<7173\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "

    export BASE_CEPH_ARGS=$CEPH_ARGS
    CEPH_ARGS+="--mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

#
# A client authenticates with a mon that goes away before the msgr2
# session handshake completes. The client must not keep that session
# forever, but hunt for another mon once mon_client_ping_timeout expires.
# See https://tracker.ceph.com/issues/81947
#
function TEST_session_never_ready() {
    local dir=$1

    run_mon $dir a --public-addr $CEPH_MON_A || return 1
    run_mon $dir b --public-addr $CEPH_MON_B || return 1
    run_mon $dir c --public-addr $CEPH_MON_C || return 1
    wait_for_quorum 300 3 || return 1

    # freeze mon.b, so that the client can only authenticate with mon.c
    local mon_b_pid=$(cat $dir/mon.b.pid)
    kill -STOP $mon_b_pid || return 1
    if ! wait_for_quorum 300 2 ; then
        kill -CONT $mon_b_pid
        return 1
    fi

    # mon.c authenticates the client, but never completes its session
    if ! CEPH_ARGS='' ceph --admin-daemon $(get_asok_path mon.c) \
        config set ms_inject_stall_server_ident true ; then
        kill -CONT $mon_b_pid
        return 1
    fi

    # without the fix, the client waits for the session with mon.c until
    # client_mount_timeout (300s) expires. msgr2 only, as the injection is
    # msgr2 specific.
    CEPH_ARGS="$BASE_CEPH_ARGS --mon-host=[v2:$CEPH_MON_B],[v2:$CEPH_MON_C] " \
        timeout 120 ceph \
        --mon-client-ping-timeout=10 \
        --log-file=$dir/client.log --debug-monc=10 \
        mon stat > $dir/client.out 2>&1 &
    local client_pid=$!

    local stalled=false
    for i in $(seq 60) ; do
        if grep -q "injecting stall" $dir/mon.c.log ; then
            stalled=true
            break
        fi
        sleep 0.5
    done

    # mon.b comes back and mon.c goes away. The client is already stuck on
    # mon.c, so the order does not matter, but resuming mon.b first keeps it
    # from being left stopped on failure.
    kill -CONT $mon_b_pid || return 1
    kill_daemons $dir KILL mon.c || return 1

    if ! $stalled ; then
        kill $client_pid
        return 1
    fi
    wait $client_pid || return 1
    cat $dir/client.out
    grep -q "session with mon.* not ready after" $dir/client.log || return 1
}

main mon-client-session "$@"

# Local Variables:
# compile-command: "cd ../../../build ; make -j4 && ../qa/run-standalone.sh mon-client-session.sh"
# End:
