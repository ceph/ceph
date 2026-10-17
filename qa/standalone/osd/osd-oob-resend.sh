#!/usr/bin/env bash
#
# A read whose push the primary holds on a transfer thread
# (osd_debug_oob_delay_done_ms), a write to the same object right behind
# it, and a change of the PG that makes the client send both again: a
# split, or a new primary. The op thread let go of the object's lock at
# the handoff, so without the fence of the object the write commits
# before the read's reply leaves. The client then sends the read again,
# drops the reply of the first attempt when it comes, and gets the
# object with the write in it. With the fence the write waits for the
# read's reply, so the read that the client sends again gets the object
# as it was before the write. Found by vstart_chaos with its oob workload
# (a pg_num change during a held push).
#
# The client windows are on the libfabric tcp provider, on 127.0.0.1, so
# this needs a build with WITH_OOB_OFI, whose ceph_test_osd_oob_read is
# the client.
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

# what the client prints in a read's flags
FLAG_DECLINED=8
FLAG_RESENT=32
SIZE=1048576

function run() {
    local dir=$1
    shift

    if ! type ceph_test_osd_oob_read > /dev/null 2>&1 ; then
        echo "ceph_test_osd_oob_read not found (built without WITH_OOB_OFI?)"
        # a run that needs the tests sets CEPH_OOB_REQUIRED, and fails here
        test -z "$CEPH_OOB_REQUIRED" ; return
    fi

    export CEPH_MON="127.0.0.1:7163" # git grep '\<7163\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

##
# Run a mon, a mgr and three OSDs that serve out-of-band reads over the
# libfabric tcp provider, on 4 transfer threads each, and a replicated
# pool of one PG on them.
#
function run_oob_cluster() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in 0 1 2 ; do
        run_osd $dir $id \
            --oob_client_transports=tcp \
            --oob_memory=268435456 \
            --osd_oob_transfer_threads=4 || return 1
    done
    create_pool rpool 1 1 || return 1
    ceph osd pool set rpool pg_autoscale_mode off || return 1
    wait_for_clean || return 1
}

##
# Print **field** of the line the client printed for **what** (get or
# behind) of **oid** in **out**.
#
function client_field() {
    local out="$1"
    local what=$2
    local oid=$3
    local field=$4
    echo "$out" | grep "^$what $oid: " | grep -o " $field=[^ ]*" | cut -d= -f2
}

##
# Read obj with its push held for 6 s on the primary, write its start
# right behind the read, and run **change** after 1 s. The client asks
# the mon for a new map once an op is out for half a second, so it
# learns of the change before the held reply comes, and sends the read
# and the write again. The others keep it busy with the OSDs meanwhile.
# The read must come back sent again, with the object as it was before
# the write, and the write must succeed after it.
#
function held_push_then_change() {
    local dir=$1
    local change="$2"

    ceph_test_osd_oob_read write rpool $SIZE obj || return 1
    rados -p rpool get obj $dir/obj.before || return 1
    local primary=$(get_primary rpool obj)
    ceph tell osd.$primary config set osd_debug_oob_delay_done_ms 6000 || return 1

    ( sleep 1 ; eval "$change" ) &
    local out
    out=$(CEPH_ARGS="$CEPH_ARGS --objecter_tick_interval=0.2 --objecter_timeout=0.5" \
        ceph_test_osd_oob_read get rpool $SIZE --write-behind --others 4000 obj)
    local rc=$?
    wait
    echo "$out"
    ceph tell osd.$primary config set osd_debug_oob_delay_done_ms 0
    grep "deliver_oob .*:::obj:head\|maybe_wait_for_oob_reads .*:::obj:head\|oob_read_kick .*:::obj:head" \
        $dir/osd.$primary.log | tail -8
    test $rc = 0 || return 1
    # the read was sent again, and served inline: the push of its first
    # attempt may still have been writing the window
    local flags=$(client_field "$out" get obj flags)
    (( flags & FLAG_RESENT )) || return 1
    (( flags & FLAG_DECLINED )) || return 1
    # with the object as it was before the write behind it
    test "$(client_field "$out" get obj data)" = ok || return 1
    # the write, once again, came back after the read
    test "$(client_field "$out" behind obj r)" = 0 || return 1
    test "$(client_field "$out" behind obj after_read)" = 1 || return 1
    # the first attempt of the write waited in the fence of the object
    grep -q "maybe_wait_for_oob_reads .*:::obj:head .*waits for the replies" \
        $dir/osd.$primary.log || return 1
    # and the object now has the write in its first 4 KiB, and nothing
    # else changed
    rados -p rpool get obj $dir/obj.after || return 1
    ! cmp -s <(head -c 4096 $dir/obj.before) <(head -c 4096 $dir/obj.after) || return 1
    cmp <(tail -c +4097 $dir/obj.before) <(tail -c +4097 $dir/obj.after) || return 1
}

# (a) the PG splits while the push is held: the client sends every op
# of a PG that split again
function TEST_split_during_held_push() {
    local dir=$1

    run_oob_cluster $dir || return 1
    held_push_then_change $dir \
        "ceph osd pool set rpool pg_num 2 ; ceph osd pool set rpool pgp_num 2" || return 1
}

# (b) the PG gets another primary while the push is held: the old
# primary drops the write that waited in its fence, and the client sends
# both ops to the new primary
function TEST_primary_change_during_held_push() {
    local dir=$1

    run_oob_cluster $dir || return 1
    local primary=$(get_primary rpool obj)
    held_push_then_change $dir "ceph osd primary-affinity osd.$primary 0" || return 1
    test $(get_primary rpool obj) != $primary || return 1
}

main osd-oob-resend "$@"

# Local Variables:
# compile-command: "cd ../../../build ; make -j4 && ../qa/run-standalone.sh osd-oob-resend.sh"
# End:
