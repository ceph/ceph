#!/usr/bin/env bash
#
# Out-of-band read pushes on the OSD's transfer threads
# (osd_oob_transfer_threads): an op thread hands the push off and goes
# on, and the reply leaves from the transfer thread once the push is
# over. The same for the push of an erasure-coded shard into its
# primary's gather window (oob_cluster_transport), whose sub-read reply
# leaves once the push is over. And the pull of a pulled write
# (OSD-direct PUT), for which the write waits in a hold of its PG,
# holding no lock, with the client ops on its object behind it, until
# the transfer thread that pulled its payload kicks the PG. The client
# windows and the gather windows are on the libfabric tcp provider, on
# 127.0.0.1, so this needs a build with WITH_OOB_OFI, whose
# ceph_test_osd_oob_read is the client. A libfabric whose memory
# registration cache monitor needs a GPU wants
# FI_MR_CACHE_MONITOR=disabled in the environment.
#
# Each pool has one PG, so that every object of a test shares the PG of
# the read whose push is held, or of the write whose pull is held
# (osd_debug_oob_delay_done_ms).
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

# what the client prints in a read's flags
FLAG_DECLINED=8
FLAG_LANDED=16
SIZE=1048576

function run() {
    local dir=$1
    shift

    if ! type ceph_test_osd_oob_read > /dev/null 2>&1 ; then
        echo "ceph_test_osd_oob_read not found (built without WITH_OOB_OFI?)"
        # a run that needs the tests sets CEPH_OOB_REQUIRED, and fails here
        test -z "$CEPH_OOB_REQUIRED" ; return
    fi

    export CEPH_MON="127.0.0.1:7161" # git grep '\<7161\>' : there must be only one
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
# libfabric tcp provider, with **threads** transfer threads each. An op
# thread that runs for 3 s makes its OSD unhealthy. The remaining
# arguments go to each OSD.
#
function run_oob_cluster() {
    local dir=$1
    local threads=$2
    shift 2

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in 0 1 2 ; do
        run_osd $dir $id \
            --oob_client_transports=tcp \
            --oob_memory=268435456 \
            --osd_oob_transfer_threads=$threads \
            --osd_op_thread_timeout=3 \
            "$@" || return 1
    done
    for id in 0 1 2 ; do
        test "$(ofi_stat $id available)" = true || return 1
    done
}

function ofi_stat() {
    local id=$1
    local field=$2
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$id) ofi status | \
        jq -r ".$field"
}

function xfer_stat() {
    local id=$1
    local field=$2
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$id) \
        perf dump oob_xfer | jq -r ".oob_xfer.$field"
}

# the transfers osd.**id** runs now, on its transfer threads or inline
function running_transfers() {
    local id=$1
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$id) \
        dump_oob_transfers | jq -r '(.oob_transfers // .).running | length'
}

# wait up to 10 s until osd.**id** runs a transfer
function wait_for_transfer() {
    local id=$1
    for i in $(seq 100) ; do
        test $(running_transfers $id) -gt 0 && return 0
        sleep 0.1
    done
    return 1
}

# a replicated pool of one PG, on all three OSDs
function create_repl_pool() {
    local pool=$1
    create_pool $pool 1 1 || return 1
    wait_for_clean || return 1
}

# only osd.**id** leads the PGs of pools created after: a PG that splits
# leaves its child on it
function only_primary() {
    local id=$1
    for other in 0 1 2 ; do
        test $other = $id && continue
        ceph osd primary-affinity osd.$other 0 || return 1
    done
}

# an erasure-coded pool of one PG, k=2 m=1, with EC optimizations
function create_ec_pool_k2m1() {
    local pool=$1
    ceph osd erasure-code-profile set oobprofile k=2 m=1 \
        crush-failure-domain=osd || return 1
    create_pool $pool 1 1 erasure oobprofile || return 1
    ceph osd pool set $pool allow_ec_overwrites true || return 1
    ceph osd pool set $pool allow_ec_optimizations true || return 1
    wait_for_clean || return 1
}

# the OSD that holds shard **shard** of the PG of **oid**
function shard_osd() {
    local pool=$1
    local oid=$2
    local shard=$3
    ceph osd map $pool $oid -f json | jq -r ".acting[$shard]"
}

##
# The OSD whose push into the primary's gather window an object of
# **pool** needs: the primary reads its own shard, shard 0, and the
# other data shard, shard 1, from its OSD. A 4 KiB read at offset 0
# needs shard 0 only, so it involves no push.
#
function gather_shard() {
    local pool=$1
    local oid=$2
    test "$(shard_osd $pool $oid 0)" = "$(get_primary $pool $oid)" || return 1
    shard_osd $pool $oid 1
}

##
# Print **field** of the line the client printed for its read of **oid**
# in **out**.
#
function read_field() {
    local out="$1"
    local oid=$2
    local field=$3
    echo "$out" | grep "^get $oid: " | grep -o " $field=[^ ]*" | cut -d= -f2
}

function others_field() {
    local out="$1"
    local field=$2
    echo "$out" | grep "^others: " | grep -o " $field=[^ ]*" | cut -d= -f2
}

# **field** of the line the client printed for its write behind **oid**
function behind_field() {
    local out="$1"
    local oid=$2
    local field=$3
    echo "$out" | grep "^behind $oid: " | grep -o " $field=[^ ]*" | cut -d= -f2
}

# the read of oid got its object out of band, landed
function expect_landed() {
    local out="$1"
    local oid=$2
    test "$(read_field "$out" $oid r)" = 0 || return 1
    test "$(read_field "$out" $oid data)" = ok || return 1
    test "$(read_field "$out" $oid crc)" = ok || return 1
    test "$(read_field "$out" $oid bytes)" = $SIZE || return 1
    test "$(read_field "$out" $oid inline)" = 0 || return 1
    local flags=$(read_field "$out" $oid flags)
    (( flags & FLAG_LANDED )) || return 1
}

# the read of oid got its object inline, declined
function expect_declined() {
    local out="$1"
    local oid=$2
    test "$(read_field "$out" $oid r)" = 0 || return 1
    test "$(read_field "$out" $oid data)" = ok || return 1
    test "$(read_field "$out" $oid bytes)" = 0 || return 1
    test "$(read_field "$out" $oid inline)" = $SIZE || return 1
    local flags=$(read_field "$out" $oid flags)
    (( flags & FLAG_DECLINED )) || return 1
}

##
# While the primary holds the push of a read for 3 s on a transfer
# thread, 200 writes, stats and reads of other objects of the same PG
# finish, and no op thread of the primary times out. A write to the
# object being read, right behind the read, waits in the object's fence
# for the read's reply: the op thread let go of the object's lock at the
# handoff, but the write is acknowledged after the read's reply, and the
# read gets the object as it was before the write.
#
function held_push_frees_op_thread() {
    local dir=$1
    local pool=$2

    ceph_test_osd_oob_read write $pool $SIZE obj || return 1
    local primary=$(get_primary $pool obj)
    ceph tell osd.$primary config set osd_debug_oob_delay_done_ms 3000 || return 1
    local queued=$(xfer_stat $primary queue_lat.avgcount)

    local out
    out=$(ceph_test_osd_oob_read get $pool $SIZE --write-behind --others 200 obj) || return 1
    echo "$out"
    expect_landed "$out" obj || return 1
    # the read was held, the others were not
    test $(read_field "$out" obj ms) -ge 3000 || return 1
    test "$(others_field "$out" failed)" = 0 || return 1
    test "$(others_field "$out" before_reads)" = 1 || return 1
    # before_reads shows that they did not wait for the held read. A
    # loaded host can take a second over each, so the bound is the delay
    test $(others_field "$out" max_ms) -lt 3000 || return 1
    # the write behind the read waited for the read's reply
    test "$(behind_field "$out" obj r)" = 0 || return 1
    test "$(behind_field "$out" obj after_read)" = 1 || return 1
    test $(behind_field "$out" obj ms) -ge 3000 || return 1
    grep -q "maybe_wait_for_oob_reads .*:::obj:head .*waits for the replies" \
        $dir/osd.$primary.log || return 1
    grep -q "oob_read_kick .*:::obj:head: the replies of its reads left, requeuing 1 ops" \
        $dir/osd.$primary.log || return 1
    # on a transfer thread, handed off by the op thread
    test $(xfer_stat $primary queue_lat.avgcount) -gt $queued || return 1
    grep -q "deliver_oob .*:::obj:head .* handed off to the transfer threads" \
        $dir/osd.$primary.log || return 1
    # past the 3 s of osd_op_thread_timeout, heartbeat checks included
    sleep 2
    ! grep -q "osd_op_tp.*had timed out" $dir/osd.$primary.log || return 1
}

# (a) a replicated pool
function TEST_held_push_frees_op_thread() {
    local dir=$1

    run_oob_cluster $dir 4 || return 1
    create_repl_pool rpool || return 1
    held_push_frees_op_thread $dir rpool || return 1
}

# (b) an erasure-coded pool: the primary's read completes through
# kick_reads
function TEST_held_push_frees_op_thread_ec() {
    local dir=$1

    run_oob_cluster $dir 4 || return 1
    create_ec_pool_k2m1 ecpool || return 1
    held_push_frees_op_thread $dir ecpool || return 1
}

# without transfer threads the op thread holds the PG for the push, and
# the other ops of the PG wait for it: what (a) shows is gone
function TEST_inline_push_holds_the_pg() {
    local dir=$1

    run_oob_cluster $dir 0 || return 1
    create_repl_pool rpool || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj || return 1
    local primary=$(get_primary rpool obj)
    ceph tell osd.$primary config set osd_debug_oob_delay_done_ms 3000 || return 1

    local out
    out=$(ceph_test_osd_oob_read get rpool $SIZE --others 200 obj) || return 1
    echo "$out"
    expect_landed "$out" obj || return 1
    test "$(others_field "$out" before_reads)" = 0 || return 1
    test $(others_field "$out" max_ms) -ge 2000 || return 1
    test $(xfer_stat $primary queue_lat.avgcount) = 0 || return 1
}

##
# Run a mon, a mgr that serves perf queries and three OSDs that push
# reads over the libfabric tcp provider, on 4 transfer threads each. The
# remaining arguments go to each OSD.
#
function run_perf_query_cluster() {
    local dir=$1
    shift

    run_mon $dir a || return 1
    # the module that serves the perf queries, before the mgr starts
    ceph mgr module enable osd_perf_query --force || return 1
    run_mgr $dir x || return 1
    for id in 0 1 2 ; do
        run_osd $dir $id \
            --oob_client_transports=tcp \
            --oob_memory=268435456 \
            --osd_oob_transfer_threads=4 \
            "$@" || return 1
    done
}

##
# Add a mgr perf query by client, wait until osd.**id** has it, and
# print its id. A read before the OSD has it is not counted.
#
function add_client_query() {
    local dir=$1
    local id=$2
    local qid
    for i in $(seq 30) ; do
        qid=$(ceph osd perf query add client_id 2>/dev/null) && break
        sleep 1
    done
    test -n "$qid" || return 1
    for i in $(seq 30) ; do
        grep -q "setting 1 queries" $dir/osd.$id.log && break
        sleep 1
    done
    grep -q "setting 1 queries" $dir/osd.$id.log || return 1
    echo $qid
}

##
# Read **oid** of **pool** with its push held for 2 s on a transfer
# thread of its primary, and check that the read's latency reaches the
# per-client results of perf query **qid**. The OSD reports its perf
# query results every mgr_stats_period, and each get takes what came
# since the last: the longest read latency, in ms, of a client that
# read. The held read is its client's only one.
#
function held_read_in_perf_query() {
    local pool=$1
    local oid=$2
    local qid=$3
    local primary=$(get_primary $pool $oid)

    ceph tell osd.$primary config set osd_debug_oob_delay_done_ms 2000 || return 1
    local out
    out=$(ceph_test_osd_oob_read get $pool $SIZE $oid) || return 1
    echo "$out"
    expect_landed "$out" $oid || return 1
    test $(read_field "$out" $oid ms) -ge 2000 || return 1

    local lat=0
    for i in $(seq 60) ; do
        lat=$(ceph osd perf counters get $qid | \
            awk -F'|' -v max=$lat '
                $4 ~ /^ *[1-9]/ { gsub(/ /, "", $8); if ($8 + 0 > max + 0) max = $8 }
                END { print max }')
        test ${lat%.*} -ge 2000 && break
        sleep 1
    done
    echo "read latency in the PG's perf stats: $lat ms"
    test ${lat%.*} -ge 2000 || return 1
}

##
# The latency of a read whose push a transfer thread holds is counted at
# the reply, in the PG's dynamic perf stats as in the OSD's logger: a
# mgr perf query by client sees the time of the push. The primary logs
# the read's stats, as it logs those of every op.
#
function TEST_held_push_latency_at_reply() {
    local dir=$1

    run_perf_query_cluster $dir || return 1
    create_repl_pool rpool || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj || return 1
    local primary=$(get_primary rpool obj)
    local qid
    qid=$(add_client_query $dir $primary) || return 1
    held_read_in_perf_query rpool obj $qid || return 1
    grep -q "log_op_stats osd_op(.*:::obj:head \[read .* lat 2\." \
        $dir/osd.$primary.log || return 1
}

##
# As above, for a read of a PG that a split made after the query was
# added. Nothing gives such a PG the queries but the OSD's next report
# of its perf query results, which hands every PG a fresh set of stats
# with them. The stats of its pushed reads must get them from there too.
#
function TEST_held_push_latency_after_split() {
    local dir=$1

    run_perf_query_cluster $dir || return 1
    only_primary 0 || return 1
    create_repl_pool rpool || return 1
    ceph osd pool set rpool pg_autoscale_mode off || return 1
    local qid
    qid=$(add_client_query $dir 0) || return 1
    local log=$dir/osd.0.log
    ceph osd pool set rpool pg_num 2 || return 1
    wait_for_clean || return 1
    test "$(ceph osd pool get rpool pg_num -f json | jq .pg_num)" = 2 || return 1
    # an object of the child PG, which osd.0 leads
    local oid
    for i in $(seq 100) ; do
        if [[ "$(get_pg rpool obj$i)" == *.1 ]] ; then
            oid=obj$i
            break
        fi
    done
    test -n "$oid" || return 1
    test $(get_primary rpool $oid) = 0 || return 1
    # until a report after the split: a read before it is not counted
    local reports=$(grep -c "reports for [0-9]* queries" $log)
    for i in $(seq 30) ; do
        test $(grep -c "reports for [0-9]* queries" $log) -gt $reports && break
        sleep 1
    done
    test $(grep -c "reports for [0-9]* queries" $log) -gt $reports || return 1
    ceph_test_osd_oob_read write rpool $SIZE $oid || return 1
    held_read_in_perf_query rpool $oid $qid || return 1
}

##
# (c) An interval change while the primary holds a push: the op is in no
# list of the PG, so nothing runs it again; the reply leaves once, with
# the object's bytes, and the PG drops the object's context without the
# op (on_flushed asserts it holds none).
#
function TEST_interval_change_during_held_push() {
    local dir=$1

    run_oob_cluster $dir 4 || return 1
    create_repl_pool rpool || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj1 obj2 || return 1
    local primary=$(get_primary rpool obj1)
    local replica=$(get_not_primary rpool obj1)
    local pgid=$(get_pg rpool obj1)
    for id in 0 1 2 ; do
        ceph tell osd.$id config set osd_debug_oob_delay_done_ms 3000 || return 1
    done

    # repeer the PG, then mark a replica down, while the push is held
    local started=$(ofi_stat $primary plans_started)
    timeout 60 ceph_test_osd_oob_read get rpool $SIZE obj1 > $dir/get1.out &
    local pid=$!
    sleep 1
    ceph pg repeer $pgid || return 1
    ceph osd down osd.$replica || return 1
    wait $pid || return 1
    cat $dir/get1.out
    test $(grep -c '^get ' $dir/get1.out) = 1 || return 1
    expect_landed "$(cat $dir/get1.out)" obj1 || return 1
    test $(ofi_stat $primary plans_started) = $((started + 1)) || return 1
    wait_for_clean || return 1

    # mark the primary down while it holds the push: its PG leaves the
    # primary role, and drops its object contexts, while the push runs.
    # The push's reply lands, unless the client learned of the new map
    # first and resent the read to the next primary, which serves it
    # inline; the client then drops the push's reply
    primary=$(get_primary rpool obj2)
    started=$(ofi_stat $primary plans_started)
    timeout 60 ceph_test_osd_oob_read get rpool $SIZE obj2 > $dir/get2.out &
    pid=$!
    sleep 1
    ceph osd down osd.$primary || return 1
    wait $pid || return 1
    cat $dir/get2.out
    test $(grep -c '^get ' $dir/get2.out) = 1 || return 1
    test "$(read_field "$(cat $dir/get2.out)" obj2 r)" = 0 || return 1
    test "$(read_field "$(cat $dir/get2.out)" obj2 data)" = ok || return 1
    # the reply can land before the old primary has the map that marks it
    # down, on a loaded host
    for i in $(seq 30) ; do
        grep -q "wrongly marked me down" $dir/osd.$primary.log && break
        sleep 1
    done
    grep -q "wrongly marked me down" $dir/osd.$primary.log || return 1
    # the push ran to its end on the old primary
    sleep 3
    test $(ofi_stat $primary plans_started) = $((started + 1)) || return 1
    wait_for_clean || return 1

    # and no OSD ran either read again past its handoff: a re-run would
    # reply without a second push, which the client drops by its tid
    for id in 0 1 2 ; do
        kill -0 $(cat $dir/osd.$id.pid) || return 1
        ! grep -q "obc still alive\|FAILED ceph_assert" $dir/osd.$id.log || return 1
        ! grep -q "already pushed" $dir/osd.$id.log || return 1
    done
}

##
# (d) A slow stop while a push is held and another waits for the only
# transfer thread: the waiting one is shed, which starts nothing, and
# the stop waits for the held one, at most until its land_by. The reads
# get their objects, from the stopping primary or, resent, from the
# next one.
#
function TEST_slow_stop_during_held_push() {
    local dir=$1

    run_oob_cluster $dir 1 --osd_fast_shutdown=false || return 1
    create_repl_pool rpool || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj1 obj2 || return 1
    local primary=$(get_primary rpool obj1)
    # long enough that the stop reaches the transfer threads before the
    # held push ends, and within the lease plus drain
    ceph tell osd.$primary config set osd_debug_oob_delay_done_ms 6000 || return 1

    timeout 60 ceph_test_osd_oob_read get rpool $SIZE obj1 obj2 > $dir/get.out &
    local pid=$!
    local queued
    for i in $(seq 100) ; do
        queued=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$primary) \
            dump_oob_transfers | jq -r '.oob_transfers.queued // .queued')
        test "$queued" = 1 && break
        sleep 0.1
    done
    test "$queued" = 1 || return 1
    kill_daemons $dir TERM osd.$primary || return 1
    wait $pid || return 1
    cat $dir/get.out
    for oid in obj1 obj2 ; do
        test "$(read_field "$(cat $dir/get.out)" $oid r)" = 0 || return 1
        test "$(read_field "$(cat $dir/get.out)" $oid data)" = ok || return 1
    done

    local log=$dir/osd.$primary.log
    grep -q "stopping: shedding 1 queued jobs, waiting for 1 running" $log || return 1
    grep -q "oob transfer threads stopped" $log || return 1
    ! grep -q "FAILED ceph_assert\|Caught signal" $log || return 1
    # no callback kept a PG past the stop of the op threads
    ! grep -q "has ref count of" $log || return 1
    # the stop waited for the held push, and not past the pool's lease
    # plus drain (8 s)
    local shed=$(grep -m1 "stopping: shedding" $log | cut -d' ' -f1)
    local stopped=$(grep -m1 "oob transfer threads stopped" $log | cut -d' ' -f1)
    local waited=$(( $(date -d "$stopped" +%s%3N) - $(date -d "$shed" +%s%3N) ))
    echo "the transfer threads stopped ${waited} ms after the shed"
    test $waited -le 8000 || return 1
}

##
# (e) A push that a transfer thread can start only after the pool's
# lease starts nothing: the read is declined, its data inline.
#
function TEST_push_past_lease_declines() {
    local dir=$1

    run_oob_cluster $dir 4 || return 1
    create_repl_pool rpool || return 1
    ceph osd pool set rpool rdma_delivery_lease 1 || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj || return 1
    local primary=$(get_primary rpool obj)
    # each push waits 1.5 s on its transfer thread before it may start
    ceph tell osd.$primary config set osd_debug_oob_delay_post_ms 1500 || return 1
    local started=$(ofi_stat $primary plans_started)
    local expired=$(xfer_stat $primary expired)

    local out
    out=$(ceph_test_osd_oob_read get rpool $SIZE obj) || return 1
    echo "$out"
    expect_declined "$out" obj || return 1
    test $(ofi_stat $primary plans_started) = $started || return 1
    test $(xfer_stat $primary expired) = $((expired + 1)) || return 1

    # within the lease, it goes out of band again
    ceph tell osd.$primary config set osd_debug_oob_delay_post_ms 0 || return 1
    out=$(ceph_test_osd_oob_read get rpool $SIZE obj) || return 1
    echo "$out"
    expect_landed "$out" obj || return 1
    test $(ofi_stat $primary plans_started) = $((started + 1)) || return 1
}

##
# (S3) While a peer shard holds its push into the primary's gather window
# for 4 s, its PG serves other ops: 100 writes of whole stripes, which
# reach every shard, finish, and no op thread of the shard times out.
# The held read gets its object once the push is over. With **threads**
# at 0 the shard's op thread holds the PG for the push, and the writes
# wait for it. Reads of the PG would wait either way: an
# erasure-coded primary completes the client reads of a PG in order.
#
function held_shard_push() {
    local dir=$1
    local threads=$2

    run_oob_cluster $dir $threads --oob_cluster_transport=tcp || return 1
    create_ec_pool_k2m1 ecpool || return 1
    ceph_test_osd_oob_read write ecpool $SIZE obj || return 1
    local primary=$(get_primary ecpool obj)
    local shard
    shard=$(gather_shard ecpool obj) || return 1
    ceph tell osd.$shard config set osd_debug_oob_delay_done_ms 4000 || return 1
    local queued=$(xfer_stat $shard queue_lat.avgcount)
    local pushed=$(ofi_stat $shard plans_completed)
    local others=$(for i in $(seq 100) ; do echo -n "other.$i " ; done)

    timeout 60 ceph_test_osd_oob_read get ecpool $SIZE obj > $dir/get.out &
    local pid=$!
    wait_for_transfer $shard || return 1
    local start=$(date +%s%3N)
    ceph_test_osd_oob_read write ecpool 65536 $others || return 1
    local took=$(( $(date +%s%3N) - start ))
    local held=$(running_transfers $shard)
    echo "the writes took $took ms"
    wait $pid || return 1
    cat $dir/get.out
    expect_landed "$(cat $dir/get.out)" obj || return 1
    test $(read_field "$(cat $dir/get.out)" obj ms) -ge 4000 || return 1
    test $(ofi_stat $shard plans_completed) = $((pushed + 1)) || return 1
    test $(ofi_stat $primary gather_crc_mismatch) = 0 || return 1
    if [ $threads = 0 ] ; then
        # the writes waited for the shard's op thread
        test $took -ge 2000 || return 1
        test $(xfer_stat $shard queue_lat.avgcount) = 0 || return 1
        return 0
    fi
    # the writes were done while the push was still held, and did not
    # wait for it. A loaded host can take seconds over them, so the bound
    # on their time is the push's delay
    test $held -gt 0 || return 1
    test $took -lt 4000 || return 1
    # on a transfer thread
    test $(xfer_stat $shard queue_lat.avgcount) -gt $queued || return 1
    grep -q "sub-read .* pushed $((SIZE / 2)) bytes to the primary" \
        $dir/osd.$shard.log || return 1
    # past the 3 s of osd_op_thread_timeout, heartbeat checks included
    sleep 2
    ! grep -q "osd_op_tp.*had timed out" $dir/osd.$shard.log || return 1
}

function TEST_held_shard_push_frees_shard() {
    held_shard_push $1 4
}

function TEST_inline_shard_push_holds_the_pg() {
    held_shard_push $1 0
}

##
# (S3) Many reads of an erasure-coded pool whose shards push from their
# transfer threads, ten at a time: every one gets its object, and no
# window the primary takes holds anything but its shard's push.
#
function TEST_gather_pushes_match() {
    local dir=$1

    run_oob_cluster $dir 4 --oob_cluster_transport=tcp || return 1
    create_ec_pool_k2m1 ecpool || return 1
    local objs="obj0 obj1 obj2 obj3 obj4 obj5 obj6 obj7 obj8 obj9"
    ceph_test_osd_oob_read write ecpool $SIZE $objs || return 1
    local primary=$(get_primary ecpool obj0)
    local shard
    shard=$(gather_shard ecpool obj0) || return 1
    local pushed=$(ofi_stat $shard plans_completed)
    local queued=$(xfer_stat $shard queue_lat.avgcount)

    for i in $(seq 100) ; do
        ceph_test_osd_oob_read get ecpool $SIZE $objs > $dir/get.out || {
            cat $dir/get.out ; return 1 ; }
    done
    cat $dir/get.out
    # 1000 reads, each pushed by the shard from a transfer thread
    test $(( $(ofi_stat $shard plans_completed) - pushed )) -ge 1000 || return 1
    test $(( $(xfer_stat $shard queue_lat.avgcount) - queued )) -ge 1000 || return 1
    test $(ofi_stat $shard plans_failed) = 0 || return 1
    for id in 0 1 2 ; do
        test $(ofi_stat $id gather_crc_mismatch) = 0 || return 1
    done
}

##
# (S3) A fast read lends no gather windows: it completes with the shards
# that replied first, and a window lent to a later shard would stay
# quarantined. Its shards reply inline, so a held push has nothing to
# hold, and the read does not wait.
#
function TEST_fast_read_with_held_shard() {
    local dir=$1

    run_oob_cluster $dir 4 --oob_cluster_transport=tcp || return 1
    create_ec_pool_k2m1 ecpool || return 1
    ceph osd pool set ecpool fast_read 1 || return 1
    ceph_test_osd_oob_read write ecpool $SIZE obj || return 1
    local primary=$(get_primary ecpool obj)
    local shard
    shard=$(gather_shard ecpool obj) || return 1
    ceph tell osd.$shard config set osd_debug_oob_delay_done_ms 3000 || return 1
    local started=$(ofi_stat $shard plans_started)
    local completed=$(ofi_stat $shard plans_completed)
    local acquired=$(ofi_stat $primary windows_acquired)
    local quarantined=$(ofi_stat $primary windows_rekeyed_quarantined)

    local out
    out=$(ceph_test_osd_oob_read get ecpool $SIZE obj) || return 1
    echo "$out"
    expect_landed "$out" obj || return 1
    # the counters below show that the shard pushed nothing. A loaded host
    # can take seconds over the read, so the bound is the shard's delay
    test $(read_field "$out" obj ms) -lt 3000 || return 1
    test $(ofi_stat $shard plans_started) = $started || return 1
    test $(ofi_stat $shard plans_completed) = $completed || return 1
    test $(ofi_stat $primary windows_acquired) = $acquired || return 1
    test $(ofi_stat $primary windows_rekeyed_quarantined) = $quarantined || return 1
    test $(ofi_stat $primary gather_crc_mismatch) = 0 || return 1
}

##
# (S3) An interval change while a shard holds its push: the shard sends
# its reply once the push is over, from the old interval, and the
# primary drops it. The read runs again in the new interval, and gets
# its object.
#
function TEST_repeer_during_held_shard_push() {
    local dir=$1

    run_oob_cluster $dir 4 --oob_cluster_transport=tcp || return 1
    create_ec_pool_k2m1 ecpool || return 1
    ceph_test_osd_oob_read write ecpool $SIZE obj || return 1
    local primary=$(get_primary ecpool obj)
    local pgid=$(get_pg ecpool obj)
    local shard
    shard=$(gather_shard ecpool obj) || return 1
    ceph tell osd.$shard config set osd_debug_oob_delay_done_ms 3000 || return 1
    local pushed=$(ofi_stat $shard plans_completed)

    timeout 60 ceph_test_osd_oob_read get ecpool $SIZE obj > $dir/get.out &
    local pid=$!
    wait_for_transfer $shard || return 1
    ceph pg repeer $pgid || return 1
    wait $pid || return 1
    cat $dir/get.out
    test $(grep -c '^get ' $dir/get.out) = 1 || return 1
    expect_landed "$(cat $dir/get.out)" obj || return 1
    # both pushes ran to their end on the shard
    test $(ofi_stat $shard plans_completed) = $((pushed + 2)) || return 1
    test $(ofi_stat $primary gather_crc_mismatch) = 0 || return 1
    # the reply to the first came from before the repeer: the primary
    # dropped it, and its read pipeline saw only the other shards'
    # replies to that read
    local log=$dir/osd.$primary.log
    local late=$(grep -m1 "<== osd.$shard .* MOSDECSubOpReadReply(" $log)
    local tid=$(echo "$late" | grep -o "ECSubReadReply(tid=[0-9]*" | \
        grep -o "[0-9]*$")
    local epoch=$(echo "$late" | grep -o "MOSDECSubOpReadReply([^ ]* [0-9]*" | \
        grep -o "[0-9]*$")
    test -n "$tid" -a -n "$epoch" || return 1
    grep -q "can_discard_replica_op pg changed .* after $epoch, dropping" \
        $log || return 1
    local got=$(grep -c "<== osd.* MOSDECSubOpReadReply(.*ECSubReadReply(tid=$tid," $log)
    local seen=$(grep -c "handle_sub_read_reply: reply ECSubReadReply(tid=$tid," $log)
    test $seen = $((got - 1)) || return 1
    for id in 0 1 2 ; do
        kill -0 $(cat $dir/osd.$id.pid) || return 1
        ! grep -q "FAILED ceph_assert\|Caught signal" $dir/osd.$id.log || return 1
    done
}

##
# (S3) A slow stop of a shard while it holds its push: the stop waits for
# the push, at most until its land_by, and the reply still goes out. The
# primary, which learns that the shard is down, reads the object from
# the other shards.
#
function TEST_slow_stop_during_held_shard_push() {
    local dir=$1

    run_oob_cluster $dir 4 --oob_cluster_transport=tcp \
        --osd_fast_shutdown=false || return 1
    create_ec_pool_k2m1 ecpool || return 1
    ceph_test_osd_oob_read write ecpool $SIZE obj || return 1
    local shard
    shard=$(gather_shard ecpool obj) || return 1
    # long enough that the stop reaches the transfer threads before the
    # held push ends, and within the lease plus drain
    ceph tell osd.$shard config set osd_debug_oob_delay_done_ms 6000 || return 1

    timeout 60 ceph_test_osd_oob_read get ecpool $SIZE obj > $dir/get.out &
    local pid=$!
    wait_for_transfer $shard || return 1
    kill_daemons $dir TERM osd.$shard || return 1
    wait $pid || return 1
    cat $dir/get.out
    test "$(read_field "$(cat $dir/get.out)" obj r)" = 0 || return 1
    test "$(read_field "$(cat $dir/get.out)" obj data)" = ok || return 1

    local log=$dir/osd.$shard.log
    grep -q "stopping: shedding 0 queued jobs, waiting for 1 running" $log || return 1
    grep -q "sub-read .* pushed $((SIZE / 2)) bytes to the primary" $log || return 1
    grep -q "oob transfer threads stopped" $log || return 1
    ! grep -q "FAILED ceph_assert\|Caught signal" $log || return 1
    # no callback kept a PG past the stop of the op threads
    ! grep -q "has ref count of" $log || return 1
    local shed=$(grep -m1 "stopping: shedding" $log | cut -d' ' -f1)
    local stopped=$(grep -m1 "oob transfer threads stopped" $log | cut -d' ' -f1)
    local waited=$(( $(date -d "$stopped" +%s%3N) - $(date -d "$shed" +%s%3N) ))
    echo "the transfer threads stopped ${waited} ms after the shed"
    test $waited -le 8000 || return 1
}

##
# (S3) A shard push that a transfer thread can start only after the
# pool's lease, counted from when the shard received the sub-read,
# starts nothing: the shard replies inline and says so, and the primary
# takes its window back clean. Within the lease, the shard pushes again.
#
function TEST_shard_push_past_lease_declines() {
    local dir=$1

    run_oob_cluster $dir 4 --oob_cluster_transport=tcp || return 1
    create_ec_pool_k2m1 ecpool || return 1
    ceph osd pool set ecpool rdma_delivery_lease 1 || return 1
    ceph_test_osd_oob_read write ecpool $SIZE obj || return 1
    local primary=$(get_primary ecpool obj)
    local pgid=$(get_pg ecpool obj)
    local shard
    shard=$(gather_shard ecpool obj) || return 1
    # each push waits 1.5 s on the shard's transfer thread before it may
    # start
    ceph tell osd.$shard config set osd_debug_oob_delay_post_ms 1500 || return 1
    local started=$(ofi_stat $shard plans_started)
    local pushed=$(ofi_stat $shard plans_completed)
    local expired=$(xfer_stat $shard expired)
    local lent=$(ofi_stat $primary windows_acquired)
    local quarantined=$(ofi_stat $primary windows_rekeyed_quarantined)

    local out
    out=$(ceph_test_osd_oob_read get ecpool $SIZE obj) || return 1
    echo "$out"
    # the primary's own push was past the lease by then too
    expect_declined "$out" obj || return 1
    test $(ofi_stat $shard plans_started) = $started || return 1
    test $(xfer_stat $shard expired) = $((expired + 1)) || return 1
    grep -q "sub-read [0-9]* of ${pgid}s1: push not started (-110), replying inline" \
        $dir/osd.$shard.log || return 1
    # the primary lent the shard a window, and took it back clean
    test $(ofi_stat $primary windows_acquired) -gt $lent || return 1
    test $(ofi_stat $primary windows_rekeyed_quarantined) = $quarantined || return 1

    # within the lease, it goes out of band again
    ceph tell osd.$shard config set osd_debug_oob_delay_post_ms 0 || return 1
    out=$(ceph_test_osd_oob_read get ecpool $SIZE obj) || return 1
    echo "$out"
    expect_landed "$out" obj || return 1
    test $(ofi_stat $shard plans_started) = $((started + 1)) || return 1
    test $(ofi_stat $shard plans_completed) = $((pushed + 1)) || return 1
    test $(ofi_stat $primary windows_rekeyed_quarantined) = $quarantined || return 1
    test $(ofi_stat $primary gather_crc_mismatch) = 0 || return 1
}

##
# (S3) A shard push that the shard's transfer service refuses starts
# nothing: the shard replies inline and says so, and the primary takes
# its window back clean. Each OSD takes one transfer at a time
# (osd_oob_transfer_queue_max), and the shard holds the push of a first
# read when a second read of the same PG comes. The second read waits
# for the first on the primary, and its own push from there may be
# refused too, so it gets its object either way.
#
function TEST_refused_shard_push_declines() {
    local dir=$1

    run_oob_cluster $dir 4 --oob_cluster_transport=tcp \
        --osd_oob_transfer_queue_max=1 || return 1
    create_ec_pool_k2m1 ecpool || return 1
    ceph_test_osd_oob_read write ecpool $SIZE obj obj2 || return 1
    local primary=$(get_primary ecpool obj)
    local pgid=$(get_pg ecpool obj)
    local shard
    shard=$(gather_shard ecpool obj) || return 1
    ceph tell osd.$shard config set osd_debug_oob_delay_done_ms 3000 || return 1
    local started=$(ofi_stat $shard plans_started)
    local pushed=$(ofi_stat $shard plans_completed)
    local refused=$(xfer_stat $shard refused)
    local lent=$(ofi_stat $primary windows_acquired)
    local quarantined=$(ofi_stat $primary windows_rekeyed_quarantined)

    timeout 60 ceph_test_osd_oob_read get ecpool $SIZE obj > $dir/get.out &
    local pid=$!
    wait_for_transfer $shard || return 1
    # the held push, under the shard's own PG
    test "$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$shard) \
        dump_oob_transfers | jq -r '(.oob_transfers // .).running[0].pgid')" \
        = ${pgid}s1 || return 1
    local out
    out=$(timeout 60 ceph_test_osd_oob_read get ecpool $SIZE obj2) || return 1
    echo "$out"
    test "$(read_field "$out" obj2 r)" = 0 || return 1
    test "$(read_field "$out" obj2 data)" = ok || return 1
    wait $pid || return 1
    cat $dir/get.out
    expect_landed "$(cat $dir/get.out)" obj || return 1

    test $(xfer_stat $shard refused) = $((refused + 1)) || return 1
    grep -q "push of sub-read [0-9]* refused (.*), replying inline" \
        $dir/osd.$shard.log || return 1
    # only the first read's push went out
    test $(ofi_stat $shard plans_started) = $((started + 1)) || return 1
    test $(ofi_stat $shard plans_completed) = $((pushed + 1)) || return 1
    # the primary lent a window for each read, and took both back clean
    test $(ofi_stat $primary windows_acquired) -ge $((lent + 2)) || return 1
    test $(ofi_stat $primary windows_rekeyed_quarantined) = $quarantined || return 1
    test $(ofi_stat $primary gather_crc_mismatch) = 0 || return 1
}

##
# (S4) A balanced read of an erasure-coded object: the client sends the
# read of each data shard to that shard's OSD, and each OSD pushes its
# shard into the client's window itself, with no gather on the primary.
# A shard reads directly only with the optimized EC code, which a PG
# takes when its pool has EC optimizations from its creation. A shard
# serves a read of an object only once a later write to the PG tells it
# that the object's last write committed on every shard: a write of
# whole stripes reaches each shard. And it serves reads only while it
# holds a read lease from the primary, which a new PG gets a moment
# after it is clean. A shard that cannot serve a read answers EAGAIN,
# and the client sends the read to the primary. So the test reads until
# the OSD of each data shard has pushed.
#
function TEST_ec_balanced_read_pushes_from_each_shard() {
    local dir=$1

    run_oob_cluster $dir 4 || return 1
    ceph config set mon osd_pool_default_flag_ec_optimizations true || return 1
    ceph osd erasure-code-profile set oobprofile k=2 m=1 \
        crush-failure-domain=osd || return 1
    create_pool ecpool 1 1 erasure oobprofile || return 1
    ceph osd pool get ecpool allow_ec_optimizations | grep -q true || return 1
    ceph osd pool set ecpool allow_ec_overwrites true || return 1
    wait_for_clean || return 1
    ceph_test_osd_oob_read write ecpool $SIZE obj || return 1
    ceph_test_osd_oob_read write ecpool $SIZE obj.commit || return 1
    local shard0 shard1
    shard0=$(shard_osd ecpool obj 0) || return 1
    shard1=$(shard_osd ecpool obj 1) || return 1
    test "$shard0" != "$shard1" || return 1
    local pushed0=$(ofi_stat $shard0 plans_completed)
    local pushed1=$(ofi_stat $shard1 plans_completed)

    local out
    for i in $(seq 50) ; do
        out=$(ceph_test_osd_oob_read get ecpool $SIZE --balance obj) || return 1
        test $(ofi_stat $shard0 plans_completed) -gt $pushed0 &&
            test $(ofi_stat $shard1 plans_completed) -gt $pushed1 && break
        sleep 0.2
    done
    echo "$out"
    expect_landed "$out" obj || return 1
    test $(ofi_stat $shard0 plans_completed) -gt $pushed0 || return 1
    test $(ofi_stat $shard1 plans_completed) -gt $pushed1 || return 1
    for id in 0 1 2 ; do
        test $(ofi_stat $id plans_failed) = 0 || return 1
    done
}

##
# S5: pulled writes. Print **field** of the line the client printed for
# its **kind** (put, write, read, check) of **oid** in **out**.
#
function op_field() {
    local out="$1"
    local kind=$2
    local oid=$3
    local field=$4
    echo "$out" | grep "^$kind $oid: " | grep -o " $field=[^ ]*" | cut -d= -f2
}

# the primary pulled the write of oid, with its checksum, and the object
# reads back as the client wrote it
function expect_pulled() {
    local out="$1"
    local oid=$2
    test "$(op_field "$out" put $oid r)" = 0 || return 1
    test "$(op_field "$out" put $oid bytes)" = $SIZE || return 1
    test "$(op_field "$out" put $oid crc)" = ok || return 1
    test "$(op_field "$out" check $oid data)" = ok || return 1
}

# the write of oid went in once, whichever attempt of it the primary
# answered: one it pulled, or a resend it found done, whose reply says
# what the attempt that did the write pulled, as the PG log kept it
function expect_written() {
    local out="$1"
    local oid=$2
    expect_pulled "$out" $oid || return 1
}

# the holds of PG **pgid**: the pulled writes that wait for their pulls
function holds() {
    local pgid=$1
    ceph pg $pgid query | jq -c '.oob_pull_holds'
}

# wait, for at most 100 queries of the PG, until a pulled write waits in
# a hold of **pgid** with at least **waiters** ops behind it
function wait_for_hold() {
    local pgid=$1
    local waiters=${2:-0}
    for i in $(seq 100) ; do
        test "$(ceph pg $pgid query | jq "[.oob_pull_holds[] | \
            select((.waiters | length) >= $waiters)] | length")" -gt 0 && return 0
        sleep 0.2
    done
    return 1
}

# wait, for at most 25 s, until **log** has a line that matches
# **pattern**. A transfer held by osd_debug_oob_delay_done_ms or
# osd_debug_oob_delay_post_ms can wait for it, and the test then lowers
# the option, which ends the hold: the map that a mon on a loaded host
# sends can take longer than any fixed hold
function wait_for_log() {
    local log=$1
    local pattern="$2"
    for i in $(seq 250) ; do
        grep -q "$pattern" $log && return 0
        sleep 0.1
    done
    return 1
}

# set **option** of osd.**id** to **value** over its admin socket, which
# does not wait for the mon, as ceph tell can for seconds on a loaded
# host: a resend that waits for a held pull must still start its own
# pull within the pool's lease of its receipt
function osd_config_set() {
    local id=$1
    local option=$2
    local value=$3
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$id) \
        config set $option $value > /dev/null
}

# the bytes of pulled and inline payloads osd.**id** holds for clients
function client_bytes() {
    local id=$1
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$id) \
        perf dump throttle-osd_client_bytes | jq -r '.["throttle-osd_client_bytes"].val'
}

# the entries of the log of PG **pgid** that **oid** has
function log_entries() {
    local pgid=$1
    local oid=$2
    ceph tell $pgid log | jq "[.pg_log_t.log[] | select(.object | test(\":::$oid:head\$\"))] | length"
}

# no OSD crashed, asserted or left a hold past an interval change
function no_osd_failed() {
    local dir=$1
    for id in "${@:2}" ; do
        kill -0 $(cat $dir/osd.$id.pid) || return 1
        ! grep -q "FAILED ceph_assert\|Caught signal\|out of order op\|obc still alive\|oob pull hold still held" \
            $dir/osd.$id.log || return 1
    done
}

##
# (a) and (b): one client writes an object with a pulled write W1 whose
# pull is held for 3 s on a transfer thread, then an inline write W2 to
# the start of the object and a read ordered after both. W2 and the read
# wait behind W1 (osd_debug_op_order aborts the OSD on a write applied
# out of order), and the read sees both. Meanwhile 200 writes, stats and
# reads of other objects of the PG run, some of them while W1 waits, and
# no op thread times out.
#
function TEST_pulled_write_keeps_order() {
    local dir=$1

    run_oob_cluster $dir 4 --osd_debug_op_order=true || return 1
    create_repl_pool rpool || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj || return 1
    local primary=$(get_primary rpool obj)
    ceph tell osd.$primary config set osd_debug_oob_delay_done_ms 3000 || return 1
    local pulls=$(ofi_stat $primary pulls_started)
    local queued=$(xfer_stat $primary queue_lat.avgcount)

    local out
    out=$(ceph_test_osd_oob_read put rpool $SIZE --then-write --others 200 obj) || return 1
    echo "$out"
    expect_pulled "$out" obj || return 1
    test $(op_field "$out" put obj ms) -ge 3000 || return 1
    test "$(op_field "$out" write obj r)" = 0 || return 1
    test "$(op_field "$out" read obj data)" = ok || return 1
    # (b) the others did not wait for the pull: the primary ran some of
    # them while the write waited in its hold, and none of them waited
    # behind it. Neither their latency nor their total time tells: each
    # write of them waits for its replicas to commit, which on a loaded
    # host can take a second or more, and 200 of them can take longer
    # than the hold
    test "$(others_field "$out" failed)" = 0 || return 1
    local log=$dir/osd.$primary.log
    local parked=$(grep -n -m1 "park_for_oob_pull .*:::obj:head .* waits for its pulls" $log | cut -d: -f1)
    local kicked=$(grep -n -m1 "oob_pull_kick .*:::obj:head .* requeuing it" $log | cut -d: -f1)
    test -n "$parked" -a -n "$kicked" || return 1
    test $(sed -n "${parked},${kicked}p" $log | grep -c "do_op osd_op(.*:::obj\.other\.") -gt 0 || return 1
    ! grep -q "maybe_wait_for_oob_pull .*:::obj\.other\..* waits behind" $log || return 1
    # one pull, on a transfer thread; the write waited for it in a hold,
    # and the write and the read behind it
    test $(ofi_stat $primary pulls_started) = $((pulls + 1)) || return 1
    test $(xfer_stat $primary queue_lat.avgcount) -gt $queued || return 1
    test $(grep -c "maybe_wait_for_oob_pull .*:::obj:head .* waits behind" $log) = 2 || return 1
    grep -q "oob_pull_kick .*:::obj:head .* requeuing it and 2 ops behind it" $log || return 1
    test "$(holds $(get_pg rpool obj))" = "[]" || return 1
    # the payload went back to the throttle with the request
    test $(client_bytes $primary) = 0 || return 1
    # past the 3 s of osd_op_thread_timeout, heartbeat checks included
    sleep 2
    ! grep -q "osd_op_tp.*had timed out" $log || return 1
    no_osd_failed $dir 0 1 2 || return 1
}

# without transfer threads the pull holds the op thread and the PG: the
# other ops of the PG wait for it, and nothing parks
function TEST_inline_pull_holds_the_pg() {
    local dir=$1

    run_oob_cluster $dir 0 --osd_debug_op_order=true || return 1
    create_repl_pool rpool || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj || return 1
    local primary=$(get_primary rpool obj)
    ceph tell osd.$primary config set osd_debug_oob_delay_done_ms 2000 || return 1

    local out
    out=$(ceph_test_osd_oob_read put rpool $SIZE --then-write --others 30 obj) || return 1
    echo "$out"
    expect_pulled "$out" obj || return 1
    test "$(op_field "$out" read obj data)" = ok || return 1
    test $(others_field "$out" max_ms) -ge 1000 || return 1
    ! grep -q "park_for_oob_pull" $dir/osd.$primary.log || return 1
    test $(xfer_stat $primary queue_lat.avgcount) = 0 || return 1
}

##
# (c) A deep scrub of the PG starts while the pull of W1 is held: the
# write holds no lock of the object, so the scrub takes the chunk with
# the object, and blocks writes to it (osd_scrub_max_preemptions=0)
# while the replicas build their maps, which takes them 8 s. W2 and an
# ordered read come during the pull, and wait in the hold behind W1.
# Once the pull settled, W1 waits for the scrub, and the ops behind it
# after it; then they run in that order. Had W2 waited for the scrub
# first, it would run before W1, and osd_debug_op_order would abort the
# OSD.
#
function TEST_scrub_during_pull_keeps_order() {
    local dir=$1

    run_oob_cluster $dir 4 --osd_debug_op_order=true \
        --osd_op_thread_timeout=15 --osd_scrub_max_preemptions=0 || return 1
    create_repl_pool rpool || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj || return 1
    local primary=$(get_primary rpool obj)
    local pgid=$(get_pg rpool obj)
    ceph tell osd.$primary config set osd_debug_oob_delay_done_ms 5000 || return 1
    local id
    for id in $(ceph pg map $pgid -f json | jq '.acting[]') ; do
        test $id = $primary && continue
        ceph tell osd.$id config set osd_debug_deep_scrub_sleep 8 || return 1
    done

    rm -f $dir/go
    timeout 120 ceph_test_osd_oob_read put rpool $SIZE --then-write \
        --wait-file $dir/go obj > $dir/put.out &
    local pid=$!
    wait_for_hold $pgid || return 1
    ceph pg deep-scrub $pgid || return 1
    # the scrub blocks the chunk once a replica builds its map
    for i in $(seq 100) ; do
        grep -q "be_deep_scrub sleeping" $dir/osd.*.log && break
        sleep 0.1
    done
    grep -q "be_deep_scrub sleeping" $dir/osd.*.log || return 1
    touch $dir/go
    wait_for_hold $pgid 2 || return 1
    wait $pid || return 1
    cat $dir/put.out
    local out="$(cat $dir/put.out)"
    expect_pulled "$out" obj || return 1
    test "$(op_field "$out" write obj r)" = 0 || return 1
    test "$(op_field "$out" read obj data)" = ok || return 1
    # the kick requeued W1 with the two ops behind it, and W1 then waited
    # for the scrub, as did W2 and the read after it
    local log=$dir/osd.$primary.log
    grep -q "oob_pull_kick .*:::obj:head .* requeuing it and 2 ops behind it" $log || return 1
    local kicked=$(grep -n -m1 "oob_pull_kick .*:::obj:head .* requeuing it" $log | cut -d: -f1)
    test $(tail -n +$kicked $log | grep -c "do_op_impl: waiting for scrub") -ge 3 || return 1
    no_osd_failed $dir 0 1 2 || return 1
}

##
# (d) The PG backfills a new OSD while the pull of W1 is held (its log
# is too short to recover the new OSD from): the write holds no lock of
# the object, so the backfill copies it, from before W1. W2 and an
# ordered read come during the backfill, and wait behind W1. Once the
# pull settled, W1, then W2, go to every OSD of the PG, the new one too:
# a deep scrub finds the copies equal.
#
function TEST_backfill_during_pull_keeps_order() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in 0 1 2 3 ; do
        run_osd $dir $id \
            --oob_client_transports=tcp \
            --oob_memory=268435456 \
            --osd_oob_transfer_threads=4 \
            --osd_debug_op_order=true \
            --osd_min_pg_log_entries=5 --osd_max_pg_log_entries=10 \
            --osd_pg_log_trim_min=1 || return 1
    done
    ceph osd pool create rpool 1 1 replicated || return 1
    ceph osd pool set rpool size 3 || return 1
    ceph osd set-require-min-compat-client luminous || return 1
    wait_for_clean || return 1
    ceph_test_osd_oob_read write rpool 4096 $(seq -f "small.%g" 30) || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj || return 1
    local primary=$(get_primary rpool obj)
    local pgid=$(get_pg rpool obj)
    local acting="$(ceph pg map $pgid -f json | jq -r '.acting[]')"
    local spare
    for id in 0 1 2 3 ; do
        echo "$acting" | grep -qx $id || spare=$id
    done
    local replica=$(echo "$acting" | grep -vx $primary | head -1)
    ceph tell osd.$primary config set osd_debug_oob_delay_done_ms 8000 || return 1

    rm -f $dir/go
    timeout 120 ceph_test_osd_oob_read put rpool $SIZE --then-write \
        --wait-file $dir/go obj > $dir/put.out &
    local pid=$!
    wait_for_hold $pgid || return 1
    # the PG moves a copy from replica to spare, which it backfills
    ceph osd pg-upmap-items $pgid $replica $spare || return 1
    touch $dir/go
    wait $pid || return 1
    cat $dir/put.out
    local out="$(cat $dir/put.out)"
    expect_written "$out" obj || return 1
    test "$(op_field "$out" write obj r)" = 0 || return 1
    test "$(op_field "$out" read obj data)" = ok || return 1
    wait_for_clean || return 1
    ceph pg map $pgid -f json | jq -e ".acting | index($spare)" || return 1
    # the new copy got both writes
    pg_deep_scrub $pgid || return 1
    test "$(rados list-inconsistent-obj $pgid | jq '.inconsistents | length')" = 0 || return 1
    # the backfill pushed the object while the write waited
    local log=$dir/osd.$primary.log
    local pushed=$(grep -n -m1 "prep_backfill_object_push .*:::obj:head" $log | cut -d: -f1)
    local kicked=$(grep -n -m1 "oob_pull_kick .*:::obj:head .* requeuing it" $log | cut -d: -f1)
    test -n "$pushed" -a -n "$kicked" || return 1
    test $pushed -lt $kicked || return 1
    no_osd_failed $dir 0 1 2 3 || return 1
}

##
# (e) The PG peers again while the pull of W1 is held: the interval
# change requeues the write, which waits for the same pulls again, and
# pulls nothing more. The client may resend it on the new interval; the
# resend waits behind it, and finds it done. One entry in the PG's log,
# and the payload goes back to the throttle.
#
function TEST_repeer_during_pull() {
    local dir=$1

    run_oob_cluster $dir 4 --osd_debug_op_order=true || return 1
    create_repl_pool rpool || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj || return 1
    local primary=$(get_primary rpool obj)
    local pgid=$(get_pg rpool obj)
    local log=$dir/osd.$primary.log
    # held until the requeued write waits for the same pulls again
    ceph tell osd.$primary config set osd_debug_oob_delay_done_ms 25000 || return 1
    local pulls=$(ofi_stat $primary pulls_started)
    local entries=$(log_entries $pgid obj)

    timeout 120 ceph_test_osd_oob_read put rpool $SIZE obj > $dir/put.out &
    local pid=$!
    wait_for_hold $pgid || return 1
    ceph pg repeer $pgid || return 1
    wait_for_log $log "release_oob_pull_holds .*:::obj:head: requeuing" || return 1
    wait_for_log $log "maybe_wait_for_oob_pull .*:::obj:head .* waits for its pulls" || return 1
    osd_config_set $primary osd_debug_oob_delay_done_ms 0 || return 1
    wait $pid || return 1
    cat $dir/put.out
    expect_written "$(cat $dir/put.out)" obj || return 1
    wait_for_clean || return 1
    grep -q "release_oob_pull_holds .*:::obj:head: requeuing" $log || return 1
    grep -q "maybe_wait_for_oob_pull .*:::obj:head .* waits for its pulls" $log || return 1
    test $(ofi_stat $primary pulls_started) = $((pulls + 1)) || return 1
    test $(log_entries $pgid obj) = $((entries + 1)) || return 1
    test $(client_bytes $primary) = 0 || return 1
    no_osd_failed $dir 0 1 2 || return 1
}

##
# (f) The primary is marked down while the pull of W1 is held: its PG
# leaves the primary role, drops the write and the hold, and abandons
# the pulls. The client resends the write to the next primary, which
# pulls it, and the write goes in once. The old primary comes back, and
# its PG asserts no hold is left (on_flushed()).
#
function TEST_primary_change_during_pull() {
    local dir=$1

    run_oob_cluster $dir 4 --osd_debug_op_order=true || return 1
    create_repl_pool rpool || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj || return 1
    local primary=$(get_primary rpool obj)
    local pgid=$(get_pg rpool obj)
    # long enough that the map that marks the primary down reaches it
    # while the write still waits: the command and the map can take
    # several seconds
    ceph tell osd.$primary config set osd_debug_oob_delay_done_ms 10000 || return 1
    local entries=$(log_entries $pgid obj)

    timeout 120 ceph_test_osd_oob_read put rpool $SIZE obj > $dir/put.out &
    local pid=$!
    wait_for_hold $pgid || return 1
    ceph osd down osd.$primary || return 1
    wait $pid || return 1
    cat $dir/put.out
    expect_written "$(cat $dir/put.out)" obj || return 1
    wait_for_clean || return 1
    local log=$dir/osd.$primary.log
    grep -q "wrongly marked me down" $log || return 1
    grep -q "release_oob_pull_holds .*:::obj:head: dropping 1 ops" $log || return 1
    # its pulls settled, and kicked nothing, unless a resend the old
    # primary served once it was back waited for them there; every hold
    # a kick ended there was a resend's
    sleep 4
    if ! grep -q "waits for the pulls of an earlier attempt" $log ; then
        grep -m1 "oob_pull: pulls of obj settled" $log | grep -q "settled$" || return 1
    fi
    ! grep "oob_pull_kick .* requeuing it" $log | grep -v -q "RETRY=" || return 1
    test $(log_entries $(get_pg rpool obj) obj) = $((entries + 1)) || return 1
    no_osd_failed $dir 0 1 2 || return 1
}

##
# (g) The pool's PG splits while the pull of W1 is held: the parent PG
# requeues the write, whose request predates the split, so the PG drops
# it, and the client sends it again to the PG of the object, on the same
# OSD. There the resend waits for the pulls of the dropped first
# attempt to settle before it pulls the window itself, and the write
# goes in once. The resend pulls only within the pool's lease of its
# receipt, so the test holds the pull of W1 until the resend waits for
# it, and then lets it go.
#
function TEST_split_during_pull() {
    local dir=$1

    run_oob_cluster $dir 4 --osd_debug_op_order=true || return 1
    only_primary 0 || return 1
    create_repl_pool rpool || return 1
    ceph osd pool set rpool pg_autoscale_mode off || return 1
    ceph osd pool set rpool rdma_delivery_lease 6 || return 1
    ceph osd pool set rpool rdma_delivery_drain 4 || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj || return 1
    local primary=$(get_primary rpool obj)
    test $primary = 0 || return 1
    local pgid=$(get_pg rpool obj)
    local log=$dir/osd.$primary.log
    ceph tell osd.$primary config set osd_debug_oob_delay_done_ms 25000 || return 1
    local entries=$(log_entries $pgid obj)

    timeout 120 ceph_test_osd_oob_read put rpool $SIZE obj > $dir/put.out &
    local pid=$!
    wait_for_hold $pgid || return 1
    ceph osd pool set rpool pg_num 2 || return 1
    wait_for_log $log "maybe_wait_for_oob_pull .*:::obj:head .* waits for the pulls of an earlier attempt" || return 1
    osd_config_set $primary osd_debug_oob_delay_done_ms 0 || return 1
    wait $pid || return 1
    cat $dir/put.out
    expect_written "$(cat $dir/put.out)" obj || return 1
    wait_for_clean || return 1
    test "$(ceph osd pool get rpool pg_num -f json | jq .pg_num)" = 2 || return 1
    grep -q "release_oob_pull_holds .*:::obj:head: requeuing" $log || return 1
    # the resend pulled the window only once the pulls of the dropped
    # first attempt settled: it waited for them
    local newpg=$(get_pg rpool obj)
    test $(get_primary rpool obj) = $primary || return 1
    grep -q "maybe_wait_for_oob_pull .*:::obj:head .* waits for the pulls of an earlier attempt" $log || return 1
    local first=$(grep -n -m1 "oob_pull: pulls of obj settled" $log | cut -d: -f1)
    local again=$(grep -n "oob_pull: pulls of obj handed off" $log | sed -n 2p | cut -d: -f1)
    test -n "$first" -a -n "$again" || return 1
    test $first -lt $again || return 1
    test $(log_entries $newpg obj) = $((entries + 1)) || return 1
    no_osd_failed $dir 0 1 2 || return 1
}

##
# (g2) As (g), but the split comes while the job of W1 still waits to
# start its pull: the parent PG drops the request, which goes away and
# abandons the pull, which then never starts. The resend waits, in the
# child PG, for the first attempt's job to settle, and only then pulls
# the window. The test holds the job of W1 until the resend waits for
# it. The job then runs and pulls nothing, or, past the pool's lease
# since W1 arrived, expires: either way it starts no pull.
#
function TEST_split_before_pull_starts() {
    local dir=$1

    run_oob_cluster $dir 4 --osd_debug_op_order=true || return 1
    only_primary 0 || return 1
    create_repl_pool rpool || return 1
    ceph osd pool set rpool pg_autoscale_mode off || return 1
    ceph osd pool set rpool rdma_delivery_lease 6 || return 1
    ceph osd pool set rpool rdma_delivery_drain 4 || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj || return 1
    local primary=$(get_primary rpool obj)
    test $primary = 0 || return 1
    local pgid=$(get_pg rpool obj)
    local log=$dir/osd.$primary.log
    ceph tell osd.$primary config set osd_debug_oob_delay_post_ms 25000 || return 1
    local entries=$(log_entries $pgid obj)
    local pulls=$(ofi_stat $primary pulls_started)

    timeout 120 ceph_test_osd_oob_read put rpool $SIZE obj > $dir/put.out &
    local pid=$!
    wait_for_hold $pgid || return 1
    ceph osd pool set rpool pg_num 2 || return 1
    # W1's job waits until the resend waits for it; the resend's job, whose
    # bounds count from its own receipt, does not wait
    wait_for_log $log "maybe_wait_for_oob_pull .*:::obj:head .* waits for the pulls of an earlier attempt" || return 1
    osd_config_set $primary osd_debug_oob_delay_post_ms 0 || return 1
    wait $pid || return 1
    cat $dir/put.out
    expect_written "$(cat $dir/put.out)" obj || return 1
    wait_for_clean || return 1
    # the parent dropped the request before its pull started, and the
    # pull never did: only the resend pulled
    grep -q "oob pull: .* dropped, its pulls that did not start never will" $log || return 1
    grep -q "oob_pull: op 0 of obj: the request was dropped, not pulling\|oob_pull: pulls of obj not started" $log || return 1
    test $(ofi_stat $primary pulls_started) = $((pulls + 1)) || return 1
    # the resend, in the object's PG on this OSD, waited for that job
    # to settle
    local newpg=$(get_pg rpool obj)
    test $(get_primary rpool obj) = $primary || return 1
    grep -q "maybe_wait_for_oob_pull .*:::obj:head .* waits for the pulls of an earlier attempt" $log || return 1
    local first=$(grep -n -m1 "oob_pull: pulls of obj settled" $log | cut -d: -f1)
    local again=$(grep -n "oob_pull: pulls of obj handed off" $log | sed -n 2p | cut -d: -f1)
    test -n "$first" -a -n "$again" || return 1
    test $first -lt $again || return 1
    test $(log_entries $newpg obj) = $((entries + 1)) || return 1
    no_osd_failed $dir 0 1 2 || return 1
}

##
# (h) The client is blocklisted while the pull of W1 is held. The PG
# answers the write -EBLOCKLISTED, as it answers any op of a
# blocklisted client, but only once the pull settled: until then the
# client may not reuse its window, and nothing answers the write.
#
function TEST_blocklist_during_pull() {
    local dir=$1

    run_oob_cluster $dir 4 || return 1
    create_repl_pool rpool || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj || return 1
    local primary=$(get_primary rpool obj)
    local pgid=$(get_pg rpool obj)
    ceph tell osd.$primary config set osd_debug_oob_delay_done_ms 6000 || return 1

    timeout 120 ceph_test_osd_oob_read put rpool $SIZE obj > $dir/put.out &
    local pid=$!
    wait_for_hold $pgid || return 1
    local addr=$(grep "^client " $dir/put.out | cut -d' ' -f2)
    test -n "$addr" || return 1
    ceph osd blocklist add $addr || return 1
    # the primary knows of it, and the write still waits
    local epoch=$(ceph osd dump -f json | jq .epoch)
    for i in $(seq 50) ; do
        test $(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$primary) \
            status | jq -r .newest_map) -ge $epoch && break
        sleep 0.1
    done
    test $(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$primary) \
        status | jq -r .newest_map) -ge $epoch || return 1
    test "$(holds $pgid | jq length)" = 1 || return 1
    wait $pid
    cat $dir/put.out
    test "$(op_field "$(cat $dir/put.out)" put obj r)" = -108 || return 1
    # answered after the settle, never before
    local log=$dir/osd.$primary.log
    local settled=$(grep -n -m1 "oob_pull: pulls of obj settled" $log | cut -d: -f1)
    local answered=$(grep -n -m1 "do_op .* is blocklisted" $log | cut -d: -f1)
    test -n "$settled" -a -n "$answered" || return 1
    test $answered -gt $settled || return 1
    ceph osd blocklist rm $addr || return 1
    no_osd_failed $dir 0 1 2 || return 1
}

##
# (i) The client's session to the primary resets while the pull of W1
# is held (its connections close when idle for 2 s), and it sends the
# write again on a new one. The resend waits behind W1, which the
# primary then drops, since nothing can answer it on the old session;
# the resend pulls the payload itself, within the pool's lease of 6 s
# since it arrived, and the write goes in once.
#
function TEST_session_reset_during_pull() {
    local dir=$1

    run_oob_cluster $dir 4 --osd_debug_op_order=true || return 1
    create_repl_pool rpool || return 1
    ceph osd pool set rpool rdma_delivery_lease 6 || return 1
    ceph osd pool set rpool rdma_delivery_drain 4 || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj || return 1
    local primary=$(get_primary rpool obj)
    local pgid=$(get_pg rpool obj)
    ceph tell osd.$primary config set osd_debug_oob_delay_done_ms 6000 || return 1
    local entries=$(log_entries $pgid obj)
    local pulls=$(ofi_stat $primary pulls_started)

    CEPH_ARGS="$CEPH_ARGS --ms_connection_idle_timeout=2" timeout 120 \
        ceph_test_osd_oob_read put rpool $SIZE obj > $dir/put.out &
    local pid=$!
    wait_for_hold $pgid || return 1
    wait_for_hold $pgid 1 || return 1
    # W1's job sleeps already; the resend's pull does not
    ceph tell osd.$primary config set osd_debug_oob_delay_done_ms 0 || return 1
    wait $pid || return 1
    cat $dir/put.out
    expect_written "$(cat $dir/put.out)" obj || return 1
    local log=$dir/osd.$primary.log
    grep -q "ms_handle_reset con .* session .*" $log || return 1
    # behind W1, whose pulls the resend waits for too
    grep -q "maybe_wait_for_oob_pull .*:::obj:head .* waits \(for the pulls of an earlier attempt, \)\?behind" $log || return 1
    test $(log_entries $pgid obj) = $((entries + 1)) || return 1
    test $(ofi_stat $primary pulls_started) -ge $((pulls + 2)) || return 1
    test $(client_bytes $primary) = 0 || return 1
    no_osd_failed $dir 0 1 2 || return 1
}

##
# (i2) A resend of a pulled write that the primary answers as a dup
# reports what the attempt that did the write pulled. W1 is pulled, and
# its commit waits for a replica that is stopped. Meanwhile the client's
# connection closes when idle for 2 s, and it sends W1 again on a new
# one. The resend finds W1 in the PG's log, and waits for it to commit;
# then the replica goes on, and the primary answers the resend with the
# byte count and the CRC-64/NVME of W1's pull, which the log kept with
# W1, not declined with no bytes. Nothing is pulled again. The log keeps
# the record of the pull whether or not the client asks for the ops'
# return data, which it does for a request that pulls only when its
# other ops return nothing: with **args** --call, a CALL ahead of the
# write returns data, and the client does not ask for it.
#
function dup_of_pulled_write_reports_the_pull() {
    local dir=$1
    shift

    run_oob_cluster $dir 4 --osd_debug_op_order=true || return 1
    create_repl_pool rpool || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj || return 1
    local primary=$(get_primary rpool obj)
    local replica=$(get_not_primary rpool obj)
    local pgid=$(get_pg rpool obj)
    local entries=$(log_entries $pgid obj)
    local pulls=$(ofi_stat $primary pulls_started)
    local log=$dir/osd.$primary.log

    kill -STOP $(cat $dir/osd.$replica.pid) || return 1
    CEPH_ARGS="$CEPH_ARGS --ms_connection_idle_timeout=2" timeout 120 \
        ceph_test_osd_oob_read put rpool $SIZE "$@" obj > $dir/put.out &
    local pid=$!
    # well within the heartbeat grace of the stopped replica
    local waits=false
    for i in $(seq 50) ; do
        if grep -q "do_op_impl dup .* version" $log &&
                grep -q " waiting for .* to commit" $log ; then
            waits=true
            break
        fi
        sleep 0.2
    done
    kill -CONT $(cat $dir/osd.$replica.pid) || return 1
    $waits || return 1
    wait $pid || return 1
    cat $dir/put.out
    expect_pulled "$(cat $dir/put.out)" obj || return 1
    grep -q "ms_handle_reset con .* session .*" $log || return 1
    test $(log_entries $pgid obj) = $((entries + 1)) || return 1
    test $(ofi_stat $primary pulls_started) = $((pulls + 1)) || return 1
    test $(client_bytes $primary) = 0 || return 1
    wait_for_clean || return 1
    no_osd_failed $dir 0 1 2 || return 1
}

function TEST_dup_of_pulled_write_reports_the_pull() {
    dup_of_pulled_write_reports_the_pull $1 || return 1
}

function TEST_dup_of_pulled_write_with_call_reports_the_pull() {
    dup_of_pulled_write_reports_the_pull $1 --call || return 1
}

##
# (i3) A pulled write that waited in its PG past the pool's lease after
# it arrived pulls nothing: its bounds count from its receipt, as a
# read's do, so that a client that gave up on it knows when it may reuse
# its window. With a lease of 1 s and a drain of 0.5 s, the pull of W1
# is held for 3 s; W2, a pulled write of the same object, waits behind
# W1 meanwhile, then fails with EOPNOTSUPP, its descriptor declined:
# nothing read its window, and a client that still waits can send the
# payload inline. W1 goes in.
#
function TEST_pulled_write_past_lease_starts_nothing() {
    local dir=$1

    run_oob_cluster $dir 4 --osd_debug_op_order=true || return 1
    create_repl_pool rpool || return 1
    ceph osd pool set rpool rdma_delivery_lease 1 || return 1
    ceph osd pool set rpool rdma_delivery_drain 0.5 || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj || return 1
    local primary=$(get_primary rpool obj)
    local pgid=$(get_pg rpool obj)
    ceph tell osd.$primary config set osd_debug_oob_delay_done_ms 3000 || return 1
    local entries=$(log_entries $pgid obj)
    local pulls=$(ofi_stat $primary pulls_started)

    # the client exits 1, since W2 failed
    local out
    out=$(ceph_test_osd_oob_read put rpool $SIZE obj obj)
    echo "$out"
    test $(echo "$out" | grep -c "^put obj: r=0 bytes=$SIZE .* crc=ok ") = 1 || return 1
    test $(echo "$out" | grep -c "^put obj: r=-95 bytes=0 flags=0x$FLAG_DECLINED ") = 1 || return 1
    test $(echo "$out" | grep -c "^check obj: data=ok") = 2 || return 1
    local log=$dir/osd.$primary.log
    grep -q "maybe_wait_for_oob_pull .*:::obj:head .* waits behind" $log || return 1
    grep -q "oob_pull: obj: received .* past the lease of 1s: pulling nothing" $log || return 1
    test $(ofi_stat $primary pulls_started) = $((pulls + 1)) || return 1
    test $(log_entries $pgid obj) = $((entries + 1)) || return 1
    test $(client_bytes $primary) = 0 || return 1
    no_osd_failed $dir 0 1 2 || return 1
}

##
# (j) A request with a CALL and a pulled write to an erasure-coded pool
# with EC optimizations runs in a coroutine: its pull runs there, on the
# op thread, and the request does not park.
#
function TEST_coroutine_pull_runs_inline() {
    local dir=$1

    run_oob_cluster $dir 4 || return 1
    create_ec_pool_k2m1 ecpool || return 1
    ceph_test_osd_oob_read write ecpool $SIZE obj || return 1
    local primary=$(get_primary ecpool obj)
    local pulled=$(xfer_stat $primary pull_lat.avgcount)
    local queued=$(xfer_stat $primary queue_lat.avgcount)

    local out
    out=$(ceph_test_osd_oob_read put ecpool $SIZE --call obj) || return 1
    echo "$out"
    expect_pulled "$out" obj || return 1
    local log=$dir/osd.$primary.log
    grep -q "spawning a coroutine for EC optimized CALL op" $log || return 1
    ! grep -q "park_for_oob_pull\|waits for its pulls" $log || return 1
    test $(xfer_stat $primary pull_lat.avgcount) = $((pulled + 1)) || return 1
    test $(xfer_stat $primary queue_lat.avgcount) = $queued || return 1

    # and without the CALL, the same write parks
    out=$(ceph_test_osd_oob_read put ecpool $SIZE obj) || return 1
    echo "$out"
    expect_pulled "$out" obj || return 1
    grep -q "park_for_oob_pull .*:::obj:head" $log || return 1
    no_osd_failed $dir 0 1 2 || return 1
}

##
# (i4) A pulled write that fails whatever its payload fails before its
# pull: past osd_max_object_size, it fails with EFBIG, its descriptor
# declined, and nothing reads its window. As for the write inline, the
# PG log keeps the error, for a resend to find.
#
function TEST_pulled_write_bound_to_fail_pulls_nothing() {
    local dir=$1

    run_oob_cluster $dir 4 || return 1
    create_repl_pool rpool || return 1
    local primary=$(get_primary rpool obj)
    local pgid=$(get_pg rpool obj)
    local entries=$(log_entries $pgid obj)
    local pulls=$(ofi_stat $primary pulls_started)
    ceph tell osd.$primary config set osd_max_object_size $((SIZE / 2)) || return 1

    # the client exits 1, since the write failed
    local out
    out=$(ceph_test_osd_oob_read put rpool $SIZE obj)
    echo "$out"
    test "$(op_field "$out" put obj r)" = -27 || return 1
    test "$(op_field "$out" put obj flags)" = 0x$FLAG_DECLINED || return 1
    test $(ofi_stat $primary pulls_started) = $pulls || return 1
    grep -q "do_op_impl .*:::obj:head .* would fail with .* in op 0, pulling nothing" \
        $dir/osd.$primary.log || return 1
    test $(log_entries $pgid obj) = $((entries + 1)) || return 1
    test $(client_bytes $primary) = 0 || return 1
    no_osd_failed $dir 0 1 2 || return 1
}

##
# (j2) A pulled write to a pool whose cache tier proxies its writes is
# refused with EOPNOTSUPP, its descriptor declined: the cache tier would
# forward the write without its payload, which the base pool fails for
# its missing data. Nothing pulls the window, and nothing is written.
#
function TEST_pulled_write_to_proxy_tier_refused() {
    local dir=$1

    run_oob_cluster $dir 4 || return 1
    create_repl_pool base || return 1
    create_repl_pool cache || return 1
    ceph osd tier add base cache || return 1
    ceph osd tier cache-mode cache proxy || return 1
    ceph osd tier set-overlay base cache || return 1
    local id pulls=0
    for id in 0 1 2 ; do
        pulls=$((pulls + $(ofi_stat $id pulls_started)))
    done

    # the client exits 1, since the write failed
    local out
    out=$(ceph_test_osd_oob_read put base $SIZE obj)
    echo "$out"
    test "$(op_field "$out" put obj r)" = -95 || return 1
    test "$(op_field "$out" put obj flags)" = 0x$FLAG_DECLINED || return 1
    grep -q "do_proxy_write .*:::obj:head .*: not proxying a pulled write" \
        $dir/osd.*.log || return 1
    local after=0
    for id in 0 1 2 ; do
        after=$((after + $(ofi_stat $id pulls_started)))
    done
    test $after = $pulls || return 1
    ! rados -p base stat obj || return 1
    ceph osd tier remove-overlay base || return 1
    ! rados -p base stat obj || return 1
    no_osd_failed $dir 0 1 2 || return 1
}

##
# (k) A slow stop of the primary while the pull of W1 is held: its PG
# drops the write and abandons the pulls, the stop waits for the running
# pull, at most until its land_by, and nothing kicks the gone PG. The
# client sends the write again to the next primary, and it goes in.
#
function TEST_slow_stop_during_pull() {
    local dir=$1

    run_oob_cluster $dir 4 --osd_fast_shutdown=false || return 1
    create_repl_pool rpool || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj || return 1
    local primary=$(get_primary rpool obj)
    local pgid=$(get_pg rpool obj)
    # long enough that the stop reaches the transfer threads before the
    # held pull ends
    ceph tell osd.$primary config set osd_debug_oob_delay_done_ms 6000 || return 1

    timeout 120 ceph_test_osd_oob_read put rpool $SIZE obj > $dir/put.out &
    local pid=$!
    wait_for_hold $pgid || return 1
    kill_daemons $dir TERM osd.$primary || return 1
    wait $pid || return 1
    cat $dir/put.out
    expect_written "$(cat $dir/put.out)" obj || return 1

    local log=$dir/osd.$primary.log
    grep -q "release_oob_pull_holds .*:::obj:head: dropping 1 ops" $log || return 1
    grep -q "stopping: shedding 0 queued jobs, waiting for 1 running" $log || return 1
    grep -q "oob_pull: pulls of obj settled$" $log || return 1
    grep -q "oob transfer threads stopped" $log || return 1
    ! grep -q "queue_oob_pull_kick\|FAILED ceph_assert\|Caught signal" $log || return 1
    # no callback kept a PG past the stop of the op threads
    ! grep -q "has ref count of" $log || return 1
}

main osd-oob-async "$@"

# Local Variables:
# compile-command: "cd ../../../build ; ninja ceph-osd ceph_test_osd_oob_read && ../qa/run-standalone.sh osd-oob-async.sh"
# End:
