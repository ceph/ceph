#!/usr/bin/env bash
#
# Reducing a pool to one zone: ceph osd pool set <pool> num_zones 1 needs
# --yes-i-really-mean-it for an erasure coded pool, as ceph mon
# disable_stretch_mode always does. The PGs that lose their OSDs wait for
# them and recover once they are back, or once num_zones is 2 again.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7481" # git grep '\<7481\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7482" # git grep '\<7482\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7483" # git grep '\<7483\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests "$@"
}

DOC='"Reducing the Number of Zones" in the Stretch Clusters documentation'

# write count objects named prefix<i> to pool, keeping what was written
function write_objects() {
    local dir=$1 pool=$2 prefix=$3 count=$4 i
    for i in $(seq 0 $((count - 1))); do
        dd if=/dev/urandom of=$dir/$prefix$i bs=4k count=$((1 + i % 8)) status=none
        rados -p $pool put $prefix$i $dir/$prefix$i || return 1
    done
}

function check_objects() {
    local dir=$1 pool=$2 prefix=$3 count=$4 i
    for i in $(seq 0 $((count - 1))); do
        timeout 120 rados -p $pool get $prefix$i $dir/got || return 1
        cmp $dir/got $dir/$prefix$i || return 1
    done
}

# the number of PGs of pool that are not active
function inactive_pgs() {
    local pool=$1
    flush_pg_stats > /dev/null || return 1
    ceph pg ls-by-pool $pool -f json |
        jq '[.pg_stats[] | select(.state | startswith("active") | not)] | length'
}

# the number of PGs of pool that are not active, once none is peering
function settled_inactive_pgs() {
    local pool=$1 i
    for i in $(seq 1 60); do
        flush_pg_stats > /dev/null || return 1
        ceph pg ls-by-pool $pool -f json | jq -e '[.pg_stats[] |
            select(.state | test("peering|activating|unknown"))] | length == 0' \
            > /dev/null && break
        sleep 2
    done
    inactive_pgs $pool
}

# no PG of pool inactive
function wait_for_active() {
    local pool=$1 i
    for i in $(seq 1 150); do
        test "$(inactive_pgs $pool)" = 0 && return 0
        sleep 2
    done
    ceph pg ls-by-pool $pool
    return 1
}

# the PGs of the erasure coded pool whose acting set has fewer than 2 of its
# first 3 shards (k=2, m=1) on OSDs, as it is before num_zones 1
function ec_pgs_short_of_first_zone() {
    local pool=$1
    flush_pg_stats > /dev/null || return 1
    ceph pg ls-by-pool $pool -f json | jq '[.pg_stats[] |
        select([.acting[0:3][] | select(. != 2147483647)] | length < 2)] | length'
}

# kill dc2's monitor and OSDs; the clients stop trying its monitor
function kill_dc2() {
    local dir=$1 osd
    kill_daemons $dir KILL mon.b || return 1
    for osd in 3 4 5; do
        kill_daemons $dir KILL osd.$osd || return 1
    done
    CEPH_ARGS=${CEPH_ARGS/--mon-host=$CEPH_MON /--mon-host=$CEPH_MON_A,$CEPH_MON_C }
    ceph osd down osd.3 osd.4 osd.5 || return 1
}

function restore_dc2() {
    local dir=$1 osd
    activate_mon $dir b --public-addr $CEPH_MON_B || return 1
    CEPH_ARGS=${CEPH_ARGS/--mon-host=$CEPH_MON_A,$CEPH_MON_C /--mon-host=$CEPH_MON }
    wait_for_quorum 300 3 || return 1
    for osd in 3 4 5; do
        activate_osd $dir $osd || return 1
    done
}

# data0, k=2 m=1 over dc1 (osd.0-2) and dc2 (osd.3-5), written
function ec_pool_with_data() {
    local dir=$1
    two_zone_cluster $dir || return 1
    ceph osd pool create data0 erasure --num_zones 2 --k 2 --m 1 --pg_num 16 || return 1
    ceph osd pool application enable data0 rados || return 1
    wait_for_clean || return 1
    write_objects $dir data0 old 20 || return 1
}

# as ec_pool_with_data, then dc2 lost and objects rewritten in degraded
# stretch mode
function degraded_ec_pool_with_data() {
    local dir=$1
    ec_pool_with_data $dir || return 1
    kill_dc2 $dir || return 1
    wait_for_stretch_state 1 0 || return 1
    wait_for_active data0 || return 1
    write_objects $dir data0 new 20 || return 1
}

# The confirmation and where it points; a healthy pool goes to one zone with
# no PG unavailable
function TEST_needs_confirmation() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    ec_pool_with_data $dir || return 1
    ! ceph osd pool set data0 num_zones 1 > $dir/out 2>&1 || return 1
    cat $dir/out
    grep -q -- "--yes-i-really-mean-it to proceed anyway" $dir/out || return 1
    grep -qF "$DOC" $dir/out || return 1
    grep -q "Pool migration" $dir/out || return 1
    test "$(pool_field data0 num_zones)" = 2 || return 1
    ! ceph osd crush rule ls | grep -qx data0-single-zone || return 1
    ! ceph mon disable_stretch_mode > $dir/out 2>&1 || return 1
    grep -qF "$DOC" $dir/out || return 1

    ceph osd pool set data0 num_zones 1 --yes-i-really-mean-it || return 1
    test "$(pool_field data0 num_zones)" = 1 || return 1
    test "$(pool_field data0 size)" = 3 || return 1
    check_objects $dir data0 old 20 || return 1
    wait_for_clean || return 1
    check_objects $dir data0 old 20 || return 1
}

# dc2 lost: the PGs whose first zone is dc2 keep only shards on its OSDs,
# so they are unavailable, and maybe PGs that the new rule maps only to dc2.
# A PG whose log still holds all its writes can recover onto new OSDs at
# once.
function ec_zone_lost() {
    local dir=$1
    test "$(ec_pgs_short_of_first_zone data0)" -gt 0 || return 1
    ceph osd pool set data0 num_zones 1 --yes-i-really-mean-it || return 1
    test "$(pool_field data0 num_zones)" = 1 || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1
    test "$(settled_inactive_pgs data0)" -gt 0 || return 1
}

function all_osds_up() {
    test "$(ceph osd dump -f json | jq '[.osds[] | select(.up == 0)] | length')" = 0
}

# the PGs left unavailable recover when dc2 is back, with everything written
# meanwhile, which dc2's shards get from their logs
function TEST_ec_zone_lost() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    degraded_ec_pool_with_data $dir || return 1
    ec_zone_lost $dir || return 1
    restore_dc2 $dir || return 1
    wait_for_clean || return 1
    check_objects $dir data0 old 20 || return 1
    check_objects $dir data0 new 20 || return 1
}

# With PG logs of 2 to 4 entries, more is written while dc2 is lost than the
# logs hold, so dc2's shards need backfill when it is back. A PG that is not
# active cannot backfill: those whose first zone is dc2 stay unavailable with
# every OSD up, until num_zones 2 makes dc1's shards part of them again.
function TEST_ec_zone_lost_backfill() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    CEPH_ARGS+="--osd_min_pg_log_entries=2 --osd_max_pg_log_entries=4 "
    CEPH_ARGS+="--osd_pg_log_trim_min=1 "
    degraded_ec_pool_with_data $dir || return 1
    write_objects $dir data0 more 160 || return 1
    ec_zone_lost $dir || return 1
    restore_dc2 $dir || return 1
    all_osds_up || return 1
    test "$(settled_inactive_pgs data0)" -gt 0 || return 1

    ceph osd pool set data0 num_zones 2 || return 1
    wait_for_clean || return 1
    check_objects $dir data0 old 20 || return 1
    check_objects $dir data0 new 20 || return 1
    check_objects $dir data0 more 160 || return 1
    # clean, so one zone leaves no PG unavailable
    ceph osd pool set data0 num_zones 1 --yes-i-really-mean-it || return 1
    wait_for_clean || return 1
    check_objects $dir data0 more 160 || return 1
}

# Recovery stretch mode with dc2's shards waiting for backfill: no OSD is
# down, but the PGs whose first zone is dc2 have no complete shards to keep,
# so they stay unavailable until num_zones is 2 again.
function TEST_ec_shrink_while_backfilling() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    CEPH_ARGS+="--osd_min_pg_log_entries=2 --osd_max_pg_log_entries=4 "
    CEPH_ARGS+="--osd_pg_log_trim_min=1 "
    degraded_ec_pool_with_data $dir || return 1
    write_objects $dir data0 more 160 || return 1
    ceph osd set nobackfill || return 1
    ceph osd set norecover || return 1
    restore_dc2 $dir || return 1
    wait_for_stretch_state 1 1 || return 1
    all_osds_up || return 1
    ceph osd pool set data0 num_zones 1 --yes-i-really-mean-it || return 1
    ceph osd unset nobackfill || return 1
    ceph osd unset norecover || return 1
    test "$(settled_inactive_pgs data0)" -gt 0 || return 1

    ceph osd pool set data0 num_zones 2 || return 1
    wait_for_clean || return 1
    check_objects $dir data0 old 20 || return 1
    check_objects $dir data0 new 20 || return 1
    check_objects $dir data0 more 160 || return 1
}

# dc2 does not come back: num_zones 2 makes dc1's shards part of the PGs
# again, so they are available in degraded stretch mode
function TEST_ec_zone_lost_two_zones_again() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    degraded_ec_pool_with_data $dir || return 1
    ec_zone_lost $dir || return 1
    ceph osd pool set data0 num_zones 2 || return 1
    wait_for_stretch_state 1 0 || return 1
    wait_for_active data0 || return 1
    check_objects $dir data0 old 20 || return 1
    check_objects $dir data0 new 20 || return 1
    restore_dc2 $dir || return 1
    wait_for_clean || return 1
    check_objects $dir data0 new 20 || return 1
}

# dc2 lost with its OSDs marked out: no rule maps a PG to them, so only the
# PGs whose first zone is dc2 are unavailable, until dc2 is back
function TEST_ec_zone_lost_and_out() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    degraded_ec_pool_with_data $dir || return 1
    ceph osd out osd.3 osd.4 osd.5 || return 1
    wait_for_active data0 || return 1
    local short=$(ec_pgs_short_of_first_zone data0)
    ceph osd pool set data0 num_zones 1 --yes-i-really-mean-it || return 1
    test "$(settled_inactive_pgs data0)" -le $short || return 1
    ceph osd in osd.3 osd.4 osd.5 || return 1
    restore_dc2 $dir || return 1
    wait_for_clean || return 1
    check_objects $dir data0 old 20 || return 1
    check_objects $dir data0 new 20 || return 1
}

# A replicated pool keeps any copy, so it goes to one zone with no
# confirmation, and a rule over dc1 leaves every PG available. A rule that
# takes dc2 maps them all to OSDs that are down; with dc2 back they recover
# from dc1's copies.
function TEST_rep_zone_lost() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    two_zone_cluster $dir || return 1
    ceph osd pool create rep0 --num_zones 2 --replica 2 --pg_num 16 || return 1
    ceph osd pool application enable rep0 rados || return 1
    ceph osd crush rule create-replicated rep-dc1 dc1 host || return 1
    ceph osd crush rule create-replicated rep-dc2 dc2 host || return 1
    wait_for_clean || return 1
    write_objects $dir rep0 old 20 || return 1
    kill_dc2 $dir || return 1
    wait_for_stretch_state 1 0 || return 1
    wait_for_active rep0 || return 1
    write_objects $dir rep0 new 20 || return 1

    ceph osd pool set rep0 num_zones 1 --replica 3 --crush_rule rep-dc1 || return 1
    wait_for_active rep0 || return 1
    check_objects $dir rep0 new 20 || return 1
    # no PG has an up OSD to be its primary, so none reports its state:
    # clients wait
    ceph osd pool set rep0 crush_rule rep-dc2 || return 1
    ! timeout 30 rados -p rep0 get new0 $dir/got || return 1
    restore_dc2 $dir || return 1
    wait_for_clean || return 1
    check_objects $dir rep0 old 20 || return 1
    check_objects $dir rep0 new 20 || return 1
}

# Two of dc1's three OSDs down with noout: dc1 is not lost, and has fewer
# than min_size (2) shards of each PG, so every PG waits. After num_zones 1
# the PGs whose first zone is dc2 go active at once, and the rest once the
# OSDs are back.
function TEST_ec_osds_down() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    ec_pool_with_data $dir || return 1
    ceph osd set noout || return 1
    kill_daemons $dir KILL osd.0 || return 1
    kill_daemons $dir KILL osd.1 || return 1
    ceph osd down osd.0 osd.1 || return 1
    test "$(settled_inactive_pgs data0)" = 16 || return 1
    local short=$(ec_pgs_short_of_first_zone data0)
    test $short -gt 0 || return 1
    test $short -lt 16 || return 1
    ceph osd pool set data0 num_zones 1 --yes-i-really-mean-it || return 1
    local inactive=$(settled_inactive_pgs data0)
    test $inactive -gt 0 || return 1
    test $inactive -le $short || return 1
    activate_osd $dir 0 || return 1
    activate_osd $dir 1 || return 1
    ceph osd unset noout || return 1
    wait_for_clean || return 1
    check_objects $dir data0 old 20 || return 1
}

# disable_stretch_mode reduces every pool over zones in the same way
function TEST_disable_stretch_mode_zone_lost() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    degraded_ec_pool_with_data $dir || return 1
    ceph mon disable_stretch_mode --yes-i-really-mean-it || return 1
    test "$(pool_field data0 num_zones)" = 1 || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1
    restore_dc2 $dir || return 1
    wait_for_clean || return 1
    check_objects $dir data0 old 20 || return 1
    check_objects $dir data0 new 20 || return 1
}

# num_zones 1 with both zones up keeps each PG's first zone, which may be the
# zone that is lost straight after: those PGs wait for it, and recover once
# it is back.
function TEST_one_zone_then_lose_and_restore() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    ec_pool_with_data $dir || return 1
    ceph osd pool set data0 num_zones 1 --yes-i-really-mean-it || return 1
    test "$(pool_field data0 num_zones)" = 1 || return 1
    kill_dc2 $dir || return 1
    settled_inactive_pgs data0 || return 1
    restore_dc2 $dir || return 1
    wait_for_clean || return 1
    check_objects $dir data0 old 20 || return 1
}

# num_zones 1 with both zones up while dc2 is behind: osd.4 missed writes and
# osd.5 is down, so a PG whose first zone is dc2 has one shard of those
# objects and recovers them with dc1's shards, of the zone the pool no longer
# has
function TEST_one_zone_recovers_from_old_copies() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    ec_pool_with_data $dir || return 1
    ceph osd set norecover || return 1
    ceph osd set nobackfill || return 1
    kill_daemons $dir KILL osd.4 || return 1
    ceph osd down osd.4 || return 1
    wait_for_active data0 || return 1
    write_objects $dir data0 new 20 || return 1
    activate_osd $dir 4 || return 1
    wait_for_active data0 || return 1
    kill_daemons $dir KILL osd.5 || return 1
    ceph osd down osd.5 || return 1
    wait_for_active data0 || return 1
    ceph osd pool set data0 num_zones 1 --yes-i-really-mean-it || return 1
    test "$(pool_field data0 num_zones)" = 1 || return 1
    ceph osd unset norecover || return 1
    ceph osd unset nobackfill || return 1
    check_objects $dir data0 new 20 || return 1
    activate_osd $dir 5 || return 1
    wait_for_clean || return 1
    check_objects $dir data0 old 20 || return 1
    check_objects $dir data0 new 20 || return 1
}

# num_zones 1 on the last pool over zones takes stretch mode out of the
# monmap. While a monmap change waits to commit, the leader waits for it
# rather than abort on proposing the monmap again.
function TEST_one_zone_while_monmap_changes() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    two_zone_cluster $dir || return 1
    ceph osd pool create data0 erasure --num_zones 2 --k 2 --m 1 || return 1
    wait_for_clean || return 1
    local profile=$(ceph osd pool get data0 erasure_code_profile -f json |
        jq -r .erasure_code_profile)
    ceph osd crush rule create-erasure data0-one-zone $profile || return 1
    test "$(ceph quorum_status -f json | jq -r .quorum_leader_name)" = a || return 1
    # mon.a, the leader, cannot commit a change while mon.b is stopped
    CEPH_ARGS=${CEPH_ARGS/--mon-host=$CEPH_MON /--mon-host=$CEPH_MON_A }
    local epoch=$(ceph mon dump -f json | jq .epoch)
    kill -STOP $(cat $dir/mon.b.pid) || return 1
    ceph mon set-weight a 1 &
    local weight=$!
    sleep 1
    test "$(ceph mon dump -f json | jq .epoch)" = $epoch || return 1
    ceph osd pool set data0 num_zones 1 --crush_rule data0-one-zone \
        --yes-i-really-mean-it &
    local one_zone=$!
    sleep 2
    kill -CONT $(cat $dir/mon.b.pid) || return 1
    wait $weight || return 1
    wait $one_zone || return 1
    test "$(pool_field data0 num_zones)" = 1 || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1
    ceph quorum_status -f json | jq -e '.quorum_names | index("a")' || return 1
}

# A command that sets a PG's pg_temp in the same update as num_zones 1 sizes
# it for the pool's new size, so the leader can build the next map. The pool
# is not the last over zones, whose change is proposed at once, and its old
# rule goes with it: the CRUSH change makes the leader prime the pg_temp of
# every PG with the next map.
function pg_temp_while_size_changes() {
    local dir=$1
    shift
    two_zone_cluster $dir || return 1
    ceph osd pool create data0 erasure --num_zones 2 --k 2 --m 1 --pg_num 4 || return 1
    ceph osd pool create rep0 --num_zones 2 --replica 2 --pg_num 4 || return 1
    local profile=$(ceph osd pool get data0 erasure_code_profile -f json |
        jq -r .erasure_code_profile)
    local old_rule=$(ceph osd pool get data0 crush_rule -f json | jq -r .crush_rule)
    ceph osd crush rule create-erasure data0-one-zone $profile || return 1
    wait_for_clean || return 1
    local leader=$(ceph quorum_status -f json | jq -r .quorum_leader_name)
    # an update straight after a commit waits paxos_propose_interval, so
    # both commands go into the same one, num_zones 1 first
    ceph tell mon.\* config set paxos_propose_interval 10 || return 1
    ceph osd set noout || return 1
    timeout 120 ceph osd pool set data0 num_zones 1 \
        --crush_rule data0-one-zone --yes-i-really-mean-it &
    local one_zone=$!
    sleep 3
    timeout 120 "$@" || return 1
    wait $one_zone || return 1
    test "$(pool_field data0 size)" = 3 || return 1
    ! ceph osd crush rule ls | grep -qx $old_rule || return 1
    ceph tell mon.\* config set paxos_propose_interval 1 || return 1
    ceph osd unset noout || return 1
    test "$(ceph quorum_status -f json | jq -r .quorum_leader_name)" = $leader || return 1
    ! grep -q "FAILED ceph_assert" $dir/mon.$leader.log || return 1
    wait_for_clean || return 1
}

function TEST_repeer_while_size_changes() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    pg_temp_while_size_changes $dir ceph pg repeer 1.0 || return 1
}

function TEST_pg_temp_while_size_changes() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    # six OSDs fit the committed size, not the pending one
    pg_temp_while_size_changes $dir \
        bash -c "ceph osd pg-temp 1.0 0 1 2 3 4 5 2>&1 |
            grep -F 'num of osds (6) > pool size (3)'" || return 1
}

main mon-stretch-reduce-num-zones "$@"
