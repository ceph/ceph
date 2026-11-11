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

UNSTRETCH_DOC='"Unstretching an Erasure-Coded Pool Without Migration" in the Stretch Clusters documentation'
DISABLE_DOC='"Disabling Stretch Mode" in the Stretch Clusters documentation'

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

# osd pool set <pool> num_zones 1 needs --yes-i-really-mean-it for an
# erasure coded pool, and so does mon disable_stretch_mode. This test uses
# data0, an erasure coded pool of k=2 m=1 with 3 shards in each of the
# zones dc1 and dc2, and writes objects to it. Without the flag, num_zones
# 1 is refused with a message that points to the documentation on
# unstretching an erasure coded pool and to pool migration, and leaves
# data0 over two zones with no new rule. mon disable_stretch_mode is
# refused with a pointer to the documentation on disabling. With the flag,
# data0 goes to one zone with 3 shards, and its objects can be read
# straight away and again once the PGs are clean.
function TEST_needs_confirmation() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    ec_pool_with_data $dir || return 1
    ! ceph osd pool set data0 num_zones 1 > $dir/out 2>&1 || return 1
    cat $dir/out
    grep -q -- "--yes-i-really-mean-it to proceed anyway" $dir/out || return 1
    grep -qF "$UNSTRETCH_DOC" $dir/out || return 1
    grep -q "Pool migration" $dir/out || return 1
    test "$(pool_field data0 num_zones)" = 2 || return 1
    ! ceph osd crush rule ls | grep -qx data0-single-zone || return 1
    ! ceph mon disable_stretch_mode > $dir/out 2>&1 || return 1
    grep -qF "$DISABLE_DOC" $dir/out || return 1

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

# osd pool set <pool> num_zones 1 keeps, for each PG of an erasure coded
# pool, the shards of its first zone: the zone that holds its first 3
# shards. This test runs it with a zone lost. data0, an erasure coded pool
# of k=2 m=1 with 3 shards in each of dc1 and dc2, has objects written
# before dc2's monitor and OSDs are killed and more written in degraded
# mode after. The command moves data0 to one zone and clears the
# cluster stretch flags, and leaves some PGs unavailable, among them those
# whose first zone is dc2, which keep only shards on dc2's OSDs. Once dc2
# is back the PGs recover and all the objects can be read.
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

# osd pool set <pool> num_zones 1 on an erasure coded pool keeps the shards
# of each PG's first zone, the zone of its first 3 shards; a PG that is not
# active cannot backfill them. data0, an erasure coded pool of k=2 m=1 with
# 3 shards in each of dc1 and dc2, has PG logs of 2 to 4 entries. dc2's
# monitor and OSDs are killed and more objects written than the logs hold,
# so dc2's shards need backfill once it is back. After num_zones 1 and
# dc2's return, with every OSD up, some PGs stay unavailable: those whose
# first zone is dc2. num_zones 2 makes dc1's shards part of them again, so
# the PGs go clean and all the objects can be read. num_zones 1 on the
# clean pool then leaves it clean with the objects readable.
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

# This test runs osd pool set <pool> num_zones 1 on an erasure coded pool
# whose shards in one zone wait for backfill. data0, an erasure coded pool
# of k=2 m=1 with 3 shards in each of dc1 and dc2, has PG logs of 2 to 4
# entries. dc2's monitor and OSDs are killed and more objects written than
# the logs hold. dc2 comes back with nobackfill and norecover set, so the
# cluster is in recovery mode with every OSD up and dc2's shards
# not backfilled. After num_zones 1 and the flags unset, some PGs stay
# unavailable: those whose first zone, the zone of their first 3 shards, is
# dc2 have no complete shards to keep. num_zones 2 makes dc1's shards part
# of them again, so the PGs go clean and all the objects can be read.
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

# After osd pool set <pool> num_zones 1 with a zone lost, num_zones 2 makes
# the pool's PGs available again without that zone. data0, an erasure coded
# pool of k=2 m=1 with 3 shards in each of dc1 and dc2, is moved to one
# zone while dc2 is lost, which leaves some PGs unavailable. num_zones 2,
# with dc2 still down, makes dc1's shards part of those PGs again: the
# cluster is in degraded mode, every PG is active, and the objects
# written before and after dc2 was lost can be read. Once dc2 is back the
# PGs go clean and the objects can still be read.
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

# When a lost zone's OSDs are marked out, no rule maps a PG to them, so
# osd pool set <pool> num_zones 1 leaves unavailable only the PGs whose
# first zone, the zone of their first 3 shards, is the lost one. data0, an
# erasure coded pool of k=2 m=1 with 3 shards in each of dc1 and dc2, has
# objects written before and after dc2's monitor and OSDs are killed, and
# osd.3-5, dc2's OSDs, are marked out. After num_zones 1, no more PGs are
# unavailable than those whose first zone is dc2. Once the OSDs are marked
# in and dc2 is back, the PGs go clean and all the objects can be read.
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

# osd pool set <pool> num_zones 1 needs no --yes-i-really-mean-it for a
# replicated pool, which can keep any of its copies. rep0 is a replicated
# pool with 2 copies in each of dc1 and dc2; objects are written before and
# after dc2's monitor and OSDs are killed. num_zones 1 --replica 3
# --crush_rule rep-dc1 (hosts of dc1) leaves every PG active and the
# objects readable. osd pool set rep0 crush_rule rep-dc2 (hosts of dc2)
# then maps every PG to OSDs that are down, so a read waits. Once dc2 is
# back the PGs recover from dc1's copies and all the objects can be read.
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

# osd pool set <pool> num_zones 1 keeps each PG's shards in its first zone,
# the zone of its first 3 shards, so PGs whose first zone is whole can go
# active while the other zone, not lost, has too few OSDs. data0 is an
# erasure coded pool of k=2 m=1 with 3 shards in each of dc1 and dc2. With
# noout set, two of dc1's three OSDs, osd.0 and osd.1, are killed: dc1
# holds one shard of each PG, too few, so every PG waits. After num_zones
# 1, the PGs whose first zone is dc2 go active, and no more stay
# unavailable than those whose first zone is dc1. Once the OSDs are back
# the PGs go clean and the objects can be read.
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

# mon disable_stretch_mode moves every pool over two zones to one zone as
# osd pool set num_zones 1 does, also with a zone lost. data0, an erasure
# coded pool of k=2 m=1 with 3 shards in each of dc1 and dc2, has objects
# written before and after dc2's monitor and OSDs are killed. The command,
# given --yes-i-really-mean-it, moves data0 to one zone and clears the
# cluster stretch flags. Once dc2 is back the PGs go clean and all the
# objects can be read.
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

# osd pool set <pool> num_zones 1 with both zones up keeps the shards of
# each PG's first zone, the zone of its first 3 shards, which may be the
# zone lost next. data0, an erasure coded pool of k=2 m=1 with 3 shards in
# each of dc1 and dc2, is moved to one zone, and then dc2's monitor and
# OSDs are killed. Once dc2 is back the PGs go clean and the objects can
# be read.
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

# After osd pool set <pool> num_zones 1, a PG of an erasure coded pool can
# still recover objects with the shards of the zone the pool has left.
# data0 is an erasure coded pool of k=2 m=1 with 3 shards in each of dc1
# and dc2. With norecover and nobackfill set, osd.4 of dc2 is down while
# new objects are written, and once it is back osd.5 is killed. A PG whose
# first zone, the zone of its first 3 shards, is dc2 then has one shard of
# each new object, too few to read it. After num_zones 1 and the flags
# unset, the new objects can be read, recovered with dc1's shards. Once
# osd.5 is back the PGs go clean and all the objects can be read.
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

# osd pool set <pool> num_zones 1 on the last pool over two zones clears
# the cluster stretch flags, including the one in the monmap. If a monmap
# change is already waiting to commit, the leader waits for it rather than
# abort on proposing the monmap again. The leader, mon.a or mon.b, cannot
# commit while the other of the two is stopped. The test stops that other
# monitor, runs mon set-weight a 1, which waits, and then moves data0
# (erasure coded, k=2 m=1, over dc1 and dc2) to one zone on data0-one-zone
# (made by create-erasure). Once the stopped monitor continues, both
# commands succeed, the cluster stretch flags are off and the leader is
# still in the quorum.
function TEST_one_zone_while_monmap_changes() {
    local dir=$1
    two_zone_cluster $dir || return 1
    ceph osd pool create data0 erasure --num_zones 2 --k 2 --m 1 || return 1
    wait_for_clean || return 1
    local profile=$(ceph osd pool get data0 erasure_code_profile -f json |
        jq -r .erasure_code_profile)
    ceph osd crush rule create-erasure data0-one-zone $profile || return 1
    # the leader, mon.a or mon.b, cannot commit a change while the other is
    # stopped
    local leader=$(ceph quorum_status -f json | jq -r .quorum_leader_name)
    test "$leader" = a -o "$leader" = b || return 1
    local peon=a
    test $leader != a || peon=b
    # the leader's own monmap: a peon serves no reads while a change is
    # proposed
    local asok=$(get_asok_path mon.$leader)
    local epoch now
    epoch=$(CEPH_ARGS='' ceph --admin-daemon $asok mon_status | jq -e .monmap.epoch) || return 1
    kill -STOP $(cat $dir/mon.$peon.pid) || return 1
    ceph mon set-weight a 1 &
    local weight=$!
    sleep 1
    now=$(CEPH_ARGS='' ceph --admin-daemon $asok mon_status | jq -e .monmap.epoch) || return 1
    test "$now" = "$epoch" || return 1
    ceph osd pool set data0 num_zones 1 --crush_rule data0-one-zone \
        --yes-i-really-mean-it &
    local one_zone=$!
    sleep 2
    kill -CONT $(cat $dir/mon.$peon.pid) || return 1
    wait $weight || return 1
    wait $one_zone || return 1
    test "$(pool_field data0 num_zones)" = 1 || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1
    ceph quorum_status -f json | jq -e ".quorum_names | index(\"$leader\")" || return 1
}

# A command that sets a PG's pg_temp in the same monitor update as
# osd pool set <pool> num_zones 1 must size it for the pool's new size, so
# that the leader can build the next OSDMap. data0 (erasure coded, k=2
# m=1, 3 shards in each of dc1 and dc2) and rep0 (replicated, 2 copies in
# each zone) are over two zones, so data0 is not the last pool over zones,
# whose change would be proposed at once. With paxos_propose_interval set
# to 10, num_zones 1 for data0, on data0-one-zone (made by create-erasure),
# and then the command for data0's PG 1.0 go into one update. That update
# also removes data0's old rule, and the CRUSH change makes the leader
# prime the pg_temp of every PG with the next map. data0 ends with 3
# shards, its old rule gone, and the same leader, with no failed assert in
# its log.
# The tests run this with pg repeer 1.0, and with osd pg-temp 1.0 of six
# OSDs, which must be refused as more than the new size, 3.
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

main mon-stretch-pool-reduce-num-zones "$@"
