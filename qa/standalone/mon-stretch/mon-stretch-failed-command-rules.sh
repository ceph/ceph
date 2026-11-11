#!/usr/bin/env bash
#
# A command that generates a CRUSH rule or an erasure code profile for a
# pool and then fails removes them if no pool uses them. A rule of the same
# name made by hand stays.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7331" # git grep '\<7331\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7332" # git grep '\<7332\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7333" # git grep '\<7333\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests $dir "$@"
}

# osd pool create of a pool over two zones generates a rule for the pool,
# and for an erasure coded pool an erasure code profile, and checks the PG
# limit per OSD with that rule. With mon_max_pg_per_osd 25 on 6 OSDs, 3 in
# each of the datacenters dc1 and dc2, pools of 64 PGs are refused:
# replicated pool big, 2 copies in each zone, and erasure coded pool bigec,
# k=2 m=1. Neither rule, big or bigec, nor the profile bigec-k2-m1 is left
# behind. When a rule named big has been made by hand, the same refused
# create of big leaves it in place.
function TEST_failed_pool_create_removes_its_rule() {
    local dir=$1

    two_zone_cluster $dir 3 || return 1
    ceph config set global mon_max_pg_per_osd 25 || return 1

    expect_failure $dir "exceeds the" \
        ceph osd pool create big --num_zones 2 --replica 2 --pg_num 64 \
        --autoscale_mode off || return 1
    ! rule_exists big || return 1

    expect_failure $dir "exceeds the" \
        ceph osd pool create bigec erasure --k 2 --m 1 --num_zones 2 \
        --pg_num 64 --autoscale_mode off || return 1
    ! rule_exists bigec || return 1
    # and so is the profile generated for it
    ! ceph osd erasure-code-profile ls | grep -qx bigec-k2-m1 || return 1

    add_stretch_rule $dir big default datacenter 2 host 2 || return 1
    expect_failure $dir "exceeds the" \
        ceph osd pool create big --num_zones 2 --replica 2 --pg_num 64 \
        --autoscale_mode off || return 1
    rule_exists big || return 1
}

# osd pool set <pool> num_zones 2 generates a rule for the pool over two
# zones and checks the PG limit per OSD with it. The pools, of 32 PGs each,
# are created in one zone before mon_max_pg_per_osd is lowered to 25. Then
# num_zones 2 is refused for erasure coded pool ec (k=2 m=1) and, with
# --replica 3, for replicated pool rep. Their new rules, ec-stretch and rep,
# are not left behind, and both pools stay in one zone.
function TEST_failed_num_zones_removes_its_rule() {
    local dir=$1

    two_zone_cluster $dir 3 || return 1
    ceph osd pool create ec erasure --k 2 --m 1 --pg_num 32 \
        --autoscale_mode off || return 1
    ceph osd pool set ec allow_ec_optimizations true || return 1
    ceph osd pool create rep --pg_num 32 --autoscale_mode off || return 1
    ceph config set global mon_max_pg_per_osd 25 || return 1

    expect_failure $dir "exceeds the" \
        ceph osd pool set ec num_zones 2 || return 1
    ! rule_exists ec-stretch || return 1
    test "$(pool_field ec num_zones)" = 1 || return 1

    expect_failure $dir "exceeds the" \
        ceph osd pool set rep num_zones 2 --replica 3 || return 1
    ! rule_exists rep || return 1
    test "$(pool_field rep num_zones)" = 1 || return 1
}

# osd pool set <pool> replica <n> for a pool over two zones generates a rule
# with n copies in each zone and checks the PG limit per OSD with it.
# Replicated pool p, of 32 PGs and 2 copies in each zone, is refused replica
# 3 once mon_max_pg_per_osd is lowered to 25. Its new rule, p-replica-3, is
# not left behind, and p keeps its size of 4.
function TEST_failed_replica_removes_its_rule() {
    local dir=$1

    two_zone_cluster $dir 3 || return 1
    ceph osd pool create p --num_zones 2 --replica 2 --pg_num 32 \
        --autoscale_mode off || return 1
    ceph config set global mon_max_pg_per_osd 25 || return 1

    expect_failure $dir "exceeds the" \
        ceph osd pool set p replica 3 || return 1
    ! rule_exists p-replica-3 || return 1
    test "$(pool_field p size)" = 4 || return 1
}

# mon enable_stretch_mode and mon disable_stretch_mode give each erasure
# coded pool a new rule of its own. When another pool refuses its change,
# the command is refused, and the new rules must not be left behind. Here
# replicated pool rep has nosizechange set, so it refuses its new size.
# mon enable_stretch_mode with stretch_rule (2 hosts in each of the
# datacenters dc1 and dc2) is refused, leaves no rule ec-stretch, and
# erasure coded pool ec stays in one zone. With nosizechange cleared it
# works, and ec goes over two zones. With nosizechange set again, mon
# disable_stretch_mode is refused, leaves no rule ec-single-zone, and ec
# stays over two zones on ec-stretch.
function TEST_failed_stretch_mode_removes_its_rules() {
    local dir=$1

    two_zone_cluster $dir 3 || return 1
    ceph osd erasure-code-profile set ec21 plugin=isa k=2 m=1 \
        crush-failure-domain=host || return 1
    ceph osd pool create ec 8 8 erasure ec21 || return 1
    ceph osd pool set ec allow_ec_optimizations true || return 1
    ceph osd pool create rep 8 8 replicated || return 1
    add_stretch_rule $dir stretch_rule default datacenter 2 host 2 || return 1
    ceph mon set election_strategy connectivity || return 1

    ceph osd pool set rep nosizechange true || return 1
    expect_failure $dir "pool size change is disabled" \
        ceph mon enable_stretch_mode c stretch_rule datacenter || return 1
    ! rule_exists ec-stretch || return 1
    test "$(pool_field ec num_zones)" = 1 || return 1

    ceph osd pool set rep nosizechange false || return 1
    ceph mon enable_stretch_mode c stretch_rule datacenter || return 1
    test "$(pool_field ec num_zones)" = 2 || return 1

    ceph osd pool set rep nosizechange true || return 1
    expect_failure $dir "pool size change is disabled" \
        ceph mon disable_stretch_mode --yes-i-really-mean-it || return 1
    ! rule_exists ec-single-zone || return 1
    rule_exists ec-stretch || return 1
    test "$(pool_field ec num_zones)" = 2 || return 1
}

main mon-stretch-failed-command-rules "$@"

# Local Variables:
# compile-command: "cd ../../../build ; make -j4 && ../qa/run-standalone.sh mon-stretch-failed-command-rules.sh"
# End:
