#!/usr/bin/env bash
#
# ceph osd pool set <pool> num_zones 2 and back to 1 for pools on existing
# CRUSH rules, with their data: which rule each pool gets, the rules that go
# when no pool uses them, where the PGs are, and that the objects can be read
# from each zone alone.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7621" # git grep '\<7621\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7622" # git grep '\<7622\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7623" # git grep '\<7623\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests "$@"
}

# osd pool set <pool> num_zones 2 moves a replicated pool over two zones, on
# a new rule with the placement of the pool's rule, and num_zones 1 moves it
# back. This test does it for rdef on replicated_rule, rssd on ssd_hosts (ssd
# OSDs, made by create-replicated) and rz1 on z1_hosts (hosts of z1).
# num_zones 2 is refused for rz1, as z1 has no zones in it, until
# --root default is given.
# With num_zones 1, rdef goes back to replicated_rule, rssd to a rule of its
# own in the ssd OSDs of z1 (--root z1) and rz1 to z2_hosts (--crush_rule).
# The objects can be read from each zone alone while the pools are over two
# zones, and after num_zones 1 a pool in one zone can be read with the other
# zone down.
# A rule is removed when its last pool leaves it; spare, which no pool uses,
# stays.
function TEST_replicated_one_two_one() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    zone_cluster $dir zone || return 1
    ceph config set global osd_pool_default_zone_failure_domain zone || return 1
    ceph osd crush rule create-replicated ssd_hosts default host ssd || return 1
    ceph osd crush rule create-replicated z1_hosts z1 host || return 1
    ceph osd crush rule create-replicated spare default osd || return 1
    ceph osd pool create rdef 8 || return 1
    ceph osd pool create rssd 8 8 replicated ssd_hosts || return 1
    ceph osd pool create rz1 8 8 replicated z1_hosts || return 1
    test "$(pool_rule rdef)" = replicated_rule || return 1
    wait_for_clean || return 1
    write_pools $dir rdef rssd rz1 || return 1
    rules_are replicated_rule ssd_hosts z1_hosts spare || return 1

    # the default rule here chooses OSDs
    ceph osd pool set rdef num_zones 2 --replica 2 || return 1
    rules_are replicated_rule ssd_hosts z1_hosts spare rdef || return 1
    test "$(rule_takes rdef)" = default || return 1
    test "$(rule_zone_type rdef)" = zone,osd || return 1
    ceph osd pool set rssd num_zones 2 --replica 2 || return 1
    rules_are replicated_rule z1_hosts spare rdef rssd || return 1
    test "$(rule_takes rssd)" = default~ssd || return 1
    test "$(rule_zone_type rssd)" = zone || return 1
    test "$(rule_leaf_type rssd)" = host || return 1
    # the root z1 is one zone
    expect_failure $dir "for type zone is not equal to num_failure_domains 2" \
        ceph osd pool set rz1 num_zones 2 --replica 2 || return 1
    test "$(pool_field rz1 num_zones)" = 1 || return 1
    rules_are replicated_rule z1_hosts spare rdef rssd || return 1
    ceph osd pool set rz1 num_zones 2 --replica 2 --root default || return 1
    rules_are replicated_rule spare rdef rssd rz1 || return 1
    test "$(rule_takes rz1)" = default || return 1
    test "$(rule_leaf_type rz1)" = host || return 1
    stretch_mode_enabled || return 1

    wait_for_clean || return 1
    for pool in rdef rssd rz1; do
        check_two_zones $pool zone 2 || return 1
    done
    check_ssd rssd || return 1
    check_pools $dir rdef rssd rz1 || return 1
    check_each_zone_alone $dir rdef rssd rz1 || return 1

    # the placement of rdef's rule is the cluster default
    ceph osd pool set rdef num_zones 1 || return 1
    test "$(pool_rule rdef)" = replicated_rule || return 1
    rules_are replicated_rule spare rssd rz1 || return 1
    ceph osd pool set rssd num_zones 1 --root z1 || return 1
    rules_are replicated_rule spare rz1 rssd-single-zone || return 1
    test "$(rule_takes rssd-single-zone)" = z1~ssd || return 1
    test "$(rule_leaf_type rssd-single-zone)" = host || return 1
    ceph osd crush rule create-replicated z2_hosts z2 host || return 1
    ceph osd pool set rz1 num_zones 1 --crush_rule z2_hosts || return 1
    rules_are replicated_rule spare rssd-single-zone z2_hosts || return 1
    ! stretch_mode_enabled || return 1
    for pool in rdef rssd rz1; do
        test "$(pool_field $pool size)" = 3 || return 1
    done

    wait_for_clean || return 1
    check_pools $dir rdef rssd rz1 || return 1
    check_one_zone rssd 1 || return 1
    check_ssd rssd || return 1
    check_one_zone rz1 2 || return 1
    check_without_zone $dir 2 rssd || return 1
    check_without_zone $dir 1 rz1 || return 1
    rules_are replicated_rule spare rssd-single-zone z2_hosts || return 1
}

# osd pool set <pool> num_zones 2 moves an erasure coded pool over two zones,
# on a new rule, <pool>-stretch, with the placement of the pool's rule, and
# num_zones 1 moves it back. This test does it for eshared on erasure-code,
# the rule that earlier releases share between erasure coded pools (eother
# uses it too), eosd on osds (OSDs, not hosts), essd on ssds (ssd OSDs) and
# ez1 on z1_hosts (hosts of z1). num_zones 2 is refused for ez1, as z1 has no
# zones in it, until --root default is given.
# With num_zones 1, eshared gets a rule over the default root, eosd one in z1
# (--root z1), essd one in the ssd OSDs of z2 (--root z2), and ez1 goes to
# z1_again (--crush_rule).
# The objects can be read from each zone alone while the pools are over two
# zones, and after num_zones 1 a pool in one zone can be read with the other
# zone down.
# A rule is removed when its last pool leaves it: erasure-code goes when
# eother is removed.
function TEST_ec_one_two_one() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    zone_cluster $dir zone || return 1
    ceph config set global osd_pool_default_zone_failure_domain zone || return 1
    ceph osd erasure-code-profile set p22 plugin=isa k=2 m=2 \
        crush-failure-domain=host || return 1
    ceph osd erasure-code-profile set p22osd plugin=isa k=2 m=2 \
        crush-failure-domain=osd || return 1
    ceph osd erasure-code-profile set p21ssd plugin=isa k=2 m=1 \
        crush-failure-domain=host crush-device-class=ssd || return 1
    ceph osd erasure-code-profile set p22z1 plugin=isa k=2 m=2 \
        crush-failure-domain=host crush-root=z1 || return 1
    ceph osd crush rule create-erasure erasure-code p22 || return 1
    ceph osd crush rule create-erasure osds p22osd || return 1
    ceph osd crush rule create-erasure ssds p21ssd || return 1
    ceph osd crush rule create-erasure z1_hosts p22z1 || return 1
    ceph osd pool create eshared 8 8 erasure p22 erasure-code || return 1
    ceph osd pool create eother 8 8 erasure p22 erasure-code || return 1
    ceph osd pool create eosd 8 8 erasure p22osd osds || return 1
    ceph osd pool create essd 8 8 erasure p21ssd ssds || return 1
    ceph osd pool create ez1 8 8 erasure p22z1 z1_hosts || return 1
    for pool in eshared eother eosd essd ez1; do
        ceph osd pool set $pool allow_ec_optimizations true || return 1
    done
    wait_for_clean || return 1
    write_pools $dir eshared eother eosd essd ez1 || return 1
    rules_are replicated_rule erasure-code osds ssds z1_hosts || return 1

    ceph osd pool set eshared num_zones 2 || return 1
    rules_are replicated_rule erasure-code osds ssds z1_hosts eshared-stretch || return 1
    test "$(rule_takes eshared-stretch)" = default || return 1
    test "$(rule_zone_type eshared-stretch)" = zone || return 1
    test "$(rule_leaf_type eshared-stretch)" = host || return 1
    ceph osd pool set eosd num_zones 2 || return 1
    rules_are replicated_rule erasure-code ssds z1_hosts eshared-stretch eosd-stretch || return 1
    test "$(rule_leaf_type eosd-stretch)" = osd || return 1
    ceph osd pool set essd num_zones 2 || return 1
    rules_are replicated_rule erasure-code z1_hosts eshared-stretch eosd-stretch \
        essd-stretch || return 1
    test "$(rule_takes essd-stretch)" = default~ssd || return 1
    # the root z1 is one zone
    expect_failure $dir "for type zone is not equal to num_failure_domains 2" \
        ceph osd pool set ez1 num_zones 2 || return 1
    test "$(pool_field ez1 num_zones)" = 1 || return 1
    rules_are replicated_rule erasure-code z1_hosts eshared-stretch eosd-stretch \
        essd-stretch || return 1
    ceph osd pool set ez1 num_zones 2 --root default || return 1
    rules_are replicated_rule erasure-code eshared-stretch eosd-stretch \
        essd-stretch ez1-stretch || return 1
    test "$(rule_takes ez1-stretch)" = default || return 1

    wait_for_clean || return 1
    check_two_zones eshared zone 4 || return 1
    check_two_zones eosd zone 4 || return 1
    check_two_zones essd zone 3 || return 1
    check_two_zones ez1 zone 4 || return 1
    check_ssd essd || return 1
    check_pools $dir eshared eother eosd essd ez1 || return 1
    check_each_zone_alone $dir eshared eosd essd ez1 || return 1

    ceph osd pool set eshared num_zones 1 --yes-i-really-mean-it || return 1
    rules_are replicated_rule erasure-code eosd-stretch essd-stretch ez1-stretch \
        eshared-single-zone || return 1
    test "$(rule_takes eshared-single-zone)" = default || return 1
    test "$(rule_leaf_type eshared-single-zone)" = host || return 1
    ceph osd pool set eosd num_zones 1 --root z1 --yes-i-really-mean-it || return 1
    rules_are replicated_rule erasure-code essd-stretch ez1-stretch \
        eshared-single-zone eosd-single-zone || return 1
    test "$(rule_takes eosd-single-zone)" = z1 || return 1
    test "$(rule_leaf_type eosd-single-zone)" = osd || return 1
    ceph osd pool set essd num_zones 1 --root z2 --yes-i-really-mean-it || return 1
    rules_are replicated_rule erasure-code ez1-stretch eshared-single-zone \
        eosd-single-zone essd-single-zone || return 1
    test "$(rule_takes essd-single-zone)" = z2~ssd || return 1
    ceph osd crush rule create-erasure z1_again p22z1 || return 1
    ceph osd pool set ez1 num_zones 1 --crush_rule z1_again --yes-i-really-mean-it || return 1
    rules_are replicated_rule erasure-code eshared-single-zone eosd-single-zone \
        essd-single-zone z1_again || return 1
    ! stretch_mode_enabled || return 1
    test "$(pool_field essd size)" = 3 || return 1
    for pool in eshared eosd ez1; do
        test "$(pool_field $pool size)" = 4 || return 1
    done

    wait_for_clean || return 1
    check_pools $dir eshared eother eosd essd ez1 || return 1
    check_one_zone eosd 1 || return 1
    check_one_zone ez1 1 || return 1
    check_one_zone essd 2 || return 1
    check_ssd essd || return 1
    check_without_zone $dir 2 eosd ez1 || return 1
    check_without_zone $dir 1 essd || return 1

    ceph osd pool rm eother eother --yes-i-really-really-mean-it || return 1
    rules_are replicated_rule eshared-single-zone eosd-single-zone \
        essd-single-zone z1_again || return 1
}

function wait_for_pg_num() {
    local pool=$1 n=$2 i
    for i in $(seq 1 120); do
        test "$(pool_field $pool pg_num)" = $n -a "$(pool_field $pool pg_placement_num)" = $n && return 0
        sleep 2
    done
    ceph osd pool ls detail
    return 1
}

# osd pool set <pool> num_zones while the pool's PGs split, while they merge,
# and while a zone is down. The pools are r (replicated, on replicated_rule),
# rt (replicated, given take2, which takes each zone) and e (erasure coded).
# Each goes over two zones while pg_num rises from 8 to 32, and back to
# num_zones 1 (rt to z1) while pg_num falls back to 8. Then, with z2 down, r
# and e go over two zones again: rt, in z1, stays readable, and the PGs of r
# and e whose copies were all in z2 wait for it. Once z2 is back, r and e are
# over both zones and all the objects can be read.
function TEST_num_zones_while_pgs_change() {
    local dir=$1 pool
    local -x CEPH_ARGS="$CEPH_ARGS"
    zone_cluster $dir zone || return 1
    ceph config set global osd_pool_default_zone_failure_domain zone || return 1
    add_hand_rules $dir zone || return 1
    ceph osd erasure-code-profile set p22 plugin=isa k=2 m=2 \
        crush-failure-domain=host || return 1
    ceph osd pool create r 8 || return 1
    ceph osd pool create rt 8 || return 1
    ceph osd pool create e 8 8 erasure p22 || return 1
    ceph osd pool set e allow_ec_optimizations true || return 1
    wait_for_clean || return 1
    write_pools $dir r rt e || return 1

    # splits
    for pool in r rt e; do
        ceph osd pool set $pool pg_num 32 || return 1
    done
    ceph osd pool set r num_zones 2 --replica 2 || return 1
    ceph osd pool set rt num_zones 2 --replica 2 --crush_rule take2 || return 1
    ceph osd pool set e num_zones 2 || return 1
    for pool in r rt e; do
        wait_for_pg_num $pool 32 || return 1
    done
    wait_for_clean || return 1
    check_two_zones r zone 2 || return 1
    check_two_zones rt zone 2 || return 1
    check_two_zones e zone 4 || return 1
    check_pools $dir r rt e || return 1

    # merges
    for pool in r rt e; do
        ceph osd pool set $pool pg_num 8 || return 1
    done
    sleep 5
    ceph osd pool set r num_zones 1 || return 1
    ceph osd pool set rt num_zones 1 --root z1 || return 1
    ceph osd pool set e num_zones 1 --yes-i-really-mean-it || return 1
    for pool in r rt e; do
        wait_for_pg_num $pool 8 || return 1
    done
    wait_for_clean || return 1
    check_one_zone rt 1 || return 1
    check_pools $dir r rt e || return 1
    rules_are replicated_rule choose2 class2 ec_take ec_choose mixed2 hosts_any \
        per_zone_one rt-single-zone e-single-zone || return 1

    # z2 down: rt stays readable from z1, and the pools go to two zones with
    # it down (a PG of r or e whose copies were all in z2 waits for it), and
    # recover when it is back
    kill_zone $dir 2 || return 1
    check_pools $dir rt || return 1
    ceph osd pool set r num_zones 2 --replica 2 || return 1
    ceph osd pool set e num_zones 2 || return 1
    wait_for_stretch_state 1 0 || return 1
    check_pools $dir rt || return 1
    restore_zone $dir 2 || return 1
    wait_for_stretch_state 0 0 || return 1
    check_two_zones r zone 2 || return 1
    check_two_zones e zone 4 || return 1
    check_pools $dir r rt e || return 1
}

main mon-stretch-pool-num-zones-rules "$@"
