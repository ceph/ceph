#!/usr/bin/env bash
#
# ceph mon disable_stretch_mode, with a rule and without, for pools on
# existing CRUSH rules, with their data: the rule each pool gets, the rules
# that go when no pool uses them, where the PGs are, and enabling again.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7631" # git grep '\<7631\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7632" # git grep '\<7632\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7633" # git grep '\<7633\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests "$@"
}

HAND="take2 choose2 class2 ec_take ec_choose mixed2 hosts_any per_zone_one"

# S2: disable_stretch_mode gives the replicated pools the rule it is given,
# one that takes z1, a generic one and one of a class, and the erasure coded
# pool a rule of its own in one zone; enable_stretch_mode then works again,
# and each rule goes when no pool uses it
function TEST_disable_to_user_rules() {
    local dir=$1 pool
    local -x CEPH_ARGS="$CEPH_ARGS"
    zone_cluster $dir zone || return 1
    add_hand_rules $dir zone || return 1
    ceph osd crush rule create-replicated z1_hosts z1 host || return 1
    ceph osd crush rule create-replicated ssd_hosts default host ssd || return 1
    ceph osd erasure-code-profile set p22 plugin=isa k=2 m=2 \
        crush-failure-domain=host || return 1
    ceph osd pool create rep 8 || return 1
    ceph osd pool create rep2 8 || return 1
    ceph osd pool create ec 8 8 erasure p22 || return 1
    ceph osd pool set ec allow_ec_optimizations true || return 1
    wait_for_clean || return 1
    write_pools $dir rep rep2 ec || return 1
    rules_are replicated_rule $HAND z1_hosts ssd_hosts ec || return 1

    ceph mon enable_stretch_mode c take2 zone || return 1
    rules_are replicated_rule $HAND z1_hosts ssd_hosts ec-stretch || return 1
    for pool in .mgr rep rep2; do
        test "$(pool_rule $pool)" = take2 || return 1
    done
    wait_for_clean || return 1
    check_two_zones rep zone 2 || return 1
    check_two_zones rep2 zone 2 || return 1
    check_two_zones ec zone 4 || return 1
    check_pools $dir rep rep2 ec || return 1

    expect_failure $dir "You can't disable stretch mode with the same crush rule you are using" \
        ceph mon disable_stretch_mode take2 --yes-i-really-mean-it || return 1
    rules_are replicated_rule $HAND z1_hosts ssd_hosts ec-stretch || return 1
    ceph mon disable_stretch_mode z1_hosts --yes-i-really-mean-it || return 1
    rules_are replicated_rule choose2 class2 ec_take ec_choose mixed2 hosts_any \
        per_zone_one z1_hosts ssd_hosts ec-single-zone || return 1
    for pool in .mgr rep rep2; do
        test "$(pool_rule $pool)" = z1_hosts || return 1
    done
    test "$(rule_takes ec-single-zone)" = default || return 1
    test "$(rule_leaf_type ec-single-zone)" = host || return 1
    wait_for_clean || return 1
    check_pools $dir rep rep2 ec || return 1
    check_one_zone rep 1 || return 1
    check_one_zone rep2 1 || return 1
    check_without_zone $dir 2 rep rep2 || return 1

    # the pools' rule takes z1, which is one zone
    expect_failure $dir "for type zone is not equal to num_failure_domains 2" \
        ceph mon enable_stretch_mode --dividing_bucket zone || return 1
    test "$(ceph mon dump -f json | jq .stretch_mode)" = false || return 1
    test "$(pool_field rep num_zones)" = 1 || return 1
    rules_are replicated_rule choose2 class2 ec_take ec_choose mixed2 hosts_any \
        per_zone_one z1_hosts ssd_hosts ec-single-zone || return 1
    ceph mon enable_stretch_mode --new_crush_rule choose2 --dividing_bucket zone || return 1
    rules_are replicated_rule choose2 class2 ec_take ec_choose mixed2 hosts_any \
        per_zone_one ssd_hosts ec-stretch || return 1
    wait_for_clean || return 1
    check_two_zones rep zone 2 || return 1
    check_two_zones ec zone 4 || return 1
    check_pools $dir rep rep2 ec || return 1

    ceph mon disable_stretch_mode hosts_any --yes-i-really-mean-it || return 1
    rules_are replicated_rule class2 ec_take ec_choose mixed2 hosts_any \
        per_zone_one ssd_hosts ec-single-zone || return 1
    for pool in .mgr rep rep2; do
        test "$(pool_rule $pool)" = hosts_any || return 1
    done
    wait_for_clean || return 1
    check_pools $dir rep rep2 ec || return 1

    # each replicated pool gets a rule of hosts_any's placement
    ceph mon enable_stretch_mode --dividing_bucket zone || return 1
    rules_are replicated_rule class2 ec_take ec_choose mixed2 per_zone_one \
        ssd_hosts ec-stretch .mgr rep rep2 || return 1
    test "$(rule_takes rep)" = default || return 1
    test "$(rule_zone_type rep)" = zone || return 1
    test "$(rule_leaf_type rep)" = host || return 1
    wait_for_clean || return 1
    check_two_zones rep zone 2 || return 1
    check_pools $dir rep rep2 ec || return 1

    ceph mon disable_stretch_mode ssd_hosts --yes-i-really-mean-it || return 1
    rules_are replicated_rule class2 ec_take ec_choose mixed2 per_zone_one \
        ssd_hosts ec-single-zone || return 1
    for pool in .mgr rep rep2; do
        test "$(pool_rule $pool)" = ssd_hosts || return 1
    done
    wait_for_clean || return 1
    check_ssd rep || return 1
    check_ssd rep2 || return 1
    check_pools $dir rep rep2 ec || return 1
}

# S2: disable_stretch_mode without a rule gives each pool over zones a rule in
# one zone of the placement of its own rule: rules that take each zone (shared
# by two pools), choose the zones, choose them in a class, erasure rules of
# each kind, and a generated rule. A replicated pool whose placement is the
# stock one (default, host, no class) or that of replicated_rule goes to
# replicated_rule; a class keeps a rule of its own.
function TEST_disable_keeps_placement_of_hand_rules() {
    local dir=$1 pool
    local -x CEPH_ARGS="$CEPH_ARGS"
    zone_cluster $dir zone || return 1
    ceph config set mon osd_pool_default_zone_failure_domain zone || return 1
    add_hand_rules $dir zone || return 1
    ceph osd erasure-code-profile set p22 plugin=isa k=2 m=2 \
        crush-failure-domain=host || return 1
    for pool in rtake rtake2 rchoose rclass rdef; do
        ceph osd pool create $pool 8 || return 1
    done
    for pool in etake echoose; do
        ceph osd pool create $pool 8 8 erasure p22 || return 1
        ceph osd pool set $pool allow_ec_optimizations true || return 1
    done
    ceph osd pool set rtake num_zones 2 --replica 2 --crush_rule take2 || return 1
    ceph osd pool set rtake2 num_zones 2 --replica 2 --crush_rule take2 || return 1
    ceph osd pool set rchoose num_zones 2 --replica 2 --crush_rule choose2 || return 1
    ceph osd pool set rclass num_zones 2 --replica 2 --crush_rule class2 || return 1
    ceph osd pool set rdef num_zones 2 --replica 2 || return 1
    ceph osd pool set etake num_zones 2 --crush_rule ec_take || return 1
    ceph osd pool set echoose num_zones 2 --crush_rule ec_choose || return 1
    rules_are replicated_rule $HAND rdef || return 1
    local all="rtake rtake2 rchoose rclass rdef etake echoose"
    wait_for_clean || return 1
    write_pools $dir $all || return 1

    ceph mon disable_stretch_mode --yes-i-really-mean-it || return 1
    rules_are replicated_rule mixed2 hosts_any per_zone_one rclass-single-zone \
        etake-single-zone echoose-single-zone || return 1
    for pool in rtake rtake2 rchoose rdef; do
        test "$(pool_rule $pool)" = replicated_rule || return 1
    done
    for pool in rclass etake echoose; do
        test "$(pool_rule $pool)" = $pool-single-zone || return 1
        test "$(rule_leaf_type $pool-single-zone)" = host || return 1
    done
    test "$(rule_takes rclass-single-zone)" = default~ssd || return 1
    test "$(rule_takes etake-single-zone)" = default || return 1
    test "$(rule_takes echoose-single-zone)" = default || return 1
    ! stretch_mode_enabled || return 1
    test "$(ceph mon dump -f json | jq .stretch_mode)" = false || return 1
    wait_for_clean || return 1
    check_ssd rclass || return 1
    check_pools $dir $all || return 1
}

main mon-stretch-disable-rules "$@"
