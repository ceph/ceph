#!/usr/bin/env bash
#
# ceph osd pool set <pool> num_zones 2 and back to 1 for pools on
# hand-written CRUSH rules, for each zone type and in a deep tree, with their
# data: which rule each pool gets, where the PGs are, and that the objects
# can be read from each zone alone.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7624" # git grep '\<7624\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7625" # git grep '\<7625\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7626" # git grep '\<7626\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests "$@"
}

# N2: pools go to two zones of type ztype on hand-written rules and back,
# with their data, from rules shared by two pools, of which one changes at a
# time, and from rules whose placement cannot be read, which need the new
# rule or the whole placement. In a deep tree the zones are not under one
# bucket, so a rule that takes each zone cannot be read either.
function pools_on_hand_rules() {
    local dir=$1 ztype=$2 deep=$3 pool
    zone_cluster $dir $ztype $deep || return 1
    ceph config set global osd_pool_default_zone_failure_domain $ztype || return 1
    add_hand_rules $dir $ztype || return 1
    ceph osd erasure-code-profile set p22 plugin=isa k=2 m=2 \
        crush-failure-domain=host || return 1
    ceph osd pool create rtake 8 8 replicated hosts_any || return 1
    ceph osd pool create rtake2 8 8 replicated hosts_any || return 1
    ceph osd pool create rchoose 8 || return 1
    ceph osd pool create rclass 8 || return 1
    for pool in runread runread2; do
        ceph osd pool create $pool 8 8 replicated per_zone_one || return 1
        ceph osd pool set $pool size 2 || return 1
    done
    for pool in etake echoose; do
        ceph osd pool create $pool 8 8 erasure p22 || return 1
        ceph osd pool set $pool allow_ec_optimizations true || return 1
    done
    local all="rtake rtake2 rchoose rclass runread runread2 etake echoose"
    wait_for_clean || return 1
    write_pools $dir $all || return 1
    local hand="take2 choose2 class2 ec_take ec_choose mixed2"
    rules_are replicated_rule $hand hosts_any per_zone_one etake echoose || return 1

    ceph osd pool set rtake num_zones 2 --replica 2 --crush_rule take2 || return 1
    rules_are replicated_rule $hand hosts_any per_zone_one etake echoose || return 1
    ceph osd pool set rtake2 num_zones 2 --replica 2 --crush_rule take2 || return 1
    rules_are replicated_rule $hand per_zone_one etake echoose || return 1
    ceph osd pool set rchoose num_zones 2 --replica 2 --crush_rule choose2 || return 1
    ceph osd pool set rclass num_zones 2 --replica 2 --crush_rule class2 || return 1
    rules_are replicated_rule $hand per_zone_one etake echoose || return 1
    expect_failure $dir "pool 'runread': cannot read the placement of its CRUSH rule: rule 'per_zone_one' does not take the root and choose OSDs" \
        ceph osd pool set runread num_zones 2 --replica 2 || return 1
    test "$(pool_field runread num_zones)" = 1 || return 1
    rules_are replicated_rule $hand per_zone_one etake echoose || return 1
    ceph osd pool set runread num_zones 2 --replica 2 --root default \
        --osd_failure_domain host --no-class || return 1
    rules_are replicated_rule $hand per_zone_one etake echoose runread || return 1
    test "$(rule_takes runread)" = default || return 1
    test "$(rule_zone_type runread)" = $ztype || return 1
    test "$(rule_leaf_type runread)" = host || return 1
    ceph osd pool set runread2 num_zones 2 --replica 2 --crush_rule mixed2 || return 1
    rules_are replicated_rule $hand etake echoose runread || return 1
    ceph osd pool set etake num_zones 2 --crush_rule ec_take || return 1
    rules_are replicated_rule $hand echoose runread || return 1
    ceph osd pool set echoose num_zones 2 --crush_rule ec_choose || return 1
    rules_are replicated_rule $hand runread || return 1
    for pool in rtake rtake2; do
        test "$(pool_rule $pool)" = take2 || return 1
    done
    test "$(pool_rule rchoose)" = choose2 || return 1
    test "$(pool_rule rclass)" = class2 || return 1
    test "$(pool_rule runread2)" = mixed2 || return 1
    test "$(pool_rule etake)" = ec_take || return 1
    test "$(pool_rule echoose)" = ec_choose || return 1

    wait_for_clean || return 1
    for pool in rtake rtake2 rchoose rclass runread runread2; do
        check_two_zones $pool $ztype 2 || return 1
    done
    check_two_zones etake $ztype 4 || return 1
    check_two_zones echoose $ztype 4 || return 1
    check_ssd rclass || return 1
    check_pools $dir $all || return 1
    check_each_zone_alone $dir $all || return 1

    # take2's placement, all of it given in a deep tree, gets a rule of its
    # own; read, it is the stock one, which takes the default replicated rule
    # however that places
    local rtake_rule=
    if [ -n "$deep" ]; then
        expect_failure $dir "pool 'rtake': cannot read the placement of its CRUSH rule: rule 'take2' takes buckets that are not alike zones under one root" \
            ceph osd pool set rtake num_zones 1 || return 1
        test "$(pool_field rtake num_zones)" = 2 || return 1
        ceph osd pool set rtake num_zones 1 --root default \
            --osd_failure_domain host --no-class || return 1
        rtake_rule=rtake-single-zone
        test "$(rule_takes rtake-single-zone)" = default || return 1
        test "$(rule_leaf_type rtake-single-zone)" = host || return 1
    else
        ceph osd pool set rtake num_zones 1 || return 1
        test "$(pool_rule rtake)" = replicated_rule || return 1
    fi
    rules_are replicated_rule $hand runread $rtake_rule || return 1
    ceph osd pool set rtake2 num_zones 1 --root z1 --osd_failure_domain host \
        --no-class || return 1
    rules_are replicated_rule choose2 class2 ec_take ec_choose mixed2 runread \
        $rtake_rule rtake2-single-zone || return 1
    test "$(rule_takes rtake2-single-zone)" = z1 || return 1
    ceph osd crush rule create-replicated hosts_z2 z2 host || return 1
    ceph osd pool set rchoose num_zones 1 --crush_rule hosts_z2 || return 1
    rules_are replicated_rule class2 ec_take ec_choose mixed2 runread \
        $rtake_rule rtake2-single-zone hosts_z2 || return 1
    ceph osd pool set rclass num_zones 1 --root z2 || return 1
    rules_are replicated_rule ec_take ec_choose mixed2 runread \
        $rtake_rule rtake2-single-zone hosts_z2 rclass-single-zone || return 1
    test "$(rule_takes rclass-single-zone)" = z2~ssd || return 1
    test "$(rule_leaf_type rclass-single-zone)" = host || return 1
    ceph osd pool set runread num_zones 1 --root z1 || return 1
    rules_are replicated_rule ec_take ec_choose mixed2 $rtake_rule \
        rtake2-single-zone hosts_z2 rclass-single-zone runread-single-zone || return 1
    test "$(rule_takes runread-single-zone)" = z1 || return 1
    expect_failure $dir "pool 'runread2': cannot read the placement of its CRUSH rule: rule 'mixed2' takes buckets that are not alike zones under one root" \
        ceph osd pool set runread2 num_zones 1 --root z2 || return 1
    test "$(pool_field runread2 num_zones)" = 2 || return 1
    ceph osd pool set runread2 num_zones 1 --root z2 --osd_failure_domain host \
        --no-class || return 1
    rules_are replicated_rule ec_take ec_choose $rtake_rule \
        rtake2-single-zone hosts_z2 rclass-single-zone runread-single-zone \
        runread2-single-zone || return 1
    ceph osd pool set etake num_zones 1 --root z1 --osd_failure_domain host \
        --no-class --yes-i-really-mean-it || return 1
    rules_are replicated_rule ec_choose $rtake_rule rtake2-single-zone \
        hosts_z2 rclass-single-zone runread-single-zone runread2-single-zone \
        etake-single-zone || return 1
    test "$(rule_takes etake-single-zone)" = z1 || return 1
    ceph osd pool set echoose num_zones 1 --root z2 --yes-i-really-mean-it || return 1
    rules_are replicated_rule $rtake_rule rtake2-single-zone hosts_z2 \
        rclass-single-zone runread-single-zone runread2-single-zone \
        etake-single-zone echoose-single-zone || return 1
    test "$(rule_takes echoose-single-zone)" = z2 || return 1
    test "$(rule_leaf_type echoose-single-zone)" = host || return 1
    ! stretch_mode_enabled || return 1

    wait_for_clean || return 1
    check_pools $dir $all || return 1
    for pool in rtake2 runread etake; do
        check_one_zone $pool 1 || return 1
    done
    for pool in rchoose rclass runread2 echoose; do
        check_one_zone $pool 2 || return 1
    done
    check_ssd rclass || return 1
    check_without_zone $dir 2 rtake2 runread etake || return 1
    check_without_zone $dir 1 rchoose rclass runread2 echoose || return 1
}

function TEST_hand_rules_rack() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    pools_on_hand_rules $dir rack || return 1
}

function TEST_hand_rules_room() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    pools_on_hand_rules $dir room || return 1
}

function TEST_hand_rules_region() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    pools_on_hand_rules $dir region || return 1
}

function TEST_hand_rules_deep() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    pools_on_hand_rules $dir datacenter deep || return 1
}

main mon-stretch-pool-num-zones-hand-rules "$@"
