#!/usr/bin/env bash
#
# ceph mon enable_stretch_mode and disable_stretch_mode, three times over,
# with zones of other bucket types than datacenter and zone, existing CRUSH
# rules, and data written and checked at every step.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7651" # git grep '\<7651\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7652" # git grep '\<7652\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7653" # git grep '\<7653\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests "$@"
}

function default_value() {
    ceph osd pool default get -f json | jq -r ".$1.value"
}

# write 5 objects to each pool as step n, and check those of every step up
# to n
function io_step() {
    local dir=$1 n=$2 pool s
    shift 2
    for pool in "$@"; do
        write_objects $dir $pool $pool-$n 5 || return 1
        for s in $(seq 1 $n); do
            check_objects $dir $pool $pool-$s 5 || return 1
        done
    done
}

# each pool is over the two zones of type ztype, on rule (the erasure coded
# pool ec on ec-stretch), and so are the defaults
function check_enabled() {
    local ztype=$1 rule=$2 pool
    shift 2
    test "$(ceph mon dump -f json | jq .stretch_mode)" = true || return 1
    test "$(default_value num_zones)" = 2 || return 1
    test "$(default_value replica)" = 2 || return 1
    test "$(default_value zone_failure_domain)" = $ztype || return 1
    for pool in "$@"; do
        test "$(pool_rule $pool)" = $rule || return 1
    done
    test "$(pool_rule ec)" = ec-stretch || return 1
    wait_for_clean || return 1
    for pool in "$@"; do
        check_two_zones $pool $ztype 2 || return 1
    done
    check_two_zones ec $ztype 4 || return 1
}

function check_disabled() {
    local pool
    test "$(ceph mon dump -f json | jq .stretch_mode)" = false || return 1
    ! stretch_mode_enabled || return 1
    test "$(default_value num_zones)" = 1 || return 1
    test "$(default_value replica)" = 3 || return 1
    for pool in "$@"; do
        test "$(pool_field $pool num_zones)" = 1 || return 1
        test "$(pool_field $pool size)" = 3 || return 1
    done
    test "$(pool_rule ec)" = ec-single-zone || return 1
    wait_for_clean || return 1
}

# S1 and S4: the replicated pools start on replicated_rule, a rule shared by
# two pools, a create-replicated rule of a class and a rule whose placement
# cannot be read, so enabling without a rule is refused and leaves nothing
# behind. Then enable_stretch_mode with a rule that takes each zone, with one
# that chooses them and with one of a class, each followed by
# disable_stretch_mode, once to a rule that takes z1; every rule goes with
# its last pool, and a pool keeps its class in one zone.
function enable_disable_cycles() {
    local dir=$1 ztype=$2 pool
    local -x CEPH_ARGS="$CEPH_ARGS"
    zone_cluster $dir $ztype || return 1
    add_hand_rules $dir $ztype || return 1
    ceph osd crush rule create-replicated ssd_hosts default host ssd || return 1
    ceph osd erasure-code-profile set p22 plugin=isa k=2 m=2 \
        crush-failure-domain=host || return 1
    ceph osd pool create rdef 8 || return 1
    ceph osd pool create rsh1 8 8 replicated hosts_any || return 1
    ceph osd pool create rsh2 8 8 replicated hosts_any || return 1
    ceph osd pool create rssd 8 8 replicated ssd_hosts || return 1
    ceph osd pool create runread 8 8 replicated per_zone_one || return 1
    ceph osd pool set runread size 2 || return 1
    ceph osd pool create ec 8 8 erasure p22 || return 1
    ceph osd pool set ec allow_ec_optimizations true || return 1
    local rep=".mgr rdef rsh1 rsh2 rssd runread"
    local data="rdef rsh1 rsh2 rssd runread ec"
    local hand="choose2 class2 ec_take ec_choose mixed2"
    wait_for_clean || return 1
    io_step $dir 1 $data || return 1
    rules_are replicated_rule take2 $hand hosts_any per_zone_one ssd_hosts ec || return 1

    expect_failure $dir "pool 'runread': cannot read the placement of its CRUSH rule: rule 'per_zone_one' does not take the root and choose OSDs" \
        ceph mon enable_stretch_mode --dividing_bucket $ztype || return 1
    test "$(ceph mon dump -f json | jq .stretch_mode)" = false || return 1
    test "$(default_value num_zones)" = 1 || return 1
    rules_are replicated_rule take2 $hand hosts_any per_zone_one ssd_hosts ec || return 1

    ceph mon enable_stretch_mode c take2 $ztype || return 1
    rules_are replicated_rule take2 $hand ec-stretch || return 1
    check_enabled $ztype take2 $rep || return 1
    io_step $dir 2 $data || return 1
    # take2's placement is the stock one, which takes the default replicated
    # rule
    ceph mon disable_stretch_mode --yes-i-really-mean-it || return 1
    rules_are replicated_rule $hand ec-single-zone || return 1
    for pool in $rep; do
        test "$(pool_rule $pool)" = replicated_rule || return 1
    done
    check_disabled $rep || return 1
    io_step $dir 3 $data || return 1

    ceph mon enable_stretch_mode --new_crush_rule choose2 --dividing_bucket $ztype || return 1
    rules_are replicated_rule $hand ec-stretch || return 1
    check_enabled $ztype choose2 $rep || return 1
    io_step $dir 4 $data || return 1
    ceph osd crush rule create-replicated hosts_z1 z1 host || return 1
    ceph mon disable_stretch_mode hosts_z1 --yes-i-really-mean-it || return 1
    rules_are replicated_rule class2 ec_take ec_choose mixed2 hosts_z1 \
        ec-single-zone || return 1
    for pool in $rep; do
        test "$(pool_rule $pool)" = hosts_z1 || return 1
    done
    check_disabled $rep || return 1
    for pool in rdef rsh1 rsh2 rssd runread; do
        check_one_zone $pool 1 || return 1
    done
    io_step $dir 5 $data || return 1

    ceph mon enable_stretch_mode --new_crush_rule class2 --dividing_bucket $ztype || return 1
    rules_are replicated_rule class2 ec_take ec_choose mixed2 ec-stretch || return 1
    check_enabled $ztype class2 $rep || return 1
    for pool in $rep; do
        check_ssd $pool || return 1
    done
    io_step $dir 6 $data || return 1
    ceph mon disable_stretch_mode --yes-i-really-mean-it || return 1
    rules_are replicated_rule ec_take ec_choose mixed2 .mgr-single-zone \
        rdef-single-zone rsh1-single-zone rsh2-single-zone rssd-single-zone \
        runread-single-zone ec-single-zone || return 1
    for pool in $rep; do
        test "$(rule_takes $pool-single-zone)" = default~ssd || return 1
    done
    check_disabled $rep || return 1
    for pool in $rep; do
        check_ssd $pool || return 1
    done
    io_step $dir 7 $data || return 1
}

function TEST_cycles_rack() {
    local dir=$1
    enable_disable_cycles $dir rack || return 1
}

function TEST_cycles_room() {
    local dir=$1
    enable_disable_cycles $dir room || return 1
}

function TEST_cycles_region() {
    local dir=$1
    enable_disable_cycles $dir region || return 1
}

main mon-stretch-enable-disable-zone-types "$@"
