#!/usr/bin/env bash
#
# The upgrade commit with existing CRUSH rules of every kind in place: it
# removes no rule and moves no pool, and afterwards each pool goes to two
# zones, or is refused, as the placement of its rule allows.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7641" # git grep '\<7641\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7642" # git grep '\<7642\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7643" # git grep '\<7643\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    CEPH_ARGS+="--mon-debug-no-require-vampire "
    run_stretch_tests "$@"
}

function committed() {
    ceph osd dump -f json | jq -r .flags | grep -q stretch_zones
}

# the rule Rook writes for stretch clusters
function add_rook_rule() {
    local dir=$1
    ceph osd getcrushmap > $dir/crushmap || return 1
    crushtool --decompile $dir/crushmap > $dir/crushmap.txt || return 1
    cat >> $dir/crushmap.txt << EOF
rule rook_stretch {
        id 20
        type replicated
        step take default
        step choose firstn 0 type zone
        step chooseleaf firstn 2 type host
        step emit
}
EOF
    crushtool --compile $dir/crushmap.txt -o $dir/crushmap.bin || return 1
    ceph osd setcrushmap -i $dir/crushmap.bin || return 1
}

# the pools' rules, and the CRUSH weight-sets, on one line each
function pool_rules() {
    ceph osd pool ls detail -f json | jq -c '[.[] | [.pool_name, .crush_rule]] | sort'
}

function weight_sets() {
    ceph osd crush dump -f json | jq -c .choose_args
}

# osd require-osd-release vampire, the upgrade commit, sets the stretch_zones
# flag. It must remove no rule, move no pool and keep the CRUSH weight-sets.
# Before it, the pools are on replicated_rule (.mgr), ssd_hosts (ssd OSDs,
# made by create-replicated; two pools), erasure-code (the rule earlier
# releases share between erasure coded pools; two pools, one of them legacy),
# osds (made by create-erasure), rook_stretch (the rule Rook writes; two
# pools) and per_zone_one, whose placement cannot be read. Hand-written rules
# that no pool uses, a compat weight-set and weight-sets of two pools are
# there too.
# After it, osd pool set num_zones 2 moves each pool over two zones. A pool
# whose rule's placement can be read gets a new rule of that placement; the
# others need a rule (rook_stretch itself, with 2 copies in each zone, or
# choose2) or the whole placement. The legacy erasure coded pool is refused
# until allow_ec_optimizations is set. A rule is removed when its last pool
# leaves it, and the weight-sets stay.
function TEST_commit_keeps_rules_of_every_kind() {
    local dir=$1 pool
    local -x CEPH_ARGS="$CEPH_ARGS"
    zone_cluster $dir zone || return 1
    test "$(ceph osd dump -f json | jq -r .require_osd_release)" = umbrella || return 1
    ceph config set global osd_pool_default_zone_failure_domain zone || return 1
    ! committed || return 1
    add_hand_rules $dir zone || return 1
    add_rook_rule $dir || return 1
    ceph osd crush rule create-replicated ssd_hosts default host ssd || return 1
    ceph osd erasure-code-profile set p22osd plugin=isa k=2 m=2 \
        crush-failure-domain=osd || return 1
    ceph osd crush rule create-erasure osds p22osd || return 1
    ceph osd pool create rcls1 8 8 replicated ssd_hosts || return 1
    ceph osd pool create rcls2 8 8 replicated ssd_hosts || return 1
    ceph config set global osd_pool_default_flag_ec_optimizations false || return 1
    ceph osd pool create e1 8 8 erasure || return 1
    ceph osd pool create e2 8 8 erasure || return 1
    ceph osd pool set e1 allow_ec_optimizations true || return 1
    ceph osd pool create eosd 8 8 erasure p22osd osds || return 1
    ceph osd pool set eosd allow_ec_optimizations true || return 1
    for pool in rrook1 rrook2; do
        ceph osd pool create $pool 8 8 replicated rook_stretch || return 1
        ceph osd pool set $pool size 4 || return 1
    done
    ceph osd pool create runread 8 8 replicated per_zone_one || return 1
    ceph osd pool set runread size 2 || return 1
    test "$(pool_rule e1)" = erasure-code || return 1
    test "$(pool_rule e2)" = erasure-code || return 1
    ceph osd crush weight-set create-compat || return 1
    ceph osd crush weight-set reweight-compat osd.0 0.9 || return 1
    # not positional on a class rule: a rule removal then breaks it, on main
    ceph osd crush weight-set create rcls1 flat || return 1
    ceph osd crush weight-set create rrook1 positional || return 1
    local hand="take2 choose2 class2 ec_take ec_choose mixed2 hosts_any"
    local all="rcls1 rcls2 e1 e2 eosd rrook1 rrook2 runread"
    wait_for_clean || return 1
    write_pools $dir $all || return 1
    local rules=$(ceph osd crush rule ls | LC_ALL=C sort | xargs)
    rules_are replicated_rule $hand per_zone_one rook_stretch ssd_hosts osds \
        erasure-code || return 1
    local before_pools=$(pool_rules) before_sets=$(weight_sets)

    ceph osd require-osd-release vampire || return 1
    committed || return 1
    rules_are $rules || return 1
    test "$(pool_rules)" = "$before_pools" || return 1
    test "$(weight_sets)" = "$before_sets" || return 1
    wait_for_clean || return 1
    check_pools $dir $all || return 1

    ceph osd pool set rcls1 num_zones 2 --replica 2 || return 1
    rules_are replicated_rule $hand per_zone_one rook_stretch ssd_hosts osds \
        erasure-code rcls1 || return 1
    test "$(rule_takes rcls1)" = default~ssd || return 1
    ceph osd pool set rcls2 num_zones 2 --replica 2 || return 1
    rules_are replicated_rule $hand per_zone_one rook_stretch osds erasure-code \
        rcls1 rcls2 || return 1
    ceph osd pool set e1 num_zones 2 || return 1
    rules_are replicated_rule $hand per_zone_one rook_stretch osds erasure-code \
        rcls1 rcls2 e1-stretch || return 1
    expect_failure $dir "pool 'e2' is a legacy EC pool, which cannot be stretched" \
        ceph osd pool set e2 num_zones 2 || return 1
    ceph osd pool set e2 allow_ec_optimizations true || return 1
    ceph osd pool set e2 num_zones 2 || return 1
    rules_are replicated_rule $hand per_zone_one rook_stretch osds rcls1 rcls2 \
        e1-stretch e2-stretch || return 1
    ceph osd pool set eosd num_zones 2 || return 1
    rules_are replicated_rule $hand per_zone_one rook_stretch rcls1 rcls2 \
        e1-stretch e2-stretch eosd-stretch || return 1
    test "$(rule_leaf_type eosd-stretch)" = osd || return 1
    # Rook's rule chooses zones, which a pool in one zone cannot read; it
    # suits two zones of 2 copies each
    expect_failure $dir "pool 'rrook1': cannot read the placement of its CRUSH rule: rule 'rook_stretch' does not take the root and choose OSDs" \
        ceph osd pool set rrook1 num_zones 2 --replica 2 || return 1
    expect_failure $dir "crush rule rook_stretch does not place 3 items in each zone" \
        ceph osd pool set rrook1 num_zones 2 --crush_rule rook_stretch || return 1
    test "$(pool_field rrook1 num_zones)" = 1 || return 1
    ceph osd pool set rrook1 num_zones 2 --replica 2 --crush_rule rook_stretch || return 1
    ceph osd pool set rrook2 num_zones 2 --replica 2 --root default \
        --osd_failure_domain host --no-class || return 1
    rules_are replicated_rule $hand per_zone_one rook_stretch rcls1 rcls2 \
        e1-stretch e2-stretch eosd-stretch rrook2 || return 1
    expect_failure $dir "pool 'runread': cannot read the placement of its CRUSH rule: rule 'per_zone_one' does not take the root and choose OSDs" \
        ceph osd pool set runread num_zones 2 --replica 2 || return 1
    ceph osd pool set runread num_zones 2 --replica 2 --crush_rule choose2 || return 1
    rules_are replicated_rule $hand rook_stretch rcls1 rcls2 e1-stretch \
        e2-stretch eosd-stretch rrook2 || return 1
    test "$(pool_rule rrook1)" = rook_stretch || return 1
    test "$(pool_rule runread)" = choose2 || return 1

    wait_for_clean || return 1
    for pool in rcls1 rcls2 rrook1 rrook2 runread; do
        check_two_zones $pool zone 2 || return 1
    done
    for pool in e1 e2 eosd; do
        check_two_zones $pool zone 4 || return 1
    done
    check_ssd rcls1 || return 1
    check_ssd rcls2 || return 1
    ceph osd crush weight-set ls | grep -qx '(compat)' || return 1
    ceph osd crush weight-set ls | grep -qx rcls1 || return 1
    ceph osd crush weight-set ls | grep -qx rrook1 || return 1
    check_pools $dir $all || return 1
}

# Global stretch mode is enabled before the upgrade commit, with
# rook_stretch, the rule Rook writes, and pool rmixed is then moved to
# mixed2, whose placement cannot be read. osd require-osd-release vampire
# makes each pool a pool over two zones of 2 copies each, on the rule it
# had. After it, osd pool set replica 3 gives rep1 a new rule read from
# rook_stretch, and is refused for rmixed. mon disable_stretch_mode with no
# rule is refused, as it cannot read mixed2, and changes nothing; given
# hosts_any, it moves every replicated pool there. A rule is removed when its
# last pool leaves it.
function TEST_commit_global_stretch_mode_on_rook_rule() {
    local dir=$1 pool
    local -x CEPH_ARGS="$CEPH_ARGS"
    zone_cluster $dir zone || return 1
    add_hand_rules $dir zone || return 1
    add_rook_rule $dir || return 1
    ceph mon set election_strategy connectivity || return 1
    ceph mon add disallowed_leader c || return 1
    ceph osd pool create rep1 8 || return 1
    ceph osd pool create rep2 8 || return 1
    wait_for_clean || return 1
    ceph mon enable_stretch_mode c rook_stretch zone || return 1
    ceph osd pool create rmixed 8 || return 1
    ceph osd pool set rmixed crush_rule mixed2 || return 1
    for pool in .mgr rep1 rep2; do
        test "$(pool_rule $pool)" = rook_stretch || return 1
    done
    local hand="take2 choose2 class2 ec_take ec_choose mixed2 hosts_any per_zone_one"
    local all="rep1 rep2 rmixed"
    wait_for_clean || return 1
    write_pools $dir $all || return 1
    rules_are replicated_rule $hand rook_stretch || return 1

    ceph osd require-osd-release vampire || return 1
    committed || return 1
    rules_are replicated_rule $hand rook_stretch || return 1
    for pool in .mgr rep1 rep2 rmixed; do
        test "$(pool_field $pool num_zones)" = 2 || return 1
        test "$(pool_field $pool replica)" = 2 || return 1
    done
    test "$(pool_rule rmixed)" = mixed2 || return 1
    wait_for_clean || return 1
    for pool in rep1 rep2 rmixed; do
        check_two_zones $pool zone 2 || return 1
    done
    check_pools $dir $all || return 1

    ceph osd pool set rep1 replica 3 || return 1
    rules_are replicated_rule $hand rook_stretch rep1-replica-3 || return 1
    test "$(rule_takes rep1-replica-3)" = default || return 1
    test "$(rule_zone_type rep1-replica-3)" = zone || return 1
    test "$(rule_leaf_type rep1-replica-3)" = host || return 1
    expect_failure $dir "pool 'rmixed': cannot read the placement of its CRUSH rule to change its replica: rule 'mixed2' takes buckets that are not alike zones under one root" \
        ceph osd pool set rmixed replica 3 || return 1
    expect_failure $dir "pool 'rmixed': cannot read the placement of its CRUSH rule: rule 'mixed2' takes buckets that are not alike zones under one root" \
        ceph mon disable_stretch_mode --yes-i-really-mean-it || return 1
    rules_are replicated_rule $hand rook_stretch rep1-replica-3 || return 1
    test "$(pool_field rep2 num_zones)" = 2 || return 1
    ceph mon disable_stretch_mode hosts_any --yes-i-really-mean-it || return 1
    rules_are replicated_rule take2 choose2 class2 ec_take ec_choose hosts_any \
        per_zone_one || return 1
    for pool in .mgr rep1 rep2 rmixed; do
        test "$(pool_rule $pool)" = hosts_any || return 1
    done
    wait_for_clean || return 1
    check_pools $dir $all || return 1
}

main mon-stretch-upgrade-rules "$@"
