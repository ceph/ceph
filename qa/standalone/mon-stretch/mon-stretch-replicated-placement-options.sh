#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON="127.0.0.1:7359" # git grep '\<7359\>' : there must be only one
    run_stretch_tests "$@"
}

# A single-zone pool builds a rule from the placement options when they differ
# from the cluster default, so --root, --osd_failure_domain and --class are
# honoured; only --zone_failure_domain, meaningless for one zone, is refused.
function TEST_single_zone_replicated_placement_options() {
    local dir=$1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    # run_osd binds each OSD to 'hdd', so drop that before setting 'ssd'
    ceph osd crush rm-device-class osd.0 osd.1 osd.2 osd.3 || return 1
    ceph osd crush set-device-class ssd osd.0 osd.1 osd.2 osd.3 || return 1
    # a non-default root to exercise --root (default is the stock root)
    ceph osd crush add-bucket altroot root || return 1
    ceph osd crush add-bucket althost host || return 1
    ceph osd crush move althost root=altroot || return 1
    ceph osd crush create-or-move osd.3 1.0 root=altroot host=althost || return 1

    # --zone_failure_domain is meaningless for one zone and is still refused
    expect_failure $dir \
        "zone_failure_domain requires num_zones > 1 for a replicated pool" \
        ceph osd pool create rep 8 replicated --zone_failure_domain datacenter \
        || return 1
    ! ceph osd pool ls | grep -qx rep || return 1

    # --root, --osd_failure_domain and --class generate a <pool>-single-zone
    # rule, unless the default rule, which chooses OSDs here, places the pool so
    ceph osd pool create rep_root 8 replicated --root altroot || return 1
    ceph osd crush rule dump rep_root-single-zone | grep -q altroot || return 1
    ceph osd pool create rep_host 8 replicated --osd_failure_domain host || return 1
    rule_places $dir rep_host default host || return 1
    ceph osd pool create rep_osd 8 replicated --osd_failure_domain osd || return 1
    test "$(ceph osd pool get rep_osd crush_rule | awk '{print $2}')" = replicated_rule || return 1
    ceph osd pool create rep_cls 8 replicated --class ssd || return 1
    ceph osd crush rule dump rep_cls-single-zone | grep -q '~ssd' || return 1

    # a plain pool keeps the default replicated rule (no generated rule)
    ceph osd pool create plain 8 replicated --num_zones 1 || return 1
    ! ceph osd crush rule ls | grep -qx plain-single-zone || return 1

    # --no-class overrides an inherited default class; --class + --no-class conflict
    ceph config set mon osd_pool_default_class ssd || return 1
    ceph osd pool create rep_nc 8 replicated --no-class || return 1
    local nc=$(ceph osd pool get rep_nc crush_rule | awk '{print $2}')
    ! ceph osd crush rule dump "$nc" | grep -q '~ssd' || return 1
    ceph config set mon osd_pool_default_class "" || return 1
    expect_failure $dir "cannot specify both --class and --no-class" \
        ceph osd pool create rep_both 8 replicated --class ssd --no-class || return 1

    # with a named rule the options conflict with the rule
    expect_failure $dir "cannot specify both crush rule and crush parameters" \
        ceph osd pool create rep2 8 8 replicated replicated_rule --root altroot \
        || return 1
}

# The placement options are honoured whatever the default rule places, and a
# pool given none still gets the default rule
function TEST_single_zone_options_with_custom_default_rule() {
    local dir=$1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    ceph osd crush rm-device-class osd.0 osd.1 osd.2 || return 1
    ceph osd crush set-device-class ssd osd.0 osd.1 osd.2 || return 1
    ceph osd crush add-bucket altroot root || return 1
    ceph osd crush add-bucket althost host || return 1
    ceph osd crush move althost root=altroot || return 1
    ceph osd crush create-or-move osd.2 1.0 root=altroot host=althost || return 1
    ceph osd crush rule create-replicated alt altroot osd ssd || return 1
    ceph config set mon osd_pool_default_crush_rule \
        $(ceph osd crush rule dump alt | jq .rule_id) || return 1

    ceph osd pool create plain 8 replicated || return 1
    test "$(ceph osd pool get plain crush_rule | awk '{print $2}')" = alt || return 1

    ceph osd pool create data 8 replicated --num_zones 1 --root default \
        --osd_failure_domain host --no-class || return 1
    rule_places $dir data default host || return 1
}

# A single-zone erasure coded pool takes --class, and a class that ceph osd
# pool default set gives the default profile reaches an EC pool that names no
# profile.
function TEST_single_zone_erasure_placement_options() {
    local dir=$1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    ceph osd crush rm-device-class osd.0 osd.1 osd.2 || return 1
    ceph osd crush set-device-class ssd osd.0 osd.1 osd.2 || return 1

    ceph osd pool create ec_cls 8 8 erasure --class ssd || return 1
    local prof=$(ceph osd pool get ec_cls erasure_code_profile | awk '{print $2}')
    ceph osd erasure-code-profile get "$prof" | grep -q 'crush-device-class=ssd' \
        || return 1

    ceph osd pool default set --pool_type erasure --class ssd || return 1
    ceph osd pool create ec_def 8 8 || return 1
    prof=$(ceph osd pool get ec_def erasure_code_profile | awk '{print $2}')
    ceph osd erasure-code-profile get "$prof" | grep -q 'crush-device-class=ssd' \
        || return 1
}

# Check the rule of pool $2 takes root $3 and places over bucket type $4.
function rule_places() {
    local dir=$1 pool=$2 root=$3 type=$4
    local rule=$(ceph osd pool get $pool crush_rule | awk '{print $2}')
    ceph osd crush rule dump "$rule" > $dir/rule.json || return 1
    jq -e ".steps[] | select(.op == \"take\" and .item_name == \"$root\")" \
        $dir/rule.json || return 1
    jq -e ".steps[] | select(.type == \"$type\")" $dir/rule.json || return 1
}

# An erasure coded pool takes its placement from its profile: a pool that
# names none from the default profile, which ceph osd pool default set
# writes while the default type is erasure, and the options alone do not
# change. Every such pool gets a rule of its own.
function TEST_single_zone_erasure_placement_defaults() {
    local dir=$1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    ceph osd crush add-bucket altroot root || return 1

    ceph osd pool default set --pool_type erasure --root altroot \
        --osd_failure_domain osd || return 1
    ceph osd pool create ec_def 8 8 || return 1
    rule_places $dir ec_def altroot osd || return 1

    ceph osd erasure-code-profile set named plugin=isa k=2 m=1 || return 1
    ceph osd pool create ec_named 8 8 erasure --erasure_code_profile named \
        || return 1
    rule_places $dir ec_named default host || return 1

    # the options alone leave the default profile, and erasure coded pools
    ceph config set global osd_pool_default_root default || return 1
    ceph config set global osd_pool_default_osd_failure_domain host || return 1
    ceph osd pool create ec_kept 8 8 || return 1
    rule_places $dir ec_kept altroot osd || return 1

    ceph osd pool default set --root default --osd_failure_domain host \
        || return 1
    ceph osd pool create ec_back 8 8 || return 1
    rule_places $dir ec_back default host || return 1
    ! ceph osd crush rule ls | grep -qx erasure-code || return 1
}

main mon-stretch-replicated-placement-options "$@"
