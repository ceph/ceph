#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7324" # git grep '\<7324\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7325" # git grep '\<7325\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7326" # git grep '\<7326\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests "$@"
}

function rule_of() {
    ceph osd pool get $1 crush_rule -f json | jq -r .crush_rule
}

function zone_steps() {
    ceph osd crush rule dump $1 | jq -c '[.steps[] | select(.type == "datacenter")]'
}

# A multi-zone EC pool created from the default profile gets a CRUSH rule of
# its own, and the shared erasure-code rule stays a single-zone rule.
function TEST_multi_zone_pool_default_profile() {
    local dir=$1
    two_zone_cluster $dir 4 || return 1

    ceph osd pool create p1 erasure || return 1
    ceph osd pool create p2 erasure --num-zones 2 || return 1
    ceph osd pool create p3 erasure --num-zones 2 || return 1
    ceph osd pool create p4 erasure || return 1

    test "$(rule_of p1)" = erasure-code || return 1
    test "$(rule_of p2)" = p2 || return 1
    test "$(rule_of p3)" = p3 || return 1
    test "$(rule_of p4)" = erasure-code || return 1
    test "$(zone_steps erasure-code)" = "[]" || return 1
    for rule in p2 p3; do
        test "$(zone_steps $rule)" = \
            '[{"op":"choose_firstn","num":2,"type":"datacenter"}]' || return 1
    done

    ceph osd pool delete p2 p2 --yes-i-really-really-mean-it || return 1
    ! ceph osd crush rule ls | grep -qx p2 || return 1
}

main mon-stretch-pool-create-default-profile "$@"
