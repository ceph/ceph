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

# osd pool create <pool> erasure, with no profile, copies the default
# profile (k=2 m=2) to a profile of the pool's own, <pool>-k2-m2, and makes a
# rule of the pool's own, named <pool>, whether the pool is in one zone or
# over two. The zones are the datacenters dc1 and dc2. This test creates p1
# and p4 in one zone, and p2 and p3 with --num_zones 2. The rules of p1 and
# p4 have no datacenter step, and those of p2 and p3 choose 2 datacenters.
# No erasure-code rule is made, and p2's rule is removed with p2.
function TEST_multi_zone_pool_default_profile() {
    local dir=$1
    two_zone_cluster $dir 4 || return 1

    ceph osd pool create p1 erasure || return 1
    ceph osd pool create p2 erasure --num_zones 2 || return 1
    ceph osd pool create p3 erasure --num_zones 2 || return 1
    ceph osd pool create p4 erasure || return 1

    # every pool has a profile of its own
    local pool
    for pool in p1 p2 p3 p4; do
        test "$(ceph osd pool get $pool erasure_code_profile -f json | \
                jq -r .erasure_code_profile)" = $pool-k2-m2 || return 1
    done

    for pool in p1 p2 p3 p4; do
        test "$(rule_of $pool)" = $pool || return 1
    done
    ! ceph osd crush rule ls | grep -qx erasure-code || return 1
    test "$(zone_steps p1)" = "[]" || return 1
    test "$(zone_steps p4)" = "[]" || return 1
    for rule in p2 p3; do
        test "$(zone_steps $rule)" = \
            '[{"op":"choose_firstn","num":2,"type":"datacenter"}]' || return 1
    done

    ceph osd pool delete p2 p2 --yes-i-really-really-mean-it || return 1
    ! ceph osd crush rule ls | grep -qx p2 || return 1
}

main mon-stretch-pool-create-default-profile "$@"
