#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON="127.0.0.1:7329" # git grep '\<7329\>' : there must be only one
    run_stretch_tests "$@"
}

# osd pool create still takes its first parameters by position, as in
# earlier releases; the parameters added for pools over zones can only be
# given by name. Erasure coded pool p1 is given pg_num, pgp_num, the type,
# the profile (default), the rule (erasure-code) and expected_num_objects
# (1000) by position. Replicated pool p2 is given replicated_rule and 1000
# one place earlier, where the profile and the rule go, as earlier releases
# took them. Both pools get expected_num_objects 1000.
function TEST_legacy_positional_pool_create() {
    local dir=$1

    run_mon $dir a || return 1
    run_osd $dir 0 || return 1
    ceph osd crush rule create-erasure erasure-code default || return 1
    ceph osd pool create p1 8 8 erasure default erasure-code 1000 || return 1
    test "$(pool_field p1 expected_num_objects)" = 1000 || return 1
    ceph osd pool create p2 8 8 replicated replicated_rule 1000 || return 1
    test "$(pool_field p2 expected_num_objects)" = 1000 || return 1
}

main mon-stretch-pool-create-positional "$@"
