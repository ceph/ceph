#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON="127.0.0.1:7359" # git grep '\<7359\>' : there must be only one
    run_stretch_tests "$@"
}

# A single-zone replicated pool does not build a rule from the placement
# options, so they are refused rather than ignored.
function TEST_single_zone_replicated_placement_options() {
    local dir=$1
    run_mon $dir a || return 1

    local opts=("--root default" "--zone_failure_domain datacenter"
                "--osd_failure_domain host" "--class hdd")
    for args in "8 replicated" "8 replicated --num_zones 1"; do
        for opt in "${opts[@]}"; do
            expect_failure $dir "require num_zones > 1" \
                ceph osd pool create rep $args $opt || return 1
        done
    done
    # with a rule, the options conflict with the rule first
    for opt in "${opts[@]}"; do
        expect_failure $dir "cannot specify both crush rule and crush parameters" \
            ceph osd pool create rep 8 8 replicated replicated_rule $opt || return 1
    done
    ! ceph osd pool ls | grep -qx rep || return 1
    ceph osd pool create plain 8 replicated --num_zones 1 || return 1
}

main mon-stretch-replicated-placement-options "$@"
