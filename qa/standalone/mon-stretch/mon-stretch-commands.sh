#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7341" # git grep '\<7341\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7342" # git grep '\<7342\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7343" # git grep '\<7343\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function pool_field() {
    ceph osd pool ls detail -f json | jq --arg p $1 ".[]|select(.pool_name==\$p)|.$2"
}

# 3 mons and zones iris and pze, no OSDs
function setup_zones() {
    local dir=$1

    run_mon $dir a --public-addr $CEPH_MON_A || return 1
    run_mon $dir b --public-addr $CEPH_MON_B || return 1
    run_mon $dir c --public-addr $CEPH_MON_C || return 1
    wait_for_quorum 300 3 || return 1
    for zone in iris pze; do
        ceph osd crush add-bucket $zone zone || return 1
        ceph osd crush move $zone root=default || return 1
    done
}

function TEST_stretch_set_replicated_pool() {
    local dir=$1
    setup_zones $dir || return 1

    ceph osd pool create rep 8 8 replicated replicated_rule || return 1
    ceph osd pool stretch set rep 2 2 zone replicated_rule 4 2 || return 1
    test "$(pool_field rep peering_crush_bucket_count)" = 2 || return 1
    test "$(pool_field rep size)" = 4 || return 1
    ceph osd pool stretch unset rep replicated_rule 3 2 || return 1
    test "$(pool_field rep peering_crush_bucket_count)" = 0 || return 1
    test "$(pool_field rep size)" = 3 || return 1
}

main mon-stretch-commands "$@"
