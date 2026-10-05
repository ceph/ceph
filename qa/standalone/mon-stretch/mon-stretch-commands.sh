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

# zone iris: mon.a, osd.0, osd.1; zone pze: mon.b, osd.2, osd.3; tiebreaker mon.c
function stretch_cluster() {
    local dir=$1

    setup_zones $dir || return 1
    ceph mon set election_strategy connectivity || return 1
    ceph mon add disallowed_leader c || return 1
    run_mgr $dir x || return 1
    for osd in 0 1 2 3; do
        run_osd $dir $osd || return 1
    done

    for host in 2 3 4 5; do
        ceph osd crush add-bucket node-$host host || return 1
    done
    ceph osd crush move node-2 zone=iris || return 1
    ceph osd crush move node-3 zone=iris || return 1
    ceph osd crush move node-4 zone=pze || return 1
    ceph osd crush move node-5 zone=pze || return 1
    ceph osd crush move osd.0 host=node-2 || return 1
    ceph osd crush move osd.1 host=node-3 || return 1
    ceph osd crush move osd.2 host=node-4 || return 1
    ceph osd crush move osd.3 host=node-5 || return 1
    ceph osd crush remove $(hostname -s) || return 1

    ceph mon set_location a zone=iris host=node-2 || return 1
    ceph mon set_location b zone=pze host=node-4 || return 1
    ceph mon set_location c zone=arbiter host=node-1 || return 1

    ceph osd getcrushmap > $dir/crushmap || return 1
    crushtool --decompile $dir/crushmap > $dir/crushmap.txt || return 1
    sed 's/^# end crush map$//' $dir/crushmap.txt > $dir/crushmap_modified.txt || return 1
    cat >> $dir/crushmap_modified.txt << EOF
rule stretch_rule {
        id 1
        type replicated
        step take iris
        step chooseleaf firstn 2 type host
        step emit
        step take pze
        step chooseleaf firstn 2 type host
        step emit
}

# end crush map
EOF
    crushtool --compile $dir/crushmap_modified.txt -o $dir/crushmap.bin || return 1
    ceph osd setcrushmap -i $dir/crushmap.bin || return 1

    ceph osd pool create stretched 8 8 stretch_rule || return 1
    ceph osd pool set stretched size 4 || return 1
    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    wait_for_clean || return 1
}

# Stretch set and unset configure individual stretch pools, the alternative
# to stretch mode, and must fail while stretch mode is enabled.
function TEST_stretch_pool_commands_in_stretch_mode() {
    local dir=$1

    stretch_cluster $dir || return 1
    expect_failure $dir "while stretch mode is enabled" \
        ceph osd pool stretch set stretched 2 2 zone replicated_rule 6 3 || return 1
    expect_failure $dir "while stretch mode is enabled" \
        ceph osd pool stretch unset stretched replicated_rule 3 2 || return 1

    ceph osd pool ls detail
    test "$(pool_field stretched peering_crush_bucket_count)" == 2 || return 1
    test "$(pool_field stretched crush_rule)" == 1 || return 1
    test "$(pool_field stretched size)" == 4 || return 1
}

function wait_for_stretch_state() {
    local degraded=$1
    local recovering=$2
    for i in $(seq 1 120); do
        local s=$(ceph osd dump -f json | jq -c '.stretch_mode|[.degraded_stretch_mode,.recovering_stretch_mode]')
        if [ "$s" == "[$degraded,$recovering]" ]; then
            return 0
        fi
        sleep 2
    done
    ceph osd dump -f json | jq '.stretch_mode'
    ceph -s
    return 1
}

function lose_zone_pze() {
    local dir=$1

    kill_daemons $dir KILL mon.b || return 1
    kill_daemons $dir KILL osd.2 || return 1
    kill_daemons $dir KILL osd.3 || return 1
    ceph osd down osd.2 osd.3
    wait_for_stretch_state 1 0 || return 1
}

function restore_zone_pze() {
    local dir=$1

    activate_mon $dir b --public-addr $CEPH_MON_B || return 1
    wait_for_quorum 300 3 || return 1
    # keep restarted OSDs in their zones
    ceph config set osd osd_crush_update_on_start false || return 1
    activate_osd $dir 2 || return 1
    activate_osd $dir 3 || return 1
}

# force_healthy_stretch_mode only ends recovery stretch mode, and must fail
# rather than claim to do something in any other state.
function TEST_force_healthy_stretch_mode_requires_recovery() {
    local dir=$1

    stretch_cluster $dir || return 1
    ceph config set mon mon_stretch_recovery_min_wait 3600 || return 1

    expect_failure $dir "recovery stretch mode" \
        ceph osd force_healthy_stretch_mode --yes-i-really-mean-it || return 1
    lose_zone_pze $dir || return 1
    expect_failure $dir "recovery stretch mode" \
        ceph osd force_healthy_stretch_mode --yes-i-really-mean-it || return 1

    restore_zone_pze $dir || return 1
    wait_for_stretch_state 1 1 || return 1
    ceph osd force_healthy_stretch_mode --yes-i-really-mean-it || return 1
    wait_for_stretch_state 0 0 || return 1
}

main mon-stretch-commands "$@"
