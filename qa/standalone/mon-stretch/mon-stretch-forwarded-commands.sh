#!/usr/bin/env bash
#
# Stretch mode commands sent through a peon change the elector's disallowed
# leaders when they commit. The leader must still reply to them; a lost reply
# made the peon resend the command, which then failed with EINVAL.
#
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7157" # git grep '\<7157\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7158" # git grep '\<7158\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7159" # git grep '\<7159\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "

    export BASE_CEPH_ARGS=$CEPH_ARGS
    CEPH_ARGS+="--mon-host=$CEPH_MON_A"

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function peon_addr() {
    local leader
    leader=$(ceph quorum_status --format=json | jq -r .quorum_leader_name) || return 1
    if [ "$leader" = "b" ]; then
        echo $CEPH_MON_A
    else
        echo $CEPH_MON_B
    fi
}

function TEST_stretch_mode_commands_via_peon() {
    local dir=$1
    local peon

    run_mon $dir a --public-addr $CEPH_MON_A || return 1
    wait_for_quorum 300 1 || return 1
    run_mon $dir b --public-addr $CEPH_MON_B || return 1
    CEPH_ARGS="$BASE_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B"
    wait_for_quorum 300 2 || return 1
    run_mon $dir c --public-addr $CEPH_MON_C || return 1
    CEPH_ARGS="$BASE_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    wait_for_quorum 300 3 || return 1

    local dc
    for dc in dc1 dc2; do
        ceph osd crush add-bucket $dc datacenter || return 1
        ceph osd crush move $dc root=default || return 1
    done
    ceph osd crush rule create-replicated stretch_rule default datacenter || return 1
    ceph mon set_location a datacenter=dc1 || return 1
    ceph mon set_location b datacenter=dc2 || return 1
    ceph mon set_location c datacenter=dc3 || return 1
    wait_for_quorum 300 3 || return 1

    peon=$(peon_addr) || return 1
    ceph --mon-host-override $peon \
        mon enable_stretch_mode c stretch_rule datacenter || return 1
    ceph mon dump --format=json | jq -e '.stretch_mode' || return 1

    wait_for_quorum 300 3 || return 1
    peon=$(peon_addr) || return 1
    ceph --mon-host-override $peon \
        mon disable_stretch_mode --yes-i-really-mean-it || return 1
    ceph mon dump --format=json | jq -e '.stretch_mode | not' || return 1
}

main mon-stretch-forwarded-commands "$@"
