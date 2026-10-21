#!/usr/bin/env bash
#
# Monmap commands that change the elector's disallowed leaders must get the
# reply from their own commit, whether they are sent to the leader or through a
# peon. A lost reply made the monitors run the command again, against the
# updated monmap. The elector must also have the new monmap before the election
# that follows the commit.
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

    # the CLI's duplicate send would print a replayed reply
    export -n CEPH_CLI_TEST_DUP_COMMAND

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function leader_name() {
    ceph quorum_status --format=json | jq -r .quorum_leader_name
}

function leader_addr() {
    case $(leader_name) in
        a) echo $CEPH_MON_A ;;
        b) echo $CEPH_MON_B ;;
        c) echo $CEPH_MON_C ;;
        *) return 1 ;;
    esac
}

function peon_addr() {
    case $(leader_name) in
        a) echo $CEPH_MON_B ;;
        b|c) echo $CEPH_MON_A ;;
        *) return 1 ;;
    esac
}

function wait_for_leader() {
    local test=$1 name=$2
    local leader

    for i in $(seq 30); do
        leader=$(leader_name)
        test -n "$leader" && test "$leader" $test "$name" && return 0
        sleep 1
    done
    return 1
}

function run_three_mons() {
    local dir=$1

    CEPH_ARGS="$BASE_CEPH_ARGS --mon-host=$CEPH_MON_A"
    run_mon $dir a --public-addr $CEPH_MON_A || return 1
    wait_for_quorum 300 1 || return 1
    run_mon $dir b --public-addr $CEPH_MON_B || return 1
    CEPH_ARGS="$BASE_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B"
    wait_for_quorum 300 2 || return 1
    run_mon $dir c --public-addr $CEPH_MON_C || return 1
    CEPH_ARGS="$BASE_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    wait_for_quorum 300 3 || return 1
}

function TEST_stretch_mode_commands_via_peon() {
    local dir=$1
    local peon out

    run_three_mons $dir || return 1

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

    # a replayed command reports the change it had already made
    peon=$(peon_addr) || return 1
    out=$(ceph --mon-host-override $peon \
        mon enable_stretch_mode c stretch_rule datacenter 2>&1) || return 1
    ! grep -q 'already engaged' <<< "$out" || return 1
    ceph mon dump --format=json | jq -e '.stretch_mode' || return 1

    wait_for_quorum 300 3 || return 1
    peon=$(peon_addr) || return 1
    out=$(ceph --mon-host-override $peon \
        mon disable_stretch_mode --yes-i-really-mean-it 2>&1) || return 1
    ! grep -q 'already disabled' <<< "$out" || return 1
    ceph mon dump --format=json | jq -e '.stretch_mode | not' || return 1
}

# Disallow and then allow the leader, sending each command to the monitor that
# $addr_of (leader_addr or peon_addr) names at the time.
function disallow_and_allow_leader() {
    local dir=$1 addr_of=$2
    local leader addr out

    run_three_mons $dir || return 1
    ceph mon set election_strategy disallow || return 1
    wait_for_quorum 300 3 || return 1

    # a replayed command reports the change it had already made
    leader=$(leader_name) || return 1
    addr=$($addr_of) || return 1
    out=$(ceph --mon-host-override $addr mon add disallowed_leader $leader 2>&1) || return 1
    ! grep -q 'already disallowed' <<< "$out" || return 1
    wait_for_quorum 300 3 || return 1
    wait_for_leader != $leader || return 1

    addr=$($addr_of) || return 1
    out=$(ceph --mon-host-override $addr mon rm disallowed_leader $leader 2>&1) || return 1
    ! grep -q 'already allowed' <<< "$out" || return 1
    wait_for_quorum 300 3 || return 1
    ceph mon dump --format=json | jq -e '.disallowed_leaders == ""' || return 1
}

function TEST_disallowed_leader_via_peon() {
    disallow_and_allow_leader $1 peon_addr
}

function TEST_disallowed_leader_via_leader() {
    disallow_and_allow_leader $1 leader_addr
}

function TEST_strategy_change_with_disallowed_leader() {
    local dir=$1

    run_three_mons $dir || return 1
    ceph mon set election_strategy disallow || return 1
    wait_for_quorum 300 3 || return 1
    ceph mon add disallowed_leader a || return 1
    wait_for_quorum 300 3 || return 1
    wait_for_leader != a || return 1

    # classic ignores disallowed leaders, so the lowest rank leads
    ceph mon set election_strategy classic || return 1
    wait_for_quorum 300 3 || return 1
    wait_for_leader = a || return 1

    ceph mon set election_strategy disallow || return 1
    wait_for_quorum 300 3 || return 1
    wait_for_leader != a || return 1
}

main mon-stretch-command-replies "$@"
