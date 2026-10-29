#!/usr/bin/env bash
#
# A monitor started with --set-crush-location sends a mon_join to the leader
# that its peers report. A restarted leader can be told that it is still the
# leader, send the join to itself, and forward it once it is back as a peon.
# The leader has applied the location by then and ignores the join, and the
# peon must not keep the forwarded request.
#
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7360" # git grep '\<7360\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7361" # git grep '\<7361\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7362" # git grep '\<7362\>' : there must be only one
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

function mon_addr() {
    case $1 in
        a) echo $CEPH_MON_A ;;
        b) echo $CEPH_MON_B ;;
        c) echo $CEPH_MON_C ;;
        *) return 1 ;;
    esac
}

function mon_name_of_rank() {
    ceph mon dump --format=json | jq -r ".mons[] | select(.rank == $1) | .name"
}

function wait_for_leader() {
    local name=$1

    for i in $(seq 60); do
        test "$(ceph quorum_status --format=json | jq -r .quorum_leader_name)" = $name && return 0
        sleep 1
    done
    return 1
}

function wait_for_log() {
    local dir=$1 name=$2 pattern=$3

    for i in $(seq 60); do
        CEPH_ARGS='' ceph --admin-daemon $(get_asok_path mon.$name) log flush > /dev/null
        grep -q "$pattern" $dir/mon.$name.log && return 0
        sleep 1
    done
    return 1
}

function wait_for_zone() {
    local name=$1 zone=$2

    for i in $(seq 60); do
        ceph mon dump --format=json |
            jq -e --arg n $name --arg z $zone '.mons[] | select(.name == $n) |
                .crush_location[] | select(.key == "zone" and .val == $z)' > /dev/null &&
            return 0
        sleep 1
    done
    return 1
}

function wait_for_no_mon_join_ops() {
    local name=$1
    local asok=$(get_asok_path mon.$name)

    for i in $(seq 60); do
        CEPH_ARGS='' ceph --admin-daemon $asok ops |
            jq -e '[.ops[] | select(.description | startswith("mon_join"))] | length == 0' > /dev/null &&
            return 0
        sleep 1
    done
    CEPH_ARGS='' ceph --admin-daemon $asok ops
    return 1
}

function TEST_restarted_leader_sets_crush_location() {
    local dir=$1

    run_mon $dir a --public-addr $CEPH_MON_A || return 1
    run_mon $dir b --public-addr $CEPH_MON_B || return 1
    run_mon $dir c --public-addr $CEPH_MON_C || return 1
    wait_for_quorum 300 3 || return 1

    local first=$(mon_name_of_rank 0)
    local second=$(mon_name_of_rank 1)
    local third=$(mon_name_of_rank 2)

    # $third must still report $second as its leader after $second restarts
    ceph tell mon.$third config set mon_lease_ack_timeout_factor 20 || return 1

    kill_daemons $dir TERM mon.$first || return 1
    wait_for_leader $second || return 1

    kill_daemons $dir TERM mon.$second || return 1
    activate_mon $dir $second --public-addr $(mon_addr $second) \
        --set-crush-location zone=z1 || return 1
    wait_for_log $dir $second 'location is wrong, trying to join' || return 1

    activate_mon $dir $first --public-addr $(mon_addr $first) || return 1
    wait_for_quorum 300 3 || return 1
    wait_for_zone $second z1 || return 1

    wait_for_log $dir $second 'forward_request [0-9]* request mon_join' || return 1
    wait_for_no_mon_join_ops $second || return 1
}

main mon-set-crush-location "$@"
