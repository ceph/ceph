#!/usr/bin/env bash
#
# Stretch mode follows the pools over zones when several pool commands are
# prepared for the same OSDMap epoch.
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7307" # git grep '\<7307\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7308" # git grep '\<7308\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7309" # git grep '\<7309\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests $dir "$@"
}

function stretch_flags() {
    echo "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" \
         "$(ceph mon dump -f json | jq .stretch_mode)"
}

# a pool over two zones on an existing rule, so that its create needs no
# proposal of its own
function create_stretch_pool() {
    ceph osd pool create $1 --num_zones 2 --replica 2 --rule two_dc
}

# two pools over two zones, and a long proposal interval so that the next
# commands share an epoch
function two_stretch_pools() {
    local dir=$1

    two_zone_cluster $dir 2 || return 1
    ceph config set mon mon_allow_pool_delete true || return 1
    ceph osd crush rule create-stretch-replicated --rule-name two_dc \
        --num-replica-per-zone 2 || return 1
    create_stretch_pool a || return 1
    create_stretch_pool b || return 1
    test "$(stretch_flags)" = "true true" || return 1
    ceph tell mon.\* config set paxos_propose_interval 5 || return 1
}

# Deleting the last two pools over zones in one epoch disables stretch mode
function TEST_delete_last_two_stretch_pools() {
    local dir=$1
    two_stretch_pools $dir || return 1

    ceph osd pool rm a a --yes-i-really-really-mean-it &
    local pid_a=$!
    ceph osd pool rm b b --yes-i-really-really-mean-it || return 1
    wait $pid_a || return 1
    ! ceph osd pool ls | grep -qx -e a -e b || return 1
    test "$(stretch_flags)" = "false false" || return 1
}

# Deleting the last pool over zones while another is created in the same
# epoch keeps stretch mode enabled
function TEST_delete_last_stretch_pool_while_creating_one() {
    local dir=$1
    two_stretch_pools $dir || return 1
    ceph tell mon.\* config set paxos_propose_interval 1 || return 1
    ceph osd pool rm a a --yes-i-really-really-mean-it || return 1
    ceph tell mon.\* config set paxos_propose_interval 5 || return 1

    create_stretch_pool c &
    local pid_c=$!
    ceph osd pool rm b b --yes-i-really-really-mean-it || return 1
    wait $pid_c || return 1
    test "$(pool_field c num_zones)" = 2 || return 1
    test "$(stretch_flags)" = "true true" || return 1

    ceph osd pool rm c c --yes-i-really-really-mean-it &
    local pid_rm=$!
    create_stretch_pool d || return 1
    wait $pid_rm || return 1
    test "$(pool_field d num_zones)" = 2 || return 1
    test "$(stretch_flags)" = "true true" || return 1
}

main mon-stretch-pending-pools "$@"
