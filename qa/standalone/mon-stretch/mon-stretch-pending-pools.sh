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
    ceph osd pool create $1 --num_zones 2 --replica 2 --rule ${2:-two_dc}
}

# two pools over two zones, and a long proposal interval so that the next
# commands share an epoch
function two_stretch_pools() {
    local dir=$1

    two_zone_cluster $dir 2 || return 1
    ceph config set mon mon_allow_pool_delete true || return 1
    add_stretch_rule $dir two_dc default datacenter 2 host 2 || return 1
    create_stretch_pool a || return 1
    create_stretch_pool b || return 1
    test "$(stretch_flags)" = "true true" || return 1
    ceph tell mon.\* config set paxos_propose_interval 5 || return 1
}

# osd pool stretch set is refused in the epoch that enables stretch mode, as it
# is once stretch mode is enabled
function TEST_stretch_set_while_stretch_mode_pending() {
    local dir=$1

    two_zone_cluster $dir 2 || return 1
    add_stretch_rule $dir two_dc default datacenter 2 host 2 || return 1
    ceph osd pool create ind 8 8 replicated two_dc || return 1
    ceph tell mon.\* config set paxos_propose_interval 5 || return 1
    # an OSDMap change just committed delays the next proposal
    ceph osd set noout || return 1

    create_stretch_pool a &
    local pid=$!
    sleep 1
    ! ceph osd pool stretch set ind 2 2 datacenter two_dc 4 2 > $dir/out 2>&1 || return 1
    grep "cannot be used while stretch mode is enabled" $dir/out || return 1
    wait $pid || return 1
    test "$(stretch_flags)" = "true true" || return 1
    test "$(pool_field ind peering_crush_bucket_count)" = 0 || return 1
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
    # the new pools get rules of their own: a rule that its last pool leaves
    # is removed, also in the epoch that creates the next pool
    for rule in rule_c rule_d; do
        add_stretch_rule $dir $rule default datacenter 2 host 2 || return 1
    done
    ceph osd pool rm a a --yes-i-really-really-mean-it || return 1
    ceph tell mon.\* config set paxos_propose_interval 5 || return 1

    create_stretch_pool c rule_c &
    local pid_c=$!
    ceph osd pool rm b b --yes-i-really-really-mean-it || return 1
    wait $pid_c || return 1
    test "$(pool_field c num_zones)" = 2 || return 1
    test "$(stretch_flags)" = "true true" || return 1

    ceph osd pool rm c c --yes-i-really-really-mean-it &
    local pid_rm=$!
    create_stretch_pool d rule_d || return 1
    wait $pid_rm || return 1
    test "$(pool_field d num_zones)" = 2 || return 1
    test "$(stretch_flags)" = "true true" || return 1
}

# Two pools over different zone types entering stretch mode in one OSDMap
# update: the second is checked against the stretch mode the first enters
function TEST_pools_entering_stretch_mode_in_one_epoch() {
    local dir=$1
    two_zone_cluster $dir 2 || return 1
    for r in 1 2; do
        ceph osd crush add-bucket r$r region || return 1
        ceph osd crush move r$r root=default || return 1
        ceph osd crush move dc$r region=r$r || return 1
    done
    ceph mon set_location a region=r1 datacenter=dc1 || return 1
    ceph mon set_location b region=r2 datacenter=dc2 || return 1
    ceph mon set_location c region=arbiter datacenter=arbiter || return 1
    ceph tell mon.\* config set paxos_propose_interval 5 || return 1

    ceph osd pool create a --num_zones 2 --replica 2 \
        --zone_failure_domain datacenter > $dir/a.out 2>&1 &
    local pid_a=$!
    ceph osd pool create b --num_zones 2 --replica 2 \
        --zone_failure_domain region > $dir/b.out 2>&1 &
    local pid_b=$!
    # whichever create is checked second is refused
    local created=0
    wait $pid_a && created=$((created + 1))
    wait $pid_b && created=$((created + 1))
    cat $dir/a.out $dir/b.out
    test $created = 1 || return 1
    ceph osd pool ls | grep -qx -e a -e b || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = true || return 1
    # every stretched pool divides the cluster as stretch mode does
    local bucket=$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_bucket)
    local pool
    for pool in a b; do
        ceph osd pool ls | grep -qx $pool || continue
        test "$(pool_field $pool peering_crush_bucket_barrier)" = "$bucket" || return 1
    done
}

main mon-stretch-pending-pools "$@"
