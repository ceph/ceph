#!/usr/bin/env bash
#
# The cluster stretch flags follow the pools over zones when several pool
# commands are prepared for the same OSDMap epoch.
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

# osd pool stretch set, the command of earlier releases for an individual
# stretch pool, is refused once the cluster stretch flags are set, and must
# be refused too in the OSDMap epoch that sets them. With proposals delayed,
# replicated pool a is created over the datacenters dc1 and dc2, which sets
# the flags, and a second later osd pool stretch set is run for pool ind,
# an ordinary pool on two_dc (2 hosts in each datacenter). It is refused, a
# is created, both flags, in the OSDMap and the monmap, are set, and ind is
# not stretched.
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

# Removing the last pool over two zones clears the cluster stretch flags.
# Here the only two such pools, a and b, 2 copies in each of the
# datacenters dc1 and dc2, are removed at once, with proposals delayed so
# that both removals are in one OSDMap epoch. Both pools are gone, and both
# flags, in the OSDMap and the monmap, are cleared.
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

# Removing the last pool over two zones in the OSDMap epoch that creates
# another must leave the cluster stretch flags set. Pools a and b start over
# the datacenters dc1 and dc2, and a is removed. With proposals delayed,
# pool c is created while b is removed, and then c is removed while pool d
# is created, the first time with the create sent first and the second
# time with the removal sent first. Each time the new pool is over two
# zones, and both flags, in the OSDMap and the monmap, stay set.
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

# The first pool over two zones sets the cluster stretch flags with the
# type of its zones, and a pool over zones of another type is refused. Here
# the datacenters dc1 and dc2 are each in a region of its own, r1 and r2.
# With proposals delayed, so that both creates are prepared for one OSDMap
# epoch, pool a is created over zones of type datacenter and pool b over
# zones of type region. The create checked second must be checked against
# the zone type the first sets, and refused. One pool is created, the
# OSDMap's stretch flag is set, and the pool's zones are of the zone type
# the flags have.
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
    # every pool over zones divides the cluster at the zone type of the
    # cluster stretch flags
    local bucket=$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_bucket)
    local pool
    for pool in a b; do
        ceph osd pool ls | grep -qx $pool || continue
        test "$(pool_field $pool peering_crush_bucket_barrier)" = "$bucket" || return 1
    done
}

main mon-stretch-pending-pools "$@"
