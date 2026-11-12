#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7320" # git grep '\<7320\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7321" # git grep '\<7321\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7322" # git grep '\<7322\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests "$@"
}

# osd pool create of the first pool over two zones sets the cluster
# stretch flags, in the OSDMap and the monmap, and a tiebreaker monitor in
# the monmap. When the create is refused, the monmap must not keep them. Here
# the OSDs of dc2 weigh twice as much as those of dc1, so erasure coded pool
# data0 (k=2 m=1) over the two datacenters is refused, as their weights
# differ too much. The monmap then has no stretch flag and no tiebreaker.
function TEST_failed_stretch_pool_create_leaves_mons() {
    local dir=$1
    two_zone_cluster $dir || return 1
    # dc2 weighs twice as much as dc1
    for osd in 3 4 5; do
        ceph osd crush reweight osd.$osd 2.0 || return 1
    done

    expect_failure $dir "Failed to validate pool stretch mode" \
        ceph osd pool create data0 erasure --num_zones 2 --k 2 --m 1 || return 1
    # commits are ordered, so any monmap change the create proposed is in
    ceph osd set noout || return 1
    test "$(ceph mon dump -f json | jq .stretch_mode)" = false || return 1
    test "$(ceph mon dump -f json | jq -r .tiebreaker_mon)" = "" || return 1
}

# osd pool set <pool> num_zones 2 for the first pool over two zones sets
# the cluster stretch flags too, and when it is refused the monmap must not
# keep them. With the OSDs of dc2 weighing twice as much as those of dc1, it
# is refused for erasure coded pool data0 (k=2 m=1), and the monmap then
# has no stretch flag and no tiebreaker.
function TEST_failed_stretch_pool_set_leaves_mons() {
    local dir=$1
    two_zone_cluster $dir || return 1
    ceph osd pool create data0 erasure --k 2 --m 1 || return 1
    ceph osd pool set data0 allow_ec_optimizations true || return 1
    # dc2 weighs twice as much as dc1
    for osd in 3 4 5; do
        ceph osd crush reweight osd.$osd 2.0 || return 1
    done

    expect_failure $dir "Failed to validate pool stretch mode" \
        ceph osd pool set data0 num_zones 2 || return 1
    # commits are ordered, so any monmap change the set staged is in
    ceph osd set noout || return 1
    test "$(ceph mon dump -f json | jq .stretch_mode)" = false || return 1
    test "$(ceph mon dump -f json | jq -r .tiebreaker_mon)" = "" || return 1
}

# The monmap's stretch flag is set in the same commit as the OSDMap that
# creates the first pool over two zones. With the monitors' proposals
# delayed (paxos_propose_interval and paxos_min_wait 10), erasure coded
# pool data0 is created over the datacenters dc1 and dc2. While the create
# runs, the monitors' report, which holds both maps at one commit, is read
# every second: whenever its monmap has the stretch flag, its OSDMap must
# have data0. At the end the monmap has the stretch flag.
function TEST_stretch_pool_create_commits_with_mons() {
    local dir=$1
    two_zone_cluster $dir || return 1
    # delay every proposal that is not forced
    for id in a b c; do
        ceph tell mon.$id config set paxos_propose_interval 10 || return 1
        ceph tell mon.$id config set paxos_min_wait 10 || return 1
    done

    timeout 300 ceph osd pool create data0 erasure --num_zones 2 --k 2 --m 1 &
    local pid=$!
    # one report holds both maps at the same commit
    while kill -0 $pid 2>/dev/null; do
        ceph report > $dir/report.json 2>/dev/null || return 1
        if test "$(jq .monmap.stretch_mode $dir/report.json)" = true; then
            jq -e '.osdmap.pools[] | select(.pool_name == "data0")' \
                $dir/report.json > /dev/null || return 1
        fi
        sleep 1
    done
    wait $pid || return 1
    test "$(ceph mon dump -f json | jq .stretch_mode)" = true || return 1
}

main mon-stretch-failed-create-mons "$@"
