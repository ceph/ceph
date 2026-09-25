#!/usr/bin/env bash
#
# The parameter checks of 'ceph osd pool create': each refused command
# leaves no pool behind.
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7303" # git grep '\<7303\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function expect_refused() {
    local dir=$1
    local message=$2
    shift 2
    ! "$@" > $dir/out 2>&1 || { cat $dir/out; return 1; }
    grep -F -- "$message" $dir/out || { cat $dir/out; return 1; }
    ! ceph osd pool ls | grep -qx p || return 1
}

function TEST_pool_create_checks() {
    local dir=$1

    run_mon $dir a || return 1

    expect_refused $dir "requires both k and m" \
        ceph osd pool create p erasure --k 2 || return 1
    expect_refused $dir "cannot specify both erasure_code_profile and k/m parameters" \
        ceph osd pool create p erasure --erasure_code_profile default --k 2 --m 1 || return 1
    expect_refused $dir "cannot specify k/m parameters for replicated pools" \
        ceph osd pool create p replicated --k 2 --m 1 || return 1
    expect_refused $dir "cannot specify both 'size' and 'replica' parameters" \
        ceph osd pool create p --size 3 --replica 3 || return 1
    expect_refused $dir "require num_zones > 1 for a replicated pool" \
        ceph osd pool create p replicated --osd_failure_domain host || return 1
    expect_refused $dir "num_zones must be from 1 to" \
        ceph osd pool create p --num_zones 3 || return 1
    expect_refused $dir "pool size must be between 1 and 10" \
        ceph osd pool create p --size 11 || return 1
    expect_refused $dir "replica must be between 1 and 10" \
        ceph osd pool create p --replica 11 || return 1
    expect_refused $dir "not in range" \
        ceph osd pool create p --replica 0 || return 1
    expect_refused $dir "pool min_size must be between 2 and 3" \
        ceph osd pool create p erasure --k 2 --m 1 --min_size 4 || return 1
    expect_refused $dir "'pgp_num' must be greater than 0 and lower or equal than 'pg_num'" \
        ceph osd pool create p --pg_num 8 --pgp_num 16 || return 1
    expect_refused $dir "erasure code profile 'nosuch' does not exist" \
        ceph osd pool create p erasure --erasure_code_profile nosuch || return 1

    # the refused commands left no pool id or rule behind
    ceph osd pool create p 8 || return 1
    test "$(ceph osd pool ls detail -f json | jq '.[] | select(.pool_name == "p") | .pool_id')" = 1 || return 1
    ceph osd pool create p 8 2>&1 | grep "already exists" || return 1

    ! ceph osd pool set p num_zones 1 --replica 0 > $dir/out 2>&1 || return 1
    grep -F "not in range" $dir/out || return 1
}

function TEST_pool_create_min_size() {
    local dir=$1

    run_mon $dir a || return 1

    ceph osd pool create r --replica 3 --min_size 1 || return 1
    test "$(ceph osd pool get r min_size | awk '{print $2}')" = 1 || return 1
    ceph osd pool create e erasure --k 2 --m 1 --min_size 3 || return 1
    test "$(ceph osd pool get e min_size | awk '{print $2}')" = 3 || return 1
    expect_refused $dir "pool min_size must be between 1 and replica" \
        ceph osd pool create p --replica 2 --min_size 3 || return 1
}

main mon-stretch-pool-create-checks "$@"
