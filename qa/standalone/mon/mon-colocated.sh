#!/usr/bin/env bash

# Can be executed using ../qa/run-standalone.sh
# The goal of this script is to test the "MON_COLOCATED" HEALTH_WARNING under different monitor ip configurations:
#   - All monitors have different IPs
#   - One pair of monitors share the same IP
#   - Two pairs of monitors share the same IP


source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7139" # git grep '\<7139\>' : there must be only one
    export CEPH_MON_B="127.0.0.2:7141" # git grep '\<7141\>' : there must be only one
    export CEPH_MON_C="127.0.0.3:7142" # git grep '\<7142\>' : there must be only one
    export CEPH_MON_D="127.0.0.1:7143" # git grep '\<7143\>' : there must be only one
    export CEPH_MON_E="127.0.0.2:7144" # git grep '\<7144\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "

    export BASE_CEPH_ARGS=$CEPH_ARGS
    CEPH_ARGS+="--mon-host=$CEPH_MON_A"

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

TEST_mon_colocated() {
    local dir=$1
    setup $dir || return 1

    run_mon $dir a --public-addr $CEPH_MON_A || return 1

    run_mon $dir b --public-addr $CEPH_MON_B || return 1
    CEPH_ARGS="$BASE_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B"

    run_mon $dir c --public-addr $CEPH_MON_C || return 1
    CEPH_ARGS="$BASE_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"

    wait_for_health_ok || return 1

    run_mon $dir d --public-addr $CEPH_MON_D || return 1
    CEPH_ARGS="$BASE_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C,$CEPH_MON_D"

    wait_for_health_ok || return 1

    ceph config set mon mon_warn_on_colocated_monitors true

    wait_for_health "MON_COLOCATED" || return 1
    wait_for_health "2 monitors (a,d) share the same ip = 127.0.0.1"

    run_mon $dir e --public-addr $CEPH_MON_E || return 1
    CEPH_ARGS="$BASE_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C,$CEPH_MON_D,$CEPH_MON_E"

    wait_for_health "MON_COLOCATED" || return 1
    wait_for_health "2 monitors (a,d) share the same ip = 127.0.0.1"
    wait_for_health "2 monitors (b,e) share the same ip = 127.0.0.2"

    ceph mon remove e

    wait_for_health "MON_COLOCATED" || return 1
    wait_for_health "2 monitors (a,d) share the same ip = 127.0.0.1"

    ceph mon remove d

    wait_for_health_ok || return 1

    teardown $dir || return 1
}

main mon-colocated "$@"
