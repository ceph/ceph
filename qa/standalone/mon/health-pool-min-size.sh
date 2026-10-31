#!/usr/bin/env bash
#
# Tests for the POOL_MIN_SIZE_TOO_LOW health check
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7155" # git grep '\<7155\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--mon-pg-warn-min-per-osd 0 --mon-max-pg-per-osd 1000 "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function wait_for_no_health() {
    local grepstr=$1
    local -a delays=($(get_timeout_delays $TIMEOUT .1))
    local -i loop=0

    while ceph health detail | grep -q "$grepstr" ; do
        if (( $loop >= ${#delays[*]} )) ; then
            ceph health detail
            return 1
        fi
        sleep ${delays[$loop]}
        loop+=1
    done
}

function start_cluster() {
    local dir=$1

    # ceph-helpers disables the warning by default; turn it back on here
    run_mon $dir a --mon-warn-on-pool-min-size-too-low=true || return 1
    run_mgr $dir x || return 1
    for id in 0 1 2 3 ; do
        run_osd $dir $id || return 1
    done
}

function TEST_replicated() {
    local dir=$1

    start_cluster $dir || return 1

    create_pool rep 8 8 replicated || return 1
    ceph osd pool application enable rep rbd || return 1
    ceph osd pool set rep size 3 --yes-i-really-mean-it || return 1
    ceph osd pool set rep min_size 2 || return 1
    wait_for_clean || return 1
    wait_for_no_health POOL_MIN_SIZE_TOO_LOW || return 1

    # size 3, min_size 1
    ceph osd pool set rep min_size 1 || return 1
    wait_for_health POOL_MIN_SIZE_TOO_LOW || return 1
    ceph health detail | grep "pool 'rep' has min_size 1 with size 3" || return 1

    # the warning can be muted
    ceph health mute POOL_MIN_SIZE_TOO_LOW || return 1
    wait_for_health_ok || return 1
    ceph health unmute POOL_MIN_SIZE_TOO_LOW || return 1
    wait_for_health POOL_MIN_SIZE_TOO_LOW || return 1

    # size 2, min_size 1
    ceph osd pool set rep size 2 --yes-i-really-mean-it || return 1
    ceph osd pool set rep min_size 1 || return 1
    wait_for_health "pool 'rep' has min_size 1 with size 2" || return 1

    # size 1 is reported by POOL_NO_REDUNDANCY only
    ceph osd pool set rep size 1 --yes-i-really-mean-it || return 1
    wait_for_health POOL_NO_REDUNDANCY || return 1
    wait_for_no_health POOL_MIN_SIZE_TOO_LOW || return 1

    # back to a safe configuration
    ceph osd pool set rep size 3 --yes-i-really-mean-it || return 1
    ceph osd pool set rep min_size 2 || return 1
    wait_for_no_health POOL_MIN_SIZE_TOO_LOW || return 1

    # the warning can be disabled
    ceph osd pool set rep min_size 1 || return 1
    wait_for_health POOL_MIN_SIZE_TOO_LOW || return 1
    ceph config set mon mon_warn_on_pool_min_size_too_low false || return 1
    wait_for_no_health POOL_MIN_SIZE_TOO_LOW || return 1
}

function TEST_erasure() {
    local dir=$1

    start_cluster $dir || return 1

    # k=2 m=1: Ceph defaults min_size to k
    ceph osd erasure-code-profile set k2m1 k=2 m=1 \
        crush-failure-domain=osd || return 1
    create_pool ec21 8 8 erasure k2m1 || return 1
    ceph osd pool application enable ec21 rbd || return 1
    wait_for_clean || return 1
    wait_for_health "pool 'ec21' has min_size 2 with k=2 m=1" || return 1
    ceph health detail | grep "use a profile with m >= 2" || return 1
    ceph osd pool delete ec21 ec21 --yes-i-really-really-mean-it || return 1
    wait_for_no_health POOL_MIN_SIZE_TOO_LOW || return 1

    # k=2 m=2: min_size defaults to k+1 and must not warn
    ceph osd erasure-code-profile set k2m2 k=2 m=2 \
        crush-failure-domain=osd || return 1
    create_pool ec22 8 8 erasure k2m2 || return 1
    ceph osd pool application enable ec22 rbd || return 1
    wait_for_clean || return 1
    wait_for_no_health POOL_MIN_SIZE_TOO_LOW || return 1

    # lowering min_size to k warns
    ceph osd pool set ec22 min_size 2 || return 1
    wait_for_health "pool 'ec22' has min_size 2 with k=2 m=2" || return 1
    ceph health detail | grep "min_size >= k+1 (3)" || return 1

    ceph osd pool set ec22 min_size 3 || return 1
    wait_for_no_health POOL_MIN_SIZE_TOO_LOW || return 1
}

main health-pool-min-size "$@"

# Local Variables:
# compile-command: "cd ../../../build ; make -j4 && ../qa/run-standalone.sh health-pool-min-size.sh"
# End:
