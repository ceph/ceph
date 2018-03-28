#!/usr/bin/env bash
#
# Copyright (C) 2018 Red Hat <contact@redhat.com>
#
# Author: David Zafman <dzafman@redhat.com>
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
source $CEPH_ROOT/qa/workunits/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7138" # git grep '\<7138\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done
}

# In Jewel the poolid and resulting osdmap is different
# Also, remapped PGs aren't clean, so this test needs a fourth OSDs
# 3,1,0 -> 1,0,2

function TEST_scrub_test() {
    local dir=$1
    local poolname=test
    local OSDS=4
    local objects=15

    TESTDATA="testdata.$$"

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=3 || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd || return 1
    done

    ceph osd pool delete rbd rbd --yes-i-really-really-mean-it

    # Create a pool with a single pg
    ceph osd pool create $poolname 1 1
    wait_for_clean || return 1
    poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')

    dd if=/dev/urandom of=$TESTDATA bs=1032 count=1
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    local primary=$(ceph --format json osd map test obj1 | jq '.up[0]')
    local otherosd=$(ceph --format json osd map test obj1 | jq '.up[1]')
    local anotherosd=$(ceph --format json osd map test obj1 | jq '.up[2]')

    ceph pg dump pgs
    objectstore_tool $dir $anotherosd obj1 set-bytes /etc/fstab

    local pgid="${poolid}.0"
    pg_deep_scrub "$pgid" || return 1
    sleep 5

    ceph pg $pgid query | grep num_scrub_errors
    ceph pg dump pgs | grep ^${pgid} | grep -q -- +inconsistent || return 1
    test "$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_scrub_errors')" = "2" || return 1

    ceph osd out $primary
    sleep 3
    wait_for_clean || return 1

    ceph pg dump pgs
    pg_deep_scrub "$pgid" || return 1
    sleep 5

    ceph pg dump pgs
    ceph pg $pgid query | grep num_scrub_errors
    test "$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_scrub_errors')" = "2" || return 1
    #test "$(ceph pg $pgid query | jq '.peer_info[0].stats.stat_sum.num_scrub_errors')" = "2" || return 1
    ceph pg dump pgs | grep ^${pgid} | grep -q -- +inconsistent || return 1

    ceph osd in $primary
    sleep 4
    wait_for_clean || return 1

    repair "$pgid" || return 1
    wait_for_clean || return 1

    ceph pg $pgid query | grep num_scrub_errors
    # This sets up the test after we've repaired with previous primary has old value
    test "$(ceph pg $pgid query | jq '.peer_info[1].stats.stat_sum.num_scrub_errors')" = "2" || return 1
    ceph pg dump pgs | grep ^${pgid} | grep -vq -- +inconsistent || return 1

    ceph osd out $primary
    sleep 4
    wait_for_clean || return 1

    ceph pg $pgid query | grep num_scrub_errors
    test "$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_scrub_errors')" = "0" || return 1
    test "$(ceph pg $pgid query | jq '.peer_info[0].stats.stat_sum.num_scrub_errors')" = "0" || return 1
    test "$(ceph pg $pgid query | jq '.peer_info[1].stats.stat_sum.num_scrub_errors')" = "0" || return 1
    ceph pg dump pgs | grep ^${pgid} | grep -vq -- +inconsistent || return 1

    teardown $dir || return 1
}

main osd-scrub-test "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && \
#    test/osd/osd-scrub-test.sh"
