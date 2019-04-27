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
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7138" # git grep '\<7138\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    export -n CEPH_CLI_TEST_DUP_COMMAND
    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done
}

function TEST_scrub_test() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local objects=15

    TESTDATA="testdata.$$"

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=3 || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd || return 1
    done

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1
    poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')

    dd if=/dev/urandom of=$TESTDATA bs=1032 count=1
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    local primary=$(get_primary $poolname obj1)
    local otherosd=$(get_not_primary $poolname obj1)
    if [ "$otherosd" = "2" ];
    then
      local anotherosd="0"
    else
      local anotherosd="2"
    fi

    objectstore_tool $dir $anotherosd obj1 set-bytes /etc/fstab

    local pgid="${poolid}.0"
    pg_deep_scrub "$pgid" || return 1

    ceph pg dump pgs | grep ^${pgid} | grep -q -- +inconsistent || return 1
    test "$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_scrub_errors')" = "2" || return 1

    ceph osd out $primary
    wait_for_clean || return 1

    pg_deep_scrub "$pgid" || return 1

    test "$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_scrub_errors')" = "2" || return 1
    test "$(ceph pg $pgid query | jq '.peer_info[0].stats.stat_sum.num_scrub_errors')" = "2" || return 1
    ceph pg dump pgs | grep ^${pgid} | grep -q -- +inconsistent || return 1

    ceph osd in $primary
    wait_for_clean || return 1

    repair "$pgid" || return 1
    wait_for_clean || return 1

    # This sets up the test after we've repaired with previous primary has old value
    test "$(ceph pg $pgid query | jq '.peer_info[0].stats.stat_sum.num_scrub_errors')" = "2" || return 1
    ceph pg dump pgs | grep ^${pgid} | grep -vq -- +inconsistent || return 1

    ceph osd out $primary
    wait_for_clean || return 1

    test "$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_scrub_errors')" = "0" || return 1
    test "$(ceph pg $pgid query | jq '.peer_info[0].stats.stat_sum.num_scrub_errors')" = "0" || return 1
    test "$(ceph pg $pgid query | jq '.peer_info[1].stats.stat_sum.num_scrub_errors')" = "0" || return 1
    ceph pg dump pgs | grep ^${pgid} | grep -vq -- +inconsistent || return 1

    teardown $dir || return 1
}

main osd-scrub-test "$@"

# Local Variables:
# compile-command: "cd build ; make -j4 && \
#    ../qa/run-standalone.sh osd-scrub-test.sh"
# End:
