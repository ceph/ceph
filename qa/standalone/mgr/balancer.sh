#!/usr/bin/env bash
#
# Copyright (C) 2019 Red Hat <contact@redhat.com>
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

    export CEPH_MON="127.0.0.1:7102" # git grep '\<7102\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done
}

TEST_POOL1=test1
TEST_POOL2=test2

function TEST_balancer() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    create_pool $TEST_POOL1 8
    create_pool $TEST_POOL2 8

    wait_for_clean || return 1

    ceph pg dump pgs
    ceph osd set-require-min-compat-client luminous
    ceph balancer status || return 1
    eval MODE=$(ceph balancer status | jq '.mode')
    test $MODE = "none" || return 1
    ACTIVE=$(ceph balancer status | jq '.active')
    test $ACTIVE = "false" || return 1

    ceph balancer ls || return 1
    PLANS=$(ceph balancer ls)
    test "$PLANS" = "[]" || return 1
    ceph balancer eval || return 1
    EVAL="$(ceph balancer eval)"
    test "$EVAL" = "current cluster score 0.000000 (lower is better)"
    ceph balancer eval-verbose || return 1

    ceph balancer pool add $TEST_POOL1 || return 1
    ceph balancer pool add $TEST_POOL2 || return 1
    ceph balancer pool ls || return 1
    eval POOL=$(ceph balancer pool ls | jq '.[0]')
    test "$POOL" = "$TEST_POOL1" || return 1
    eval POOL=$(ceph balancer pool ls | jq '.[1]')
    test "$POOL" = "$TEST_POOL2" || return 1
    ceph balancer pool rm $TEST_POOL1 || return 1
    ceph balancer pool rm $TEST_POOL2 || return 1
    ceph balancer pool ls || return 1
    ceph balancer pool add $TEST_POOL1 || return 1

    ceph balancer mode crush-compat || return 1
    ceph balancer status || return 1
    eval MODE=$(ceph balancer status | jq '.mode')
    test $MODE = "crush-compat" || return 1
    ! ceph balancer optimize plan_crush $TEST_POOL1 || return 1
    ceph balancer status || return 1
    eval RESULT=$(ceph balancer status | jq '.optimize_result')
    test "$RESULT" = "Distribution is already perfect" || return 1

    ceph balancer on || return 1
    ACTIVE=$(ceph balancer status | jq '.active')
    test $ACTIVE = "true" || return 1
    sleep 2
    ceph balancer status || return 1
    ceph balancer off || return 1
    ACTIVE=$(ceph balancer status | jq '.active')
    test $ACTIVE = "false" || return 1
    sleep 2

    ceph balancer reset || return 1

    ceph balancer mode upmap || return 1
    ceph balancer status || return 1
    eval MODE=$(ceph balancer status | jq '.mode')
    test $MODE = "upmap" || return 1
    ! ceph balancer optimize plan_upmap $TEST_POOL || return 1
    ceph balancer status || return 1
    eval RESULT=$(ceph balancer status | jq '.optimize_result')
    test "$RESULT" = "Unable to find further optimization, or pool(s)' pg_num is decreasing, or distribution is already perfect" || return 1

    ceph balancer on || return 1
    ACTIVE=$(ceph balancer status | jq '.active')
    test $ACTIVE = "true" || return 1
    sleep 2
    ceph balancer status || return 1
    ceph balancer off || return 1
    ACTIVE=$(ceph balancer status | jq '.active')
    test $ACTIVE = "false" || return 1

    teardown $dir || return 1
}

main balancer "$@"

# Local Variables:
# compile-command: "make -j4 && ../qa/run-standalone.sh balancer.sh"
# End:
