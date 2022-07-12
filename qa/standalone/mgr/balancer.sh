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
    ceph balancer status || return 1
    eval MODE=$(ceph balancer status | jq '.mode')
    test $MODE = "upmap" || return 1
    ACTIVE=$(ceph balancer status | jq '.active')
    test $ACTIVE = "true" || return 1

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
    eval POOL=$(ceph balancer pool ls | jq 'sort | .[0]')
    test "$POOL" = "$TEST_POOL1" || return 1
    eval POOL=$(ceph balancer pool ls | jq 'sort | .[1]')
    test "$POOL" = "$TEST_POOL2" || return 1
    ceph balancer pool rm $TEST_POOL1 || return 1
    ceph balancer pool rm $TEST_POOL2 || return 1
    ceph balancer pool ls || return 1
    ceph balancer pool add $TEST_POOL1 || return 1

    ceph balancer mode crush-compat || return 1
    ceph balancer status || return 1
    eval MODE=$(ceph balancer status | jq '.mode')
    test $MODE = "crush-compat" || return 1
    ceph balancer off || return 1
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
    test "$RESULT" = "Unable to find further optimization, or pool(s) pg_num is decreasing, or distribution is already perfect" || return 1

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

function TEST_balancer2() {
    local dir=$1
    TEST_PGS1=118
    TEST_PGS2=132
    TOTAL_PGS=$(expr $TEST_PGS1 + $TEST_PGS2)
    OSDS=5
    DEFAULT_REPLICAS=3
    # Integer average of PGS per OSD (70.8), so each OSD >= this
    FINAL_PER_OSD1=$(expr \( $TEST_PGS1 \* $DEFAULT_REPLICAS \) / $OSDS)
    # Integer average of PGS per OSD (150)
    FINAL_PER_OSD2=$(expr \( \( $TEST_PGS1 + $TEST_PGS2 \) \* $DEFAULT_REPLICAS \) / $OSDS)

    CEPH_ARGS+="--osd_pool_default_pg_autoscale_mode=off "
    CEPH_ARGS+="--debug_osd=20 "
    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for i in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $i || return 1
    done

    ceph osd set-require-min-compat-client luminous
    ceph config set mgr mgr/balancer/upmap_max_deviation 1
    ceph balancer mode upmap || return 1
    ceph balancer on || return 1
    ceph config set mgr mgr/balancer/sleep_interval 5

    create_pool $TEST_POOL1 $TEST_PGS1

    wait_for_clean || return 1

    # Wait up to 2 minutes
    OK=no
    for i in $(seq 1 25)
    do
      sleep 5
      if grep -q "Optimization plan is almost perfect" $dir/mgr.x.log
      then
        OK=yes
        break
      fi
    done
    test $OK = "yes" || return 1
    # Plan is found, but PGs still need to move
    sleep 10
    wait_for_clean || return 1
    ceph osd df

    PGS=$(ceph osd df --format=json-pretty | jq '.nodes[0].pgs')
    test $PGS -ge $FINAL_PER_OSD1 || return 1
    PGS=$(ceph osd df --format=json-pretty | jq '.nodes[1].pgs')
    test $PGS -ge $FINAL_PER_OSD1 || return 1
    PGS=$(ceph osd df --format=json-pretty | jq '.nodes[2].pgs')
    test $PGS -ge $FINAL_PER_OSD1 || return 1
    PGS=$(ceph osd df --format=json-pretty | jq '.nodes[3].pgs')
    test $PGS -ge $FINAL_PER_OSD1 || return 1
    PGS=$(ceph osd df --format=json-pretty | jq '.nodes[4].pgs')
    test $PGS -ge $FINAL_PER_OSD1 || return 1

    create_pool $TEST_POOL2 $TEST_PGS2

    # Wait up to 2 minutes
    OK=no
    for i in $(seq 1 25)
    do
      sleep 5
      COUNT=$(grep "Optimization plan is almost perfect" $dir/mgr.x.log | wc -l)
      if test $COUNT = "2"
      then
        OK=yes
        break
      fi
    done
    test $OK = "yes" || return 1
    # Plan is found, but PGs still need to move
    sleep 10
    wait_for_clean || return 1
    ceph osd df

    # We should be with plus or minus 2 of FINAL_PER_OSD2
    # This is because here each pool is balanced independently
    MIN=$(expr $FINAL_PER_OSD2 - 2)
    MAX=$(expr $FINAL_PER_OSD2 + 2)
    PGS=$(ceph osd df --format=json-pretty | jq '.nodes[0].pgs')
    test $PGS -ge $MIN -a $PGS -le $MAX || return 1
    PGS=$(ceph osd df --format=json-pretty | jq '.nodes[1].pgs')
    test $PGS -ge $MIN -a $PGS -le $MAX || return 1
    PGS=$(ceph osd df --format=json-pretty | jq '.nodes[2].pgs')
    test $PGS -ge $MIN -a $PGS -le $MAX || return 1
    PGS=$(ceph osd df --format=json-pretty | jq '.nodes[3].pgs')
    test $PGS -ge $MIN -a $PGS -le $MAX || return 1
    PGS=$(ceph osd df --format=json-pretty | jq '.nodes[4].pgs')
    test $PGS -ge $MIN -a $PGS -le $MAX || return 1

    teardown $dir || return 1
}

main balancer "$@"

# Local Variables:
# compile-command: "make -j4 && ../qa/run-standalone.sh balancer.sh"
# End:
