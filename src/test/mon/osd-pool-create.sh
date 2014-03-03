#!/bin/bash
#
# Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
#
# Author: Loic Dachary <loic@dachary.org>
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
source test/mon/mon-test-helpers.sh

function run() {
    local dir=$1

    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=127.0.0.1 "

    for TEST_function in $(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p') ; do
        setup $dir || return 1
        $TEST_function $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_default_deprectated_0() {
    local dir=$1
    # explicitly set the default crush rule
    expected=66
    run_mon $dir a --public-addr 127.0.0.1 \
        --osd_pool_default_crush_replicated_ruleset $expected
    ./ceph --format json osd dump | grep '"crush_ruleset":'$expected
    ! grep "osd_pool_default_crush_rule is deprecated " $dir/a/log || return 1
}

function TEST_default_deprectated_1() {
    local dir=$1
    # explicitly set the default crush rule using deprecated option
    expected=55
    run_mon $dir a --public-addr 127.0.0.1 \
        --osd_pool_default_crush_rule $expected
    ./ceph --format json osd dump | grep '"crush_ruleset":'$expected
    grep "osd_pool_default_crush_rule is deprecated " $dir/a/log || return 1
}

function TEST_default_deprectated_2() {
    local dir=$1
    expected=77
    unexpected=33
    run_mon $dir a --public-addr 127.0.0.1 \
        --osd_pool_default_crush_rule $expected \
        --osd_pool_default_crush_replicated_ruleset $unexpected
    ./ceph --format json osd dump | grep '"crush_ruleset":'$expected
    ! ./ceph --format json osd dump | grep '"crush_ruleset":'$unexpected || return 1
    grep "osd_pool_default_crush_rule is deprecated " $dir/a/log || return 1
}

function TEST_erasure_crush_rule() {
    local dir=$1
    run_mon $dir a --public-addr 127.0.0.1
    local crush_ruleset=erasure_ruleset
    ./ceph osd crush rule create-erasure $crush_ruleset
    ./ceph osd crush rule ls | grep $crush_ruleset
    ! ./ceph --format json osd dump | grep '"crush_ruleset":1' || return 1
    local poolname
    poolname=pool_erasure1
    ./ceph osd pool create $poolname 12 12 erasure crush_ruleset=$crush_ruleset
    ./ceph --format json osd dump | grep '"crush_ruleset":1' || return 1
    poolname=pool_erasure2
    ./ceph osd pool create $poolname 12 12 erasure 
    ./ceph osd crush rule ls | grep $poolname
}

function TEST_erasure_crush_rule_pending() {
    local dir=$1
    run_mon $dir a --public-addr 127.0.0.1
    # try again if the ruleset creation is pending
    crush_ruleset=erasure_ruleset
    # add to the pending OSD map without triggering a paxos proposal
    result=$(echo '{"prefix":"osdmonitor_prepare_command","prepare":"osd crush rule create-erasure","name":"'$crush_ruleset'"}' | nc -U $dir/a/ceph-mon.a.asok | cut --bytes=5-)
    test $result = true || return 1
    ./ceph osd pool create pool_erasure 12 12 erasure crush_ruleset=$crush_ruleset || return 1
    grep "$crush_ruleset try again" $dir/a/log || return 1
}

function TEST_erasure_code_property_format() {
    local dir=$1
    # osd_pool_default_erasure_code_properties is
    # valid JSON but not of the expected type
    run_mon $dir a --public-addr 127.0.0.1 \
        --osd_pool_default_erasure_code_properties 1
    ./ceph osd pool create poolA 12 12 erasure 2>&1 | grep 'must be a JSON object' || return 1
}

function TEST_erasure_crush_property_format_json() {
    local dir=$1
    # osd_pool_default_erasure_code_properties is JSON
    expected='"erasure-code-plugin":"example"'
    run_mon $dir a --public-addr 127.0.0.1 \
        --osd_pool_default_erasure_code_properties "{$expected}"
    ! ./ceph --format json osd dump | grep "$expected" || return 1
    crush_ruleset=erasure_ruleset
    ./ceph osd crush rule create-erasure $crush_ruleset
    ./ceph osd pool create pool_erasure 12 12 erasure crush_ruleset=$crush_ruleset
    ./ceph --format json osd dump | grep "$expected" || return 1
}

function TEST_erasure_crush_property_format_plain() {
    local dir=$1
    # osd_pool_default_erasure_code_properties is plain text
    expected='"erasure-code-plugin":"example"'
    run_mon $dir a --public-addr 127.0.0.1 \
        --osd_pool_default_erasure_code_properties "erasure-code-plugin=example"
    ! ./ceph --format json osd dump | grep "$expected" || return 1
    crush_ruleset=erasure_ruleset
    ./ceph osd crush rule create-erasure $crush_ruleset
    ./ceph osd pool create pool_erasure 12 12 erasure crush_ruleset=$crush_ruleset
    ./ceph --format json osd dump | grep "$expected" || return 1
}

function TEST_erasure_crush_stripe_width() {
    local dir=$1
    # the default stripe width is used to initialize the pool
    run_mon $dir a --public-addr 127.0.0.1
    stripe_width=$(./ceph-conf --show-config-value osd_pool_erasure_code_stripe_width)
    crush_ruleset=erasure_ruleset
    ./ceph osd crush rule create-erasure $crush_ruleset
    ./ceph osd pool create pool_erasure 12 12 erasure crush_ruleset=$crush_ruleset
    ./ceph --format json osd dump | tee $dir/osd.json
    grep '"stripe_width":'$stripe_width $dir/osd.json > /dev/null || return 1
}

function TEST_erasure_crush_stripe_width_padded() {
    local dir=$1
    # setting osd_pool_erasure_code_stripe_width modifies the stripe_width
    # and it is padded as required by the default plugin
    properties+=" erasure-code-plugin=jerasure"
    properties+=" erasure-code-technique=reed_sol_van"
    k=4
    properties+=" erasure-code-k=$k"
    properties+=" erasure-code-m=2"
    expected_chunk_size=2048
    actual_stripe_width=$(($expected_chunk_size * $k))
    desired_stripe_width=$(($actual_stripe_width - 1))
    run_mon $dir a --public-addr 127.0.0.1 \
        --osd_pool_erasure_code_stripe_width $desired_stripe_width \
        --osd_pool_default_erasure_code_properties "$properties"
    crush_ruleset=erasure_ruleset
    ./ceph osd crush rule create-erasure $crush_ruleset
    ./ceph osd pool create pool_erasure 12 12 erasure crush_ruleset=$crush_ruleset
    ./ceph osd dump | tee $dir/osd.json
    grep "stripe_width $actual_stripe_width" $dir/osd.json > /dev/null || return 1
}

function TEST_erasure_code_pool() {
    local dir=$1
    run_mon $dir a --public-addr 127.0.0.1
    ./ceph --format json osd dump > $dir/osd.json
    ! grep "erasure-code-plugin" $dir/osd.json || return 1
    ./ceph osd crush rule create-erasure erasure_ruleset
    ./ceph osd pool create erasurecodes 12 12 erasure crush_ruleset=erasure_ruleset
    ./ceph --format json osd dump | tee $dir/osd.json
    grep "erasure-code-plugin" $dir/osd.json > /dev/null || return 1
    grep "erasure-code-directory" $dir/osd.json > /dev/null || return 1

    ./ceph osd pool create erasurecodes 12 12 erasure 2>&1 | \
        grep 'already exists' || return 1
    ./ceph osd pool create erasurecodes 12 12 2>&1 | \
        grep 'cannot change to type replicated' || return 1
}

function TEST_replicated_pool() {
    local dir=$1
    run_mon $dir a --public-addr 127.0.0.1
    ./ceph osd pool create replicated 12 12 replicated
    ./ceph osd pool create replicated 12 12 replicated 2>&1 | \
        grep 'already exists' || return 1
    ./ceph osd pool create replicated 12 12 # default is replicated
    ./ceph osd pool create replicated 12    # default is replicated, pgp_num = pg_num
    ./ceph osd pool create replicated 12 12 erasure 2>&1 | \
        grep 'cannot change to type erasure' || return 1
}

main osd-pool-create

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/mon/osd-pool-create.sh"
# End:
