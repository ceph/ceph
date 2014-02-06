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
set -xe
PS4='$LINENO: '

# must be larger than the time it takes for osd pool create to perform
# on the slowest http://ceph.com/gitbuilder.cgi machine
TIMEOUT=${TIMEOUT:-30}
DIR=osd-pool-create
rm -fr $DIR
trap "test \$? = 0 || cat $DIR/log ; set +x ; kill_mon || true && rm -fr $DIR" EXIT
mkdir $DIR
export CEPH_ARGS="--conf /dev/null --auth-supported=none --mon-host=127.0.0.1" 

function run_mon() {
    ./ceph-mon --id a \
        --public-addr=127.0.0.1 --mkfs \
        --fsid=$(uuidgen) --mon-data=$DIR --run-dir=$DIR

    ./ceph-mon --id a \
        --chdir= \
        --paxos-propose-interval=0.1 \
        --osd-pool-default-erasure-code-directory=.libs \
        --mon-data=$DIR \
        --log-file=$DIR/log \
        --mon-cluster-log-file=$DIR/log \
        --run-dir=$DIR \
        --pid-file=$DIR/pidfile \
        "$@"
}

function kill_mon() {
    for try in 0 1 1 1 2 3 ; do
        if [ ! -e $DIR/pidfile ] ||
            ! kill -9 $(cat $DIR/pidfile) ; then
            break
        fi
        sleep $try
    done

    rm -fr $DIR/*
}

# explicitly set the default crush rule
expected=66
run_mon --osd_pool_default_crush_replicated_ruleset $expected
./ceph --format json osd dump | grep '"crush_ruleset":'$expected
! grep "osd_pool_default_crush_rule is deprecated " $DIR/log || exit 1
kill_mon

# explicitly set the default crush rule using deprecated option
expected=55
run_mon --osd_pool_default_crush_rule $expected
./ceph --format json osd dump | grep '"crush_ruleset":'$expected
grep "osd_pool_default_crush_rule is deprecated " $DIR/log
kill_mon

expected=77
unexpected=33
run_mon \
    --osd_pool_default_crush_rule $expected \
    --osd_pool_default_crush_replicated_ruleset $unexpected
./ceph --format json osd dump | grep '"crush_ruleset":'$expected
! ./ceph --format json osd dump | grep '"crush_ruleset":'$unexpected || exit 1
grep "osd_pool_default_crush_rule is deprecated " $DIR/log
kill_mon

# osd_pool_default_erasure_code_properties is 
# valid JSON but not of the expected type
run_mon --osd_pool_default_erasure_code_properties 1 
./ceph osd pool create poolA 12 12 erasure 2>&1 | grep 'must be a JSON object'
kill_mon

# set the erasure crush rule
run_mon 
crush_ruleset=erasure_ruleset
./ceph osd crush rule create-erasure $crush_ruleset
./ceph osd crush rule ls | grep $crush_ruleset
./ceph osd pool create pool_erasure 12 12 erasure 2>&1 | 
  grep 'crush_ruleset is missing'
! ./ceph osd pool create pool_erasure 12 12 erasure crush_ruleset=WRONG > $DIR/out 2>&1 
grep 'WRONG does not exist' $DIR/out
grep 'EINVAL' $DIR/out
! ./ceph --format json osd dump | grep '"crush_ruleset":1' || exit 1
./ceph osd pool create pool_erasure 12 12 erasure crush_ruleset=$crush_ruleset
./ceph --format json osd dump | grep '"crush_ruleset":1'
kill_mon

# try again if the ruleset creation is pending
run_mon --paxos-propose-interval=200 --debug-mon=20 --debug-paxos=20 
crush_ruleset=erasure_ruleset
./ceph osd crush rule create-erasure nevermind # the first modification ignores the propose interval, get past it
! timeout $TIMEOUT ./ceph osd crush rule create-erasure $crush_ruleset || exit 1
! timeout $TIMEOUT ./ceph osd pool create pool_erasure 12 12 erasure crush_ruleset=$crush_ruleset
grep "$crush_ruleset try again" $DIR/log
kill_mon

# osd_pool_default_erasure_code_properties is JSON
expected='"erasure-code-plugin":"example"'
run_mon --osd_pool_default_erasure_code_properties "{$expected}"
! ./ceph --format json osd dump | grep "$expected" || exit 1
crush_ruleset=erasure_ruleset
./ceph osd crush rule create-erasure $crush_ruleset
./ceph osd pool create pool_erasure 12 12 erasure crush_ruleset=$crush_ruleset
./ceph --format json osd dump | grep "$expected"
kill_mon

# osd_pool_default_erasure_code_properties is plain text
expected='"erasure-code-plugin":"example"'
run_mon --osd_pool_default_erasure_code_properties "erasure-code-plugin=example"
! ./ceph --format json osd dump | grep "$expected" || exit 1
crush_ruleset=erasure_ruleset
./ceph osd crush rule create-erasure $crush_ruleset
./ceph osd pool create pool_erasure 12 12 erasure crush_ruleset=$crush_ruleset
./ceph --format json osd dump | grep "$expected"
kill_mon

# the default stripe width is used to initialize the pool
run_mon
stripe_width=$(./ceph-conf --show-config-value osd_pool_erasure_code_stripe_width)
crush_ruleset=erasure_ruleset
./ceph osd crush rule create-erasure $crush_ruleset
./ceph osd pool create pool_erasure 12 12 erasure crush_ruleset=$crush_ruleset
./ceph --format json osd dump | tee $DIR/osd.json
grep '"stripe_width":'$stripe_width $DIR/osd.json > /dev/null
kill_mon

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
run_mon \
    --osd_pool_erasure_code_stripe_width $desired_stripe_width \
    --osd_pool_default_erasure_code_properties "$properties"
crush_ruleset=erasure_ruleset
./ceph osd crush rule create-erasure $crush_ruleset
./ceph osd pool create pool_erasure 12 12 erasure crush_ruleset=$crush_ruleset
./ceph osd dump | tee $DIR/osd.json
grep "stripe_width $actual_stripe_width" $DIR/osd.json > /dev/null
kill_mon

run_mon
# creating an erasure code pool sets defaults properties
./ceph --format json osd dump > $DIR/osd.json
! grep "erasure-code-plugin" $DIR/osd.json || exit 1
./ceph osd crush rule create-erasure erasure_ruleset
./ceph osd pool create erasurecodes 12 12 erasure crush_ruleset=erasure_ruleset
./ceph --format json osd dump | tee $DIR/osd.json
grep "erasure-code-plugin" $DIR/osd.json > /dev/null
grep "erasure-code-directory" $DIR/osd.json > /dev/null

./ceph osd pool create erasurecodes 12 12 erasure 2>&1 |
   grep 'already exists'
./ceph osd pool create erasurecodes 12 12 2>&1 |
   grep 'cannot change to type replicated'
./ceph osd pool create replicated 12 12 replicated
./ceph osd pool create replicated 12 12 replicated 2>&1 |
   grep 'already exists'
./ceph osd pool create replicated 12 12 # default is replicated
./ceph osd pool create replicated 12    # default is replicated, pgp_num = pg_num
./ceph osd pool create replicated 12 12 erasure 2>&1 |
   grep 'cannot change to type erasure'

kill_mon

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && TIMEOUT=1 test/mon/osd-pool-create.sh"
# End:
