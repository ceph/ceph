#!/bin/bash
#
# Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
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

DIR=osd-pool-create
rm -fr $DIR
trap "set +x ; kill_mon || true ; rm -fr $DIR" EXIT
mkdir $DIR
export CEPH_ARGS="--conf /dev/null --auth-supported=none --mon-host=127.0.0.1" 

function run_mon() {
    ./ceph-mon --id a \
        --public-addr=127.0.0.1 --mkfs --keyring /dev/null \
        --fsid=$(uuidgen) --mon-data=$DIR --run-dir=$DIR
    touch $DIR/keyring

    ./ceph-mon --id a \
        --chdir= \
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
            ! kill $(cat $DIR/pidfile) ; then
            break
        fi
        sleep $try
    done

    rm -fr $DIR/store.db
}

# explicitly set the default crush rule
expected=66
run_mon --osd_pool_default_crush_replicated_ruleset $expected
./ceph --format json osd dump | grep '"crush_ruleset":'$expected
grep "osd_pool_default_crush_rule is deprecated " $DIR/log && exit 1
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
./ceph --format json osd dump | grep '"crush_ruleset":'$unexpected && exit 1
grep "osd_pool_default_crush_rule is deprecated " $DIR/log
kill_mon

# osd_pool_default_erasure_code_properties is 
# valid JSON but not of the expected type
run_mon --osd_pool_default_erasure_code_properties 1 
./ceph osd pool create poolA 12 12 erasure 2>&1 | grep 'must be a JSON object'
kill_mon

expected='"foo":"bar"'
# osd_pool_default_erasure_code_properties is JSON
run_mon --osd_pool_default_erasure_code_properties "{$expected}"
./ceph --format json osd dump | grep "$expected" && exit 1
./ceph osd pool create poolA 12 12 erasure
./ceph --format json osd dump | grep "$expected"
kill_mon

# osd_pool_default_erasure_code_properties is plain text
run_mon --osd_pool_default_erasure_code_properties 'foo=bar'
./ceph --format json osd dump | grep "$expected" && exit 1
./ceph osd pool create poolA 12 12 erasure
./ceph --format json osd dump | grep "$expected"
kill_mon

run_mon

# creating an erasure code plugin sets defaults properties
./ceph --format json osd dump > $DIR/osd.json
grep "erasure-code-plugin" $DIR/osd.json && exit 1
./ceph osd pool create erasurecodes 12 12 erasure
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
# compile-command: "cd ../.. ; make TESTS=test/mon/osd-pool-create.sh check"
# End:
