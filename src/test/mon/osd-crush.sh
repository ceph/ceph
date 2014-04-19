#!/bin/bash
#
# Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
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

    setup $dir || return 1
    run_mon $dir a --public-addr 127.0.0.1
    FUNCTIONS=${FUNCTIONS:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for TEST_function in $FUNCTIONS ; do
        if ! $TEST_function $dir ; then
            cat $dir/a/log
            return 1
        fi
    done
    teardown $dir || return 1
}

function TEST_crush_rule_create_simple() {
    local dir=$1
    ./ceph --format xml osd crush rule dump replicated_ruleset | \
        egrep '<op>take</op><item>[^<]+</item><item_name>default</item_name>' | \
        grep '<op>chooseleaf_firstn</op><num>0</num><type>host</type>' || return 1
    local ruleset=ruleset0
    local root=host1
    ./ceph osd crush add-bucket $root host
    local failure_domain=osd
    ./ceph osd crush rule create-simple $ruleset $root $failure_domain || return 1
    ./ceph osd crush rule create-simple $ruleset $root $failure_domain 2>&1 | \
        grep "$ruleset already exists" || return 1
    ./ceph --format xml osd crush rule dump $ruleset | \
        egrep '<op>take</op><item>[^<]+</item><item_name>'$root'</item_name>' | \
        grep '<op>choose_firstn</op><num>0</num><type>'$failure_domain'</type>' || return 1
    ./ceph osd crush rule rm $ruleset || return 1
}

function TEST_crush_rule_dump() {
    local dir=$1
    local ruleset=ruleset1
    ./ceph osd crush rule create-erasure $ruleset || return 1
    local expected
    expected="<rule_name>$ruleset</rule_name>"
    ./ceph --format xml osd crush rule dump $ruleset | grep $expected || return 1
    expected='"rule_name": "'$ruleset'"'
    ./ceph osd crush rule dump | grep "$expected" || return 1
    ! ./ceph osd crush rule dump non_existent_ruleset || return 1
    ./ceph osd crush rule rm $ruleset || return 1
}

function TEST_crush_rule_rm() {
    local ruleset=erasure2
    ./ceph osd crush rule create-erasure $ruleset default || return 1
    ./ceph osd crush rule ls | grep $ruleset || return 1
    ./ceph osd crush rule rm $ruleset || return 1
    ! ./ceph osd crush rule ls | grep $ruleset || return 1
}

function TEST_crush_rule_create_simple_exists() {
    local dir=$1
    local ruleset=ruleset2
    local root=default
    local failure_domain=host
    # add to the pending OSD map without triggering a paxos proposal
    result=$(echo '{"prefix":"osdmonitor_prepare_command","prepare":"osd crush rule create-simple","name":"'$ruleset'","root":"'$root'","type":"'$failure_domain'"}' | nc -U $dir/a/ceph-mon.a.asok | cut --bytes=5-)
    test $result = true || return 1
    ./ceph osd crush rule create-simple $ruleset $root $failure_domain 2>&1 | \
        grep "$ruleset already exists" || return 1
    ./ceph osd crush rule rm $ruleset || return 1
}

function TEST_crush_rule_create_erasure() {
    local dir=$1
    local ruleset=ruleset3
    ./ceph osd crush rule create-erasure $ruleset || return 1
    ./ceph osd crush rule create-erasure $ruleset 2>&1 | \
        grep "$ruleset already exists" || return 1
    ./ceph --format xml osd crush rule dump $ruleset | \
        egrep '<op>take</op><item>[^<]+</item><item_name>default</item_name>' | \
        grep '<op>chooseleaf_indep</op><num>0</num><type>host</type>' || return 1
    ./ceph osd crush rule rm $ruleset || return 1
    ! ./ceph osd crush rule ls | grep $ruleset || return 1
    ./ceph osd crush rule create-erasure $ruleset default || return 1
    ./ceph osd crush rule ls | grep $ruleset || return 1
    ./ceph osd crush rule rm $ruleset || return 1
}

function TEST_crush_rule_create_erasure_exists() {
    local dir=$1
    local ruleset=ruleset5
    # add to the pending OSD map without triggering a paxos proposal
    result=$(echo '{"prefix":"osdmonitor_prepare_command","prepare":"osd crush rule create-erasure","name":"'$ruleset'"}' | nc -U $dir/a/ceph-mon.a.asok | cut --bytes=5-)
    test $result = true || return 1
    ./ceph osd crush rule create-erasure $ruleset 2>&1 | \
        grep "$ruleset already exists" || return 1
    ./ceph osd crush rule rm $ruleset || return 1
}

main osd-crush

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/mon/osd-crush.sh"
# End:
