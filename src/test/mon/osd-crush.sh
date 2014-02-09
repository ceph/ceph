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


function TEST_crush_rule_dump() {
    local dir=$1
    local ruleset=ruleset
    ./ceph osd crush rule create-erasure $ruleset || return 1
    local expected
    expected="<rule_name>$ruleset</rule_name>"
    ./ceph osd crush rule dump $ruleset xml | grep $expected || return 1
    ./ceph osd crush rule dump $ruleset xml-pretty | grep $expected || return 1
    expected='"rule_name":"'$ruleset'"'
    ./ceph osd crush rule dump $ruleset json | grep "$expected" || return 1
    expected='"rule_name": "'$ruleset'"'
    ./ceph osd crush rule dump $ruleset json-pretty | grep "$expected" || return 1
    ./ceph osd crush rule dump | grep "$expected" || return 1
    ! ./ceph osd crush rule dump non_existent_ruleset || return 1
    ./ceph osd crush rule rm $ruleset || return 1
}

function TEST_crush_rule_all() {
    local dir=$1
    local crush_ruleset=erasure2
    ! ./ceph osd crush rule ls | grep $crush_ruleset || return 1
    ./ceph osd crush rule create-erasure $crush_ruleset || return 1
    ./ceph osd crush rule ls | grep $crush_ruleset || return 1

    ./ceph osd crush rule create-erasure $crush_ruleset || return 1

    ./ceph osd crush dump | grep $crush_ruleset || return 1

    ./ceph osd crush rule rm $crush_ruleset || return 1
    ! ./ceph osd crush rule ls | grep $crush_ruleset || return 1
}

main osd-crush

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/mon/osd-crush.sh"
# End:
