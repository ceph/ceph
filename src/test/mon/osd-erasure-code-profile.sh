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

    local id=a
    call_TEST_functions $dir $id --public-addr 127.0.0.1 || return 1
}

function SHARE_MON_TEST_set() {
    local dir=$1
    local id=$2

    local profile=myprofile
    ./ceph osd set erasure_code_profile $profile 2>&1 || return 1
    ./ceph osd get erasure_code_profile $profile | \
        grep plugin=jerasure || return 1
    ./ceph osd set erasure_code_profile $profile key=value || return 1
    ./ceph osd get erasure_code_profile $profile | \
        grep key=value || return 1
}

function SHARE_MON_TEST_get() {
    local dir=$1
    local id=$2

    local default_profile=default
    ./ceph osd get erasure_code_profile $default_profile | \
        grep plugin=jerasure || return 1
    ./ceph --format xml osd get erasure_code_profile $default_profile | \
        grep '<plugin>jerasure</plugin>' || return 1
    ! ./ceph osd get erasure_code_profile WRONG > $dir/out 2>&1 || return 1
    grep -q "unknown erasure code profile 'WRONG'" $dir/out || return 1
}

function SHARE_MON_TEST_set_pending() {
    local dir=$1
    local id=$2

    # try again if the profile is pending
    local profile=profile
    # add to the pending OSD map without triggering a paxos proposal
    result=$(echo '{"prefix":"osdmonitor_prepare_command","prepare":"osd set erasure_code_profile","name":"'$profile'"}' | nc -U $dir/$id/ceph-mon.$id.asok | cut --bytes=5-)
    test $result = true || return 1
    ./ceph osd set erasure_code_profile $profile || return 1
    grep "$profile try again" $dir/$id/log || return 1
}

function TEST_format_invalid() {
    local dir=$1

    local profile=profile
    # osd_pool_default_erasure_code_profile is
    # valid JSON but not of the expected type
    run_mon $dir a --public-addr 127.0.0.1 \
        --osd_pool_default_erasure_code_profile 1
    ! ./ceph osd set erasure_code_profile $profile > $dir/out 2>&1 || return 1
    cat $dir/out
    grep 'must be a JSON object' $dir/out || return 1
}

function TEST_format_json() {
    local dir=$1

    # osd_pool_default_erasure_code_profile is JSON
    expected='"plugin":"example"'
    run_mon $dir a --public-addr 127.0.0.1 \
        --osd_pool_default_erasure_code_profile "{$expected}"
    ./ceph --format json osd get erasure_code_profile default | \
        grep "$expected" || return 1
}

function TEST_format_plain() {
    local dir=$1

    # osd_pool_default_erasure_code_profile is plain text
    expected='"plugin":"example"'
    run_mon $dir a --public-addr 127.0.0.1 \
        --osd_pool_default_erasure_code_profile "plugin=example"
    ./ceph --format json osd get erasure_code_profile default | \
        grep "$expected" || return 1
}

main osd-erasure-code-profile

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/mon/osd-erasure-code-profile.sh"
# End:
