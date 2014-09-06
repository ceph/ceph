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
    #
    # no key=value pairs : use the default configuration
    #
    ./ceph osd erasure-code-profile set $profile 2>&1 || return 1
    ./ceph osd erasure-code-profile get $profile | \
        grep plugin=jerasure || return 1
    ./ceph osd erasure-code-profile rm $profile
    #
    # key=value pairs override the default
    #
    ./ceph osd erasure-code-profile set $profile \
        key=value plugin=example || return 1
    ./ceph osd erasure-code-profile get $profile | \
        grep -e key=value -e plugin=example || return 1
    #
    # --force is required to override an existing profile
    #
    ! ./ceph osd erasure-code-profile set $profile > $dir/out 2>&1 || return 1
    grep 'will not override' $dir/out || return 1
    ./ceph osd erasure-code-profile set $profile key=other --force || return 1
    ./ceph osd erasure-code-profile get $profile | \
        grep key=other || return 1

    ./ceph osd erasure-code-profile rm $profile # cleanup
}

function SHARE_MON_TEST_set_pending() {
    local dir=$1
    local id=$2

    # try again if the profile is pending
    local profile=profile
    CEPH_ARGS='' ./ceph --mon-host=127.0.0.1 tell 'mon.*' injectargs -- --mon-debug-idempotency || return 1
    ./ceph osd erasure-code-profile set $profile --force || return 1
    CEPH_ARGS='' ./ceph --mon-host=127.0.0.1 tell 'mon.*' injectargs -- --no-mon-debug-idempotency || return 1
    CEPH_ARGS='' ./ceph --admin-daemon $dir/$id/ceph-mon.$id.asok log flush || return 1
    grep "$profile try again" $dir/$id/log || return 1

    ./ceph osd erasure-code-profile rm $profile # cleanup
}

function SHARE_MON_TEST_ls() {
    local dir=$1
    local id=$2

    local profile=myprofile
    ! ./ceph osd erasure-code-profile ls | grep $profile || return 1
    ./ceph osd erasure-code-profile set $profile 2>&1 || return 1
    ./ceph osd erasure-code-profile ls | grep $profile || return 1
    ./ceph --format xml osd erasure-code-profile ls | \
        grep "<profile>$profile</profile>" || return 1

    ./ceph osd erasure-code-profile rm $profile # cleanup
}

function SHARE_MON_TEST_rm() {
    local dir=$1
    local id=$2

    local profile=myprofile
    ./ceph osd erasure-code-profile set $profile 2>&1 || return 1
    ./ceph osd erasure-code-profile ls | grep $profile || return 1
    ./ceph osd erasure-code-profile rm $profile || return 1
    ! ./ceph osd erasure-code-profile ls | grep $profile || return 1
    ./ceph osd erasure-code-profile rm WRONG 2>&1 | \
        grep "WRONG does not exist" || return 1

    ./ceph osd erasure-code-profile set $profile || return 1
    ./ceph osd pool create poolname 12 12 erasure $profile || return 1
    ! ./ceph osd erasure-code-profile rm $profile > $dir/out 2>&1 || return 1
    grep "poolname.*using.*$profile" $dir/out || return 1
    ./ceph osd pool delete poolname poolname --yes-i-really-really-mean-it || return 1
    ./ceph osd erasure-code-profile rm $profile || return 1

    ./ceph osd erasure-code-profile rm $profile # cleanup
}

function SHARE_MON_TEST_rm_pending() {
    local dir=$1
    local id=$2

    # try again if the profile is pending
    local profile=myprofile
    ./ceph osd erasure-code-profile ls
    # add to the pending OSD map without triggering a paxos proposal
    result=$(echo '{"prefix":"osdmonitor_prepare_command","prepare":"osd erasure-code-profile set","name":"'$profile'"}' | nc -U $dir/$id/ceph-mon.$id.asok | cut --bytes=5-)
    test $result = true || return 1
    ./ceph osd erasure-code-profile rm $profile || return 1
    CEPH_ARGS='' ./ceph --admin-daemon $dir/$id/ceph-mon.$id.asok log flush || return 1
    grep "$profile: creation canceled" $dir/$id/log || return 1
}

function SHARE_MON_TEST_get() {
    local dir=$1
    local id=$2

    local default_profile=default
    ./ceph osd erasure-code-profile get $default_profile | \
        grep plugin=jerasure || return 1
    ./ceph --format xml osd erasure-code-profile get $default_profile | \
        grep '<plugin>jerasure</plugin>' || return 1
    ! ./ceph osd erasure-code-profile get WRONG > $dir/out 2>&1 || return 1
    grep -q "unknown erasure code profile 'WRONG'" $dir/out || return 1
}

function TEST_format_invalid() {
    local dir=$1

    local profile=profile
    # osd_pool_default_erasure-code-profile is
    # valid JSON but not of the expected type
    run_mon $dir a --public-addr 127.0.0.1 \
        --osd_pool_default_erasure-code-profile 1
    ! ./ceph osd erasure-code-profile set $profile > $dir/out 2>&1 || return 1
    cat $dir/out
    grep 'must be a JSON object' $dir/out || return 1
}

function TEST_format_json() {
    local dir=$1

    # osd_pool_default_erasure-code-profile is JSON
    expected='"plugin":"example"'
    run_mon $dir a --public-addr 127.0.0.1 \
        --osd_pool_default_erasure-code-profile "{$expected}"
    ./ceph --format json osd erasure-code-profile get default | \
        grep "$expected" || return 1
}

function TEST_format_plain() {
    local dir=$1

    # osd_pool_default_erasure-code-profile is plain text
    expected='"plugin":"example"'
    run_mon $dir a --public-addr 127.0.0.1 \
        --osd_pool_default_erasure-code-profile "plugin=example"
    ./ceph --format json osd erasure-code-profile get default | \
        grep "$expected" || return 1
}

main osd-erasure-code-profile

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/mon/osd-erasure-code-profile.sh"
# End:
