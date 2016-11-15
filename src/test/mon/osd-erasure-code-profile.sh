#!/bin/bash
#
# Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
# Copyright (C) 2014, 2015 Red Hat <contact@redhat.com>
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
source $(dirname $0)/../detect-build-env-vars.sh
source $CEPH_ROOT/qa/workunits/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7220" # git grep '\<7220\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_set() {
    local dir=$1
    local id=$2

    run_mon $dir a || return 1

    local profile=myprofile
    #
    # no key=value pairs : use the default configuration
    #
    ceph osd erasure-code-profile set $profile 2>&1 || return 1
    ceph osd erasure-code-profile get $profile | \
        grep plugin=jerasure || return 1
    ceph osd erasure-code-profile rm $profile
    #
    # key=value pairs override the default
    #
    ceph osd erasure-code-profile set $profile \
        key=value plugin=example || return 1
    ceph osd erasure-code-profile get $profile | \
        grep -e key=value -e plugin=example || return 1
    #
    # --force is required to override an existing profile
    #
    ! ceph osd erasure-code-profile set $profile > $dir/out 2>&1 || return 1
    grep 'will not override' $dir/out || return 1
    ceph osd erasure-code-profile set $profile key=other --force || return 1
    ceph osd erasure-code-profile get $profile | \
        grep key=other || return 1

    ceph osd erasure-code-profile rm $profile # cleanup
}

function TEST_ls() {
    local dir=$1
    local id=$2

    run_mon $dir a || return 1

    local profile=myprofile
    ! ceph osd erasure-code-profile ls | grep $profile || return 1
    ceph osd erasure-code-profile set $profile 2>&1 || return 1
    ceph osd erasure-code-profile ls | grep $profile || return 1
    ceph --format xml osd erasure-code-profile ls | \
        grep "<profile>$profile</profile>" || return 1

    ceph osd erasure-code-profile rm $profile # cleanup
}

function TEST_rm() {
    local dir=$1
    local id=$2

    run_mon $dir a || return 1

    local profile=myprofile
    ceph osd erasure-code-profile set $profile 2>&1 || return 1
    ceph osd erasure-code-profile ls | grep $profile || return 1
    ceph osd erasure-code-profile rm $profile || return 1
    ! ceph osd erasure-code-profile ls | grep $profile || return 1
    ceph osd erasure-code-profile rm WRONG 2>&1 | \
        grep "WRONG does not exist" || return 1

    ceph osd erasure-code-profile set $profile || return 1
    ceph osd pool create poolname 12 12 erasure $profile || return 1
    ! ceph osd erasure-code-profile rm $profile > $dir/out 2>&1 || return 1
    grep "poolname.*using.*$profile" $dir/out || return 1
    ceph osd pool delete poolname poolname --yes-i-really-really-mean-it || return 1
    ceph osd erasure-code-profile rm $profile || return 1

    ceph osd erasure-code-profile rm $profile # cleanup
}

function TEST_get() {
    local dir=$1
    local id=$2

    run_mon $dir a || return 1

    local default_profile=default
    ceph osd erasure-code-profile get $default_profile | \
        grep plugin=jerasure || return 1
    ceph --format xml osd erasure-code-profile get $default_profile | \
        grep '<plugin>jerasure</plugin>' || return 1
    ! ceph osd erasure-code-profile get WRONG > $dir/out 2>&1 || return 1
    grep -q "unknown erasure code profile 'WRONG'" $dir/out || return 1
}

function TEST_set_idempotent() {
    local dir=$1
    local id=$2

    run_mon $dir a || return 1
    #
    # The default profile is set using a code path different from 
    # ceph osd erasure-code-profile set: verify that it is idempotent,
    # as if it was using the same code path.
    #
    ceph osd erasure-code-profile set default k=2 m=1 2>&1 || return 1
    local profile
    #
    # Because plugin=jerasure is the default, it uses a slightly
    # different code path where defaults (m=1 for instance) are added
    # implicitly.
    #
    profile=profileidempotent1
    ! ceph osd erasure-code-profile ls | grep $profile || return 1
    ceph osd erasure-code-profile set $profile k=2 ruleset-failure-domain=osd 2>&1 || return 1
    ceph osd erasure-code-profile ls | grep $profile || return 1
    ceph osd erasure-code-profile set $profile k=2 ruleset-failure-domain=osd 2>&1 || return 1
    ceph osd erasure-code-profile rm $profile # cleanup

    #
    # In the general case the profile is exactly what is on
    #
    profile=profileidempotent2
    ! ceph osd erasure-code-profile ls | grep $profile || return 1
    ceph osd erasure-code-profile set $profile plugin=lrc k=4 m=2 l=3 ruleset-failure-domain=osd 2>&1 || return 1
    ceph osd erasure-code-profile ls | grep $profile || return 1
    ceph osd erasure-code-profile set $profile plugin=lrc k=4 m=2 l=3 ruleset-failure-domain=osd 2>&1 || return 1
    ceph osd erasure-code-profile rm $profile # cleanup
}

function TEST_format_invalid() {
    local dir=$1

    local profile=profile
    # osd_pool_default_erasure-code-profile is
    # valid JSON but not of the expected type
    run_mon $dir a \
        --osd_pool_default_erasure-code-profile 1 || return 1
    ! ceph osd erasure-code-profile set $profile > $dir/out 2>&1 || return 1
    cat $dir/out
    grep 'must be a JSON object' $dir/out || return 1
}

function TEST_format_json() {
    local dir=$1

    # osd_pool_default_erasure-code-profile is JSON
    expected='"plugin":"example"'
    run_mon $dir a \
        --osd_pool_default_erasure-code-profile "{$expected}" || return 1
    ceph --format json osd erasure-code-profile get default | \
        grep "$expected" || return 1
}

function TEST_format_plain() {
    local dir=$1

    # osd_pool_default_erasure-code-profile is plain text
    expected='"plugin":"example"'
    run_mon $dir a \
        --osd_pool_default_erasure-code-profile "plugin=example" || return 1
    ceph --format json osd erasure-code-profile get default | \
        grep "$expected" || return 1
}

function TEST_profile_k_sanity() {
    local dir=$1
    local profile=profile-sanity

    run_mon $dir a || return 1

    expect_failure $dir 'k must be a multiple of (k + m) / l' \
        ceph osd erasure-code-profile set $profile \
        plugin=lrc \
        l=1 \
        k=1 \
        m=1 || return 1

    if erasure_code_plugin_exists isa ; then
        expect_failure $dir 'k=1 must be >= 2' \
            ceph osd erasure-code-profile set $profile \
            plugin=isa \
            k=1 \
            m=1 || return 1
    else
        echo "SKIP because plugin isa has not been built"
    fi

    expect_failure $dir 'k=1 must be >= 2' \
        ceph osd erasure-code-profile set $profile \
        plugin=jerasure \
        k=1 \
        m=1 || return 1
}

main osd-erasure-code-profile "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/mon/osd-erasure-code-profile.sh"
# End:
