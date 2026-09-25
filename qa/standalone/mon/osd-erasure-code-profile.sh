#!/usr/bin/env bash
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
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7220" # git grep '\<7220\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
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
        grep plugin=isa || return 1
    ceph osd erasure-code-profile rm $profile
    #
    # key=value pairs override the default
    #
    ceph osd erasure-code-profile set $profile \
        key=value plugin=jerasure || return 1
    ceph osd erasure-code-profile get $profile | \
        grep -e key=value -e plugin=jerasure || return 1
    #
    # --force & --yes-i-really-mean-it are required to override
    # an existing profile
    #
    ! ceph osd erasure-code-profile set $profile > $dir/out 2>&1 || return 1
    grep 'will not override' $dir/out || return 1
    ceph osd erasure-code-profile set $profile key=other --force --yes-i-really-mean-it || return 1
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
    create_pool poolname 12 12 erasure $profile || return 1
    ! ceph osd erasure-code-profile rm $profile > $dir/out 2>&1 || return 1
    grep "poolname.*using.*$profile" $dir/out || return 1
    ceph osd pool delete poolname poolname --yes-i-really-really-mean-it || return 1
    ceph osd erasure-code-profile rm $profile || return 1

    ceph osd erasure-code-profile rm $profile # cleanup
}

# A pool created while the deletion of a profile's last pool is pending must
# not get the profile, or the rule, that the deletion removes: the monitor
# would crash computing the new pool's PGs with a rule that is gone
function TEST_create_while_profile_removed() {
    local dir=$1

    run_mon $dir a || return 1
    ceph config set mon mon_allow_pool_delete true || return 1
    ceph osd erasure-code-profile set p21 k=2 m=1 crush-failure-domain=osd || return 1
    ceph osd crush rule create-erasure r21 p21 || return 1
    ceph osd pool create a 8 8 erasure p21 r21 || return 1
    ceph tell mon.\* config set paxos_propose_interval 5 || return 1
    # an OSDMap change just committed delays the next proposal
    ceph osd set noout || return 1

    timeout 60 ceph osd pool rm a a --yes-i-really-really-mean-it &
    local pid=$!
    sleep 1
    timeout 60 ceph osd pool create b 8 8 erasure p21 r21 > $dir/out 2>&1
    wait $pid || return 1
    cat $dir/out
    timeout 60 ceph osd pool ls > $dir/pools || return 1
    if grep -qx b $dir/pools; then
        ceph osd erasure-code-profile ls | grep -qx p21 || return 1
    else
        grep "does not exist" $dir/out || return 1
    fi
}

# Deleting an erasure coded pool deletes its profile once no pool uses it,
# never the default profile, and also when the profile's pools are deleted
# in one epoch
function TEST_profile_removed_with_last_pool() {
    local dir=$1

    run_mon $dir a || return 1
    ceph config set mon mon_allow_pool_delete true || return 1
    ceph osd erasure-code-profile set p21 k=2 m=1 crush-failure-domain=osd || return 1
    ceph osd pool create a 8 8 erasure p21 || return 1
    ceph osd pool create b 8 8 erasure p21 || return 1
    ceph osd pool rm a a --yes-i-really-really-mean-it || return 1
    ceph osd erasure-code-profile ls | grep -qx p21 || return 1
    ceph osd pool rm b b --yes-i-really-really-mean-it || return 1
    ! ceph osd erasure-code-profile ls | grep -qx p21 || return 1

    ceph osd pool create d 8 8 erasure || return 1
    ceph osd pool rm d d --yes-i-really-really-mean-it || return 1
    ceph osd erasure-code-profile ls | grep -qx default || return 1

    ceph osd erasure-code-profile set q21 k=2 m=1 crush-failure-domain=osd || return 1
    ceph osd pool create c1 8 8 erasure q21 || return 1
    ceph osd pool create c2 8 8 erasure q21 || return 1
    ceph tell mon.\* config set paxos_propose_interval 5 || return 1
    ceph osd pool rm c1 c1 --yes-i-really-really-mean-it &
    local pid=$!
    ceph osd pool rm c2 c2 --yes-i-really-really-mean-it || return 1
    wait $pid || return 1
    ! ceph osd erasure-code-profile ls | grep -qx q21 || return 1
}

function TEST_get() {
    local dir=$1
    local id=$2

    run_mon $dir a || return 1

    local default_profile=default
    ceph osd erasure-code-profile get $default_profile | \
        grep plugin=isa || return 1
    ceph --format xml osd erasure-code-profile get $default_profile | \
        grep '<plugin>isa</plugin>' || return 1
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
    ceph osd erasure-code-profile set default k=2 m=2 2>&1 || return 1
    local profile
    #
    # Because plugin=jerasure is the default, it uses a slightly
    # different code path where defaults (m=1 for instance) are added
    # implicitly.
    #
    profile=profileidempotent1
    ! ceph osd erasure-code-profile ls | grep $profile || return 1
    ceph osd erasure-code-profile set $profile k=2 crush-failure-domain=osd 2>&1 || return 1
    ceph osd erasure-code-profile ls | grep $profile || return 1
    ceph osd erasure-code-profile set $profile k=2 crush-failure-domain=osd 2>&1 || return 1
    ceph osd erasure-code-profile rm $profile # cleanup

    #
    # In the general case the profile is exactly what is on
    #
    profile=profileidempotent2
    ! ceph osd erasure-code-profile ls | grep $profile || return 1
    ceph osd erasure-code-profile set $profile plugin=lrc k=4 m=2 l=3 crush-failure-domain=osd 2>&1 || return 1
    ceph osd erasure-code-profile ls | grep $profile || return 1
    ceph osd erasure-code-profile set $profile plugin=lrc k=4 m=2 l=3 crush-failure-domain=osd 2>&1 || return 1
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
    expected='"plugin":"isa"'
    run_mon $dir a \
        --osd_pool_default_erasure-code-profile "{$expected}" || return 1
    ceph --format json osd erasure-code-profile get default | \
        grep "$expected" || return 1
}

function TEST_format_plain() {
    local dir=$1

    # osd_pool_default_erasure-code-profile is plain text
    expected='"plugin":"isa"'
    run_mon $dir a \
        --osd_pool_default_erasure-code-profile "plugin=isa" || return 1
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

function TEST_invalid_crush_failure_domain() {
    local dir=$1

    run_mon $dir a || return 1

    local profile=ec_profile
    local crush_failure_domain=invalid_failure_domain

    ! ceph osd erasure-code-profile set $profile k=4 m=2 crush-failure-domain=$crush_failure_domain 2>&1 || return 1
}

# A pool created while the deletion of the last pool of the rule it would
# reuse by name is pending must not get that rule: the monitor would crash
# computing the new pool's PGs with a rule that is gone
function TEST_create_while_rule_of_its_name_removed() {
    local dir=$1

    run_mon $dir a || return 1
    ceph osd erasure-code-profile set prof k=2 m=1 crush-failure-domain=osd || return 1
    # keeps the profile while the rule named a goes
    ceph osd pool create keep 8 8 erasure prof || return 1
    ceph osd pool create a 8 8 erasure prof || return 1
    ceph osd pool rename a z || return 1
    ceph tell mon.\* config set paxos_propose_interval 5 || return 1
    # an OSDMap change just committed delays the next proposal
    ceph osd set noout || return 1

    timeout 60 ceph osd pool rm z z --yes-i-really-really-mean-it &
    local pid=$!
    sleep 1
    timeout 60 ceph osd pool create a 8 8 erasure prof || return 1
    wait $pid || return 1
    timeout 60 ceph osd pool get a crush_rule > $dir/rule || return 1
    ceph osd crush rule ls | grep -qx "$(awk '{print $2}' $dir/rule)" || return 1
}

# A profile set again while its removal with its last pool is pending exists
# afterwards
function TEST_profile_set_while_removed() {
    local dir=$1

    run_mon $dir a || return 1
    ceph osd erasure-code-profile set prof k=2 m=1 crush-failure-domain=osd || return 1
    ceph osd pool create a 8 8 erasure prof || return 1
    ceph tell mon.\* config set paxos_propose_interval 5 || return 1
    # an OSDMap change just committed delays the next proposal
    ceph osd set noout || return 1

    timeout 60 ceph osd pool rm a a --yes-i-really-really-mean-it &
    local pid=$!
    sleep 1
    timeout 60 ceph osd erasure-code-profile set prof k=2 m=1 \
        crush-failure-domain=osd || return 1
    wait $pid || return 1
    ceph osd erasure-code-profile ls | grep -qx prof || return 1
}

main osd-erasure-code-profile "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/mon/osd-erasure-code-profile.sh"
# End:
