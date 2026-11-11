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
    # a profile that no pool uses changes without flags
    #
    ceph osd erasure-code-profile set $profile key=other || return 1
    ceph osd erasure-code-profile get $profile | \
        grep key=other || return 1

    ceph osd erasure-code-profile rm $profile # cleanup
}

# osd erasure-code-profile set changes an existing profile without flags
# while no pool uses it, and needs --yes-i-really-mean-it once one does.
# This test changes myprofile from k=2 to k=3, creates erasure coded pool
# poolname on it and then sets k=4. That is refused, with a message naming
# poolname and --yes-i-really-mean-it, and also with --force, and the
# profile keeps k=3; with --yes-i-really-mean-it it gets k=4. The default
# profile, which no pool uses, is changed without flags.
function TEST_set_in_use() {
    local dir=$1

    run_mon $dir a || return 1
    local profile=myprofile
    ceph osd erasure-code-profile set $profile k=2 m=1 || return 1
    ceph osd erasure-code-profile set $profile k=3 m=1 || return 1
    create_pool poolname 12 12 erasure $profile || return 1
    ! ceph osd erasure-code-profile set $profile k=4 m=1 > $dir/out 2>&1 || return 1
    grep "will not override.*poolname.*--yes-i-really-mean-it" $dir/out || return 1
    ! ceph osd erasure-code-profile set $profile k=4 m=1 --force || return 1
    ceph osd erasure-code-profile get $profile | grep k=3 || return 1
    ceph osd erasure-code-profile set $profile k=4 m=1 --yes-i-really-mean-it || return 1
    ceph osd erasure-code-profile get $profile | grep k=4 || return 1
    # no pool uses default
    ceph osd erasure-code-profile set default k=3 m=2 || return 1
    ceph osd erasure-code-profile get default | grep k=3 || return 1
    ceph osd pool delete poolname poolname --yes-i-really-really-mean-it || return 1
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

# When a pool is deleted, its erasure code profile and its rule are removed
# too if no other pool uses them. A pool created while that deletion is
# pending must not take the profile or the rule, or the monitor would crash
# computing the new pool's PGs with a rule that is gone. This test deletes
# pool a, the only pool on profile p21 and rule r21, and one second later,
# while a paxos_propose_interval of 5 seconds keeps the deletion pending,
# creates pool b on p21 and r21. Either b is created on r21, and p21 and
# r21 are kept, or the create is refused as p21 or r21 no longer exists;
# either way the monitor still answers.
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
        ceph osd crush rule ls | grep -qx r21 || return 1
        test "$(ceph osd pool get b crush_rule | awk '{print $2}')" = r21 || return 1
    else
        grep "does not exist" $dir/out || return 1
    fi
}

# Deleting an erasure coded pool removes its erasure code profile once no
# other pool uses it, but never the default profile. This test deletes
# pools a and b, both on profile p21: p21 stays after the first and goes
# with the second. Deleting pool d, created with no profile, leaves the
# default profile. Pools c1 and c2, on q21, are deleted at the same time,
# with a paxos_propose_interval of 5 seconds to put both deletions in one
# OSDMap update, and q21 goes too.
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

# osd pool create gives an erasure coded pool the rule named after the pool
# if there is one, and otherwise makes it from the pool's profile. A pool
# created while a pending update removes that rule with its last pool must
# not get it, or the monitor would crash computing the new pool's PGs with
# a rule that is gone. This test creates pool a on profile prof, which
# gives it rule a, and renames the pool z; pool keep, also on prof, keeps
# the profile. It deletes z and one second later, while a
# paxos_propose_interval of 5 seconds keeps the deletion pending, creates a
# new pool a on prof. The create succeeds, and the new pool's rule exists.
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

# osd erasure-code-profile set, run while a pending update removes the
# profile with its last pool, waits for that update and then sets the
# profile. This test deletes pool a, the only pool on profile prof, and one
# second later, while a paxos_propose_interval of 5 seconds keeps the
# deletion pending, sets prof again with the same values. prof exists
# afterwards.
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
