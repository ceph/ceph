#!/usr/bin/env bash
#
# Copyright (C) 2022 Red Hat <contact@redhat.com>
#
# Author: Sridhar Seshasayee <sseshasa@redhat.com>
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

    export CEPH_MON="127.0.0.1:7124" # git grep '\<7124\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--debug-bluestore 20 "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_profile_builtin_to_custom() {
    local dir=$1
    local OSDS=3

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=$OSDS || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd --osd_op_queue=mclock_scheduler || return 1
    done

    # Verify that the default mclock profile is set on the OSDs
    for id in $(seq 0 $(expr $OSDS - 1))
    do
      local mclock_profile=$(ceph config get osd.$id osd_mclock_profile)
      test "$mclock_profile" = "high_client_ops" || return 1
    done

    # Change the mclock profile to 'custom'
    ceph config set osd osd_mclock_profile custom || return 1

    # Verify that the mclock profile is set to 'custom' on the OSDs
    for id in $(seq 0 $(expr $OSDS - 1))
    do
      local mclock_profile=$(ceph config get osd.$id osd_mclock_profile)
      test "$mclock_profile" = "custom" || return 1
    done

    # Change a mclock config param and confirm the change
    local client_res=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path \
      osd.$id) config get osd_mclock_scheduler_client_res | \
      jq .osd_mclock_scheduler_client_res | bc)
    echo "client_res = $client_res"
    local client_res_new=$(expr $client_res + 10)
    echo "client_res_new = $client_res_new"
    ceph config set osd osd_mclock_scheduler_client_res \
      $client_res_new || return 1
    for id in $(seq 0 $(expr $OSDS - 1))
    do
      # Check value in config monitor db
      local res=$(ceph config get osd.$id \
        osd_mclock_scheduler_client_res) || return 1
      test $res -eq $client_res_new || return 1
      # Check value in the in-memory 'values' map
      res=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path \
        osd.$id) config get osd_mclock_scheduler_client_res | \
        jq .osd_mclock_scheduler_client_res | bc)
      test $res -eq $client_res_new || return 1
    done

    teardown $dir || return 1
}

function TEST_profile_custom_to_builtin() {
    local dir=$1
    local OSDS=3

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=$OSDS || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd --osd_op_queue=mclock_scheduler || return 1
    done

    # Verify that the default mclock profile is set on the OSDs
    for id in $(seq 0 $(expr $OSDS - 1))
    do
      local mclock_profile=$(ceph config get osd.$id osd_mclock_profile)
      test "$mclock_profile" = "high_client_ops" || return 1
    done

    # Change the mclock profile to 'custom'
    ceph config set osd osd_mclock_profile custom || return 1

    # Verify that the mclock profile is set to 'custom' on the OSDs
    for id in $(seq 0 $(expr $OSDS - 1))
    do
      local mclock_profile=$(ceph config get osd.$id osd_mclock_profile)
      test "$mclock_profile" = "custom" || return 1
    done

    # Save the original client reservations allocated to the OSDs
    local client_res=()
    for id in $(seq 0 $(expr $OSDS - 1))
    do
      client_res+=( $(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path \
        osd.$id) config get osd_mclock_scheduler_client_res | \
        jq .osd_mclock_scheduler_client_res | bc) )
      echo "Original client_res for osd.$id = ${client_res[$id]}"
    done

    # Change a mclock config param and confirm the change
    local client_res_new=$(expr ${client_res[0]} + 10)
    echo "client_res_new = $client_res_new"
    ceph config set osd osd_mclock_scheduler_client_res \
      $client_res_new || return 1
    for id in $(seq 0 $(expr $OSDS - 1))
    do
      # Check value in config monitor db
      local res=$(ceph config get osd.$id \
        osd_mclock_scheduler_client_res) || return 1
      test $res -eq $client_res_new || return 1
      # Check value in the in-memory 'values' map
      res=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path \
        osd.$id) config get osd_mclock_scheduler_client_res | \
        jq .osd_mclock_scheduler_client_res | bc)
      test $res -eq $client_res_new || return 1
    done

    # Switch the mclock profile back to the original built-in profile.
    # The config subsystem prevents the overwrite of the changed QoS config
    # option above i.e. osd_mclock_scheduler_client_res. This fact is verified
    # before proceeding to remove the entry from the config monitor db. After
    # the config entry is removed, the original value for the config option is
    # restored and is verified.
    ceph config set osd osd_mclock_profile high_client_ops || return 1
    # Verify that the mclock profile is set to 'high_client_ops' on the OSDs
    for id in $(seq 0 $(expr $OSDS - 1))
    do
      local mclock_profile=$(ceph config get osd.$id osd_mclock_profile)
      test "$mclock_profile" = "high_client_ops" || return 1
    done

    # Verify that the new value is still in effect
    for id in $(seq 0 $(expr $OSDS - 1))
    do
      # Check value in config monitor db
      local res=$(ceph config get osd.$id \
        osd_mclock_scheduler_client_res) || return 1
      test $res -eq $client_res_new || return 1
      # Check value in the in-memory 'values' map
      res=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path \
        osd.$id) config get osd_mclock_scheduler_client_res | \
        jq .osd_mclock_scheduler_client_res | bc)
      test $res -eq $client_res_new || return 1
    done

    # Remove the changed QoS config option from monitor db
    ceph config rm osd osd_mclock_scheduler_client_res || return 1

    # Verify that the original values are now restored
    for id in $(seq 0 $(expr $OSDS - 1))
    do
      # Check value in the in-memory 'values' map
      res=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path \
        osd.$id) config get osd_mclock_scheduler_client_res | \
        jq .osd_mclock_scheduler_client_res | bc)
      test $res -eq ${client_res[$id]} || return 1
    done

    teardown $dir || return 1
}

main test-mclock-profile-switch "$@"

# Local Variables:
# compile-command: "cd build ; make -j4 && \
#   ../qa/run-standalone.sh test-mclock-profile-switch.sh"
# End:
