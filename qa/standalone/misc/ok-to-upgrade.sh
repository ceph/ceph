#!/usr/bin/env bash
#
# Copyright (C) 2025 IBM
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

    export CEPH_MON="127.0.0.1:7170" # git grep '\<7170\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    export ORIG_CEPH_ARGS="$CEPH_ARGS"

    local funcs=${@:-$(set | ${SED} -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        kill_daemons $dir KILL || return 1
        teardown $dir || return 1
    done
}

function TEST_ok_to_upgrade_invalid_args() {
    local dir=$1
    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON "

    run_mon $dir a --public-addr=$CEPH_MON || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 --osd-mclock-skip-benchmark=true || return 1

    # test with no args
    ! ceph osd ok-to-upgrade || return 1

    # test with invalid crush bucket name
    local invalid_bucket="foo"
    local ceph_version_short="01.00.00-gversion-test"
    ! ceph osd ok-to-upgrade $invalid_bucket $ceph_version_short || return 1

    # test with 'root' crush bucket name
    invalid_bucket="default"
    local ceph_version_short="01.00.00-gversion-test"
    ! ceph osd ok-to-upgrade $invalid_bucket $ceph_version_short || return 1
}

function TEST_ok_to_upgrade_replicated_pool() {
    local dir=$1
    local poolname="test"
    local OSDS=10
    local ceph_version="01.00.00-gversion-test"

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON "

    run_mon $dir a --public-addr=$CEPH_MON || return 1
    run_mgr $dir x || return 1

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd --osd-mclock-skip-benchmark=true || return 1
    done

    create_pool $poolname 32 32
    ceph osd pool set $poolname min_size 1
    sleep 5

    wait_for_clean || return 1

    # Test for upgradability with min_size=1
    local exp_osds_upgradable=3
    local crush_bucket=$(ceph osd tree | grep host | awk '{ print $4 }')
    local res=$(ceph osd ok-to-upgrade $crush_bucket $ceph_version --format=json)
    # Specifying hostname as the crush bucket with a 3x replicated pool on 10 OSDs
    # and with the default 'mgr_osd_upgrade_check_convergence_factor' would result
    # in 3 OSDs being reported as upgradable.
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = true || return 1
    local num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length' | bc)
    test $exp_osds_upgradable = $num_osds_upgradable || return 1
    local num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length' | bc)
    test $num_osds_upgraded -eq 0 || return 1

    # Test for upgradability with min_size=1, 1 OSD to upgrade and max=3.
    # This tests the functionality of the 'max' parameter and checks the
    # logic to find more OSDs in the crush bucket.
    local max=3
    crush_bucket="osd.0"
    res=$(ceph osd ok-to-upgrade $crush_bucket $ceph_version $max --format=json)
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = true || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length' | bc)
    test $exp_osds_upgradable = $num_osds_upgradable || return 1
    test $max = $num_osds_upgradable || return 1
    num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length' | bc)
    test $num_osds_upgraded -eq 0 || return 1

    # Test for upgradability with min_size=2
    ceph osd pool set $poolname min_size 2
    sleep 5
    wait_for_clean || return 1
    exp_osds_upgradable=1
    crush_bucket=$(ceph osd tree | grep host | awk '{ print $4 }')
    res=$(ceph osd ok-to-upgrade $crush_bucket $ceph_version --format=json)
    # 1 OSD should be reported as upgradable.
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = true || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length' | bc)
    test $exp_osds_upgradable = $num_osds_upgradable || return 1
    num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length' | bc)
    test $num_osds_upgraded -eq 0 || return 1

    # Test for upgradability with min_size=3
    ceph osd pool set $poolname min_size 3
    sleep 5
    wait_for_clean || return 1
    exp_osds_upgradable=0
    res=$(ceph osd ok-to-upgrade $crush_bucket $ceph_version --format=json)
    # No OSD should be reported as upgradable.
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = false || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length' | bc)
    test $exp_osds_upgradable = $num_osds_upgradable || return 1
    num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length' | bc)
    test $num_osds_upgraded -eq 0 || return 1

    # Test for condition when all OSDs are running desired version.
    upgrade_version=$(ceph osd metadata 0 --format=json | \
      jq '.ceph_version_short' | sed 's/"//g')
    res=$(ceph osd ok-to-upgrade $crush_bucket $upgrade_version --format=json)
    test $(echo $res | jq '.all_osds_upgraded') = true || return 1
    test $(echo $res | jq '.ok_to_upgrade') = false || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length' | bc)
    test $num_osds_upgradable -eq 0 || return 1
    num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length' | bc)
    test $num_osds_upgraded -eq $OSDS || return 1
}

function TEST_ok_to_upgrade_erasure_pool() {
    local dir=$1
    local poolname="ec"
    local OSDS=10
    local ceph_version="01.00.00-gversion-test"

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON "

    run_mon $dir a --public-addr=$CEPH_MON || return 1
    run_mgr $dir x || return 1

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd --osd-mclock-skip-benchmark=true || return 1
    done

    ceph osd erasure-code-profile set ec-profile m=3 k=5 crush-failure-domain=osd || return 1
    ceph osd pool create $poolname erasure ec-profile || return 1
    ceph osd pool set $poolname min_size 5
    sleep 5

    wait_for_clean || return 1

    # Test for upgradability with min_size=5
    local exp_osds_upgradable=3
    local crush_bucket=$(ceph osd tree | grep host | awk '{ print $4 }')
    local res=$(ceph osd ok-to-upgrade $crush_bucket $ceph_version --format=json)
    # Specifying hostname as the crush bucket with a ec5+3 pool on 10 OSDs
    # and with the default 'mgr_osd_upgrade_check_convergence_factor' would result
    # in 3 OSDs being reported as upgradable.
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = true || return 1
    local num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length' | bc)
    test $exp_osds_upgradable = $num_osds_upgradable || return 1
    local num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length' | bc)
    test $num_osds_upgraded -eq 0 || return 1

    # Test for upgradability with min_size=1, 1 OSD to upgrade and max=3.
    # This tests the functionality of the 'max' parameter and also checks
    # the logic to find more OSDs in the crush bucket.
    local max=3
    crush_bucket="osd.0"
    res=$(ceph osd ok-to-upgrade $crush_bucket $ceph_version $max --format=json)
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = true || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length' | bc)
    test $exp_osds_upgradable = $num_osds_upgradable || return 1
    test $max = $num_osds_upgradable || return 1
    num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length' | bc)
    test $num_osds_upgraded -eq 0 || return 1

    # Test for upgradability with min_size=6
    ceph osd pool set $poolname min_size 6
    sleep 5
    wait_for_clean || return 1
    exp_osds_upgradable=2
    crush_bucket=$(ceph osd tree | grep host | awk '{ print $4 }')
    res=$(ceph osd ok-to-upgrade $crush_bucket $ceph_version --format=json)
    # 2 OSDs should be reported as upgradable.
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = true || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length' | bc)
    test $exp_osds_upgradable = $num_osds_upgradable || return 1
    num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length' | bc)
    test $num_osds_upgraded -eq 0 || return 1

    # Test for upgradability with min_size=8
    ceph osd pool set $poolname min_size 8
    sleep 5
    wait_for_clean || return 1
    exp_osds_upgradable=0
    res=$(ceph osd ok-to-upgrade $crush_bucket $ceph_version --format=json)
    # No OSD should be reported as upgradable.
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = false || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length' | bc)
    test $exp_osds_upgradable = $num_osds_upgradable || return 1
    num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length' | bc)
    test $num_osds_upgraded -eq 0 || return 1

    # Test for condition when all OSDs are running desired version.
    ceph_version=$(ceph osd metadata 0 --format=json | \
      jq '.ceph_version_short' | sed 's/"//g')
    res=$(ceph osd ok-to-upgrade $crush_bucket $ceph_version --format=json)
    test $(echo $res | jq '.all_osds_upgraded') = true || return 1
    test $(echo $res | jq '.ok_to_upgrade') = false || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length' | bc)
    test $num_osds_upgradable -eq 0 || return 1
    num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length' | bc)
    test $num_osds_upgraded -eq $OSDS || return 1
}

function TEST_ok_to_upgrade_bad_osd_version() {
    local dir=$1
    local poolname="test"
    local OSDS=3
    local ceph_version="01.00.00-gversion-test"

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON "

    run_mon $dir a --public-addr=$CEPH_MON || return 1
    run_mgr $dir x || return 1

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd --osd-mclock-skip-benchmark=true || return 1
    done

    create_pool $poolname 8 8
    ceph osd pool set $poolname min_size 1
    sleep 5

    wait_for_clean || return 1

    # Set the option to enable testing metadata errors
    ceph config set mgr mgr_test_metadata_error true

    # Test for upgradability with min_size=1
    local exp_osds_upgradable=0
    local exp_osds_bad_version=3
    local crush_bucket=$(ceph osd tree | grep host | awk '{ print $4 }')
    local res=$(ceph osd ok-to-upgrade $crush_bucket $ceph_version --format=json)
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = false || return 1
    local num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length' | bc)
    test $exp_osds_upgradable = $num_osds_upgradable || return 1
    local num_osds_bad_version=$(echo $res | jq '.bad_no_version | length' | bc)
    test $num_osds_bad_version -eq 3 || return 1
}


main ok-to-upgrade "$@"
