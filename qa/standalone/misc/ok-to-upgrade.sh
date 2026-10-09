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
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    export ORIG_CEPH_ARGS="$CEPH_ARGS"

    local funcs=${@:-$(set | ${SED} -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        kill_daemons $dir KILL || return 1
        teardown $dir || return 1
    done
}

# Populate the global pg_count associative array with each OSD's acting
# PG count from 'ceph osd df'.
function snapshot_osd_pg_counts() {
    declare -gA pg_count
    pg_count=()
    while read -r osd_id count; do
        pg_count[$osd_id]=$count
    done < <(ceph osd df --format=json | \
             jq -r '.nodes[] | select(.id >= 0) | "\(.id) \(.pgs)"')
}

# Verify that the named OSD list in a JSON result is ordered in non-decreasing
# acting PG count. Uses the global pg_count array populated by
# snapshot_osd_pg_counts().
# Usage: assert_osds_sorted_by_pg_count <field> <json>
function assert_osds_sorted_by_pg_count() {
    local field=$1
    local json=$2
    local prev_count=-1
    local osd_id count
    for osd_id in $(echo "$json" | jq -r ".${field}[]"); do
        count=${pg_count[$osd_id]}
        test "$count" -ge "$prev_count" || return 1
        prev_count=$count
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
    local crush_bucket="foo"
    local ceph_version_short="01.2.3-1234-g1234deed"
    ! ceph osd ok-to-upgrade $crush_bucket $ceph_version_short || return 1

    # test with 'root' crush bucket name
    crush_bucket="default"
    ! ceph osd ok-to-upgrade $crush_bucket $ceph_version_short || return 1

    # test with invalid ceph_version formats
    crush_bucket=$(ceph osd tree | grep host | awk '{ print $4 }')
    ceph_versions=("" "foo" "20" "20.3.0" "20.3.0-1234" \
                   "20.3.0-1234-g" "20.1.0-145.el")
    for ver in "${ceph_versions[@]}"; do
      ! ceph osd ok-to-upgrade $crush_bucket $ver || return 1
    done

    # Invalid max parameter
    max=-20
    ! ceph osd ok-to-upgrade $crush_bucket $ver $max|| return 1
}

function TEST_ok_to_upgrade_replicated_pool() {
    local dir=$1
    local poolname="test"
    local OSDS=10
    local ceph_version="01.2.3-1234-g1234deed"

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
    local exp_osds_upgradable=2
    local crush_bucket=$(ceph osd tree | grep host | awk '{ print $4 }')
    local res=$(ceph osd ok-to-upgrade $crush_bucket $ceph_version --format=json)
    # Specifying hostname as the crush bucket with a 3x replicated pool on 10
    # OSDs, with the default 'mgr_osd_upgrade_check_convergence_factor' and
    # with min_size=1 should result in at least 2 OSDs being reported as
    # upgradable. But it is very likely that more than 2 OSDs could be found
    # due to the way PGs are spread out across the replicas. The same is true
    # with min_size=2. Therefore, the check for upgradable OSDs considers this
    # and verifies that at least the expected minimum OSDs are returned.
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = true || return 1
    local num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $num_osds_upgradable -ge $exp_osds_upgradable || return 1
    local num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length')
    test $num_osds_upgraded -eq 0 || return 1

    # Test the same command as above, but exercise the 'max' parameter.
    # Only the 'max' specified number of OSDs from the crush bucket must be returned.
    local max=1
    exp_osds_upgradable=1
    # Test command with terse syntax which tests type inferencing
    res=$(ceph osd ok-to-upgrade $crush_bucket $ceph_version $max --format=json)
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = true || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $num_osds_upgradable -eq $exp_osds_upgradable || return 1
    num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length')
    test $num_osds_upgraded -eq 0 || return 1

    # Test same command above with verbose syntax
    res=$(ceph osd ok-to-upgrade --crush_bucket $crush_bucket \
        --ceph_version $ceph_version --max $max --format=json)
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = true || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $num_osds_upgradable -eq $exp_osds_upgradable || return 1
    num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length')
    test $num_osds_upgraded -eq 0 || return 1

    # Test for upgradability with min_size=1 and 1 OSD to upgrade. The outcome
    # must be the specified osd as the command limits the search within the
    # provided crush bucket.
    exp_osds_upgradable=1
    crush_bucket="osd.0"
    res=$(ceph osd ok-to-upgrade $crush_bucket $ceph_version --format=json)
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = true || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $exp_osds_upgradable = $num_osds_upgradable || return 1
    num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length')
    test $num_osds_upgraded -eq 0 || return 1

    # Test for upgradability with min_size=2
    ceph osd pool set $poolname min_size 2
    sleep 5
    wait_for_clean || return 1
    exp_osds_upgradable=1
    crush_bucket=$(ceph osd tree | grep host | awk '{ print $4 }')
    res=$(ceph osd ok-to-upgrade $crush_bucket $ceph_version --format=json)
    # 3 OSDs should be reported as upgradable.
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = true || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $num_osds_upgradable -ge $exp_osds_upgradable || return 1
    num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length')
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
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $exp_osds_upgradable = $num_osds_upgradable || return 1
    num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length')
    test $num_osds_upgraded -eq 0 || return 1

    # Test for condition when all OSDs are running desired version.
    upgrade_version=$(ceph osd metadata 0 --format=json | \
      jq '.ceph_version_short' | sed 's/"//g')
    res=$(ceph osd ok-to-upgrade $crush_bucket $upgrade_version --format=json)
    test $(echo $res | jq '.all_osds_upgraded') = true || return 1
    test $(echo $res | jq '.ok_to_upgrade') = false || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $num_osds_upgradable -eq 0 || return 1
    num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length')
    test $num_osds_upgraded -eq $OSDS || return 1
}

function TEST_ok_to_upgrade_erasure_pool() {
    local dir=$1
    local poolname="ec"
    local OSDS=10
    local ceph_version="01.2.3-1234-g1234deed"

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
    local num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $exp_osds_upgradable = $num_osds_upgradable || return 1
    local num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length')
    test $num_osds_upgraded -eq 0 || return 1

    # Test the same command as above, but exercise the 'max' parameter.
    # Only the 'max' specified number of OSDs from the crush bucket must be returned.
    local max=1
    exp_osds_upgradable=1
    # Test command with terse syntax which tests type inferencing
    res=$(ceph osd ok-to-upgrade $crush_bucket $ceph_version $max --format=json)
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = true || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $num_osds_upgradable -eq $exp_osds_upgradable || return 1
    num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length')
    test $num_osds_upgraded -eq 0 || return 1

    # Test command above with verbose syntax
    res=$(ceph osd ok-to-upgrade --crush_bucket $crush_bucket \
        --ceph_version $ceph_version --max $max --format=json)
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = true || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $num_osds_upgradable -eq $exp_osds_upgradable || return 1
    num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length')
    test $num_osds_upgraded -eq 0 || return 1

    # Test for upgradability with min_size=5 and 1 OSD to upgrade. The outcome
    # must be the specified osd as the command limits the search within the
    # provided crush bucket.
    exp_osds_upgradable=1
    crush_bucket="osd.0"
    res=$(ceph osd ok-to-upgrade $crush_bucket $ceph_version --format=json)
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = true || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $exp_osds_upgradable = $num_osds_upgradable || return 1
    num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length')
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
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $exp_osds_upgradable = $num_osds_upgradable || return 1
    num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length')
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
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $exp_osds_upgradable = $num_osds_upgradable || return 1
    num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length')
    test $num_osds_upgraded -eq 0 || return 1

    # Test for condition when all OSDs are running desired version.
    ceph_version=$(ceph osd metadata 0 --format=json | \
      jq '.ceph_version_short' | sed 's/"//g')
    res=$(ceph osd ok-to-upgrade $crush_bucket $ceph_version --format=json)
    test $(echo $res | jq '.all_osds_upgraded') = true || return 1
    test $(echo $res | jq '.ok_to_upgrade') = false || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $num_osds_upgradable -eq 0 || return 1
    num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length')
    test $num_osds_upgraded -eq $OSDS || return 1
}

function TEST_ok_to_upgrade_bad_osd_version() {
    local dir=$1
    local poolname="test"
    local OSDS=3
    local ceph_version="01.2.3-1234-g1234deed"

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
    local num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $exp_osds_upgradable = $num_osds_upgradable || return 1
    local num_osds_bad_version=$(echo $res | jq '.bad_no_version | length')
    test $num_osds_bad_version -eq 3 || return 1
}

function TEST_ok_to_upgrade_chassis_bucket() {
    local dir=$1
    local poolname="test"
    local OSDS=4
    local ceph_version="01.2.3-1234-g1234deed"

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON "

    run_mon $dir a --public-addr=$CEPH_MON || return 1
    run_mgr $dir x || return 1

    for osd in $(seq 0 $(expr $OSDS - 1)); do
      run_osd $dir $osd --osd-mclock-skip-benchmark=true || return 1
    done

    # Use size=2 so the pool can be satisfied with 2 distinct host buckets.
    # min_size=1 allows upgrading all OSDs on one host simultaneously.
    create_pool $poolname 8 8
    ceph osd pool set $poolname pg_autoscale_mode off
    ceph osd pool set $poolname size 2
    ceph osd pool set $poolname min_size 1
    sleep 5
    wait_for_clean || return 1

    # Build a chassis CRUSH hierarchy:
    #   root "default"
    #     chassis "chassis-0"
    #       host "host-0-0" -> osd.0, osd.1
    #       host "host-0-1" -> osd.2, osd.3
    #
    # OSDs start in auto-generated host buckets; crush set replaces each
    # OSD's location with the new custom hierarchy.
    ceph osd crush add-bucket chassis-0 chassis || return 1
    ceph osd crush add-bucket host-0-0 host || return 1
    ceph osd crush add-bucket host-0-1 host || return 1
    ceph osd crush move host-0-0 chassis=chassis-0 || return 1
    ceph osd crush move host-0-1 chassis=chassis-0 || return 1
    ceph osd crush move chassis-0 root=default || return 1
    ceph osd crush set 0 1.0 root=default chassis=chassis-0 host=host-0-0 || return 1
    ceph osd crush set 1 1.0 root=default chassis=chassis-0 host=host-0-0 || return 1
    ceph osd crush set 2 1.0 root=default chassis=chassis-0 host=host-0-1 || return 1
    ceph osd crush set 3 1.0 root=default chassis=chassis-0 host=host-0-1 || return 1
    sleep 5
    wait_for_clean || return 1

    # Snapshot acting PG counts per OSD. In a clean cluster 'pgs' in
    # osd df equals the acting count the mgr's PGMap records internally.
    snapshot_osd_pg_counts

    # With min_size=1 some OSDs inside chassis-0 must be reported upgradable.
    local res=$(ceph osd ok-to-upgrade chassis-0 $ceph_version --format=json)
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = true || return 1
    local num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $num_osds_upgradable -ge 1 || return 1
    local num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length')
    test $num_osds_upgraded -eq 0 || return 1

    # Verify that osds_in_crush_bucket lists all chassis OSDs in non-decreasing
    # order of acting PG count.  This directly tests the global sort inside
    # _populate_crush_bucket_osds(): consecutive entries must never decrease.
    assert_osds_sorted_by_pg_count osds_in_crush_bucket "$res" || return 1

    # With max=1, exactly one OSD is returned.
    local max=1
    res=$(ceph osd ok-to-upgrade chassis-0 $ceph_version $max --format=json)
    test $(echo $res | jq '.ok_to_upgrade') = true || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $num_osds_upgradable -eq 1 || return 1

    # With min_size=2 on a size=2 pool, taking any OSD offline drops a PG
    # below the minimum replica count -- no OSD should be upgradable.
    ceph osd pool set $poolname min_size 2
    sleep 5
    wait_for_clean || return 1
    res=$(ceph osd ok-to-upgrade chassis-0 $ceph_version --format=json)
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = false || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $num_osds_upgradable -eq 0 || return 1
}

function TEST_ok_to_upgrade_rack_bucket() {
    local dir=$1
    local poolname="test"
    local OSDS=4
    local ceph_version="01.2.3-1234-g1234deed"

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON "

    run_mon $dir a --public-addr=$CEPH_MON || return 1
    run_mgr $dir x || return 1

    for osd in $(seq 0 $(expr $OSDS - 1)); do
      run_osd $dir $osd --osd-mclock-skip-benchmark=true || return 1
    done

    # Use size=2 so the pool can be satisfied with 2 distinct host buckets.
    # min_size=1 allows upgrading all OSDs on one host simultaneously.
    create_pool $poolname 8 8
    ceph osd pool set $poolname pg_autoscale_mode off
    ceph osd pool set $poolname size 2
    ceph osd pool set $poolname min_size 1
    sleep 5
    wait_for_clean || return 1

    # Build a rack CRUSH hierarchy:
    #   root "default"
    #     rack "rack-0"
    #       host "rack-host-0" -> osd.0, osd.1
    #       host "rack-host-1" -> osd.2, osd.3
    ceph osd crush add-bucket rack-0 rack || return 1
    ceph osd crush add-bucket rack-host-0 host || return 1
    ceph osd crush add-bucket rack-host-1 host || return 1
    ceph osd crush move rack-host-0 rack=rack-0 || return 1
    ceph osd crush move rack-host-1 rack=rack-0 || return 1
    ceph osd crush move rack-0 root=default || return 1
    ceph osd crush set 0 1.0 root=default rack=rack-0 host=rack-host-0 || return 1
    ceph osd crush set 1 1.0 root=default rack=rack-0 host=rack-host-0 || return 1
    ceph osd crush set 2 1.0 root=default rack=rack-0 host=rack-host-1 || return 1
    ceph osd crush set 3 1.0 root=default rack=rack-0 host=rack-host-1 || return 1
    sleep 5
    wait_for_clean || return 1

    # Snapshot acting PG counts per OSD. In a clean cluster 'pgs' in
    # osd df equals the acting count the mgr's PGMap records internally.
    snapshot_osd_pg_counts

    # With min_size=1 some OSDs inside rack-0 must be reported upgradable.
    local res=$(ceph osd ok-to-upgrade rack-0 $ceph_version --format=json)
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = true || return 1
    local num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $num_osds_upgradable -ge 1 || return 1
    local num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length')
    test $num_osds_upgraded -eq 0 || return 1

    # Verify that osds_in_crush_bucket lists all rack OSDs in non-decreasing
    # order of acting PG count.  This directly tests the global sort inside
    # _populate_crush_bucket_osds(): consecutive entries must never decrease.
    assert_osds_sorted_by_pg_count osds_in_crush_bucket "$res" || return 1

    # With max=1, exactly one OSD is returned.
    local max=1
    res=$(ceph osd ok-to-upgrade rack-0 $ceph_version $max --format=json)
    test $(echo $res | jq '.ok_to_upgrade') = true || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $num_osds_upgradable -eq 1 || return 1

    # With min_size=2 on a size=2 pool, no OSD can go offline without
    # violating the pool's minimum replica count.
    ceph osd pool set $poolname min_size 2
    sleep 5
    wait_for_clean || return 1
    res=$(ceph osd ok-to-upgrade rack-0 $ceph_version --format=json)
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = false || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $num_osds_upgradable -eq 0 || return 1
}

function TEST_ok_to_upgrade_rack_chassis_bucket() {
    local dir=$1
    local poolname="test"
    local OSDS=4
    local ceph_version="01.2.3-1234-g1234deed"

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON "

    run_mon $dir a --public-addr=$CEPH_MON || return 1
    run_mgr $dir x || return 1

    for osd in $(seq 0 $(expr $OSDS - 1)); do
      run_osd $dir $osd --osd-mclock-skip-benchmark=true || return 1
    done

    # Use size=2 so the pool can be satisfied with 2 distinct host buckets.
    # min_size=1 allows upgrading all OSDs on one host simultaneously.
    create_pool $poolname 8 8
    ceph osd pool set $poolname pg_autoscale_mode off
    ceph osd pool set $poolname size 2
    ceph osd pool set $poolname min_size 1
    sleep 5
    wait_for_clean || return 1

    # Build a rack CRUSH hierarchy:
    #   root "default"
    #     rack "rack-0"
    #       chassis "chassis-0"
    #         host "chassis-host-0" -> osd.0, osd.1
    #         host "chassis-host-1" -> osd.2, osd.3
    ceph osd crush add-bucket rack-0 rack || return 1
    ceph osd crush add-bucket chassis-0 chassis || return 1
    ceph osd crush add-bucket chassis-host-0 host || return 1
    ceph osd crush add-bucket chassis-host-1 host || return 1
    ceph osd crush move chassis-host-0 chassis=chassis-0 || return 1
    ceph osd crush move chassis-host-1 chassis=chassis-0 || return 1
    ceph osd crush move chassis-0 rack=rack-0 || return 1
    ceph osd crush move rack-0 root=default || return 1
    ceph osd crush set 0 1.0 root=default rack=rack-0 host=chassis-host-0 || return 1
    ceph osd crush set 1 1.0 root=default rack=rack-0 host=chassis-host-0 || return 1
    ceph osd crush set 2 1.0 root=default rack=rack-0 host=chassis-host-1 || return 1
    ceph osd crush set 3 1.0 root=default rack=rack-0 host=chassis-host-1 || return 1
    sleep 5
    wait_for_clean || return 1

    # Snapshot acting PG counts per OSD. In a clean cluster 'pgs' in
    # osd df equals the acting count the mgr's PGMap records internally.
    snapshot_osd_pg_counts

    # With min_size=1 some OSDs inside chassis-0 must be reported upgradable.
    local res=$(ceph osd ok-to-upgrade chassis-0 $ceph_version --format=json)
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = true || return 1
    local num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $num_osds_upgradable -ge 1 || return 1
    local num_osds_upgraded=$(echo $res | jq '.osds_upgraded | length')
    test $num_osds_upgraded -eq 0 || return 1

    # Verify that osds_in_crush_bucket lists all chassis OSDs in non-decreasing
    # order of acting PG count.  This directly tests the global sort inside
    # _populate_crush_bucket_osds(): consecutive entries must never decrease.
    assert_osds_sorted_by_pg_count osds_in_crush_bucket "$res" || return 1

    # With max=1, exactly one OSD is returned.
    local max=1
    res=$(ceph osd ok-to-upgrade chassis-0 $ceph_version $max --format=json)
    test $(echo $res | jq '.ok_to_upgrade') = true || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $num_osds_upgradable -eq 1 || return 1

    # With min_size=2 on a size=2 pool, no OSD can go offline without
    # violating the pool's minimum replica count.
    ceph osd pool set $poolname min_size 2
    sleep 5
    wait_for_clean || return 1
    res=$(ceph osd ok-to-upgrade chassis-0 $ceph_version --format=json)
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = false || return 1
    num_osds_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $num_osds_upgradable -eq 0 || return 1
}

# Verify that ok-to-upgrade returns OSDs in ascending order of acting PG
# count globally across the entire chassis (not per-host).
#
# _populate_crush_bucket_osds collects OSDs from every child host bucket,
# then sorts them once globally by (num_pgs, osd_id) ascending before
# returning to _maximize_ok_to_upgrade_set.  The convergence algorithm
# prunes from the tail (osds.resize), preserving this global order in the
# final osds_ok_to_upgrade output.
#
# Test design
# -----------
# 6 OSDs in a chassis (3 per host), pool with 7 PGs and size=2: CRUSH
# maps each PG to one OSD per host. 7 % 3 != 0 produces an unequal
# per-OSD acting count across the chassis, making the global ordering
# non-trivial to predict without actually measuring it.  Using 7 (rather
# than a larger number such as 16) widens the relative spread between
# OSD PG counts, reducing ties and making the ordering invariant more
# discriminating.
#
# The test snapshots the acting PG count per OSD from 'ceph osd df', then
# checks two invariants:
#   1. The returned list is non-decreasing by acting PG count (global sort).
#   2. The first returned OSD holds the minimum acting PG count among all
#      OSDs in chassis-0, confirming that the globally-least-loaded OSD
#      is always offered for upgrade first.
function TEST_ok_to_upgrade_osd_order_by_pg_count() {
    local dir=$1
    local poolname="test"
    local OSDS=6
    local ALL_CHASSIS_OSDS="0 1 2 3 4 5"
    local ceph_version="01.2.3-1234-g1234deed"

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON "

    run_mon $dir a --public-addr=$CEPH_MON || return 1
    run_mgr $dir x || return 1

    for osd in $(seq 0 $(expr $OSDS - 1)); do
      run_osd $dir $osd --osd-mclock-skip-benchmark=true || return 1
    done

    # 7 PGs, size=2, min_size=1. 7 % 3 != 0 guarantees an unequal
    # per-OSD acting count within the chassis.
    create_pool $poolname 7 7
    ceph osd pool set $poolname pg_autoscale_mode off
    ceph osd pool set $poolname size 2
    ceph osd pool set $poolname min_size 1
    sleep 5
    wait_for_clean || return 1

    # Build a chassis hierarchy:
    #   root "default"
    #     chassis "chassis-0"
    #       host "host-0-0" -> osd.0, osd.1, osd.2
    #       host "host-0-1" -> osd.3, osd.4, osd.5
    ceph osd crush add-bucket chassis-0 chassis || return 1
    ceph osd crush add-bucket host-0-0 host || return 1
    ceph osd crush add-bucket host-0-1 host || return 1
    ceph osd crush move host-0-0 chassis=chassis-0 || return 1
    ceph osd crush move host-0-1 chassis=chassis-0 || return 1
    ceph osd crush move chassis-0 root=default || return 1
    ceph osd crush set 0 1.0 root=default chassis=chassis-0 host=host-0-0 || return 1
    ceph osd crush set 1 1.0 root=default chassis=chassis-0 host=host-0-0 || return 1
    ceph osd crush set 2 1.0 root=default chassis=chassis-0 host=host-0-0 || return 1
    ceph osd crush set 3 1.0 root=default chassis=chassis-0 host=host-0-1 || return 1
    ceph osd crush set 4 1.0 root=default chassis=chassis-0 host=host-0-1 || return 1
    ceph osd crush set 5 1.0 root=default chassis=chassis-0 host=host-0-1 || return 1
    sleep 5
    wait_for_clean || return 1

    # Snapshot acting PG counts per OSD. In a clean cluster 'pgs' in
    # osd df equals the acting count the mgr's PGMap records internally.
    snapshot_osd_pg_counts

    local res=$(ceph osd ok-to-upgrade chassis-0 $ceph_version --format=json)
    test $(echo $res | jq '.all_osds_upgraded') = false || return 1
    test $(echo $res | jq '.ok_to_upgrade') = true || return 1
    local num_upgradable=$(echo $res | jq '.osds_ok_to_upgrade | length')
    test $num_upgradable -ge 1 || return 1

    # Verify that osds_in_crush_bucket lists all chassis OSDs in non-decreasing
    # order of acting PG count.  osds_in_crush_bucket is the direct output of
    # _populate_crush_bucket_osds() before the convergence algorithm runs, so
    # this check covers every OSD in the bucket, not just the upgrade subset.
    assert_osds_sorted_by_pg_count osds_in_crush_bucket "$res" || return 1

    # Invariant 1: the returned OSD list must be globally non-decreasing by
    # acting PG count.  _populate_crush_bucket_osds sorts all chassis OSDs
    # together; the convergence algorithm only prunes from the tail, so the
    # ascending order is always preserved in the output.
    local prev_count=-1
    for osd_id in $(echo $res | jq -r '.osds_ok_to_upgrade[]'); do
        local count=${pg_count[$osd_id]}
        test "$count" -ge "$prev_count" || return 1
        prev_count=$count
    done

    # Invariant 2: the first OSD in the list must have the globally minimum
    # acting PG count across all OSDs in chassis-0.  This confirms that the
    # globally-least-loaded OSD is always offered for upgrade first.
    local first_osd=$(echo $res | jq -r '.osds_ok_to_upgrade[0]')
    local first_count=${pg_count[$first_osd]}
    for osd_id in $ALL_CHASSIS_OSDS; do
        test "${pg_count[$osd_id]}" -ge "$first_count" || return 1
    done
}

# Wait until the mgr's PGMap - what `osd ok-to-stop` and `osd ok-to-upgrade`
# read, as opposed to the mon's OSDMap that get_osds() reads - reports PG
# pgid with the given acting set (space separated) and a state containing
# the given string, or until a timeout of $WAIT_FOR_CLEAN_TIMEOUT seconds.
function wait_for_pg_acting() {
    local pgid=$1
    local acting="$2"
    local state=${3:-active}
    local -a delays=($(get_timeout_delays $WAIT_FOR_CLEAN_TIMEOUT .1))
    local -i loop=0

    flush_pg_stats || return 1
    while true ; do
        ceph --format json pg dump pgs 2>/dev/null | \
            jq -e --arg pgid "$pgid" --arg acting "$acting" --arg state "$state" \
               '.pg_stats[] | select(.pgid == $pgid)
                | select((.acting | map(tostring) | join(" ")) == $acting)
                | select(.state | contains($state))' > /dev/null && return 0
        (( loop >= ${#delays[*]} )) && return 1
        sleep ${delays[$loop]}
        loop+=1
    done
}

# `osd ok-to-upgrade` grows and shrinks its candidate set with the same
# _check_offlines_pgs() as `osd ok-to-stop`. A regression (cf54988c504)
# made that check skip every PG whose *last* acting OSD is not among the
# candidates, so a subset that leaves a PG below min_size could be
# reported as upgradable. With one PG, 3 copies on the 3 OSDs of a host and
# min_size 2, no two OSDs can go together. Pin the acting order of the PG
# to the order in which the command walks the bucket, so that the pair it
# tries first ([first, second]) never contains the last acting OSD, and
# check that only one OSD is reported.
function TEST_ok_to_upgrade_last_acting_outside_set() {
    local dir=$1
    local poolname="test"
    local OSDS=3
    local ceph_version="01.2.3-1234-g1234deed"

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON "

    run_mon $dir a --public-addr=$CEPH_MON || return 1
    run_mgr $dir x || return 1

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd --osd-mclock-skip-benchmark=true || return 1
    done

    create_pool $poolname 1 1 || return 1
    ceph osd pool set $poolname size 3 || return 1
    ceph osd pool set $poolname min_size 2 || return 1
    wait_for_clean || return 1

    local crush_bucket=$(ceph osd tree | grep host | awk '{ print $4 }')

    # the order in which the command considers the OSDs of the bucket
    local res=$(ceph osd ok-to-upgrade $crush_bucket $ceph_version --format=json)
    test $(echo $res | jq '.osds_in_crush_bucket | length') -eq $OSDS || return 1
    local order=$(echo $res | jq -r '.osds_in_crush_bucket | join(" ")')

    # pin the acting order of the only PG to that order: the last OSD the
    # command would add to a pair is the last acting member
    local pgid=$(get_pg $poolname obj)
    ceph osd set-require-min-compat-client luminous || return 1
    ceph osd pg-upmap $pgid $order || return 1
    wait_for_clean || return 1
    wait_for_pg_acting $pgid "$order" clean || return 1

    # the upmap must not have changed the order in which the command walks
    # the bucket, or the pair it tries first may contain the last acting
    # OSD and the test would pass without the fix
    res=$(ceph osd ok-to-upgrade $crush_bucket $ceph_version --format=json)
    test "$(echo $res | jq -r '.osds_in_crush_bucket | join(" ")')" = "$order" || return 1

    # 3 copies, min_size 2: exactly one OSD of the host can be upgraded at
    # a time, whatever max says. The regression reported two.
    local max
    for max in 0 2 3; do
      res=$(ceph osd ok-to-upgrade $crush_bucket $ceph_version $max --format=json)
      test $(echo $res | jq '.all_osds_upgraded') = false || return 1
      test $(echo $res | jq '.ok_to_upgrade') = true || return 1
      test $(echo $res | jq '.osds_ok_to_upgrade | length') -eq 1 || return 1
    done
}

main ok-to-upgrade "$@"
