#!/usr/bin/env bash
#
# Copyright (C) 2020 Red Hat <contact@redhat.com>
#
# Author: David Zafman <dzafman@redhat.com>
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

    export CEPH_MON_A="127.0.0.1:7165" # git grep '\<7165\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7166" # git grep '\<7166\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--mon_health_to_clog_tick_interval=1.0 "
    export ORIG_CEPH_ARGS="$CEPH_ARGS"

    local funcs=${@:-$(set | ${SED} -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function wait_for_health_string() {
    local grep_string=$1
    local seconds=${2:-20}

    # Allow mon to notice version difference
    set -o pipefail
    PASSED="false"
    for ((i=0; i < $seconds; i++)); do
      if ceph health | grep -q "$grep_string"
      then
	PASSED="true"
        break
      fi
      sleep 1
    done
    set +o pipefail

    # Make sure health changed
    if [ $PASSED = "false" ];
    then
      return 1
    fi
    return 0
}



# Test a single OSD with an old version and multiple OSDs with 2 different old versions
function TEST_check_version_health_1() {
    local dir=$1

    # Assume MON_A is leader?
    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON_A "
    # setup
    setup $dir || return 1

    # create a cluster with two monitors and three osds
    run_mon $dir a --public-addr=$CEPH_MON_A --mon_warn_older_version_delay=0 || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B --mon_warn_older_version_delay=0 || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    sleep 5
    ceph health detail
    # should not see this yet
    ceph health detail | grep DAEMON_OLD_VERSION && return 1

    kill_daemons $dir KILL osd.1
    ceph_debug_version_for_testing=01.00.00-gversion-test activate_osd $dir 1

    wait_for_health_string "HEALTH_WARN .*There is a daemon running an older version of ceph" || return 1

    ceph health detail
    # Should notice that osd.1 is a different version
    ceph health detail | grep -q "HEALTH_WARN .*There is a daemon running an older version of ceph" || return 1
    ceph health detail | grep -q "^[[]WRN[]] DAEMON_OLD_VERSION: There is a daemon running an older version of ceph" || return 1
    ceph health detail | grep -q "osd.1 is running an older version of ceph: 01.00.00-gversion-test" || return 1

    kill_daemons $dir KILL osd.2
    ceph_debug_version_for_testing=01.00.00-gversion-test activate_osd $dir 2
    kill_daemons $dir KILL osd.0
    ceph_debug_version_for_testing=02.00.00-gversion-test activate_osd $dir 0

    wait_for_health_string "HEALTH_ERR .*There are daemons running multiple old versions of ceph" || return 1

    ceph health detail
    ceph health detail | grep -q "HEALTH_ERR .*There are daemons running multiple old versions of ceph" || return 1
    ceph health detail | grep -q "^[[]ERR[]] DAEMON_OLD_VERSION: There are daemons running multiple old versions of ceph" || return 1
    ceph health detail | grep -q "osd.1 osd.2 are running an older version of ceph: 01.00.00-gversion-test" || return 1
    ceph health detail | grep -q "osd.0 is running an older version of ceph: 02.00.00-gversion-test" || return 1
}

# Test with 1 MON and 1 MDS with an older version, and add 2 OSDs with different versions
function TEST_check_version_health_2() {
    local dir=$1

    # Assume MON_A is leader?
    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON_A "
    # setup
    setup $dir || return 1

    # create a cluster with all daemon types
    run_mon $dir a --public-addr=$CEPH_MON_A --mon_warn_older_version_delay=0 || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B --mon_warn_older_version_delay=0 || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_mgr $dir x || return 1
    run_mgr $dir y || return 1
    run_mds $dir m || return 1
    run_mds $dir n || return 1

    sleep 5
    ceph health detail
    # should not see this yet
    ceph health detail | grep DAEMON_OLD_VERSION && return 1

    kill_daemons $dir KILL mon.b
    ceph_debug_version_for_testing=01.00.00-gversion-test run_mon $dir b --mon_warn_older_version_delay=0
    # XXX: Manager doesn't seem to use the test specific config for version
    #kill_daemons $dir KILL mgr.x
    #ceph_debug_version_for_testing=02.00.00-gversion-test run_mgr $dir x
    kill_daemons $dir KILL mds.m
    ceph_debug_version_for_testing=01.00.00-gversion-test run_mds $dir m

    wait_for_health_string "HEALTH_WARN .*There are daemons running an older version of ceph" || return 1

    ceph health detail
    # Should notice that mon.b and mds.m is a different version
    ceph health detail | grep -q "HEALTH_WARN .*There are daemons running an older version of ceph" || return 1
    ceph health detail | grep -q "^[[]WRN[]] DAEMON_OLD_VERSION: There are daemons running an older version of ceph" || return 1
    ceph health detail | grep -q "mon.b mds.m are running an older version of ceph: 01.00.00-gversion-test" || return 1

    kill_daemons $dir KILL osd.2
    ceph_debug_version_for_testing=01.00.00-gversion-test activate_osd $dir 2
    kill_daemons $dir KILL osd.0
    ceph_debug_version_for_testing=02.00.00-gversion-test activate_osd $dir 0

    wait_for_health_string "HEALTH_ERR .*There are daemons running multiple old versions of ceph" || return 1

    ceph health detail
    ceph health | grep -q "HEALTH_ERR .*There are daemons running multiple old versions of ceph" || return 1
    ceph health detail | grep -q "HEALTH_ERR .*There are daemons running multiple old versions of ceph" || return 1
    ceph health detail | grep -q "^[[]ERR[]] DAEMON_OLD_VERSION: There are daemons running multiple old versions of ceph" || return 1
    ceph health detail | grep -q "mon.b osd.2 mds.m are running an older version of ceph: 01.00.00-gversion-test" || return 1
    ceph health detail | grep -q "osd.0 is running an older version of ceph: 02.00.00-gversion-test" || return 1
}

# Verify delay handling with same setup as test 1
function TEST_check_version_health_3() {
    local dir=$1

    # Assume MON_A is leader?
    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON_A "
    # setup
    setup $dir || return 1

    # create a cluster with two monitors and three osds
    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1

    local start_osd_time=$SECONDS
    # use memstore for faster bootup
    EXTRA_OPTS=" --osd-objectstore=memstore" run_osd $dir 0 || return 1
    EXTRA_OPTS=" --osd-objectstore=memstore" run_osd $dir 1 || return 1
    EXTRA_OPTS=" --osd-objectstore=memstore" run_osd $dir 2 || return 1
    # take the time used for boot osds into consideration
    local warn_older_version_delay=$(($SECONDS - $start_osd_time + 20))

    sleep 5
    ceph health detail
    # should not see this yet
    ceph health detail | grep DAEMON_OLD_VERSION && return 1
    ceph tell 'mon.*' injectargs "--mon_warn_older_version_delay $warn_older_version_delay"
    kill_daemons $dir KILL osd.1
    EXTRA_OPTS=" --osd-objectstore=memstore" \
          ceph_debug_version_for_testing=01.00.00-gversion-test \
          activate_osd $dir 1

    # Wait 50% of 20 second delay config
    sleep 10
    # should not see this yet
    ceph health detail | grep DAEMON_OLD_VERSION && return 1

    # Now make sure that at least 20 seconds have passed
    wait_for_health_string "HEALTH_WARN .*There is a daemon running an older version of ceph" 20 || return 1

    ceph health detail
    # Should notice that osd.1 is a different version
    ceph health detail | grep -q "HEALTH_WARN .*There is a daemon running an older version of ceph" || return 1
    ceph health detail | grep -q "^[[]WRN[]] DAEMON_OLD_VERSION: There is a daemon running an older version of ceph" || return 1
    ceph health detail | grep -q "osd.1 is running an older version of ceph: 01.00.00-gversion-test" || return 1

    kill_daemons $dir KILL osd.2
    ceph_debug_version_for_testing=01.00.00-gversion-test activate_osd $dir 2
    kill_daemons $dir KILL osd.0
    ceph_debug_version_for_testing=02.00.00-gversion-test activate_osd $dir 0

    wait_for_health_string "HEALTH_ERR .*There are daemons running multiple old versions of ceph" || return 1

    ceph health detail
    ceph health detail | grep -q "HEALTH_ERR .*There are daemons running multiple old versions of ceph" || return 1
    ceph health detail | grep -q "^[[]ERR[]] DAEMON_OLD_VERSION: There are daemons running multiple old versions of ceph" || return 1
    ceph health detail | grep -q "osd.1 osd.2 are running an older version of ceph: 01.00.00-gversion-test" || return 1
    ceph health detail | grep -q "osd.0 is running an older version of ceph: 02.00.00-gversion-test" || return 1
}

main ver-health "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && ../qa/run-standalone.sh ver-health.sh"
# End:
