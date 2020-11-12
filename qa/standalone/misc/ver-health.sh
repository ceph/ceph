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
        kill_daemons $dir KILL || return 1
        teardown $dir || return 1
    done
}

# Test a single OSD with an old version and multiple OSDs with 2 different old versions
function TEST_check_version_health_1() {
    local dir=$1

    # Asssume MON_A is leader?
    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON_A "
    # setup
    setup $dir || return 1

    # create a cluster with two monitors and three osds
    run_mon $dir a --public-addr=$CEPH_MON_A --mon_warn_older_version_delay=0.0 || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B --mon_warn_older_version_delay=0.0 || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    sleep 5
    ceph health detail
    # should not see this yet
    ceph health detail | grep DAEMON_OLD_VERSION && return 1

    kill_daemons $dir KILL osd.1
    EXTRA_OPTS=" --debug_version_for_testing=01.00.00-gversion-test" activate_osd $dir 1
    sleep 5

    ceph health detail
    # Should notice that osd.1 is a different version
    ceph health | grep -q "HEALTH_WARN .*There is a daemon running an older version of ceph" || return 1
    ceph health detail | grep -q "HEALTH_WARN .*There is a daemon running an older version of ceph" || return 1
    ceph health detail | grep -q "^[[]WRN[]] DAEMON_OLD_VERSION: There is a daemon running an older version of ceph" || return 1
    ceph health detail | grep -q "osd.1 is running an older version of ceph: 01.00.00-gversion-test" || return 1

    kill_daemons $dir KILL osd.2
    EXTRA_OPTS=" --debug_version_for_testing=01.00.00-gversion-test" activate_osd $dir 2
    kill_daemons $dir KILL osd.0
    EXTRA_OPTS=" --debug_version_for_testing=02.00.00-gversion-test" activate_osd $dir 0
    sleep 5

    ceph health detail
    ceph health | grep -q "HEALTH_ERR .*There are daemons running multiple old versions of ceph" || return 1
    ceph health detail | grep -q "HEALTH_ERR .*There are daemons running multiple old versions of ceph" || return 1
    ceph health detail | grep -q "^[[]ERR[]] DAEMON_OLD_VERSION: There are daemons running multiple old versions of ceph" || return 1
    ceph health detail | grep -q "osd.1 osd.2 are running an older version of ceph: 01.00.00-gversion-test" || return 1
    ceph health detail | grep -q "osd.0 is running an older version of ceph: 02.00.00-gversion-test" || return 1
}

# Test with 1 MON and 1 MDS with an older version, and add 2 OSDs with different versions
function TEST_check_version_health_2() {
    local dir=$1

    # Asssume MON_A is leader?
    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON_A "
    # setup
    setup $dir || return 1

    # create a cluster with all daemon types
    run_mon $dir a --public-addr=$CEPH_MON_A --mon_warn_older_version_delay=0.0 || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B --mon_warn_older_version_delay=0.0 || return 1
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
    EXTRA_OPTS=" --debug_version_for_testing=01.00.00-gversion-test" run_mon $dir b --mon_warn_older_version_delay=0.0
    # XXX: Manager doesn't seem to use the test specific config for version
    #kill_daemons $dir KILL mgr.x
    #EXTRA_OPTS=" --debug_version_for_testing=02.00.00-gversion-test" run_mgr $dir x
    kill_daemons $dir KILL mds.m
    EXTRA_OPTS=" --debug_version_for_testing=01.00.00-gversion-test" run_mds $dir m
    sleep 10

    ceph health detail
    # Should notice that mon.b and mds.m is a different version
    ceph health | grep -q "HEALTH_WARN .*There are daemons running an older version of ceph" || return 1
    ceph health detail | grep -q "HEALTH_WARN .*There are daemons running an older version of ceph" || return 1
    ceph health detail | grep -q "^[[]WRN[]] DAEMON_OLD_VERSION: There are daemons running an older version of ceph" || return 1
    ceph health detail | grep -q "mon.b mds.m are running an older version of ceph: 01.00.00-gversion-test" || return 1

    kill_daemons $dir KILL osd.2
    EXTRA_OPTS=" --debug_version_for_testing=01.00.00-gversion-test" activate_osd $dir 2
    kill_daemons $dir KILL osd.0
    EXTRA_OPTS=" --debug_version_for_testing=02.00.00-gversion-test" activate_osd $dir 0
    sleep 5

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

    # Asssume MON_A is leader?
    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON_A "
    # setup
    setup $dir || return 1

    # create a cluster with two monitors and three osds
    run_mon $dir a --public-addr=$CEPH_MON_A --mon_warn_older_version_delay=20.0 || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B --mon_warn_older_version_delay=20.0 || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    sleep 5
    ceph health detail
    # should not see this yet
    ceph health detail | grep DAEMON_OLD_VERSION && return 1

    kill_daemons $dir KILL osd.1
    EXTRA_OPTS=" --debug_version_for_testing=01.00.00-gversion-test" activate_osd $dir 1
    sleep 5 # give kill time

    # Wait 50% of 20 second delay config
    sleep 10
    # should not see this yet
    ceph health detail | grep DAEMON_OLD_VERSION && return 1

    # Now make sure that at least 20 seconds have passed
    sleep 10

    ceph health detail
    # Should notice that osd.1 is a different version
    ceph health | grep -q "HEALTH_WARN .*There is a daemon running an older version of ceph" || return 1
    ceph health detail | grep -q "HEALTH_WARN .*There is a daemon running an older version of ceph" || return 1
    ceph health detail | grep -q "^[[]WRN[]] DAEMON_OLD_VERSION: There is a daemon running an older version of ceph" || return 1
    ceph health detail | grep -q "osd.1 is running an older version of ceph: 01.00.00-gversion-test" || return 1

    kill_daemons $dir KILL osd.2
    EXTRA_OPTS=" --debug_version_for_testing=01.00.00-gversion-test" activate_osd $dir 2
    kill_daemons $dir KILL osd.0
    EXTRA_OPTS=" --debug_version_for_testing=02.00.00-gversion-test" activate_osd $dir 0
    sleep 5

    ceph health detail
    ceph health | grep -q "HEALTH_ERR .*There are daemons running multiple old versions of ceph" || return 1
    ceph health detail | grep -q "HEALTH_ERR .*There are daemons running multiple old versions of ceph" || return 1
    ceph health detail | grep -q "^[[]ERR[]] DAEMON_OLD_VERSION: There are daemons running multiple old versions of ceph" || return 1
    ceph health detail | grep -q "osd.1 osd.2 are running an older version of ceph: 01.00.00-gversion-test" || return 1
    ceph health detail | grep -q "osd.0 is running an older version of ceph: 02.00.00-gversion-test" || return 1
}

main ver-health "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && ../qa/run-standalone.sh ver-health.sh"
# End:
