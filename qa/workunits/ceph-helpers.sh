#!/bin/bash
#
# Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
# Copyright (C) 2014,2015 Red Hat <contact@redhat.com>
# Copyright (C) 2014 Federico Gimenez <fgimenez@coit.es>
#
# Author: Loic Dachary <loic@dachary.org>
# Author: Federico Gimenez <fgimenez@coit.es>
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
TIMEOUT=300
PG_NUM=4
: ${CEPH_BUILD_VIRTUALENV:=/tmp}

if type xmlstarlet > /dev/null 2>&1; then
    XMLSTARLET=xmlstarlet
elif type xml > /dev/null 2>&1; then
    XMLSTARLET=xml
else
	echo "Missing xmlstarlet binary!"
	exit 1
fi

#! @file ceph-helpers.sh
#  @brief Toolbox to manage Ceph cluster dedicated to testing
#
#  Example use case:
#
#  ~~~~~~~~~~~~~~~~{.sh}
#  source ceph-helpers.sh
#
#  function mytest() {
#    # cleanup leftovers and reset mydir
#    setup mydir
#    # create a cluster with one monitor and three osds
#    run_mon mydir a
#    run_osd mydir 0
#    run_osd mydir 2
#    run_osd mydir 3
#    # put and get an object
#    rados --pool rbd put GROUP /etc/group
#    rados --pool rbd get GROUP /tmp/GROUP
#    # stop the cluster and cleanup the directory
#    teardown mydir
#  }
#  ~~~~~~~~~~~~~~~~
#
#  The focus is on simplicity and efficiency, in the context of
#  functional tests. The output is intentionally very verbose
#  and functions return as soon as an error is found. The caller
#  is also expected to abort on the first error so that debugging
#  can be done by looking at the end of the output.
#
#  Each function is documented, implemented and tested independently.
#  When modifying a helper, the test and the documentation are
#  expected to be updated and it is easier of they are collocated. A
#  test for a given function can be run with
#
#  ~~~~~~~~~~~~~~~~{.sh}
#    ceph-helpers.sh TESTS test_get_osds
#  ~~~~~~~~~~~~~~~~
#
#  and all the tests (i.e. all functions matching test_*) are run
#  with:
#
#  ~~~~~~~~~~~~~~~~{.sh}
#    ceph-helpers.sh TESTS
#  ~~~~~~~~~~~~~~~~
#
#  A test function takes a single argument : the directory dedicated
#  to the tests. It is expected to not create any file outside of this
#  directory and remove it entirely when it completes successfully.
#


##
# Cleanup any leftovers found in **dir** via **teardown**
# and reset **dir** as an empty environment.
#
# @param dir path name of the environment
# @return 0 on success, 1 on error
#
function setup() {
    local dir=$1
    teardown $dir || return 1
    mkdir -p $dir
}

function test_setup() {
    local dir=$dir
    setup $dir || return 1
    test -d $dir || return 1
    setup $dir || return 1
    test -d $dir || return 1
    teardown $dir
}

#######################################################################

##
# Kill all daemons for which a .pid file exists in **dir** and remove
# **dir**. If the file system in which **dir** is btrfs, delete all
# subvolumes that relate to it.
#
# @param dir path name of the environment
# @return 0 on success, 1 on error
#
function teardown() {
    local dir=$1
    kill_daemons $dir KILL
    if [ $(stat -f -c '%T' .) == "btrfs" ]; then
        __teardown_btrfs $dir
    fi
    rm -fr $dir
}

function __teardown_btrfs() {
    local btrfs_base_dir=$1

    btrfs_dirs=`ls -l $btrfs_base_dir | egrep '^d' | awk '{print $9}'`
    current_path=`pwd`
    # extracting the current existing subvolumes
    for subvolume in $(cd $btrfs_base_dir; btrfs subvolume list . -t |egrep '^[0-9]' | awk '{print $4}' |grep "$btrfs_base_dir/$btrfs_dir"); do
       # Compute the relative path by removing the local path
       # Like "erwan/chroot/ceph/src/testdir/test-7202/dev/osd1/snap_439" while we want "testdir/test-7202/dev/osd1/snap_439"
       local_subvolume=$(echo $subvolume | sed -e "s|.*$current_path/||"g)
       btrfs subvolume delete $local_subvolume
    done
}

function test_teardown() {
    local dir=$dir
    setup $dir || return 1
    teardown $dir || return 1
    ! test -d $dir || return 1
}

#######################################################################

##
# Sends a signal to a single daemon.
# This is a helper function for kill_daemons
#
# After the daemon is sent **signal**, its actual termination
# will be verified by sending it signal 0. If the daemon is
# still alive, kill_daemon will pause for a few seconds and
# try again. This will repeat for a fixed number of times
# before kill_daemon returns on failure. The list of
# sleep intervals can be specified as **delays** and defaults
# to:
#
#  0.1 0.2 1 1 1 2 3 5 5 5 10 10 20 60 60 60 120
#
# This sequence is designed to run first a very short sleep time (0.1)
# if the machine is fast enough and the daemon terminates in a fraction of a
# second. The increasing sleep numbers should give plenty of time for
# the daemon to die even on the slowest running machine. If a daemon
# takes more than a few minutes to stop (the sum of all sleep times),
# there probably is no point in waiting more and a number of things
# are likely to go wrong anyway: better give up and return on error.
#
# @param pid the process id to send a signal
# @param send_signal the signal to send
# @param delays sequence of sleep times before failure
#
function kill_daemon() {
    local pid=$(cat $1)
    local send_signal=$2
    local delays=${3:-0.1 0.2 1 1 1 2 3 5 5 5 10 10 20 60 60 60 120}
    local exit_code=1
    for try in $delays ; do
         if kill -$send_signal $pid 2> /dev/null ; then
            exit_code=1
         else
            exit_code=0
            break
         fi
         send_signal=0
         sleep $try
    done;
    return $exit_code
}

function test_kill_daemon() {
    local dir=$1
    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    run_osd $dir 0 || return 1

    name_prefix=osd
    for pidfile in $(find $dir 2>/dev/null | grep $name_prefix'[^/]*\.pid') ; do
        #
        # sending signal 0 won't kill the daemon
        # waiting just for one second instead of the default schedule
        # allows us to quickly verify what happens when kill fails
        # to stop the daemon (i.e. it must return false)
        #
        ! kill_daemon $pidfile 0 1 || return 1
        #
        # killing just the osd and verify the mon still is responsive
        #
        kill_daemon $pidfile TERM || return 1
    done

    ceph osd dump | grep "osd.0 down" || return 1

    name_prefix=mon
    for pidfile in $(find $dir 2>/dev/null | grep $name_prefix'[^/]*\.pid') ; do
        #
        # kill the mon and verify it cannot be reached
        #
        kill_daemon $pidfile TERM || return 1
        ! ceph --connect-timeout 60 status || return 1
    done

    teardown $dir || return 1
}

##
# Kill all daemons for which a .pid file exists in **dir**.  Each
# daemon is sent a **signal** and kill_daemons waits for it to exit
# during a few minutes. By default all daemons are killed. If a
# **name_prefix** is provided, only the daemons for which a pid
# file is found matching the prefix are killed. See run_osd and
# run_mon for more information about the name conventions for
# the pid files.
#
# Send TERM to all daemons : kill_daemons $dir
# Send KILL to all daemons : kill_daemons $dir KILL
# Send KILL to all osds : kill_daemons $dir KILL osd
# Send KILL to osd 1 : kill_daemons $dir KILL osd.1
#
# If a daemon is sent the TERM signal and does not terminate
# within a few minutes, it will still be running even after
# kill_daemons returns. 
#
# If all daemons are kill successfully the function returns 0
# if at least one daemon remains, this is treated as an 
# error and the function return 1.
#
# @param dir path name of the environment
# @param signal name of the first signal (defaults to TERM)
# @param name_prefix only kill match daemons (defaults to all)
# @param delays sequence of sleep times before failure
# @return 0 on success, 1 on error
#
function kill_daemons() {
    local trace=$(shopt -q -o xtrace && echo true || echo false)
    $trace && shopt -u -o xtrace
    local dir=$1
    local signal=${2:-TERM}
    local name_prefix=$3 # optional, osd, mon, osd.1
    local delays=$4 #optional timing
    local status=0
    local pids=""

    for pidfile in $(find $dir 2>/dev/null | grep $name_prefix'[^/]*\.pid') ; do
	run_in_background pids kill_daemon $pidfile $signal $delays
    done

    wait_background pids
    status=$?

    $trace && shopt -s -o xtrace
    return $status
}

function test_kill_daemons() {
    local dir=$1
    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    run_osd $dir 0 || return 1
    #
    # sending signal 0 won't kill the daemon
    # waiting just for one second instead of the default schedule
    # allows us to quickly verify what happens when kill fails 
    # to stop the daemon (i.e. it must return false)
    #
    ! kill_daemons $dir 0 osd 1 || return 1
    #
    # killing just the osd and verify the mon still is responsive
    #
    kill_daemons $dir TERM osd || return 1
    ceph osd dump | grep "osd.0 down" || return 1
    #
    # kill the mon and verify it cannot be reached
    #
    kill_daemons $dir TERM || return 1
    ! ceph --connect-timeout 60 status || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Run a monitor by the name mon.**id** with data in **dir**/**id**.
# The logs can be found in **dir**/mon.**id**.log and the pid file
# is **dir**/mon.**id**.pid and the admin socket is
# **dir**/**id**/ceph-mon.**id**.asok.
#
# The remaining arguments are passed verbatim to ceph-mon --mkfs
# and the ceph-mon daemon.
#
# Two mandatory arguments must be provided: --fsid and --mon-host
# Instead of adding them to every call to run_mon, they can be
# set in the CEPH_ARGS environment variable to be read implicitly
# by every ceph command.
#
# The CEPH_CONF variable is expected to be set to /dev/null to
# only rely on arguments for configuration.
#
# Examples:
#
# CEPH_ARGS="--fsid=$(uuidgen) "
# CEPH_ARGS+="--mon-host=127.0.0.1:7018 "
# run_mon $dir a # spawn a mon and bind port 7018
# run_mon $dir a --debug-filestore=20 # spawn with filestore debugging
#
# If mon_initial_members is not set, the default rbd pool is deleted
# and replaced with a replicated pool with less placement groups to
# speed up initialization. If mon_initial_members is set, no attempt
# is made to recreate the rbd pool because it would hang forever,
# waiting for other mons to join.
#
# A **dir**/ceph.conf file is created but not meant to be used by any
# function.  It is convenient for debugging a failure with:
#
#     ceph --conf **dir**/ceph.conf -s
#
# @param dir path name of the environment
# @param id mon identifier
# @param ... can be any option valid for ceph-mon
# @return 0 on success, 1 on error
#
function run_mon() {
    local dir=$1
    shift
    local id=$1
    shift
    local data=$dir/$id

    ceph-mon \
        --id $id \
        --mkfs \
        --mon-data=$data \
        --run-dir=$dir \
        "$@" || return 1

    ceph-mon \
        --id $id \
        --mon-osd-full-ratio=.99 \
        --mon-data-avail-crit=1 \
        --paxos-propose-interval=0.1 \
        --osd-crush-chooseleaf-type=0 \
        --erasure-code-dir=$CEPH_LIB \
        --plugin-dir=$CEPH_LIB \
        --debug-mon 20 \
        --debug-ms 20 \
        --debug-paxos 20 \
        --chdir= \
        --mon-data=$data \
        --log-file=$dir/\$name.log \
        --admin-socket=$dir/\$cluster-\$name.asok \
        --mon-cluster-log-file=$dir/log \
        --run-dir=$dir \
        --pid-file=$dir/\$name.pid \
        "$@" || return 1

    cat > $dir/ceph.conf <<EOF
[global]
fsid = $(get_config mon $id fsid)
mon host = $(get_config mon $id mon_host)
EOF
    if test -z "$(get_config mon $id mon_initial_members)" ; then
        ceph osd pool delete rbd rbd --yes-i-really-really-mean-it || return 1
        ceph osd pool create rbd $PG_NUM || return 1
    fi
}

function test_run_mon() {
    local dir=$1

    setup $dir || return 1

    run_mon $dir a --mon-initial-members=a || return 1
    # rbd has not been deleted / created, hence it has pool id 0
    ceph osd dump | grep "pool 0 'rbd'" || return 1
    kill_daemons $dir || return 1

    run_mon $dir a || return 1
    # rbd has been deleted / created, hence it does not have pool id 0
    ! ceph osd dump | grep "pool 0 'rbd'" || return 1
    local size=$(CEPH_ARGS='' ceph --format=json daemon $dir/ceph-mon.a.asok \
        config get osd_pool_default_size)
    test "$size" = '{"osd_pool_default_size":"3"}' || return 1

    ! CEPH_ARGS='' ceph status || return 1
    CEPH_ARGS='' ceph --conf $dir/ceph.conf status || return 1

    kill_daemons $dir || return 1

    run_mon $dir a --osd_pool_default_size=1 || return 1
    local size=$(CEPH_ARGS='' ceph --format=json daemon $dir/ceph-mon.a.asok \
        config get osd_pool_default_size)
    test "$size" = '{"osd_pool_default_size":"1"}' || return 1
    kill_daemons $dir || return 1

    CEPH_ARGS="$CEPH_ARGS --osd_pool_default_size=2" \
        run_mon $dir a || return 1
    local size=$(CEPH_ARGS='' ceph --format=json daemon $dir/ceph-mon.a.asok \
        config get osd_pool_default_size)
    test "$size" = '{"osd_pool_default_size":"2"}' || return 1
    kill_daemons $dir || return 1

    teardown $dir || return 1
}

#######################################################################

##
# Create (prepare) and run (activate) an osd by the name osd.**id**
# with data in **dir**/**id**.  The logs can be found in
# **dir**/osd.**id**.log, the pid file is **dir**/osd.**id**.pid and
# the admin socket is **dir**/**id**/ceph-osd.**id**.asok.
#
# The remaining arguments are passed verbatim to ceph-osd.
#
# Two mandatory arguments must be provided: --fsid and --mon-host
# Instead of adding them to every call to run_osd, they can be
# set in the CEPH_ARGS environment variable to be read implicitly
# by every ceph command.
#
# The CEPH_CONF variable is expected to be set to /dev/null to
# only rely on arguments for configuration.
#
# The run_osd function creates the OSD data directory with ceph-disk
# prepare on the **dir**/**id** directory and relies on the
# activate_osd function to run the daemon.
#
# Examples:
#
# CEPH_ARGS="--fsid=$(uuidgen) "
# CEPH_ARGS+="--mon-host=127.0.0.1:7018 "
# run_osd $dir 0 # prepare and activate an osd using the monitor listening on 7018
#
# @param dir path name of the environment
# @param id osd identifier
# @param ... can be any option valid for ceph-osd
# @return 0 on success, 1 on error
#
function run_osd() {
    local dir=$1
    shift
    local id=$1
    shift
    local osd_data=$dir/$id

    local ceph_disk_args
    ceph_disk_args+=" --statedir=$dir"
    ceph_disk_args+=" --sysconfdir=$dir"
    ceph_disk_args+=" --prepend-to-path="

    mkdir -p $osd_data
    ceph-disk $ceph_disk_args \
        prepare $osd_data || return 1

    activate_osd $dir $id "$@"
}

function test_run_osd() {
    local dir=$1

    setup $dir || return 1

    run_mon $dir a || return 1

    run_osd $dir 0 || return 1
    local backfills=$(CEPH_ARGS='' ceph --format=json daemon $dir//ceph-osd.0.asok \
        config get osd_max_backfills)
    echo "$backfills" | grep --quiet 'osd_max_backfills' || return 1

    run_osd $dir 1 --osd-max-backfills 20 || return 1
    local backfills=$(CEPH_ARGS='' ceph --format=json daemon $dir//ceph-osd.1.asok \
        config get osd_max_backfills)
    test "$backfills" = '{"osd_max_backfills":"20"}' || return 1

    CEPH_ARGS="$CEPH_ARGS --osd-max-backfills 30" run_osd $dir 2 || return 1
    local backfills=$(CEPH_ARGS='' ceph --format=json daemon $dir//ceph-osd.2.asok \
        config get osd_max_backfills)
    test "$backfills" = '{"osd_max_backfills":"30"}' || return 1

    teardown $dir || return 1
}

#######################################################################

##
# Shutdown and remove all traces of the osd by the name osd.**id**.
#
# The OSD is shutdown with the TERM signal. It is then removed from
# the auth list, crush map, osd map etc and the files associated with
# it are also removed.
#
# @param dir path name of the environment
# @param id osd identifier
# @return 0 on success, 1 on error
#
function destroy_osd() {
    local dir=$1
    local id=$2

    kill_daemons $dir TERM osd.$id || return 1
    ceph osd out osd.$id || return 1
    ceph auth del osd.$id || return 1
    ceph osd crush remove osd.$id || return 1
    ceph osd rm $id || return 1
    teardown $dir/$id || return 1
    rm -fr $dir/$id
}

function test_destroy_osd() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a || return 1
    run_osd $dir 0 || return 1
    destroy_osd $dir 0 || return 1
    ! ceph osd dump | grep "osd.$id " || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Run (activate) an osd by the name osd.**id** with data in
# **dir**/**id**.  The logs can be found in **dir**/osd.**id**.log,
# the pid file is **dir**/osd.**id**.pid and the admin socket is
# **dir**/**id**/ceph-osd.**id**.asok.
#
# The remaining arguments are passed verbatim to ceph-osd.
#
# Two mandatory arguments must be provided: --fsid and --mon-host
# Instead of adding them to every call to activate_osd, they can be
# set in the CEPH_ARGS environment variable to be read implicitly
# by every ceph command.
#
# The CEPH_CONF variable is expected to be set to /dev/null to
# only rely on arguments for configuration.
#
# The activate_osd function expects a valid OSD data directory
# in **dir**/**id**, either just created via run_osd or re-using
# one left by a previous run of ceph-osd. The ceph-osd daemon is
# run indirectly via ceph-disk activate.
#
# The activate_osd function blocks until the monitor reports the osd
# up. If it fails to do so within $TIMEOUT seconds, activate_osd
# fails.
#
# Examples:
#
# CEPH_ARGS="--fsid=$(uuidgen) "
# CEPH_ARGS+="--mon-host=127.0.0.1:7018 "
# activate_osd $dir 0 # activate an osd using the monitor listening on 7018
#
# @param dir path name of the environment
# @param id osd identifier
# @param ... can be any option valid for ceph-osd
# @return 0 on success, 1 on error
#
function activate_osd() {
    local dir=$1
    shift
    local id=$1
    shift
    local osd_data=$dir/$id

    local ceph_disk_args
    ceph_disk_args+=" --statedir=$dir"
    ceph_disk_args+=" --sysconfdir=$dir"
    ceph_disk_args+=" --prepend-to-path="

    local ceph_args="$CEPH_ARGS"
    ceph_args+=" --osd-backfill-full-ratio=.99"
    ceph_args+=" --osd-failsafe-full-ratio=.99"
    ceph_args+=" --osd-journal-size=100"
    ceph_args+=" --osd-scrub-load-threshold=2000"
    ceph_args+=" --osd-data=$osd_data"
    ceph_args+=" --chdir="
    ceph_args+=" --erasure-code-dir=$CEPH_LIB"
    ceph_args+=" --plugin-dir=$CEPH_LIB"
    ceph_args+=" --osd-class-dir=$CEPH_LIB"
    ceph_args+=" --run-dir=$dir"
    ceph_args+=" --debug-osd=20"
    ceph_args+=" --log-file=$dir/\$name.log"
    ceph_args+=" --pid-file=$dir/\$name.pid"
    ceph_args+=" "
    ceph_args+="$@"
    mkdir -p $osd_data
    CEPH_ARGS="$ceph_args " ceph-disk $ceph_disk_args \
        activate \
        --mark-init=none \
        $osd_data || return 1

    [ "$id" = "$(cat $osd_data/whoami)" ] || return 1

    ceph osd crush create-or-move "$id" 1 root=default host=localhost

    wait_for_osd up $id || return 1
}

function test_activate_osd() {
    local dir=$1

    setup $dir || return 1

    run_mon $dir a || return 1

    run_osd $dir 0 || return 1
    local backfills=$(CEPH_ARGS='' ceph --format=json daemon $dir//ceph-osd.0.asok \
        config get osd_max_backfills)
    echo "$backfills" | grep --quiet 'osd_max_backfills' || return 1

    kill_daemons $dir TERM osd || return 1

    activate_osd $dir 0 --osd-max-backfills 20 || return 1
    local backfills=$(CEPH_ARGS='' ceph --format=json daemon $dir//ceph-osd.0.asok \
        config get osd_max_backfills)
    test "$backfills" = '{"osd_max_backfills":"20"}' || return 1

    teardown $dir || return 1
}

#######################################################################

##
# Wait until the OSD **id** is either up or down, as specified by
# **state**. It fails after $TIMEOUT seconds.
#
# @param state either up or down
# @param id osd identifier
# @return 0 on success, 1 on error
#
function wait_for_osd() {
    local state=$1
    local id=$2

    status=1
    for ((i=0; i < $TIMEOUT; i++)); do
        echo $i
        if ! ceph osd dump | grep "osd.$id $state"; then
            sleep 1
        else
            status=0
            break
        fi
    done
    return $status
}

function test_wait_for_osd() {
    local dir=$1
    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    run_osd $dir 0 || return 1
    wait_for_osd up 0 || return 1
    kill_daemons $dir TERM osd || return 1
    wait_for_osd down 0 || return 1
    ( TIMEOUT=1 ; ! wait_for_osd up 0 ) || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Display the list of OSD ids supporting the **objectname** stored in
# **poolname**, as reported by ceph osd map.
#
# @param poolname an existing pool
# @param objectname an objectname (may or may not exist)
# @param STDOUT white space separated list of OSD ids
# @return 0 on success, 1 on error
#
function get_osds() {
    local poolname=$1
    local objectname=$2

    local osds=$(ceph --format xml osd map $poolname $objectname 2>/dev/null | \
        $XMLSTARLET sel -t -m "//acting/osd" -v . -o ' ')
    # get rid of the trailing space
    echo $osds
}

function test_get_osds() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=2 || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    wait_for_clean || return 1
    get_osds rbd GROUP | grep --quiet '^[0-1] [0-1]$' || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Return the PG of supporting the **objectname** stored in
# **poolname**, as reported by ceph osd map.
#
# @param poolname an existing pool
# @param objectname an objectname (may or may not exist)
# @param STDOUT a PG
# @return 0 on success, 1 on error
#
function get_pg() {
    local poolname=$1
    local objectname=$2

    ceph --format xml osd map $poolname $objectname 2>/dev/null | \
        $XMLSTARLET sel -t -m "//pgid" -v . -n
}

function test_get_pg() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    run_osd $dir 0 || return 1
    wait_for_clean || return 1
    get_pg rbd GROUP | grep --quiet '^[0-9]\.[0-9a-f][0-9a-f]*$' || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Return the value of the **config**, obtained via the config get command
# of the admin socket of **daemon**.**id**.
#
# @param daemon mon or osd
# @param id mon or osd ID
# @param config the configuration variable name as found in config_opts.h
# @param STDOUT the config value
# @return 0 on success, 1 on error
#
function get_config() {
    local daemon=$1
    local id=$2
    local config=$3

    CEPH_ARGS='' \
        ceph --format xml daemon $dir/ceph-$daemon.$id.asok \
        config get $config 2> /dev/null | \
        $XMLSTARLET sel -t -m "//$config" -v . -n
}

function test_get_config() {
    local dir=$1

    # override the default config using command line arg and check it
    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    test $(get_config mon a osd_pool_default_size) = 1 || return 1
    run_osd $dir 0 --osd_max_scrubs=3 || return 1
    test $(get_config osd 0 osd_max_scrubs) = 3 || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Set the **config** to specified **value**, via the config set command
# of the admin socket of **daemon**.**id**
#
# @param daemon mon or osd
# @param id mon or osd ID
# @param config the configuration variable name as found in config_opts.h
# @param value the config value
# @return 0 on success, 1 on error
#
function set_config() {
    local daemon=$1
    local id=$2
    local config=$3
    local value=$4

    CEPH_ARGS='' \
        ceph --format xml daemon $dir/ceph-$daemon.$id.asok \
        config set $config $value 2> /dev/null | \
        $XMLSTARLET sel -Q -t -m "//success" -v .
}

function test_set_config() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    test $(get_config mon a ms_crc_header) = true || return 1
    set_config mon a ms_crc_header false || return 1
    test $(get_config mon a ms_crc_header) = false || return 1
    set_config mon a ms_crc_header true || return 1
    test $(get_config mon a ms_crc_header) = true || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Return the OSD id of the primary OSD supporting the **objectname**
# stored in **poolname**, as reported by ceph osd map.
#
# @param poolname an existing pool
# @param objectname an objectname (may or may not exist)
# @param STDOUT the primary OSD id
# @return 0 on success, 1 on error
#
function get_primary() {
    local poolname=$1
    local objectname=$2

    ceph --format xml osd map $poolname $objectname 2>/dev/null | \
        $XMLSTARLET sel -t -m "//acting_primary" -v . -n
}

function test_get_primary() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    local osd=0
    run_osd $dir $osd || return 1
    wait_for_clean || return 1
    test $(get_primary rbd GROUP) = $osd || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Return the id of any OSD supporting the **objectname** stored in
# **poolname**, as reported by ceph osd map, except the primary.
#
# @param poolname an existing pool
# @param objectname an objectname (may or may not exist)
# @param STDOUT the OSD id
# @return 0 on success, 1 on error
#
function get_not_primary() {
    local poolname=$1
    local objectname=$2

    local primary=$(get_primary $poolname $objectname)
    ceph --format xml osd map $poolname $objectname 2>/dev/null | \
        $XMLSTARLET sel -t -m "//acting/osd[not(.='$primary')]" -v . -n | \
        head -1
}

function test_get_not_primary() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=2 || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    wait_for_clean || return 1
    local primary=$(get_primary rbd GROUP)
    local not_primary=$(get_not_primary rbd GROUP)
    test $not_primary != $primary || return 1
    test $not_primary = 0 -o $not_primary = 1 || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Run ceph-objectstore-tool against the OSD **id** using the data path
# **dir**. The OSD is killed with TERM prior to running
# ceph-objectstore-tool because access to the data path is
# exclusive. The OSD is restarted after the command completes. The
# objectstore_tool returns after all PG are active+clean again.
#
# @param dir the data path of the OSD
# @param id the OSD id
# @param ... arguments to ceph-objectstore-tool
# @param STDIN the input of ceph-objectstore-tool
# @param STDOUT the output of ceph-objectstore-tool
# @return 0 on success, 1 on error
#
function objectstore_tool() {
    local dir=$1
    shift
    local id=$1
    shift
    local osd_data=$dir/$id

    kill_daemons $dir TERM osd.$id >&2 < /dev/null || return 1
    ceph-objectstore-tool \
        --data-path $osd_data \
        --journal-path $osd_data/journal \
        "$@" || return 1
    activate_osd $dir $id >&2 || return 1
    wait_for_clean >&2
}

function test_objectstore_tool() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    local osd=0
    run_osd $dir $osd || return 1
    wait_for_clean || return 1
    rados --pool rbd put GROUP /etc/group || return 1
    objectstore_tool $dir $osd GROUP get-bytes | \
        diff - /etc/group
    ! objectstore_tool $dir $osd NOTEXISTS get-bytes || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Predicate checking if there is an ongoing recovery in the
# cluster. If any of the recovering_{keys,bytes,objects}_per_sec
# counters are reported by ceph status, it means recovery is in
# progress.
#
# @return 0 if recovery in progress, 1 otherwise
#
function get_is_making_recovery_progress() {
    local progress=$(ceph --format xml status 2>/dev/null | \
        $XMLSTARLET sel \
        -t -m "//pgmap/recovering_keys_per_sec" -v . -o ' ' \
        -t -m "//pgmap/recovering_bytes_per_sec" -v . -o ' ' \
        -t -m "//pgmap/recovering_objects_per_sec" -v .)
    test -n "$progress"
}

function test_get_is_making_recovery_progress() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a || return 1
    ! get_is_making_recovery_progress || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Return the number of active PGs in the cluster. A PG is active if
# ceph pg dump pgs reports it both **active** and **clean** and that
# not **stale**.
#
# @param STDOUT the number of active PGs
# @return 0 on success, 1 on error
#
function get_num_active_clean() {
    local expression="("
    expression+="contains(.,'active') and "
    expression+="contains(.,'clean') and "
    expression+="not(contains(.,'stale'))"
    expression+=")"
    # xmlstarlet 1.3.0 (which is on Ubuntu precise)
    # add extra new lines that must be ignored with
    # grep -v '^$' 
    ceph --format xml pg dump pgs 2>/dev/null | \
        $XMLSTARLET sel -t -m "//pg_stat/state[$expression]" -v . -n | \
        grep -cv '^$'
}

function test_get_num_active_clean() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    run_osd $dir 0 || return 1
    wait_for_clean || return 1
    local num_active_clean=$(get_num_active_clean)
    test "$num_active_clean" = $PG_NUM || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Return the number of PGs in the cluster, according to
# ceph pg dump pgs.
#
# @param STDOUT the number of PGs
# @return 0 on success, 1 on error
#
function get_num_pgs() {
    ceph --format xml status 2>/dev/null | \
        $XMLSTARLET sel -t -m "//pgmap/num_pgs" -v .
}

function test_get_num_pgs() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    run_osd $dir 0 || return 1
    wait_for_clean || return 1
    local num_pgs=$(get_num_pgs)
    test "$num_pgs" -gt 0 || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Return the date and time of the last completed scrub for **pgid**,
# as reported by ceph pg dump pgs. Note that a repair also sets this
# date.
#
# @param pgid the id of the PG
# @param STDOUT the date and time of the last scrub
# @return 0 on success, 1 on error
#
function get_last_scrub_stamp() {
    local pgid=$1
    ceph --format xml pg dump pgs 2>/dev/null | \
        $XMLSTARLET sel -t -m "//pg_stat[pgid='$pgid']/last_scrub_stamp" -v .
}

function test_get_last_scrub_stamp() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    run_osd $dir 0 || return 1
    wait_for_clean || return 1
    stamp=$(get_last_scrub_stamp 1.0)
    test -n "$stamp" || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Predicate checking if the cluster is clean, i.e. all of its PGs are
# in a clean state (see get_num_active_clean for a definition).
#
# @return 0 if the cluster is clean, 1 otherwise
#
function is_clean() {
    num_pgs=$(get_num_pgs)
    test $num_pgs != 0 || return 1
    test $(get_num_active_clean) = $num_pgs || return 1
}

function test_is_clean() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    run_osd $dir 0 || return 1
    ! is_clean || return 1
    wait_for_clean || return 1
    is_clean || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Wait until the cluster becomes clean or if it does not make progress
# for $TIMEOUT seconds.
# Progress is measured either via the **get_is_making_recovery_progress**
# predicate or if the number of clean PGs changes (as returned by get_num_active_clean)
#
# @return 0 if the cluster is clean, 1 otherwise
#
function wait_for_clean() {
    local status=1
    local num_active_clean=-1
    local cur_active_clean
    local -i timer=0
    local num_pgs=$(get_num_pgs)
    test $num_pgs != 0 || return 1

    while true ; do
        # Comparing get_num_active_clean & get_num_pgs is used to determine
        # if the cluster is clean. That's almost an inline of is_clean() to
        # get more performance by avoiding multiple calls of get_num_active_clean.
        cur_active_clean=$(get_num_active_clean)
        test $cur_active_clean = $num_pgs && break
        if test $cur_active_clean != $num_active_clean ; then
            timer=0
            num_active_clean=$cur_active_clean
        elif get_is_making_recovery_progress ; then
            timer=0
        elif (( timer >= $(($TIMEOUT * 10)))) ; then
            ceph report
            return 1
        fi
        sleep .1
        timer=$(expr $timer + 1)
    done
    return 0
}

function test_wait_for_clean() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    ! TIMEOUT=1 wait_for_clean || return 1
    run_osd $dir 0 || return 1
    wait_for_clean || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Run repair on **pgid** and wait until it completes. The repair
# function will fail if repair does not complete within $TIMEOUT
# seconds.
#
# @param pgid the id of the PG
# @return 0 on success, 1 on error
#
function repair() {
    local pgid=$1
    local last_scrub=$(get_last_scrub_stamp $pgid)
    ceph pg repair $pgid
    wait_for_scrub $pgid "$last_scrub"
}

function test_repair() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    run_osd $dir 0 || return 1
    wait_for_clean || return 1
    repair 1.0 || return 1
    kill_daemons $dir KILL osd || return 1
    ! TIMEOUT=1 repair 1.0 || return 1
    teardown $dir || return 1
}
#######################################################################

##
# Run scrub on **pgid** and wait until it completes. The pg_scrub
# function will fail if repair does not complete within $TIMEOUT
# seconds. The pg_scrub is complete whenever the
# **get_last_scrub_stamp** function reports a timestamp different from
# the one stored before starting the scrub.
#
# @param pgid the id of the PG
# @return 0 on success, 1 on error
#
function pg_scrub() {
    local pgid=$1
    local last_scrub=$(get_last_scrub_stamp $pgid)
    ceph pg scrub $pgid
    wait_for_scrub $pgid "$last_scrub"
}

function test_pg_scrub() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    run_osd $dir 0 || return 1
    wait_for_clean || return 1
    pg_scrub 1.0 || return 1
    kill_daemons $dir KILL osd || return 1
    ! TIMEOUT=1 pg_scrub 1.0 || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Run the *command* and expect it to fail (i.e. return a non zero status).
# The output (stderr and stdout) is stored in a temporary file in *dir*
# and is expected to contain the string *expected*.
#
# Return 0 if the command failed and the string was found. Otherwise
# return 1 and cat the full output of the command on stderr for debug.
#
# @param dir temporary directory to store the output
# @param expected string to look for in the output
# @param command ... the command and its arguments
# @return 0 on success, 1 on error
#

function expect_failure() {
    local dir=$1
    shift
    local expected="$1"
    shift
    local success

    if "$@" > $dir/out 2>&1 ; then
        success=true
    else
        success=false
    fi

    if $success || ! grep --quiet "$expected" $dir/out ; then
        cat $dir/out >&2
        return 1
    else
        return 0
    fi
}

function test_expect_failure() {
    local dir=$1

    setup $dir || return 1
    expect_failure $dir FAIL bash -c 'echo FAIL ; exit 1' || return 1
    # the command did not fail
    ! expect_failure $dir FAIL bash -c 'echo FAIL ; exit 0' > $dir/out || return 1
    grep --quiet FAIL $dir/out || return 1
    # the command failed but the output does not contain the expected string
    ! expect_failure $dir FAIL bash -c 'echo UNEXPECTED ; exit 1' > $dir/out || return 1
    ! grep --quiet FAIL $dir/out || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Given the *last_scrub*, wait for scrub to happen on **pgid**.  It
# will fail if scrub does not complete within $TIMEOUT seconds. The
# repair is complete whenever the **get_last_scrub_stamp** function
# reports a timestamp different from the one given in argument.
#
# @param pgid the id of the PG
# @param last_scrub timestamp of the last scrub for *pgid*
# @return 0 on success, 1 on error
#
function wait_for_scrub() {
    local pgid=$1
    local last_scrub="$2"

    for ((i=0; i < $TIMEOUT; i++)); do
        if test "$last_scrub" != "$(get_last_scrub_stamp $pgid)" ; then
            return 0
        fi
        sleep 1
    done
    return 1
}

function test_wait_for_scrub() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    run_osd $dir 0 || return 1
    wait_for_clean || return 1
    local pgid=1.0
    ceph pg repair $pgid
    local last_scrub=$(get_last_scrub_stamp $pgid)
    wait_for_scrub $pgid "$last_scrub" || return 1
    kill_daemons $dir KILL osd || return 1
    last_scrub=$(get_last_scrub_stamp $pgid)
    ! TIMEOUT=1 wait_for_scrub $pgid "$last_scrub" || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Return 0 if the erasure code *plugin* is available, 1 otherwise.
#
# @param plugin erasure code plugin
# @return 0 on success, 1 on error
#

function erasure_code_plugin_exists() {
    local plugin=$1

    local status
    if ceph osd erasure-code-profile set TESTPROFILE plugin=$plugin 2>&1 |
        grep "$plugin.*No such file" ; then
        status=1
    else
        status=0
        ceph osd erasure-code-profile rm TESTPROFILE
    fi
    return $status
}

function test_erasure_code_plugin_exists() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a || return 1
    erasure_code_plugin_exists jerasure || return 1
    ! erasure_code_plugin_exists FAKE || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Display all log files from **dir** on stdout.
#
# @param dir directory in which all data is stored
#

function display_logs() {
    local dir=$1

    find $dir -maxdepth 1 -name '*.log' | \
        while read file ; do
            echo "======================= $file"
            cat $file
        done
}

function test_display_logs() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a || return 1
    kill_daemons $dir || return 1
    display_logs $dir > $dir/log.out
    grep --quiet mon.a.log $dir/log.out || return 1
    teardown $dir || return 1
}

#######################################################################
##
# Spawn a command in background and save the pid in the variable name
# passed in argument. To make the output reading easier, the output is
# prepend with the process id.
#
# Example:
#   pids1=""
#   run_in_background pids1 bash -c 'sleep 1; exit 1'
#
# @param pid_variable the variable name (not value) where the pids will be stored
# @param ... the command to execute
# @return only the pid_variable output should be considered and used with **wait_background**
#
function run_in_background() {
    local pid_variable=$1
    shift;
    # Execute the command and prepend the output with its pid
    # We enforce to return the exit status of the command and not the awk one.
    ("$@" |& awk '{ a[i++] = $0 }END{for (i = 0; i in a; ++i) { print PROCINFO["pid"] ": " a[i]} }'; return ${PIPESTATUS[0]}) &
    eval "$pid_variable+=\" $!\""
}

function test_run_in_background() {
    local pids
    run_in_background pids sleep 1
    run_in_background pids sleep 1
    test $(echo $pids | wc -w) = 2 || return 1
    wait $pids || return 1
}

#######################################################################
##
# Wait for pids running in background to complete.
# This function is usually used after a **run_in_background** call
# Example:
#   pids1=""
#   run_in_background pids1 bash -c 'sleep 1; exit 1'
#   wait_background pids1
#
# @param pids The variable name that contains the active PIDS. Set as empty at then end of the function.
# @return returns 1 if at least one process exits in error unless returns 0
#
function wait_background() {
    # We extract the PIDS from the variable name
    pids=${!1}

    return_code=0
    for pid in $pids; do
        if ! wait $pid; then
            # If one process failed then return 1
            return_code=1
        fi
    done

    # We empty the variable reporting that all process ended
    eval "$1=''"

    return $return_code
}


function test_wait_background() {
    local pids=""
    run_in_background pids bash -c "sleep 1; exit 1"
    run_in_background pids bash -c "sleep 2; exit 0"
    wait_background pids
    if [ $? -ne 1 ]; then return 1; fi

    run_in_background pids bash -c "sleep 1; exit 0"
    run_in_background pids bash -c "sleep 2; exit 0"
    wait_background pids
    if [ $? -ne 0 ]; then return 1; fi

    if [ ! -z "$pids" ]; then return 1; fi
}

#######################################################################

##
# Call the **run** function (which must be defined by the caller) with
# the **dir** argument followed by the caller argument list.
#
# If the **run** function returns on error, all logs found in **dir**
# are displayed for diagnostic purposes.
#
# **teardown** function is called when the **run** function returns
# (on success or on error), to cleanup leftovers. The CEPH_CONF is set
# to /dev/null and CEPH_ARGS is unset so that the tests are protected from
# external interferences.
#
# It is the responsibility of the **run** function to call the
# **setup** function to prepare the test environment (create a temporary
# directory etc.).
#
# The shell is required (via PS4) to display the function and line
# number whenever a statement is executed to help debugging.
#
# @param dir directory in which all data is stored
# @param ... arguments passed transparently to **run**
# @return 0 on success, 1 on error
#
function main() {
    local dir=testdir/$1
    shift

    shopt -s -o xtrace
    PS4='${BASH_SOURCE[0]}:$LINENO: ${FUNCNAME[0]}:  '

    export PATH=${CEPH_BUILD_VIRTUALENV}/ceph-disk-virtualenv/bin:${CEPH_BUILD_VIRTUALENV}/ceph-detect-init-virtualenv/bin:.:$PATH # make sure program from sources are prefered
    #export PATH=$CEPH_ROOT/src/ceph-disk/virtualenv/bin:$CEPH_ROOT/src/ceph-detect-init/virtualenv/bin:.:$PATH # make sure program from sources are prefered

    export CEPH_CONF=/dev/null
    unset CEPH_ARGS

    local code
    if run $dir "$@" ; then
        code=0
    else
        display_logs $dir
        code=1
    fi
    teardown $dir || return 1
    return $code
}

#######################################################################

function run_tests() {
    shopt -s -o xtrace
    PS4='${BASH_SOURCE[0]}:$LINENO: ${FUNCNAME[0]}:  '

    export PATH=${CEPH_BUILD_VIRTUALENV}/ceph-disk-virtualenv/bin:${CEPH_BUILD_VIRTUALENV}/ceph-detect-init-virtualenv/bin:.:$PATH # make sure program from sources are prefered
    #export PATH=$CEPH_ROOT/src/ceph-disk/virtualenv/bin:$CEPH_ROOT/src/ceph-detect-init/virtualenv/bin:.:$PATH # make sure program from sources are prefered

    export CEPH_MON="127.0.0.1:7109" # git grep '\<7109\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    export CEPH_CONF=/dev/null

    local funcs=${@:-$(set | sed -n -e 's/^\(test_[0-9a-z_]*\) .*/\1/p')}
    local dir=testdir/ceph-helpers

    for func in $funcs ; do
        $func $dir || return 1
    done
}

if test "$1" = TESTS ; then
    shift
    run_tests "$@"
fi

# Local Variables:
# compile-command: "cd ../../src ; make -j4 && ../qa/workunits/ceph-helpers.sh TESTS # test_get_config"
# End:
