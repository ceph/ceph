#!/usr/bin/env bash
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
WAIT_FOR_CLEAN_TIMEOUT=90
MAX_TIMEOUT=15
PG_NUM=4
TMPDIR=${TMPDIR:-/tmp}
CEPH_BUILD_VIRTUALENV=${TMPDIR}
TESTDIR=${TESTDIR:-${TMPDIR}}

if type xmlstarlet > /dev/null 2>&1; then
    XMLSTARLET=xmlstarlet
elif type xml > /dev/null 2>&1; then
    XMLSTARLET=xml
else
	echo "Missing xmlstarlet binary!"
	exit 1
fi

if [ `uname` = FreeBSD ]; then
    SED=gsed
    AWK=gawk
    DIFFCOLOPTS=""
    KERNCORE="kern.corefile"
else
    SED=sed
    AWK=awk
    termwidth=$(stty -a | head -1 | sed -e 's/.*columns \([0-9]*\).*/\1/')
    if [ -n "$termwidth" -a "$termwidth" != "0" ]; then
        termwidth="-W ${termwidth}"
    fi
    DIFFCOLOPTS="-y $termwidth"
    KERNCORE="kernel.core_pattern"
fi

EXTRA_OPTS=""

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


function get_asok_dir() {
    if [ -n "$CEPH_ASOK_DIR" ]; then
        echo "$CEPH_ASOK_DIR"
    else
        echo ${TMPDIR:-/tmp}/ceph-asok.$$
    fi
}

function get_asok_path() {
    local name=$1
    if [ -n "$name" ]; then
        echo $(get_asok_dir)/ceph-$name.asok
    else
        echo $(get_asok_dir)/\$cluster-\$name.asok
    fi
}
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
    mkdir -p $(get_asok_dir)
    if [ $(ulimit -n) -le 1024 ]; then
        ulimit -n 4096 || return 1
    fi
    if [ -z "$LOCALRUN" ]; then
        trap "teardown $dir 1" TERM HUP INT
    fi
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
# @param dumplogs pass "1" to dump logs otherwise it will only if cores found
# @return 0 on success, 1 on error
#
function teardown() {
    local dir=$1
    local dumplogs=$2
    kill_daemons $dir KILL
    if [ `uname` != FreeBSD ] \
        && [ $(stat -f -c '%T' .) == "btrfs" ]; then
        __teardown_btrfs $dir
    fi
    local cores="no"
    local pattern="$(sysctl -n $KERNCORE)"
    # See if we have apport core handling
    if [ "${pattern:0:1}" = "|" ]; then
      # TODO: Where can we get the dumps?
      # Not sure where the dumps really are so this will look in the CWD
      pattern=""
    fi
    # Local we start with core and teuthology ends with core
    if ls $(dirname "$pattern") | grep -q '^core\|core$' ; then
        cores="yes"
        if [ -n "$LOCALRUN" ]; then
	    mkdir /tmp/cores.$$ 2> /dev/null || true
	    for i in $(ls $(dirname $(sysctl -n $KERNCORE)) | grep '^core\|core$'); do
		mv $i /tmp/cores.$$
	    done
        fi
    fi
    if [ "$cores" = "yes" -o "$dumplogs" = "1" ]; then
	if [ -n "$LOCALRUN" ]; then
	    display_logs $dir
        else
	    # Move logs to where Teuthology will archive it
	    mkdir -p $TESTDIR/archive/log
	    mv $dir/*.log $TESTDIR/archive/log
	fi
    fi
    rm -fr $dir
    rm -rf $(get_asok_dir)
    if [ "$cores" = "yes" ]; then
        echo "ERROR: Failure due to cores found"
        if [ -n "$LOCALRUN" ]; then
	    echo "Find saved core files in /tmp/cores.$$"
        fi
        return 1
    fi
    return 0
}

function __teardown_btrfs() {
    local btrfs_base_dir=$1
    local btrfs_root=$(df -P . | tail -1 | $AWK '{print $NF}')
    local btrfs_dirs=$(cd $btrfs_base_dir; sudo btrfs subvolume list -t . | $AWK '/^[0-9]/ {print $4}' | grep "$btrfs_base_dir/$btrfs_dir")
    for subvolume in $btrfs_dirs; do
       sudo btrfs subvolume delete $btrfs_root/$subvolume
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
    # In order to try after the last large sleep add 0 at the end so we check
    # one last time before dropping out of the loop
    for try in $delays 0 ; do
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
    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x || return 1
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

    name_prefix=mgr
    for pidfile in $(find $dir 2>/dev/null | grep $name_prefix'[^/]*\.pid') ; do
        #
        # kill the mgr
        #
        kill_daemon $pidfile TERM || return 1
    done

    name_prefix=mon
    for pidfile in $(find $dir 2>/dev/null | grep $name_prefix'[^/]*\.pid') ; do
        #
        # kill the mon and verify it cannot be reached
        #
        kill_daemon $pidfile TERM || return 1
        ! timeout 5 ceph status || return 1
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
    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x || return 1
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
    #
    # kill the mgr
    #
    kill_daemons $dir TERM mgr || return 1
    #
    # kill the mon and verify it cannot be reached
    #
    kill_daemons $dir TERM || return 1
    ! timeout 5 ceph status || return 1
    teardown $dir || return 1
}

#
# return a random TCP port which is not used yet
#
# please note, there could be racing if we use this function for
# a free port, and then try to bind on this port.
#
function get_unused_port() {
    local ip=127.0.0.1
    python3 -c "import socket; s=socket.socket(); s.bind(('$ip', 0)); print(s.getsockname()[1]); s.close()"
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
	--osd-failsafe-full-ratio=.99 \
        --mon-osd-full-ratio=.99 \
        --mon-data-avail-crit=1 \
        --mon-data-avail-warn=5 \
        --paxos-propose-interval=0.1 \
        --osd-crush-chooseleaf-type=0 \
        $EXTRA_OPTS \
        --debug-mon 20 \
        --debug-ms 20 \
        --debug-paxos 20 \
        --chdir= \
        --mon-data=$data \
        --log-file=$dir/\$name.log \
        --admin-socket=$(get_asok_path) \
        --mon-cluster-log-file=$dir/log \
        --run-dir=$dir \
        --pid-file=$dir/\$name.pid \
	--mon-allow-pool-delete \
	--mon-allow-pool-size-one \
	--osd-pool-default-pg-autoscale-mode off \
	--mon-osd-backfillfull-ratio .99 \
	--mon-warn-on-insecure-global-id-reclaim-allowed=false \
        "$@" || return 1

    cat > $dir/ceph.conf <<EOF
[global]
fsid = $(get_config mon $id fsid)
mon host = $(get_config mon $id mon_host)
EOF
}

function test_run_mon() {
    local dir=$1

    setup $dir || return 1

    run_mon $dir a || return 1
    ceph mon dump | grep "mon.a" || return 1
    kill_daemons $dir || return 1

    run_mon $dir a --osd_pool_default_size=3 || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    create_rbd_pool || return 1
    ceph osd dump | grep "pool 1 'rbd'" || return 1
    local size=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path mon.a) \
        config get osd_pool_default_size)
    test "$size" = '{"osd_pool_default_size":"3"}' || return 1

    ! CEPH_ARGS='' ceph status || return 1
    CEPH_ARGS='' ceph --conf $dir/ceph.conf status || return 1

    kill_daemons $dir || return 1

    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    local size=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path mon.a) \
        config get osd_pool_default_size)
    test "$size" = '{"osd_pool_default_size":"1"}' || return 1
    kill_daemons $dir || return 1

    CEPH_ARGS="$CEPH_ARGS --osd_pool_default_size=2" \
        run_mon $dir a || return 1
    local size=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path mon.a) \
        config get osd_pool_default_size)
    test "$size" = '{"osd_pool_default_size":"2"}' || return 1
    kill_daemons $dir || return 1

    teardown $dir || return 1
}

function create_rbd_pool() {
    ceph osd pool delete rbd rbd --yes-i-really-really-mean-it || return 1
    create_pool rbd $PG_NUM || return 1
    rbd pool init rbd
}

function create_pool() {
    ceph osd pool create "$@"
    sleep 1
}

function delete_pool() {
    local poolname=$1
    ceph osd pool delete $poolname $poolname --yes-i-really-really-mean-it
}

#######################################################################

function run_mgr() {
    local dir=$1
    shift
    local id=$1
    shift
    local data=$dir/$id

    ceph config set mgr mgr_pool false --force
    ceph-mgr \
        --id $id \
        $EXTRA_OPTS \
	--osd-failsafe-full-ratio=.99 \
        --debug-mgr 20 \
	--debug-objecter 20 \
        --debug-ms 20 \
        --debug-paxos 20 \
        --chdir= \
        --mgr-data=$data \
        --log-file=$dir/\$name.log \
        --admin-socket=$(get_asok_path) \
        --run-dir=$dir \
        --pid-file=$dir/\$name.pid \
        --mgr-module-path=$(realpath ${CEPH_ROOT}/src/pybind/mgr) \
        "$@" || return 1
}

function run_mds() {
    local dir=$1
    shift
    local id=$1
    shift
    local data=$dir/$id

    ceph-mds \
        --id $id \
        $EXTRA_OPTS \
	--debug-mds 20 \
	--debug-objecter 20 \
        --debug-ms 20 \
        --chdir= \
        --mds-data=$data \
        --log-file=$dir/\$name.log \
        --admin-socket=$(get_asok_path) \
        --run-dir=$dir \
        --pid-file=$dir/\$name.pid \
        "$@" || return 1
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
# The run_osd function creates the OSD data directory on the **dir**/**id**
# directory and relies on the activate_osd function to run the daemon.
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

    local ceph_args="$CEPH_ARGS"
    ceph_args+=" --osd-failsafe-full-ratio=.99"
    ceph_args+=" --osd-journal-size=100"
    ceph_args+=" --osd-scrub-load-threshold=2000"
    ceph_args+=" --osd-data=$osd_data"
    ceph_args+=" --osd-journal=${osd_data}/journal"
    ceph_args+=" --chdir="
    ceph_args+=$EXTRA_OPTS
    ceph_args+=" --run-dir=$dir"
    ceph_args+=" --admin-socket=$(get_asok_path)"
    ceph_args+=" --debug-osd=20"
    ceph_args+=" --debug-ms=1"
    ceph_args+=" --debug-monc=20"
    ceph_args+=" --log-file=$dir/\$name.log"
    ceph_args+=" --pid-file=$dir/\$name.pid"
    ceph_args+=" --osd-max-object-name-len=460"
    ceph_args+=" --osd-max-object-namespace-len=64"
    ceph_args+=" --enable-experimental-unrecoverable-data-corrupting-features=*"
    ceph_args+=" --osd-mclock-profile=high_recovery_ops"
    ceph_args+=" "
    ceph_args+="$@"
    mkdir -p $osd_data

    local uuid=`uuidgen`
    echo "add osd$id $uuid"
    OSD_SECRET=$(ceph-authtool --gen-print-key)
    echo "{\"cephx_secret\": \"$OSD_SECRET\"}" > $osd_data/new.json
    ceph osd new $uuid -i $osd_data/new.json
    rm $osd_data/new.json
    ceph-osd -i $id $ceph_args --mkfs --key $OSD_SECRET --osd-uuid $uuid

    local key_fn=$osd_data/keyring
    cat > $key_fn<<EOF
[osd.$id]
key = $OSD_SECRET
EOF
    echo adding osd$id key to auth repository
    ceph -i "$key_fn" auth add osd.$id osd "allow *" mon "allow profile osd" mgr "allow profile osd"
    echo start osd.$id
    ceph-osd -i $id $ceph_args &

    # If noup is set, then can't wait for this osd
    if ceph osd dump --format=json | jq '.flags_set[]' | grep -q '"noup"' ; then
      return 0
    fi
    wait_for_osd up $id || return 1

}

function run_osd_filestore() {
    local dir=$1
    shift
    local id=$1
    shift
    local osd_data=$dir/$id

    local ceph_args="$CEPH_ARGS"
    ceph_args+=" --osd-failsafe-full-ratio=.99"
    ceph_args+=" --osd-journal-size=100"
    ceph_args+=" --osd-scrub-load-threshold=2000"
    ceph_args+=" --osd-data=$osd_data"
    ceph_args+=" --osd-journal=${osd_data}/journal"
    ceph_args+=" --chdir="
    ceph_args+=$EXTRA_OPTS
    ceph_args+=" --run-dir=$dir"
    ceph_args+=" --admin-socket=$(get_asok_path)"
    ceph_args+=" --debug-osd=20"
    ceph_args+=" --debug-ms=1"
    ceph_args+=" --debug-monc=20"
    ceph_args+=" --log-file=$dir/\$name.log"
    ceph_args+=" --pid-file=$dir/\$name.pid"
    ceph_args+=" --osd-max-object-name-len=460"
    ceph_args+=" --osd-max-object-namespace-len=64"
    ceph_args+=" --enable-experimental-unrecoverable-data-corrupting-features=*"
    ceph_args+=" "
    ceph_args+="$@"
    mkdir -p $osd_data

    local uuid=`uuidgen`
    echo "add osd$osd $uuid"
    OSD_SECRET=$(ceph-authtool --gen-print-key)
    echo "{\"cephx_secret\": \"$OSD_SECRET\"}" > $osd_data/new.json
    ceph osd new $uuid -i $osd_data/new.json
    rm $osd_data/new.json
    ceph-osd -i $id $ceph_args --mkfs --key $OSD_SECRET --osd-uuid $uuid --osd-objectstore=filestore

    local key_fn=$osd_data/keyring
    cat > $key_fn<<EOF
[osd.$osd]
key = $OSD_SECRET
EOF
    echo adding osd$id key to auth repository
    ceph -i "$key_fn" auth add osd.$id osd "allow *" mon "allow profile osd" mgr "allow profile osd"
    echo start osd.$id
    ceph-osd -i $id $ceph_args &

    # If noup is set, then can't wait for this osd
    if ceph osd dump --format=json | jq '.flags_set[]' | grep -q '"noup"' ; then
      return 0
    fi
    wait_for_osd up $id || return 1


}

function test_run_osd() {
    local dir=$1

    setup $dir || return 1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1

    run_osd $dir 0 || return 1
    local backfills=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path osd.0) \
        config get osd_max_backfills)
    echo "$backfills" | grep --quiet 'osd_max_backfills' || return 1

    run_osd $dir 1 --osd-max-backfills 20 || return 1
    local backfills=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path osd.1) \
        config get osd_max_backfills)
    test "$backfills" = '{"osd_max_backfills":"20"}' || return 1

    CEPH_ARGS="$CEPH_ARGS --osd-max-backfills 30" run_osd $dir 2 || return 1
    local backfills=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path osd.2) \
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

    ceph osd out osd.$id || return 1
    kill_daemons $dir TERM osd.$id || return 1
    ceph osd down osd.$id || return 1
    ceph osd purge osd.$id --yes-i-really-mean-it || return 1
    teardown $dir/$id || return 1
    rm -fr $dir/$id
}

function test_destroy_osd() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
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
# run directly on the foreground
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

    local ceph_args="$CEPH_ARGS"
    ceph_args+=" --osd-failsafe-full-ratio=.99"
    ceph_args+=" --osd-journal-size=100"
    ceph_args+=" --osd-scrub-load-threshold=2000"
    ceph_args+=" --osd-data=$osd_data"
    ceph_args+=" --osd-journal=${osd_data}/journal"
    ceph_args+=" --chdir="
    ceph_args+=$EXTRA_OPTS
    ceph_args+=" --run-dir=$dir"
    ceph_args+=" --admin-socket=$(get_asok_path)"
    ceph_args+=" --debug-osd=20"
    ceph_args+=" --log-file=$dir/\$name.log"
    ceph_args+=" --pid-file=$dir/\$name.pid"
    ceph_args+=" --osd-max-object-name-len=460"
    ceph_args+=" --osd-max-object-namespace-len=64"
    ceph_args+=" --enable-experimental-unrecoverable-data-corrupting-features=*"
    ceph_args+=" --osd-mclock-profile=high_recovery_ops"
    ceph_args+=" "
    ceph_args+="$@"
    mkdir -p $osd_data

    echo start osd.$id
    ceph-osd -i $id $ceph_args &

    [ "$id" = "$(cat $osd_data/whoami)" ] || return 1

    # If noup is set, then can't wait for this osd
    if ceph osd dump --format=json | jq '.flags_set[]' | grep -q '"noup"' ; then
      return 0
    fi
    wait_for_osd up $id || return 1
}

function test_activate_osd() {
    local dir=$1

    setup $dir || return 1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1

    run_osd $dir 0 || return 1
    local backfills=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path osd.0) \
        config get osd_max_backfills)
    echo "$backfills" | grep --quiet 'osd_max_backfills' || return 1

    kill_daemons $dir TERM osd || return 1

    activate_osd $dir 0 --osd-max-backfills 20 || return 1
    local backfills=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path osd.0) \
        config get osd_max_backfills)
    test "$backfills" = '{"osd_max_backfills":"20"}' || return 1

    teardown $dir || return 1
}

function test_activate_osd_after_mark_down() {
    local dir=$1

    setup $dir || return 1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1

    run_osd $dir 0 || return 1
    local backfills=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path osd.0) \
        config get osd_max_backfills)
    echo "$backfills" | grep --quiet 'osd_max_backfills' || return 1

    kill_daemons $dir TERM osd || return 1
    ceph osd down 0 || return 1
    wait_for_osd down 0 || return 1

    activate_osd $dir 0 --osd-max-backfills 20 || return 1
    local backfills=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path osd.0) \
        config get osd_max_backfills)
    test "$backfills" = '{"osd_max_backfills":"20"}' || return 1

    teardown $dir || return 1
}

function test_activate_osd_skip_benchmark() {
    local dir=$1

    setup $dir || return 1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1

    # Skip the osd benchmark during first osd bring-up.
    run_osd $dir 0 --osd-op-queue=mclock_scheduler \
        --osd-mclock-skip-benchmark=true || return 1
    local max_iops_hdd_def=$(CEPH_ARGS='' ceph --format=json daemon \
        $(get_asok_path osd.0) config get osd_mclock_max_capacity_iops_hdd)
    local max_iops_ssd_def=$(CEPH_ARGS='' ceph --format=json daemon \
        $(get_asok_path osd.0) config get osd_mclock_max_capacity_iops_ssd)

    kill_daemons $dir TERM osd || return 1
    ceph osd down 0 || return 1
    wait_for_osd down 0 || return 1

    # Skip the osd benchmark during activation as well. Validate that
    # the max osd capacities are left unchanged.
    activate_osd $dir 0 --osd-op-queue=mclock_scheduler \
        --osd-mclock-skip-benchmark=true || return 1
    local max_iops_hdd_after_boot=$(CEPH_ARGS='' ceph --format=json daemon \
        $(get_asok_path osd.0) config get osd_mclock_max_capacity_iops_hdd)
    local max_iops_ssd_after_boot=$(CEPH_ARGS='' ceph --format=json daemon \
        $(get_asok_path osd.0) config get osd_mclock_max_capacity_iops_ssd)

    test "$max_iops_hdd_def" = "$max_iops_hdd_after_boot" || return 1
    test "$max_iops_ssd_def" = "$max_iops_ssd_after_boot" || return 1

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
    run_mon $dir a --osd_pool_default_size=1  --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    wait_for_osd up 0 || return 1
    wait_for_osd up 1 || return 1
    kill_daemons $dir TERM osd.0 || return 1
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

    local osds=$(ceph --format json osd map $poolname $objectname 2>/dev/null | \
        jq '.acting | .[]')
    # get rid of the trailing space
    echo $osds
}

function test_get_osds() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=2 || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    create_rbd_pool || return 1
    wait_for_clean || return 1
    create_rbd_pool || return 1
    get_osds rbd GROUP | grep --quiet '^[0-1] [0-1]$' || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Wait for the monitor to form quorum (optionally, of size N)
#
# @param timeout duration (lower-bound) to wait for quorum to be formed
# @param quorumsize size of quorum to wait for
# @return 0 on success, 1 on error
#
function wait_for_quorum() {
    local timeout=$1
    local quorumsize=$2

    if [[ -z "$timeout" ]]; then
      timeout=300
    fi

    if [[ -z "$quorumsize" ]]; then
      timeout $timeout ceph quorum_status --format=json >&/dev/null || return 1
      return 0
    fi

    no_quorum=1
    wait_until=$((`date +%s` + $timeout))
    while [[ $(date +%s) -lt $wait_until ]]; do
        jqfilter='.quorum | length == '$quorumsize
        jqinput="$(timeout $timeout ceph quorum_status --format=json 2>/dev/null)"
        res=$(echo $jqinput | jq "$jqfilter")
        if [[ "$res" == "true" ]]; then
          no_quorum=0
          break
        fi
    done
    return $no_quorum
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

    ceph --format json osd map $poolname $objectname 2>/dev/null | jq -r '.pgid'
}

function test_get_pg() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1  --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    create_rbd_pool || return 1
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
        ceph --format json daemon $(get_asok_path $daemon.$id) \
        config get $config 2> /dev/null | \
        jq -r ".$config"
}

function test_get_config() {
    local dir=$1

    # override the default config using command line arg and check it
    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    test $(get_config mon a osd_pool_default_size) = 1 || return 1
    run_mgr $dir x || return 1
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

    test $(env CEPH_ARGS='' ceph --format json daemon $(get_asok_path $daemon.$id) \
               config set $config $value 2> /dev/null | \
           jq 'has("success")') == true
}

function test_set_config() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
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

    ceph --format json osd map $poolname $objectname 2>/dev/null | \
        jq '.acting_primary'
}

function test_get_primary() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    local osd=0
    run_mgr $dir x || return 1
    run_osd $dir $osd || return 1
    create_rbd_pool || return 1
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
    ceph --format json osd map $poolname $objectname 2>/dev/null | \
        jq ".acting | map(select (. != $primary)) | .[0]"
}

function test_get_not_primary() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=2 || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    create_rbd_pool || return 1
    wait_for_clean || return 1
    local primary=$(get_primary rbd GROUP)
    local not_primary=$(get_not_primary rbd GROUP)
    test $not_primary != $primary || return 1
    test $not_primary = 0 -o $not_primary = 1 || return 1
    teardown $dir || return 1
}

#######################################################################

function _objectstore_tool_nodown() {
    local dir=$1
    shift
    local id=$1
    shift
    local osd_data=$dir/$id

    ceph-objectstore-tool \
        --data-path $osd_data \
        "$@" || return 1
}

function _objectstore_tool_nowait() {
    local dir=$1
    shift
    local id=$1
    shift

    kill_daemons $dir TERM osd.$id >&2 < /dev/null || return 1

    _objectstore_tool_nodown $dir $id "$@" || return 1
    activate_osd $dir $id $ceph_osd_args >&2 || return 1
}

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
# The value of $ceph_osd_args will be passed to restarted osds
#
function objectstore_tool() {
    local dir=$1
    shift
    local id=$1
    shift

    _objectstore_tool_nowait $dir $id "$@" || return 1
    wait_for_clean >&2
}

function test_objectstore_tool() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    local osd=0
    run_mgr $dir x || return 1
    run_osd $dir $osd || return 1
    create_rbd_pool || return 1
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
    local recovery_progress
    recovery_progress+=".recovering_keys_per_sec + "
    recovery_progress+=".recovering_bytes_per_sec + "
    recovery_progress+=".recovering_objects_per_sec"
    local progress=$(ceph --format json status 2>/dev/null | \
                     jq -r ".pgmap | $recovery_progress")
    test "$progress" != null
}

function test_get_is_making_recovery_progress() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
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
    local expression
    expression+="select(contains(\"active\") and contains(\"clean\")) | "
    expression+="select(contains(\"stale\") | not)"
    ceph --format json pg dump pgs 2>/dev/null | \
        jq ".pg_stats | [.[] | .state | $expression] | length"
}

function test_get_num_active_clean() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    create_rbd_pool || return 1
    wait_for_clean || return 1
    local num_active_clean=$(get_num_active_clean)
    test "$num_active_clean" = $PG_NUM || return 1
    teardown $dir || return 1
}

##
# Return the number of active or peered PGs in the cluster. A PG matches if
# ceph pg dump pgs reports it is either **active** or **peered** and that
# not **stale**.
#
# @param STDOUT the number of active PGs
# @return 0 on success, 1 on error
#
function get_num_active_or_peered() {
    local expression
    expression+="select(contains(\"active\") or contains(\"peered\")) | "
    expression+="select(contains(\"stale\") | not)"
    ceph --format json pg dump pgs 2>/dev/null | \
        jq ".pg_stats | [.[] | .state | $expression] | length"
}

function test_get_num_active_or_peered() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    create_rbd_pool || return 1
    wait_for_clean || return 1
    local num_peered=$(get_num_active_or_peered)
    test "$num_peered" = $PG_NUM || return 1
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
    ceph --format json status 2>/dev/null | jq '.pgmap.num_pgs'
}

function test_get_num_pgs() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    create_rbd_pool || return 1
    wait_for_clean || return 1
    local num_pgs=$(get_num_pgs)
    test "$num_pgs" -gt 0 || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Return the OSD ids in use by at least one PG in the cluster (either
# in the up or the acting set), according to ceph pg dump pgs. Every
# OSD id shows as many times as they are used in up and acting sets.
# If an OSD id is in both the up and acting set of a given PG, it will
# show twice.
#
# @param STDOUT a sorted list of OSD ids
# @return 0 on success, 1 on error
#
function get_osd_id_used_by_pgs() {
    ceph --format json pg dump pgs 2>/dev/null | jq '.pg_stats | .[] | .up[], .acting[]' | sort
}

function test_get_osd_id_used_by_pgs() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    create_rbd_pool || return 1
    wait_for_clean || return 1
    local osd_ids=$(get_osd_id_used_by_pgs | uniq)
    test "$osd_ids" = "0" || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Wait until the OSD **id** shows **count** times in the
# PGs (see get_osd_id_used_by_pgs for more information about
# how OSD ids are counted).
#
# @param id the OSD id
# @param count the number of time it must show in the PGs
# @return 0 on success, 1 on error
#
function wait_osd_id_used_by_pgs() {
    local id=$1
    local count=$2

    status=1
    for ((i=0; i < $TIMEOUT / 5; i++)); do
        echo $i
        if ! test $(get_osd_id_used_by_pgs | grep -c $id) = $count ; then
            sleep 5
        else
            status=0
            break
        fi
    done
    return $status
}

function test_wait_osd_id_used_by_pgs() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    create_rbd_pool || return 1
    wait_for_clean || return 1
    wait_osd_id_used_by_pgs 0 8 || return 1
    ! TIMEOUT=1 wait_osd_id_used_by_pgs 123 5 || return 1
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
    local sname=${2:-last_scrub_stamp}
    ceph --format json pg dump pgs 2>/dev/null | \
        jq -r ".pg_stats | .[] | select(.pgid==\"$pgid\") | .$sname"
}

function test_get_last_scrub_stamp() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    create_rbd_pool || return 1
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
    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    create_rbd_pool || return 1
    wait_for_clean || return 1
    is_clean || return 1
    teardown $dir || return 1
}

#######################################################################

calc() { $AWK "BEGIN{print $*}"; }

##
# Return a list of numbers that are increasingly larger and whose
# total is **timeout** seconds. It can be used to have short sleep
# delay while waiting for an event on a fast machine. But if running
# very slowly the larger delays avoid stressing the machine even
# further or spamming the logs.
#
# @param timeout sum of all delays, in seconds
# @return a list of sleep delays
#
function get_timeout_delays() {
    local trace=$(shopt -q -o xtrace && echo true || echo false)
    $trace && shopt -u -o xtrace
    local timeout=$1
    local first_step=${2:-1}
    local max_timeout=${3:-$MAX_TIMEOUT}

    local i
    local total="0"
    i=$first_step
    while test "$(calc $total + $i \<= $timeout)" = "1"; do
        echo -n "$(calc $i) "
        total=$(calc $total + $i)
        i=$(calc $i \* 2)
        if [ $max_timeout -gt 0 ]; then
            # Did we reach max timeout ?
            if [ ${i%.*} -eq ${max_timeout%.*} ] && [ ${i#*.} \> ${max_timeout#*.} ] || [ ${i%.*} -gt ${max_timeout%.*} ]; then
                # Yes, so let's cap the max wait time to max
                i=$max_timeout
            fi
        fi
    done
    if test "$(calc $total \< $timeout)" = "1"; then
        echo -n "$(calc $timeout - $total) "
    fi
    $trace && shopt -s -o xtrace
}

function test_get_timeout_delays() {
    test "$(get_timeout_delays 1)" = "1 " || return 1
    test "$(get_timeout_delays 5)" = "1 2 2 " || return 1
    test "$(get_timeout_delays 6)" = "1 2 3 " || return 1
    test "$(get_timeout_delays 7)" = "1 2 4 " || return 1
    test "$(get_timeout_delays 8)" = "1 2 4 1 " || return 1
    test "$(get_timeout_delays 1 .1)" = "0.1 0.2 0.4 0.3 " || return 1
    test "$(get_timeout_delays 1.5 .1)" = "0.1 0.2 0.4 0.8 " || return 1
    test "$(get_timeout_delays 5 .1)" = "0.1 0.2 0.4 0.8 1.6 1.9 " || return 1
    test "$(get_timeout_delays 6 .1)" = "0.1 0.2 0.4 0.8 1.6 2.9 " || return 1
    test "$(get_timeout_delays 6.3 .1)" = "0.1 0.2 0.4 0.8 1.6 3.2 " || return 1
    test "$(get_timeout_delays 20 .1)" = "0.1 0.2 0.4 0.8 1.6 3.2 6.4 7.3 " || return 1
    test "$(get_timeout_delays 300 .1 0)" = "0.1 0.2 0.4 0.8 1.6 3.2 6.4 12.8 25.6 51.2 102.4 95.3 " || return 1
    test "$(get_timeout_delays 300 .1 10)" = "0.1 0.2 0.4 0.8 1.6 3.2 6.4 10 10 10 10 10 10 10 10 10 10 10 10 10 10 10 10 10 10 10 10 10 10 10 10 10 10 10 10 7.3 " || return 1
}

#######################################################################

##
# Wait until the cluster becomes clean or if it does not make progress
# for $WAIT_FOR_CLEAN_TIMEOUT seconds.
# Progress is measured either via the **get_is_making_recovery_progress**
# predicate or if the number of clean PGs changes (as returned by get_num_active_clean)
#
# @return 0 if the cluster is clean, 1 otherwise
#
function wait_for_clean() {
    local cmd=$1
    local num_active_clean=-1
    local cur_active_clean
    local -a delays=($(get_timeout_delays $WAIT_FOR_CLEAN_TIMEOUT .1))
    local -i loop=0

    flush_pg_stats || return 1
    while test $(get_num_pgs) == 0 ; do
	sleep 1
    done

    while true ; do
        # Comparing get_num_active_clean & get_num_pgs is used to determine
        # if the cluster is clean. That's almost an inline of is_clean() to
        # get more performance by avoiding multiple calls of get_num_active_clean.
        cur_active_clean=$(get_num_active_clean)
        test $cur_active_clean = $(get_num_pgs) && break
        if test $cur_active_clean != $num_active_clean ; then
            loop=0
            num_active_clean=$cur_active_clean
        elif get_is_making_recovery_progress ; then
            loop=0
        elif (( $loop >= ${#delays[*]} )) ; then
            ceph report
            return 1
        fi
	# eval is a no-op if cmd is empty
        eval $cmd
        sleep ${delays[$loop]}
        loop+=1
    done
    return 0
}

function test_wait_for_clean() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=2 || return 1
    run_osd $dir 0 || return 1
    run_mgr $dir x || return 1
    create_rbd_pool || return 1
    ! WAIT_FOR_CLEAN_TIMEOUT=1 wait_for_clean || return 1
    run_osd $dir 1 || return 1
    wait_for_clean || return 1
    teardown $dir || return 1
}

##
# Wait until the cluster becomes peered or if it does not make progress
# for $WAIT_FOR_CLEAN_TIMEOUT seconds.
# Progress is measured either via the **get_is_making_recovery_progress**
# predicate or if the number of peered PGs changes (as returned by get_num_active_or_peered)
#
# @return 0 if the cluster is clean, 1 otherwise
#
function wait_for_peered() {
    local cmd=$1
    local num_peered=-1
    local cur_peered
    local -a delays=($(get_timeout_delays $WAIT_FOR_CLEAN_TIMEOUT .1))
    local -i loop=0

    flush_pg_stats || return 1
    while test $(get_num_pgs) == 0 ; do
	sleep 1
    done

    while true ; do
        # Comparing get_num_active_clean & get_num_pgs is used to determine
        # if the cluster is clean. That's almost an inline of is_clean() to
        # get more performance by avoiding multiple calls of get_num_active_clean.
        cur_peered=$(get_num_active_or_peered)
        test $cur_peered = $(get_num_pgs) && break
        if test $cur_peered != $num_peered ; then
            loop=0
            num_peered=$cur_peered
        elif get_is_making_recovery_progress ; then
            loop=0
        elif (( $loop >= ${#delays[*]} )) ; then
            ceph report
            return 1
        fi
	# eval is a no-op if cmd is empty
        eval $cmd
        sleep ${delays[$loop]}
        loop+=1
    done
    return 0
}

function test_wait_for_peered() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=2 || return 1
    run_osd $dir 0 || return 1
    run_mgr $dir x || return 1
    create_rbd_pool || return 1
    ! WAIT_FOR_CLEAN_TIMEOUT=1 wait_for_clean || return 1
    run_osd $dir 1 || return 1
    wait_for_peered || return 1
    teardown $dir || return 1
}


#######################################################################

##
# Wait until the cluster's health condition disappeared.
# $TIMEOUT default
#
# @param string to grep for in health detail
# @return 0 if the cluster health doesn't matches request,
# 1 otherwise if after $TIMEOUT seconds health condition remains.
#
function wait_for_health_gone() {
    local grepstr=$1
    local -a delays=($(get_timeout_delays $TIMEOUT .1))
    local -i loop=0

    while ceph health detail | grep "$grepstr" ; do
	if (( $loop >= ${#delays[*]} )) ; then
            ceph health detail
            return 1
        fi
        sleep ${delays[$loop]}
        loop+=1
    done
}

##
# Wait until the cluster has health condition passed as arg
# again for $TIMEOUT seconds.
#
# @param string to grep for in health detail
# @return 0 if the cluster health matches request, 1 otherwise
#
function wait_for_health() {
    local grepstr=$1
    local -a delays=($(get_timeout_delays $TIMEOUT .1))
    local -i loop=0

    while ! ceph health detail | grep "$grepstr" ; do
	if (( $loop >= ${#delays[*]} )) ; then
            ceph health detail
            return 1
        fi
        sleep ${delays[$loop]}
        loop+=1
    done
}

##
# Wait until the cluster becomes HEALTH_OK again or if it does not make progress
# for $TIMEOUT seconds.
#
# @return 0 if the cluster is HEALTHY, 1 otherwise
#
function wait_for_health_ok() {
     wait_for_health "HEALTH_OK" || return 1
}

function test_wait_for_health_ok() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_failsafe_full_ratio=.99 --mon_pg_warn_min_per_osd=0 || return 1
    run_mgr $dir x --mon_pg_warn_min_per_osd=0 || return 1
    # start osd_pool_default_size OSDs
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    kill_daemons $dir TERM osd || return 1
    ceph osd down 0 || return 1
    # expect TOO_FEW_OSDS warning
    ! TIMEOUT=1 wait_for_health_ok || return 1
    # resurrect all OSDs
    activate_osd $dir 0 || return 1
    activate_osd $dir 1 || return 1
    activate_osd $dir 2 || return 1
    wait_for_health_ok || return 1
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
    run_mon $dir a --osd_pool_default_size=1  --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    create_rbd_pool || return 1
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
# The scrub is initiated using the "operator initiated" method, and
# the scrub triggered is not subject to no-scrub flags etc.
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

function pg_deep_scrub() {
    local pgid=$1
    local last_scrub=$(get_last_scrub_stamp $pgid last_deep_scrub_stamp)
    ceph pg deep-scrub $pgid
    wait_for_scrub $pgid "$last_scrub" last_deep_scrub_stamp
}

function test_pg_scrub() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    create_rbd_pool || return 1
    wait_for_clean || return 1
    pg_scrub 1.0 || return 1
    kill_daemons $dir KILL osd || return 1
    ! TIMEOUT=1 pg_scrub 1.0 || return 1
    teardown $dir || return 1
}

#######################################################################

##
# Trigger a "scheduled" scrub on **pgid** (by mnaually modifying the relevant
# last-scrub stamp) and wait until it completes. The pg_scrub
# function will fail if scrubbing does not complete within $TIMEOUT
# seconds. The pg_scrub is complete whenever the
# **get_last_scrub_stamp** function reports a timestamp different from
# the one stored before starting the scrub.
#
# @param pgid the id of the PG
# @return 0 on success, 1 on error
#
function pg_schedule_scrub() {
    local pgid=$1
    local last_scrub=$(get_last_scrub_stamp $pgid)
    ceph pg scrub $pgid
    wait_for_scrub $pgid "$last_scrub"
}

function pg_schedule_deep_scrub() {
    local pgid=$1
    local last_scrub=$(get_last_scrub_stamp $pgid last_deep_scrub_stamp)
    ceph pg deep-scrub $pgid
    wait_for_scrub $pgid "$last_scrub" last_deep_scrub_stamp
}

function test_pg_schedule_scrub() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    create_rbd_pool || return 1
    wait_for_clean || return 1
    pg_schedule_scrub 1.0 || return 1
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
    local sname=${3:-last_scrub_stamp}

    for ((i=0; i < $TIMEOUT; i++)); do
        if test "$(get_last_scrub_stamp $pgid $sname)" '>' "$last_scrub" ; then
            return 0
        fi
        sleep 1
    done
    return 1
}

function test_wait_for_scrub() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    create_rbd_pool || return 1
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
    local grepstr
    local s
    case `uname` in
        FreeBSD) grepstr="Cannot open.*$plugin" ;;
        *) grepstr="$plugin.*No such file" ;;
    esac

    s=$(ceph osd erasure-code-profile set TESTPROFILE plugin=$plugin 2>&1)
    local status=$?
    if [ $status -eq 0 ]; then
        ceph osd erasure-code-profile rm TESTPROFILE
    elif ! echo $s | grep --quiet "$grepstr" ; then
        status=1
        # display why the string was rejected.
        echo $s
    fi
    return $status
}

function test_erasure_code_plugin_exists() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
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
    shift
    # Execute the command and prepend the output with its pid
    # We enforce to return the exit status of the command and not the sed one.
    ("$@" |& sed 's/^/'$BASHPID': /'; return "${PIPESTATUS[0]}") >&2 &
    eval "$pid_variable+=\" $!\""
}

function save_stdout {
    local out="$1"
    shift
    "$@" > "$out"
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

function flush_pg_stats()
{
    local timeout=${1:-$TIMEOUT}

    ids=`ceph osd ls`
    seqs=''
    for osd in $ids; do
	    seq=`ceph tell osd.$osd flush_pg_stats`
	    if test -z "$seq"
	    then
		continue
	    fi
	    seqs="$seqs $osd-$seq"
    done

    for s in $seqs; do
	    osd=`echo $s | cut -d - -f 1`
	    seq=`echo $s | cut -d - -f 2`
	    echo "waiting osd.$osd seq $seq"
	    while test $(ceph osd last-stat-seq $osd) -lt $seq; do
            sleep 1
            if [ $((timeout--)) -eq 0 ]; then
                return 1
            fi
        done
    done
}

function test_flush_pg_stats()
{
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    create_rbd_pool || return 1
    rados -p rbd put obj /etc/group
    flush_pg_stats || return 1
    local jq_filter='.pools | .[] | select(.name == "rbd") | .stats'
    stored=`ceph df detail --format=json | jq "$jq_filter.stored"`
    stored_raw=`ceph df detail --format=json | jq "$jq_filter.stored_raw"`
    test $stored -gt 0 || return 1
    test $stored == $stored_raw || return 1
    teardown $dir
}

########################################################################
##
# Get the current op scheduler enabled on an osd by reading the
# osd_op_queue config option
#
# Example:
#   get_op_scheduler $osdid
#
# @param id the id of the OSD
# @return the name of the op scheduler enabled for the OSD
#
function get_op_scheduler() {
   local id=$1

   get_config osd $id osd_op_queue
}

function test_get_op_scheduler() {
    local dir=$1

    setup $dir || return 1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1

    run_osd $dir 0 --osd_op_queue=wpq || return 1
    test $(get_op_scheduler 0) = "wpq" || return 1

    run_osd $dir 1 --osd_op_queue=mclock_scheduler || return 1
    test $(get_op_scheduler 1) = "mclock_scheduler" || return 1
    teardown $dir || return 1
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
    local dir=td/$1
    shift

    shopt -s -o xtrace
    PS4='${BASH_SOURCE[0]}:$LINENO: ${FUNCNAME[0]}:  '

    export PATH=.:$PATH # make sure program from sources are preferred
    export PYTHONWARNINGS=ignore
    export CEPH_CONF=/dev/null
    unset CEPH_ARGS

    local code
    if run $dir "$@" ; then
        code=0
    else
        code=1
    fi
    teardown $dir $code || return 1
    return $code
}

#######################################################################

function run_tests() {
    shopt -s -o xtrace
    PS4='${BASH_SOURCE[0]}:$LINENO: ${FUNCNAME[0]}:  '

    export .:$PATH # make sure program from sources are preferred

    export CEPH_MON="127.0.0.1:7109" # git grep '\<7109\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+=" --fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    export CEPH_CONF=/dev/null

    local funcs=${@:-$(set | sed -n -e 's/^\(test_[0-9a-z_]*\) .*/\1/p')}
    local dir=td/ceph-helpers

    for func in $funcs ; do
        if ! $func $dir; then
            teardown $dir 1
            return 1
        fi
    done
}

if test "$1" = TESTS ; then
    shift
    run_tests "$@"
    exit $?
fi

# NOTE:
# jq only support --exit-status|-e from version 1.4 forwards, which makes
# returning on error waaaay prettier and straightforward.
# However, the current automated upstream build is running with v1.3,
# which has no idea what -e is. Hence the convoluted error checking we
# need. Sad.
# The next time someone changes this code, please check if v1.4 is now
# a thing, and, if so, please change these to use -e. Thanks.

# jq '.all.supported | select([.[] == "foo"] | any)'
function jq_success() {
  input="$1"
  filter="$2"
  expects="\"$3\""

  in_escaped=$(printf %s "$input" | sed "s/'/'\\\\''/g")
  filter_escaped=$(printf %s "$filter" | sed "s/'/'\\\\''/g")

  ret=$(echo "$in_escaped" | jq "$filter_escaped")
  if [[ "$ret" == "true" ]]; then
    return 0
  elif [[ -n "$expects" ]]; then
    if [[ "$ret" == "$expects" ]]; then
      return 0
    fi
  fi
  return 1
  input=$1
  filter=$2
  expects="$3"

  ret="$(echo $input | jq \"$filter\")"
  if [[ "$ret" == "true" ]]; then
    return 0
  elif [[ -n "$expects" && "$ret" == "$expects" ]]; then
    return 0
  fi
  return 1
}

function inject_eio() {
    local pooltype=$1
    shift
    local which=$1
    shift
    local poolname=$1
    shift
    local objname=$1
    shift
    local dir=$1
    shift
    local shard_id=$1
    shift

    local -a initial_osds=($(get_osds $poolname $objname))
    local osd_id=${initial_osds[$shard_id]}
    if [ "$pooltype" != "ec" ]; then
        shard_id=""
    fi
    type=$(cat $dir/$osd_id/type)
    set_config osd $osd_id ${type}_debug_inject_read_err true || return 1
    local loop=0
    while ( CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$osd_id) \
             inject${which}err $poolname $objname $shard_id | grep -q Invalid ); do
        loop=$(expr $loop + 1)
        if [ $loop = "10" ]; then
            return 1
        fi
        sleep 1
    done
}

function multidiff() {
    if ! diff $@ ; then
        if [ "$DIFFCOLOPTS" = "" ]; then
            return 1
        fi
        diff $DIFFCOLOPTS $@
    fi
}

function create_ec_pool() {
    local pool_name=$1
    shift
    local allow_overwrites=$1
    shift

    ceph osd erasure-code-profile set myprofile crush-failure-domain=osd "$@" || return 1

    create_pool "$poolname" 1 1 erasure myprofile || return 1

    if [ "$allow_overwrites" = "true" ]; then
        ceph osd pool set "$poolname" allow_ec_overwrites true || return 1
    fi

    wait_for_clean || return 1
    return 0
}

# Local Variables:
# compile-command: "cd ../../src ; make -j4 && ../qa/standalone/ceph-helpers.sh TESTS # test_get_config"
# End:
