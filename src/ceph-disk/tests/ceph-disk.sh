#!/bin/bash
#
# Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
# Copyright (C) 2014, 2015, 2016 Red Hat <contact@redhat.com>
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

#
# Removes btrfs subvolumes under the given directory param
#
function teardown_btrfs() {
    local btrfs_base_dir=$1

    btrfs_dirs=`ls -l $btrfs_base_dir | egrep '^d' | awk '{print $9}'`
    for btrfs_dir in $btrfs_dirs
    do
        btrfs_subdirs=`ls -l $btrfs_base_dir/$btrfs_dir | egrep '^d' | awk '{print $9}'` 
        for btrfs_subdir in $btrfs_subdirs
        do
	    btrfs subvolume delete $btrfs_base_dir/$btrfs_dir/$btrfs_subdir
        done
    done
}

PS4='${BASH_SOURCE[0]}:$LINENO: ${FUNCNAME[0]}:  '

export PATH=..:.:$PATH # make sure program from sources are preferred
export PATH=../ceph-detect-init/virtualenv/bin:$PATH
export PATH=virtualenv/bin:$PATH
DIR=test-ceph-disk
: ${CEPH_DISK:=ceph-disk}
OSD_DATA=$DIR/osd
MON_ID=a
MONA=127.0.0.1:7451
TEST_POOL=rbd
FSID=$(uuidgen)
export CEPH_CONF=$DIR/ceph.conf
export CEPH_ARGS="--fsid $FSID"
CEPH_ARGS+=" --chdir="
CEPH_ARGS+=" --journal-dio=false"
CEPH_ARGS+=" --run-dir=$DIR"
CEPH_ARGS+=" --osd-failsafe-full-ratio=.99"
CEPH_ARGS+=" --mon-host=$MONA"
CEPH_ARGS+=" --log-file=$DIR/\$name.log"
CEPH_ARGS+=" --pid-file=$DIR/\$name.pidfile"
if [ -d ../.libs ]; then
    CEPH_ARGS+=" --erasure-code-dir=../.libs"
    CEPH_ARGS+=" --compression-dir=../.libs"
fi
CEPH_ARGS+=" --auth-supported=none"
CEPH_ARGS+=" --osd-journal-size=100"
CEPH_ARGS+=" --debug-mon=20"
CEPH_ARGS+=" --debug-osd=20"
CEPH_ARGS+=" --debug-bdev=20"
CEPH_ARGS+=" --debug-bluestore=20"
CEPH_DISK_ARGS=
CEPH_DISK_ARGS+=" --statedir=$DIR"
CEPH_DISK_ARGS+=" --sysconfdir=$DIR"
CEPH_DISK_ARGS+=" --prepend-to-path="
CEPH_DISK_ARGS+=" --verbose"
TIMEOUT=600

cat=$(which cat)
timeout=$(which timeout)
diff=$(which diff)
mkdir=$(which mkdir)
rm=$(which rm)
uuidgen=$(which uuidgen)

function setup() {
    teardown
    mkdir $DIR
    mkdir $OSD_DATA
    touch $DIR/ceph.conf # so ceph-disk think ceph is the cluster
}

function teardown() {
    kill_daemons
    if [ x`uname`x != xFreeBSDx ] \
        && [ $(stat -f -c '%T' .) == "btrfs" ]
    then
        rm -fr $DIR/*/*db
        teardown_btrfs $DIR
    fi
   
    MOUNTS="/proc/mounts"
    if [ x`uname`x = xFreeBSDx ]; then
        MOUNTS="/compat/linux/"$MOUNTS
    fi
    grep " $(pwd)/$DIR/" < $MOUNTS | while read mounted rest ; do
        umount $mounted
    done
    rm -fr $DIR
}

function run_mon() {
    local mon_dir=$DIR/$MON_ID

    ceph-mon \
        --id $MON_ID \
        --mkfs \
        --mon-data=$mon_dir \
        --mon-initial-members=$MON_ID \
        "$@"

    ceph-mon \
        --id $MON_ID \
        --mon-data=$mon_dir \
        --mon-osd-full-ratio=.99 \
        --mon-data-avail-crit=1 \
        --mon-cluster-log-file=$mon_dir/log \
        --public-addr $MONA \
        "$@"
}

function kill_daemons() {
    if ! [ -e $DIR ]; then
        return
    fi
    for pidfile in $(find $DIR | grep pidfile) ; do
        pid=$(cat $pidfile)
        for try in 0 1 1 1 2 3 ; do
            kill $pid 2>/dev/null || break
            sleep $try
        done
    done
}

function command_fixture() {
    local command=$1
    local fpath=`readlink -f $(which $command)`
    [ "$fpath" = `readlink -f ../$command` ] || [ "$fpath" = `readlink -f $(pwd)/$command` ] || return 1

    cat > $DIR/$command <<EOF
#!/bin/bash
touch $DIR/used-$command
exec ../$command "\$@"
EOF
    chmod +x $DIR/$command
}

function tweak_path() {
    local tweaker=$1

    setup

    command_fixture ceph-conf || return 1
    command_fixture ceph-osd || return 1

    test_activate_dir || return 1

    [ ! -f $DIR/used-ceph-conf ] || return 1
    [ ! -f $DIR/used-ceph-osd ] || return 1

    teardown

    setup

    command_fixture ceph-conf || return 1
    command_fixture ceph-osd || return 1

    $tweaker test_activate_dir || return 1

    [ -f $DIR/used-ceph-osd ] || return 1

    teardown
}

function use_prepend_to_path() {
    local ceph_disk_args
    ceph_disk_args+=" --statedir=$DIR"
    ceph_disk_args+=" --sysconfdir=$DIR"
    ceph_disk_args+=" --prepend-to-path=$DIR"
    ceph_disk_args+=" --verbose"
    CEPH_DISK_ARGS="$ceph_disk_args" \
        "$@" || return 1
}

function test_prepend_to_path() {
    tweak_path use_prepend_to_path || return 1
}

function use_path() {
    PATH="$DIR:$PATH" \
        "$@" || return 1
}

function test_path() {
    tweak_path use_path || return 1
}

function test_no_path() {
    ( export PATH=../ceph-detect-init/virtualenv/bin:virtualenv/bin:..:/usr/bin:/bin:/usr/local/bin ; test_activate_dir ) || return 1
}

function run_timeout() {
    local status
	local cmd
	cmd="$@"
	
    $timeout $TIMEOUT $cmd
    status=$?
    case $status in
      0)
        return 0
        ;;
      124)
        echo Command "$cmd" has timed out.
        ;;
      126)
        echo Command "$cmd" was invalid
        ;;
      127)
        echo Command "$cmd" does not exist
        ;;
      *)
        echo Timeout for "$cmd" returned status $?
        ;;
    esac
    return 1

}

function test_mark_init() {
    run_mon

    local osd_data=$(pwd)/$DIR/dir
    $mkdir -p $osd_data

    local osd_uuid=$($uuidgen)

    $mkdir -p $OSD_DATA

    ${CEPH_DISK} $CEPH_DISK_ARGS \
        prepare --osd-uuid $osd_uuid $osd_data || return 1

    run_timeout ${CEPH_DISK} $CEPH_DISK_ARGS \
        --verbose \
        activate \
        --mark-init=auto \
        --no-start-daemon \
        $osd_data || return 1

    [ -f $osd_data/$(ceph-detect-init) ] || return 1

    if [ systemd = $(ceph-detect-init) ] ; then
        expected=sysvinit
    else
        expected=systemd
    fi
    run_timeout ${CEPH_DISK} $CEPH_DISK_ARGS \
        --verbose \
        activate \
        --mark-init=$expected \
        --no-start-daemon \
        $osd_data || return 1

    [ ! -f $osd_data/$(ceph-detect-init) ] || return 1
    [ -f $osd_data/$expected ] || return 1
}

function test_zap() {
    local osd_data=$DIR/dir
    $mkdir -p $osd_data

    ${CEPH_DISK} $CEPH_DISK_ARGS zap $osd_data 2>&1 | grep -q 'not full block device' || return 1

    $rm -fr $osd_data
}

# ceph-disk prepare returns immediately on success if the magic file
# exists in the --osd-data directory.
function test_activate_dir_magic() {
    local uuid=$($uuidgen)
    local osd_data=$DIR/osd

    echo a failure to create the fsid file implies the magic file is not created

    mkdir -p $osd_data/fsid
    CEPH_ARGS="--fsid $uuid" \
     ${CEPH_DISK} $CEPH_DISK_ARGS prepare $osd_data > $DIR/out 2>&1
    grep --quiet 'Is a directory' $DIR/out || return 1
    ! [ -f $osd_data/magic ] || return 1
    rmdir $osd_data/fsid

    echo successfully prepare the OSD

    CEPH_ARGS="--fsid $uuid" \
     ${CEPH_DISK} $CEPH_DISK_ARGS prepare $osd_data 2>&1 | tee $DIR/out
    grep --quiet 'Preparing osd data dir' $DIR/out || return 1
    grep --quiet $uuid $osd_data/ceph_fsid || return 1
    [ -f $osd_data/magic ] || return 1

    echo will not override an existing OSD

    CEPH_ARGS="--fsid $($uuidgen)" \
     ${CEPH_DISK} $CEPH_DISK_ARGS prepare $osd_data 2>&1 | tee $DIR/out
    grep --quiet 'Data dir .* already exists' $DIR/out || return 1
    grep --quiet $uuid $osd_data/ceph_fsid || return 1
}

function test_pool_read_write() {
    local osd_uuid=$1

    run_timeout ceph osd pool set $TEST_POOL size 1 || return 1

    local id=$(ceph osd create $osd_uuid)
    local weight=1
    ceph osd crush add osd.$id $weight root=default host=localhost || return 1
    echo FOO > $DIR/BAR
    run_timeout rados --pool $TEST_POOL put BAR $DIR/BAR || return 1
    run_timeout rados --pool $TEST_POOL get BAR $DIR/BAR.copy || return 1
    $diff $DIR/BAR $DIR/BAR.copy || return 1
}

function test_activate() {
    local to_prepare=$1
    local to_activate=$2
    local osd_uuid=$($uuidgen)

    $mkdir -p $OSD_DATA

    ${CEPH_DISK} $CEPH_DISK_ARGS \
        prepare --osd-uuid $osd_uuid $to_prepare || return 1

	
    ${CEPH_DISK} $CEPH_DISK_ARGS \
        activate \
        --mark-init=none \
        $to_activate || return 1

    test_pool_read_write $osd_uuid || return 1
}

function test_activate_dir() {
    run_mon

    local osd_data=$DIR/dir
    $mkdir -p $osd_data
    test_activate $osd_data $osd_data || return 1
}

function test_activate_dir_bluestore() {
    run_mon

    local osd_data=$DIR/dir
    $mkdir -p $osd_data
    local to_prepare=$osd_data
    local to_activate=$osd_data
    local osd_uuid=$($uuidgen)

    CEPH_ARGS=" --bluestore-block-size=10737418240 $CEPH_ARGS" \
      ${CEPH_DISK} $CEPH_DISK_ARGS \
        prepare --bluestore --block-file --osd-uuid $osd_uuid $to_prepare || return 1

    CEPH_ARGS=" --osd-objectstore=bluestore --bluestore-fsck-on-mount=true --enable_experimental_unrecoverable_data_corrupting_features=* --bluestore-block-db-size=67108864 --bluestore-block-wal-size=134217728 --bluestore-block-size=10737418240 $CEPH_ARGS" \
      run_timeout ${CEPH_DISK} $CEPH_DISK_ARGS \
        activate \
        --mark-init=none \
        $to_activate || return 1
    test_pool_read_write $osd_uuid || return 1
}

function test_find_cluster_by_uuid() {
    setup
    test_activate_dir 2>&1 | tee $DIR/test_find
    ! grep "No cluster conf found in $DIR" $DIR/test_find || return 1
    teardown

    setup
    rm $DIR/ceph.conf
    test_activate_dir > $DIR/test_find 2>&1 
    grep --quiet "No cluster conf found in $DIR" $DIR/test_find || return 1
    teardown
}

# http://tracker.ceph.com/issues/9653
function test_keyring_path() {
    test_activate_dir 2>&1 | tee $DIR/test_keyring
    grep --quiet "keyring $DIR/bootstrap-osd/ceph.keyring" $DIR/test_keyring || return 1
}

# http://tracker.ceph.com/issues/13522
function ceph_osd_fail_once_fixture() {
    local command=ceph-osd
    local fpath=`readlink -f $(which $command)`
    [ "$fpath" = `readlink -f ../$command` ] || [ "$fpath" = `readlink -f $(pwd)/$command` ] || return 1

    cat > $DIR/$command <<EOF
#!/bin/bash
if echo "\$@" | grep -e --mkfs && ! test -f $DIR/used-$command ; then
   touch $DIR/used-$command
   # sleep longer than the first CEPH_OSD_MKFS_DELAYS value (5) below
   sleep 600
else
   exec ../$command "\$@"
fi
EOF
    chmod +x $DIR/$command
}

function test_ceph_osd_mkfs() {
    ceph_osd_fail_once_fixture || return 1
    CEPH_OSD_MKFS_DELAYS='5 300 300' use_path test_activate_dir || return 1
    [ -f $DIR/used-ceph-osd ] || return 1
}

function run() {
    local default_actions
    default_actions+="test_path "
    default_actions+="test_no_path "
    default_actions+="test_find_cluster_by_uuid "
    default_actions+="test_prepend_to_path "
    default_actions+="test_activate_dir_magic "
    default_actions+="test_activate_dir "
    default_actions+="test_keyring_path "
    [ x`uname`x != xFreeBSDx ] && default_actions+="test_mark_init "
    default_actions+="test_zap "
    [ x`uname`x != xFreeBSDx ] && default_actions+="test_activate_dir_bluestore "
    default_actions+="test_ceph_osd_mkfs "
    local actions=${@:-$default_actions}
    local status
    for action in $actions  ; do
        setup
        set -x
        $action
        status=$?
        set +x
        if [ $status -ne 0 ] ; then
            break
        else
	    # Only teardown if the $action has completed in order
            teardown
        fi
    done
    rm -fr virtualenv-$DIR
    # return the value that we saved from running the $action test
    return $status
}

run $@
status=$?
exit $status

# Local Variables:
# compile-command: "cd .. ; test/ceph-disk.sh # test_activate_dir"
# End:
