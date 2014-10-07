#!/bin/bash
#
# Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
# Copyright (C) 2014 Red Hat <contact@redhat.com>
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
set -xe

source test/test_btrfs_common.sh

PS4='${FUNCNAME[0]}: $LINENO: '

export PATH=:$PATH # make sure program from sources are prefered
DIR=test-ceph-disk
OSD_DATA=$DIR/osd
MON_ID=a
MONA=127.0.0.1:7451
TEST_POOL=rbd
FSID=$(uuidgen)
export CEPH_CONF=/dev/null
export CEPH_ARGS="--fsid $FSID"
CEPH_ARGS+=" --chdir="
CEPH_ARGS+=" --run-dir=$DIR"
CEPH_ARGS+=" --osd-failsafe-full-ratio=.99"
CEPH_ARGS+=" --mon-host=$MONA"
CEPH_ARGS+=" --log-file=$DIR/\$name.log"
CEPH_ARGS+=" --pid-file=$DIR/\$name.pidfile"
CEPH_ARGS+=" --osd-pool-default-erasure-code-directory=.libs"
CEPH_ARGS+=" --auth-supported=none"
CEPH_ARGS+=" --osd-journal-size=100"
CEPH_DISK_ARGS=
CEPH_DISK_ARGS+=" --statedir=$DIR"
CEPH_DISK_ARGS+=" --sysconfdir=$DIR"
CEPH_DISK_ARGS+=" --prepend-to-path="
CEPH_DISK_ARGS+=" --verbose"
TIMEOUT=360

cat=$(which cat)
timeout=$(which timeout)
diff=$(which diff)
mkdir=$(which mkdir)
rm=$(which rm)

function setup() {
    teardown
    mkdir $DIR
    mkdir $OSD_DATA
#    mkdir $OSD_DATA/ceph-0
    touch $DIR/ceph.conf # so ceph-disk think ceph is the cluster
}

function teardown() {
    kill_daemons
    if [ $(stat -f -c '%T' .) == "btrfs" ]; then
        rm -fr $DIR/*/*db
        teardown_btrfs $DIR
    fi
    rm -fr $DIR
}

function run_mon() {
    local mon_dir=$DIR/$MON_ID

    ./ceph-mon \
        --id $MON_ID \
        --mkfs \
        --mon-data=$mon_dir \
        --mon-initial-members=$MON_ID \
        "$@"

    ./ceph-mon \
        --id $MON_ID \
        --mon-data=$mon_dir \
        --mon-osd-full-ratio=.99 \
        --mon-data-avail-crit=1 \
        --mon-cluster-log-file=$mon_dir/log \
        --public-addr $MONA \
        "$@"
}

function kill_daemons() {
    for pidfile in $(find $DIR | grep pidfile) ; do
        pid=$(cat $pidfile)
        for try in 0 1 1 1 2 3 ; do
            kill $pid || break
            sleep $try
        done
    done
}

function command_fixture() {
    local command=$1

    [ $(which $command) = ./$command ] || [ $(which $command) = `readlink -f $(pwd)/$command` ] || return 1

    cat > $DIR/$command <<EOF
#!/bin/bash
touch $DIR/used-$command
exec ./$command "\$@"
EOF
    chmod +x $DIR/$command
}

function tweak_path() {
    local tweaker=$1

    setup

    command_fixture ceph-conf || return 1
    command_fixture ceph-osd || return 1

    test_activate_dir

    [ ! -f $DIR/used-ceph-conf ] || return 1
    [ ! -f $DIR/used-ceph-osd ] || return 1

    teardown

    setup

    command_fixture ceph-conf || return 1
    command_fixture ceph-osd || return 1

    $tweaker test_activate_dir || return 1

    [ -f $DIR/used-ceph-conf ] || return 1
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
    ( unset PATH ; test_activate_dir ) || return 1
}

# ceph-disk prepare returns immediately on success if the magic file
# exists in the --osd-data directory.
function test_activate_dir_magic() {
    local uuid=$(uuidgen)
    local osd_data=$DIR/osd

    echo a failure to create the fsid file implies the magic file is not created

    mkdir -p $osd_data/fsid
    CEPH_ARGS="--fsid $uuid" \
     ./ceph-disk $CEPH_DISK_ARGS prepare $osd_data > $DIR/out 2>&1
    grep --quiet 'Is a directory' $DIR/out || return 1
    ! [ -f $osd_data/magic ] || return 1
    rmdir $osd_data/fsid

    echo successfully prepare the OSD

    CEPH_ARGS="--fsid $uuid" \
     ./ceph-disk $CEPH_DISK_ARGS prepare $osd_data 2>&1 | tee $DIR/out
    grep --quiet 'Preparing osd data dir' $DIR/out || return 1
    grep --quiet $uuid $osd_data/ceph_fsid || return 1
    [ -f $osd_data/magic ] || return 1

    echo will not override an existing OSD

    CEPH_ARGS="--fsid $(uuidgen)" \
     ./ceph-disk $CEPH_DISK_ARGS prepare $osd_data 2>&1 | tee $DIR/out
    grep --quiet 'ceph-disk:Data dir .* already exists' $DIR/out || return 1
    grep --quiet $uuid $osd_data/ceph_fsid || return 1
}

function test_activate() {
    local to_prepare=$1
    local to_activate=$2

    $mkdir -p $OSD_DATA

    ./ceph-disk $CEPH_DISK_ARGS \
        prepare $to_prepare || return 1

    $timeout $TIMEOUT ./ceph-disk $CEPH_DISK_ARGS \
        activate \
        --mark-init=none \
        $to_activate || return 1
    $timeout $TIMEOUT ./ceph osd pool set $TEST_POOL size 1 || return 1

    local id=$($cat $OSD_DATA/ceph-?/whoami || $cat $to_activate/whoami)
    local weight=1
    ./ceph osd crush add osd.$id $weight root=default host=localhost || return 1
    echo FOO > $DIR/BAR
    $timeout $TIMEOUT ./rados --pool $TEST_POOL put BAR $DIR/BAR || return 1
    $timeout $TIMEOUT ./rados --pool $TEST_POOL get BAR $DIR/BAR.copy || return 1
    $diff $DIR/BAR $DIR/BAR.copy || return 1
}

function test_activate_dir() {
    run_mon

    local osd_data=$DIR/dir
    $mkdir -p $osd_data
    test_activate $osd_data $osd_data || return 1
    $rm -fr $osd_data
}

function test_activate_dev() {
    run_mon

    if test $(id -u) != 0 ; then
        echo "SKIP because not root"
        return 0
    fi

    dd if=/dev/zero of=vde.disk bs=1024k count=200
    losetup --find vde.disk
    local disk=$(losetup --associated vde.disk | cut -f1 -d:)
    ./ceph-disk zap $disk
    test_activate ${disk} ${disk}p1
    kill_daemons
    umount ${disk}p1
    ./ceph-disk zap $disk
    status=$?
    losetup --detach $disk
    rm vde.disk
    return $status
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

function run() {
    local default_actions
    default_actions+="test_path "
    default_actions+="test_no_path "
    default_actions+="test_find_cluster_by_uuid "
    default_actions+="test_prepend_to_path "
    default_actions+="test_activate_dir_magic "
    default_actions+="test_activate_dir "
    default_actions+="test_keyring_path "
    local actions=${@:-$default_actions}
    for action in $actions  ; do
        setup
        $action || return 1
        teardown
    done
}

run $@

# Local Variables:
# compile-command: "cd .. ; test/ceph-disk.sh # test_activate_dir"
# End:
