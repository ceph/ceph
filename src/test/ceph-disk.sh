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
export CEPH_CONF=$DIR/ceph.conf
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

function test_zap() {
    local osd_data=$DIR/dir
    $mkdir -p $osd_data

    ./ceph-disk $CEPH_DISK_ARGS zap $osd_data 2>&1 | grep 'not full block device' || return 1

    $rm -fr $osd_data
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
    local journal=$3

    $mkdir -p $OSD_DATA

    ./ceph-disk $CEPH_DISK_ARGS \
        prepare $to_prepare $journal || return 1

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

function test_activate_dmcrypt() {
    local to_prepare=$1
    local to_activate=$2
    local journal=$3
    local journal_p=$4
    local uuid=$5
    local juuid=$6

    $mkdir -p $OSD_DATA

    ./ceph-disk $CEPH_DISK_ARGS \
		prepare --dmcrypt --dmcrypt-key-dir $DIR/keys --osd-uuid=$uuid --journal-uuid=$juuid $to_prepare $journal || return 1

    /sbin/cryptsetup --key-file $DIR/keys/$uuid.luks.key luksOpen $to_activate $uuid
    /sbin/cryptsetup --key-file $DIR/keys/$juuid.luks.key luksOpen ${journal}${journal_p} $juuid
    
    $timeout $TIMEOUT ./ceph-disk $CEPH_DISK_ARGS \
        activate \
        --mark-init=none \
        /dev/mapper/$uuid || return 1
    $timeout $TIMEOUT ./ceph osd pool set $TEST_POOL size 1 || return 1

    local id=$($cat $OSD_DATA/ceph-?/whoami || $cat $to_activate/whoami)
    local weight=1
    ./ceph osd crush add osd.$id $weight root=default host=localhost || return 1
    echo FOO > $DIR/BAR
    $timeout $TIMEOUT ./rados --pool $TEST_POOL put BAR $DIR/BAR || return 1
    $timeout $TIMEOUT ./rados --pool $TEST_POOL get BAR $DIR/BAR.copy || return 1
    $diff $DIR/BAR $DIR/BAR.copy || return 1
}

function test_activate_dmcrypt_plain() {
    local to_prepare=$1
    local to_activate=$2
    local journal=$3
    local journal_p=$4
    local uuid=$5
    local juuid=$6

    $mkdir -p $OSD_DATA

    echo "osd_dmcrypt_type=plain" > $DIR/ceph.conf
    
    ./ceph-disk $CEPH_DISK_ARGS \
		prepare --dmcrypt --dmcrypt-key-dir $DIR/keys --osd-uuid=$uuid --journal-uuid=$juuid $to_prepare $journal || return 1

    /sbin/cryptsetup --key-file $DIR/keys/$uuid --key-size 256 create $uuid $to_activate
    /sbin/cryptsetup --key-file $DIR/keys/$juuid --key-size 256 create $juuid $journal
    
    $timeout $TIMEOUT ./ceph-disk $CEPH_DISK_ARGS \
        activate \
        --mark-init=none \
        /dev/mapper/$uuid || return 1
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

function create_dev() {
    local name=$1

    dd if=/dev/zero of=$name bs=1024k count=200
    losetup --find $name
    local dev=$(losetup --associated $name | cut -f1 -d:)
    ceph-disk zap $dev > /dev/null 2>&1
    echo $dev
}

function destroy_dev() {
    local name=$1
    local dev=$2

    for partition in 1 2 3 4 ; do
        umount ${dev}p${partition} || true
    done
    losetup --detach $dev
    rm $name
}

function activate_dev_body() {
    local disk=$1
    local journal=$2
    local newdisk=$3

    setup
    run_mon
    test_activate $disk ${disk}p1 $journal || return 1
    kill_daemons
    umount ${disk}p1 || return 1
    teardown

    # reuse the journal partition
    setup
    run_mon
    test_activate $newdisk ${newdisk}p1 ${journal}p1 || return 1
    kill_daemons
    umount ${newdisk}p1 || return 1
    teardown
}

function test_activate_dev() {
    if test $(id -u) != 0 ; then
        echo "SKIP because not root"
        return 0
    fi

    local disk=$(create_dev vdf.disk)
    local journal=$(create_dev vdg.disk)
    local newdisk=$(create_dev vdh.disk)

    activate_dev_body $disk $journal $newdisk
    status=$?

    destroy_dev vdf.disk $disk
    destroy_dev vdg.disk $journal
    destroy_dev vdh.disk $newdisk

    return $status
}

function destroy_dmcrypt_dev() {
    local name=$1
    local dev=$2
    local uuid=$3

    for partition in 1 2 3 4 ; do
        umount /dev/mapper/$uuid || true
	/sbin/cryptsetup remove /dev/mapper/$uuid || true
	dmsetup remove /dev/mapper/$uuid || true
    done
    losetup --detach $dev
    rm $name
}

function activate_dmcrypt_dev_body() {
    local disk=$1
    local journal=$2
    local newdisk=$3
    local uuid=$(uuidgen)
    local juuid=$(uuidgen)

    setup
    run_mon
    test_activate_dmcrypt $disk ${disk}p1 $journal p1 $uuid $juuid|| return 1
    kill_daemons
    umount /dev/mapper/$uuid || return 1
    teardown
}

function test_activate_dmcrypt_dev() {
    if test $(id -u) != 0 ; then
        echo "SKIP because not root"
        return 0
    fi

    local disk=$(create_dev vdf.disk)
    local journal=$(create_dev vdg.disk)
    local newdisk=$(create_dev vdh.disk)

    activate_dmcrypt_dev_body $disk $journal $newdisk
    status=$?

    destroy_dmcrypt_dev vdf.disk $disk
    destroy_dmcrypt_dev vdg.disk $journal
    destroy_dmcrypt_dev vdh.disk $newdisk

    return $status
}

function activate_dmcrypt_plain_dev_body() {
    local disk=$1
    local journal=$2
    local newdisk=$3
    local uuid=$(uuidgen)
    local juuid=$(uuidgen)

    setup
    run_mon
    test_activate_dmcrypt_plain $disk ${disk}p1 $journal p1 $uuid $juuid|| return 1
    kill_daemons
    umount /dev/mapper/$uuid || return 1
    teardown
}

function test_activate_dmcrypt_plain_dev() {
    if test $(id -u) != 0 ; then
        echo "SKIP because not root"
        return 0
    fi

    local disk=$(create_dev vdf.disk)
    local journal=$(create_dev vdg.disk)
    local newdisk=$(create_dev vdh.disk)

    activate_dmcrypt_plain_dev_body $disk $journal $newdisk
    status=$?

    destroy_dmcrypt_dev vdf.disk $disk
    destroy_dmcrypt_dev vdg.disk $journal
    destroy_dmcrypt_dev vdh.disk $newdisk

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
    default_actions+="test_zap "
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
