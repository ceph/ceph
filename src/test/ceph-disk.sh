#!/bin/bash
#
# Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
# Copyright (C) 2014, 2015 Red Hat <contact@redhat.com>
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
source test/test_btrfs_common.sh

PS4='${BASH_SOURCE[0]}:$LINENO: ${FUNCNAME[0]}:  '

export PATH=.:$PATH # make sure program from sources are prefered
DIR=test-ceph-disk
virtualenv virtualenv-$DIR
. virtualenv-$DIR/bin/activate
(
    # older versions of pip will not install wrap_console scripts
    # when using wheel packages
    pip install --upgrade 'pip >= 6.1'
    if test -d ceph-detect-init/wheelhouse ; then
        wheelhouse="--no-index --use-wheel --find-links=ceph-detect-init/wheelhouse"
    fi
    pip --log virtualenv-$DIR/log.txt install $wheelhouse --editable ceph-detect-init
)
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
uuidgen=$(which uuidgen)

function setup() {
    teardown
    mkdir $DIR
    mkdir $OSD_DATA
    touch $DIR/ceph.conf # so ceph-disk think ceph is the cluster
}

function teardown() {
    kill_daemons
    if [ $(stat -f -c '%T' .) == "btrfs" ]; then
        rm -fr $DIR/*/*db
        teardown_btrfs $DIR
    fi
    grep " $(pwd)/$DIR/" < /proc/mounts | while read mounted rest ; do
        umount $mounted
    done
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
    if ! test -e $DIR ; then
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

function test_mark_init() {
    run_mon

    local osd_data=$(pwd)/$DIR/dir
    $mkdir -p $osd_data

    local osd_uuid=$($uuidgen)

    $mkdir -p $OSD_DATA

    ./ceph-disk $CEPH_DISK_ARGS \
        prepare --osd-uuid $osd_uuid $osd_data || return 1

    $timeout $TIMEOUT ./ceph-disk $CEPH_DISK_ARGS \
        --verbose \
        activate \
        --mark-init=auto \
        --no-start-daemon \
        $osd_data || return 1

    test -f $osd_data/$(ceph-detect-init) || return 1

    if test systemd = $(ceph-detect-init) ; then
        expected=sysvinit
    else
        expected=systemd
    fi
    $timeout $TIMEOUT ./ceph-disk $CEPH_DISK_ARGS \
        --verbose \
        activate \
        --mark-init=$expected \
        --no-start-daemon \
        $osd_data || return 1

    ! test -f $osd_data/$(ceph-detect-init) || return 1
    test -f $osd_data/$expected || return 1

    $rm -fr $osd_data
}

function test_zap() {
    local osd_data=$DIR/dir
    $mkdir -p $osd_data

    ./ceph-disk $CEPH_DISK_ARGS zap $osd_data 2>&1 | grep -q 'not full block device' || return 1

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

    CEPH_ARGS="--fsid $($uuidgen)" \
     ./ceph-disk $CEPH_DISK_ARGS prepare $osd_data 2>&1 | tee $DIR/out
    grep --quiet 'ceph-disk:Data dir .* already exists' $DIR/out || return 1
    grep --quiet $uuid $osd_data/ceph_fsid || return 1
}

function test_pool_read_write() {
    local osd_uuid=$1

    $timeout $TIMEOUT ./ceph osd pool set $TEST_POOL size 1 || return 1

    local id=$(ceph osd create $osd_uuid)
    local weight=1
    ./ceph osd crush add osd.$id $weight root=default host=localhost || return 1
    echo FOO > $DIR/BAR
    $timeout $TIMEOUT ./rados --pool $TEST_POOL put BAR $DIR/BAR || return 1
    $timeout $TIMEOUT ./rados --pool $TEST_POOL get BAR $DIR/BAR.copy || return 1
    $diff $DIR/BAR $DIR/BAR.copy || return 1
}

function test_activate() {
    local to_prepare=$1
    local to_activate=$2
    local journal=$3
    local osd_uuid=$($uuidgen)

    $mkdir -p $OSD_DATA

    ./ceph-disk $CEPH_DISK_ARGS \
        prepare --osd-uuid $osd_uuid $to_prepare $journal || return 1

    $timeout $TIMEOUT ./ceph-disk $CEPH_DISK_ARGS \
        activate \
        --mark-init=none \
        $to_activate || return 1

    test_pool_read_write $osd_uuid || return 1
}

function test_activate_dmcrypt() {
    local to_prepare=$1
    local to_activate=$2
    local journal=$3
    local journal_p=$4
    local uuid=$5
    local juuid=$6
    local plain=$7

    $mkdir -p $OSD_DATA

    if test $plain = plain ; then
        echo "osd_dmcrypt_type=plain" > $DIR/ceph.conf
    fi
    
    ./ceph-disk $CEPH_DISK_ARGS \
		prepare --dmcrypt --dmcrypt-key-dir $DIR/keys --osd-uuid=$uuid --journal-uuid=$juuid $to_prepare $journal || return 1

    if test $plain = plain ; then
        /sbin/cryptsetup --key-file $DIR/keys/$uuid --key-size 256 create $uuid $to_activate
        /sbin/cryptsetup --key-file $DIR/keys/$juuid --key-size 256 create $juuid $journal
    else
        /sbin/cryptsetup --key-file $DIR/keys/$uuid.luks.key luksOpen $to_activate $uuid
        /sbin/cryptsetup --key-file $DIR/keys/$juuid.luks.key luksOpen ${journal}${journal_p} $juuid
    fi
    
    $timeout $TIMEOUT ./ceph-disk $CEPH_DISK_ARGS \
        activate \
        --mark-init=none \
        /dev/mapper/$uuid || return 1

    test_pool_read_write $uuid || return 1
}

function test_activate_dir() {
    run_mon

    local osd_data=$DIR/dir
    $mkdir -p $osd_data
    test_activate $osd_data $osd_data || return 1
    $rm -fr $osd_data
}

function loop_sanity_check_body() {
    local dev=$1
    local guid=$2

    #
    # Check if /dev/loop is configured with max_part > 0 to handle
    # partition tables and expose the partition devices in /dev
    #
    sgdisk --largest-new=1 --partition-guid=1:$guid $dev
    if ! test -e ${dev}p1 ; then
        if grep loop.max_part /proc/cmdline ; then
            echo "the loop module max_part parameter is configured but when"
            echo "creating a new partition on $dev, it the expected node"
            echo "${dev}p1 does not exist"
            return 1
        fi
        perl -pi -e 's/$/ loop.max_part=16/ if(/kernel/ && !/max_part/)' /boot/grub/grub.conf
        echo "the loop.max_part=16 was added to the kernel in /boot/grub/grub.conf"
        cat /boot/grub/grub.conf
        echo "you need to reboot for it to be taken into account"
        return 1
    fi
    
    #
    # Install the minimal files supporting the maintenance of /dev/disk/by-partuuid
    #
    udevadm trigger --sysname-match=$(basename $dev)
    udevadm settle
    if test ! -e /dev/disk/by-partuuid/$guid ; then
        cp -a ../udev/95-ceph-osd-alt.rules /lib/udev/rules.d/95-ceph-osd.rules
        cp -a ceph-disk ceph-disk-udev /usr/sbin
        udevadm trigger --sysname-match=$(basename $dev)
        if test ! -e /dev/disk/by-partuuid/$guid ; then
            echo "/dev/disk/by-partuuid/$guid not found although the"
            echo "following support files are installed: "
            ls -l /lib/udev/rules.d/95-ceph-osd.rules /usr/sbin/ceph-disk{,-udev}
            return 1
        fi
    fi

    return 0
}

function loop_sanity_check() {
    local id=$(lsb_release -si)
    local major=$(lsb_release -rs | cut -f1 -d.)
    if test $major != 6 || test $id != CentOS -a $id != RedHatEnterpriseServer ; then
        echo "/dev/loop is assumed to be configured with max_part > 0"
        echo "and /dev/disk/by-partuuid to be populated by udev on"
        lsb_release -a
        return 0
    fi
    local name=$DIR/sanity.disk
    dd if=/dev/zero of=$name bs=1024k count=10 > /dev/null 2>&1
    losetup --find $name
    local dev=$(losetup --associated $name | cut -f1 -d:)
    local guid=$($uuidgen)

    loop_sanity_check_body $dev $guid
    status=$?

    losetup --detach $dev
    rm $name
    rm -f /dev/disk/by-partuuid/$guid

    return $status
}

function reset_dev() {
    local dev=$1

    if test -z "$dev" ; then
        return
    fi

    grep "^$dev" < /proc/mounts | while read mounted rest ; do
        umount $mounted
    done
    local dev_base=$(basename $dev)
    (
        ls /sys/block/$dev_base/$dev_base*/holders 2> /dev/null
        ls /sys/block/$dev_base/holders 2> /dev/null
        ) | grep '^dm-' | while read dm ; do
	dmsetup remove /dev/$dm
    done
    ceph-disk zap $dev > /dev/null 2>&1
}

function reset_leftover_dev() {
    local path=$1

    losetup --all | sed -e 's/://' | while read dev id associated_path ; do
        # if $path has been deleted with a dev attached, then $associated_path
        # will carry "($path (deleted))".
        if test "$associated_path" = "($path)" ; then
            reset_dev $dev
            losetup --detach $dev
        fi
    done
}

function create_dev() {
    local path=$1

    echo -n "create_dev $path ... " >&2
    reset_leftover_dev $path
    dd if=/dev/zero of=$path bs=1024k count=400 > /dev/null 2>&1
    losetup --find $path
    local dev=$(losetup --associated $path | cut -f1 -d:)
    test "$dev" || return 1
    reset_dev $dev
    echo $dev >&2
    echo $dev
}

function destroy_dev() {
    local path=$1
    local dev=$2

    echo destroy_dev $path $dev >&2
    reset_dev $dev
    losetup --detach $dev
    rm -f $path
}

function activate_dev_body() {
    local disk=$1
    local journal=$2
    local newdisk=$3

    setup
    run_mon
    #
    # Create an OSD without a journal and an objectstore
    # that does not use a journal.
    #
    ceph-disk zap $disk || return 1
    CEPH_ARGS="$CEPH_ARGS --osd-objectstore=memstore" \
        test_activate $disk ${disk}p1 || return 1
    kill_daemons
    umount ${disk}p1 || return 1
    teardown

    setup
    run_mon
    #
    # Create an OSD with data on a disk, journal on another
    #
    ceph-disk zap $disk || return 1
    test_activate $disk ${disk}p1 $journal || return 1
    kill_daemons
    umount ${disk}p1 || return 1
    teardown

    setup
    run_mon
    #
    # Create an OSD with data on a disk, journal on another
    # This will add a new partition to $journal, the previous
    # one will remain.
    #
    ceph-disk zap $disk || return 1
    test_activate $disk ${disk}p1 $journal || return 1
    kill_daemons
    umount ${disk}p1 || return 1
    teardown

    setup
    run_mon
    #
    # Create an OSD and reuse an existing journal partition
    #
    test_activate $newdisk ${newdisk}p1 ${journal}p1 || return 1
    #
    # Create an OSD and get a journal partition from a disk that
    # already contains a journal partition which is in use. Updates of
    # the kernel partition table may behave differently when a
    # partition is in use. See http://tracker.ceph.com/issues/7334 for
    # more information.
    #
    ceph-disk zap $disk || return 1
    test_activate $disk ${disk}p1 $journal || return 1
    kill_daemons
    umount ${newdisk}p1 || return 1
    umount ${disk}p1 || return 1
    teardown
}

function test_activate_dev() {
    test_setup_dev_and_run activate_dev_body
}

function test_setup_dev_and_run() {
    local action=$1
    if test $(id -u) != 0 ; then
        echo "SKIP because not root"
        return 0
    fi

    loop_sanity_check || return 1

    local dir=$(pwd)/$DIR
    local disk
    disk=$(create_dev $dir/vdf.disk) || return 1
    local journal
    journal=$(create_dev $dir/vdg.disk) || return 1
    local newdisk
    newdisk=$(create_dev $dir/vdh.disk) || return 1

    $action $disk $journal $newdisk
    status=$?

    destroy_dev $dir/vdf.disk $disk
    destroy_dev $dir/vdg.disk $journal
    destroy_dev $dir/vdh.disk $newdisk

    return $status
}

function activate_dmcrypt_dev_body() {
    local disk=$1
    local journal=$2
    local newdisk=$3
    local uuid=$($uuidgen)
    local juuid=$($uuidgen)

    setup
    run_mon
    test_activate_dmcrypt $disk ${disk}p1 $journal p1 $uuid $juuid not_plain || return 1
    kill_daemons
    umount /dev/mapper/$uuid || return 1
    teardown
}

function test_activate_dmcrypt_dev() {
    test_setup_dev_and_run activate_dmcrypt_dev_body
}

function activate_dmcrypt_plain_dev_body() {
    local disk=$1
    local journal=$2
    local newdisk=$3
    local uuid=$($uuidgen)
    local juuid=$($uuidgen)

    setup
    run_mon
    test_activate_dmcrypt $disk ${disk}p1 $journal p1 $uuid $juuid plain || return 1
    kill_daemons
    umount /dev/mapper/$uuid || return 1
    teardown
}

function test_activate_dmcrypt_plain_dev() {
    test_setup_dev_and_run activate_dmcrypt_plain_dev_body
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
    default_actions+="test_mark_init "
    default_actions+="test_zap "
    local actions=${@:-$default_actions}
    local status
    for action in $actions  ; do
        setup
        set -x
        $action
        status=$?
        set +x
        teardown
        if test $status != 0 ; then
            break
        fi
    done
    rm -fr virtualenv-$DIR
    return $status
}

run $@

# Local Variables:
# compile-command: "cd .. ; test/ceph-disk.sh # test_activate_dir"
# End:
