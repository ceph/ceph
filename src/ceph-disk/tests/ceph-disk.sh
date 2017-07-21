#!/bin/bash
#
# Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
# Copyright (C) 2014, 2015, 2016, 2017 Red Hat <contact@redhat.com>
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

# ceph-disk.sh is launched by tox which expects tox.ini in current
# directory. so we cannot run ceph-disk.sh in build directory directly,
# and hence not able to use detect-build-env-vars.sh to set the build
# env vars.
if [ -z "$CEPH_ROOT" ] || [ -z "$CEPH_BIN" ] || [ -z "$CEPH_LIB" ]; then
    CEPH_ROOT=`readlink -f $(dirname $0)/../../..`
    CEPH_BIN=$CEPH_ROOT
    CEPH_LIB=$CEPH_ROOT/.libs
fi
source $CEPH_ROOT/qa/workunits/ceph-helpers.sh

set -x

PS4='${BASH_SOURCE[0]}:$LINENO: ${FUNCNAME[0]}:  '

export PATH=$CEPH_BIN:.:$PATH # make sure program from sources are preferred
export LD_LIBRARY_PATH=$CEPH_LIB
: ${CEPH_DISK:=ceph-disk}
CEPH_DISK_ARGS=
CEPH_DISK_ARGS+=" --verbose"
CEPH_DISK_ARGS+=" --prepend-to-path="
: ${CEPH_DISK_TIMEOUT:=360}
if [ `uname` != FreeBSD ]; then
    PROCDIR=""
else
    PROCDIR="/compat/linux"
fi

cat=$(which cat)
diff=$(which diff)
mkdir=$(which mkdir)
rm=$(which rm)
uuidgen=$(which uuidgen)
if [ `uname` = FreeBSD ]; then
    # for unknown reasons FreeBSD timeout does not return sometimes
    timeout=""
else
    timeout="$(which timeout) $CEPH_DISK_TIMEOUT"
fi

function setup() {
    local dir=$1
    teardown $dir
    mkdir -p $dir/osd
    touch $dir/ceph.conf # so ceph-disk think ceph is the cluster
}

function teardown() {
    local dir=$1
    if ! test -e $dir ; then
        return
    fi
    kill_daemons $dir
    if [ `uname` != FreeBSD ] && \
       [ $(stat -f -c '%T' .) == "btrfs" ]; then
        rm -fr $dir/*/*db
        __teardown_btrfs $dir
    fi
    grep " $(pwd)/$dir/" < ${PROCDIR}/proc/mounts | while read mounted rest ; do
        umount $mounted
    done
    rm -fr $dir
}

function command_fixture() {
    local dir=$1
    shift
    local command=$1
    shift
    local fpath=`readlink -f $(which $command)`
    [ "$fpath" = `readlink -f $CEPH_BIN/$command` ] || [ "$fpath" = `readlink -f $(pwd)/$command` ] || return 1

    cat > $dir/$command <<EOF
#!/bin/bash
touch $dir/used-$command
exec $CEPH_BIN/$command "\$@"
EOF
    chmod +x $dir/$command
}

function tweak_path() {
    local dir=$1
    shift
    local tweaker=$1
    shift

    setup $dir

    command_fixture $dir ceph-conf || return 1
    command_fixture $dir ceph-osd || return 1

    test_activate_dir $dir || return 1

    [ ! -f $dir/used-ceph-conf ] || return 1
    [ ! -f $dir/used-ceph-osd ] || return 1

    teardown $dir

    setup $dir

    command_fixture $dir ceph-conf || return 1
    command_fixture $dir ceph-osd || return 1

    $tweaker $dir test_activate_dir || return 1

    [ -f $dir/used-ceph-osd ] || return 1

    teardown $dir
}

function use_prepend_to_path() {
    local dir=$1
    shift

    local ceph_disk_args
    ceph_disk_args+=" --statedir=$dir"
    ceph_disk_args+=" --sysconfdir=$dir"
    ceph_disk_args+=" --prepend-to-path=$dir"
    ceph_disk_args+=" --verbose"
    CEPH_DISK_ARGS="$ceph_disk_args" \
        "$@" $dir || return 1
}

function test_prepend_to_path() {
    local dir=$1
    shift
    tweak_path $dir use_prepend_to_path || return 1
}

function use_path() {
    local dir=$1
    shift
    PATH="$dir:$PATH" \
        "$@" $dir || return 1
}

function test_path() {
    local dir=$1
    shift
    tweak_path $dir use_path || return 1
}

function test_no_path() {
    local dir=$1
    shift
    ( export PATH=../ceph-detect-init/virtualenv/bin:virtualenv/bin:$CEPH_BIN:/usr/bin:/bin:/usr/local/bin ; test_activate_dir $dir) || return 1
}

function test_mark_init() {
    local dir=$1
    shift

    run_mon $dir a

    local osd_data=$dir/dir
    $mkdir -p $osd_data

    local osd_uuid=$($uuidgen)

    $mkdir -p $osd_data

    ${CEPH_DISK} $CEPH_DISK_ARGS \
        prepare --filestore --osd-uuid $osd_uuid $osd_data || return 1

    ${CEPH_DISK} $CEPH_DISK_ARGS \
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
    $timeout ${CEPH_DISK} $CEPH_DISK_ARGS \
        --verbose \
        activate \
        --mark-init=$expected \
        --no-start-daemon \
        $osd_data || return 1

    ! test -f $osd_data/$(ceph-detect-init) || return 1
    test -f $osd_data/$expected || return 1
}

function test_zap() {
    local dir=$1
    local osd_data=$dir/dir
    $mkdir -p $osd_data

    ${CEPH_DISK} $CEPH_DISK_ARGS zap $osd_data 2>&1 | grep -q 'not full block device' || return 1

    $rm -fr $osd_data
}

# ceph-disk prepare returns immediately on success if the magic file
# exists in the --osd-data directory.
function test_activate_dir_magic() {
    local dir=$1
    local uuid=$($uuidgen)
    local osd_data=$dir/osd

    echo a failure to create the fsid file implies the magic file is not created

    mkdir -p $osd_data/fsid
    CEPH_ARGS="--fsid $uuid" \
     ${CEPH_DISK} $CEPH_DISK_ARGS prepare --filestore $osd_data > $dir/out 2>&1
    grep --quiet 'Is a directory' $dir/out || return 1
    ! [ -f $osd_data/magic ] || return 1
    rmdir $osd_data/fsid

    echo successfully prepare the OSD

    CEPH_ARGS="--fsid $uuid" \
     ${CEPH_DISK} $CEPH_DISK_ARGS prepare --filestore $osd_data 2>&1 | tee $dir/out
    grep --quiet 'Preparing osd data dir' $dir/out || return 1
    grep --quiet $uuid $osd_data/ceph_fsid || return 1
    [ -f $osd_data/magic ] || return 1

    echo will not override an existing OSD

    CEPH_ARGS="--fsid $($uuidgen)" \
     ${CEPH_DISK} $CEPH_DISK_ARGS prepare --filestore $osd_data 2>&1 | tee $dir/out
    grep --quiet 'Data dir .* already exists' $dir/out || return 1
    grep --quiet $uuid $osd_data/ceph_fsid || return 1
}

function read_write() {
    local dir=$1
    local file=${2:-$(uuidgen)}
    local pool=rbd

    echo FOO > $dir/$file
    $timeout rados --pool $pool put $file $dir/$file || return 1
    $timeout rados --pool $pool get $file $dir/$file.copy || return 1
    $diff $dir/$file $dir/$file.copy || return 1
}

function test_pool_read_write() {
    local dir=$1
    local pool=rbd

    $timeout ceph osd pool set $pool size 1 || return 1
    read_write $dir || return 1
}

function test_activate() {
    local dir=$1
    shift
    local osd_data=$1
    shift

    mkdir -p $osd_data

    ${CEPH_DISK} $CEPH_DISK_ARGS \
        prepare --filestore "$@" $osd_data || return 1

    $timeout ${CEPH_DISK} $CEPH_DISK_ARGS \
        activate \
        --mark-init=none \
        $osd_data || return 1

    test_pool_read_write $dir || return 1
}

function test_reuse_osd_id() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1

    test_activate $dir $dir/dir1 --osd-uuid $(uuidgen) || return 1

    #
    # add a new OSD with a given OSD id (13)
    #
    local osd_uuid=$($uuidgen)
    local osd_id=13
    test_activate $dir $dir/dir2 --osd-id $osd_id --osd-uuid $osd_uuid || return 1
    test $osd_id = $(ceph osd new $osd_uuid) || return 1

    #
    # make sure the OSD is in use by the PGs
    #
    wait_osd_id_used_by_pgs $osd_id 6 || return 1
    read_write $dir SOMETHING || return 1

    #
    # set the OSD out and verify it is no longer used by the PGs
    #
    ceph osd out osd.$osd_id || return 1
    wait_osd_id_used_by_pgs $osd_id 0 || return 1

    #
    # kill the OSD and destroy it (do not purge, retain its place in the crushmap)
    #
    kill_daemons $dir TERM osd.$osd_id || return 1
    ceph osd destroy osd.$osd_id --yes-i-really-mean-it || return 1

    #
    # add a new OSD with the same id as the destroyed OSD
    #
    osd_uuid=$($uuidgen)
    test_activate $dir $dir/dir3 --osd-id $osd_id --osd-uuid $osd_uuid || return 1
    test $osd_id = $(ceph osd new $osd_uuid) || return 1
}

function test_activate_dir() {
    local dir=$1
    shift

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    $@

    test_activate $dir $dir/dir || return 1
}

function test_activate_dir_bluestore() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1

    local osd_data=$dir/dir
    $mkdir -p $osd_data
    local to_prepare=$osd_data
    local to_activate=$osd_data
    local osd_uuid=$($uuidgen)

    CEPH_ARGS=" --bluestore-block-size=10737418240 $CEPH_ARGS" \
      ${CEPH_DISK} $CEPH_DISK_ARGS \
        prepare --bluestore --block-file --osd-uuid $osd_uuid $to_prepare || return 1

    CEPH_ARGS=" --osd-objectstore=bluestore --bluestore-fsck-on-mount=true --bluestore-block-db-size=67108864 --bluestore-block-wal-size=134217728 --bluestore-block-size=10737418240 $CEPH_ARGS" \
      $timeout ${CEPH_DISK} $CEPH_DISK_ARGS \
        activate \
        --mark-init=none \
        $to_activate || return 1

    test_pool_read_write $dir || return 1
}

function test_find_cluster_by_uuid() {
    local dir=$1
    test_activate_dir $dir 2>&1 | tee $dir/test_find
    ! grep "No cluster conf found in $dir" $dir/test_find || return 1
    teardown $dir

    setup $dir
    test_activate_dir $dir "rm $dir/ceph.conf" > $dir/test_find 2>&1
    cp $dir/test_find /tmp
    grep --quiet "No cluster conf found in $dir" $dir/test_find || return 1
}

# http://tracker.ceph.com/issues/9653
function test_keyring_path() {
    local dir=$1
    test_activate_dir $dir 2>&1 | tee $dir/test_keyring
    grep --quiet "keyring $dir/bootstrap-osd/ceph.keyring" $dir/test_keyring || return 1
}

# http://tracker.ceph.com/issues/13522
function ceph_osd_fail_once_fixture() {
    local dir=$1
    local command=ceph-osd
    local fpath=`readlink -f $(which $command)`
    [ "$fpath" = `readlink -f $CEPH_BIN/$command` ] || [ "$fpath" = `readlink -f $(pwd)/$command` ] || return 1

    cat > $dir/$command <<EOF
#!/bin/bash
if echo "\$@" | grep -e --mkfs && ! test -f $dir/used-$command ; then
   touch $dir/used-$command
   # sleep longer than the first CEPH_OSD_MKFS_DELAYS value (5) below
   sleep 600
else
   exec $CEPH_BIN/$command "\$@"
fi
EOF
    chmod +x $dir/$command
}

function test_ceph_osd_mkfs() {
    local dir=$1
    ceph_osd_fail_once_fixture $dir || return 1
    CEPH_OSD_MKFS_DELAYS='5 300 300' use_path $dir test_activate_dir || return 1
    [ -f $dir/used-ceph-osd ] || return 1
}

function test_crush_device_class() {
    local dir=$1
    shift

    run_mon $dir a

    local osd_data=$dir/dir
    $mkdir -p $osd_data

    local osd_uuid=$($uuidgen)

    $mkdir -p $osd_data

    ${CEPH_DISK} $CEPH_DISK_ARGS \
        prepare --filestore --osd-uuid $osd_uuid \
                --crush-device-class CRUSH_CLASS \
                $osd_data || return 1
    test -f $osd_data/crush_device_class || return 1
    test $(cat $osd_data/crush_device_class) = CRUSH_CLASS || return 1

    ceph osd crush class create CRUSH_CLASS || return 1

    CEPH_ARGS="--crush-location=root=default $CEPH_ARGS" \
      ${CEPH_DISK} $CEPH_DISK_ARGS \
        --verbose \
        activate \
        --mark-init=none \
        $osd_data || return 1

    ok=false
    for delay in 2 4 8 16 32 64 128 256 ; do
        if ceph osd crush dump | grep --quiet 'CRUSH_CLASS' ; then
            ok=true
            break
        fi
        sleep $delay
        ceph osd crush dump # for debugging purposes
    done
    $ok || return 1
}

function run() {
    local dir=$1
    shift
    CEPH_DISK_ARGS+=" --statedir=$dir"
    CEPH_DISK_ARGS+=" --sysconfdir=$dir"

    export CEPH_MON="127.0.0.1:7451" # git grep '\<7451\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+=" --fsid=$(uuidgen)"
    CEPH_ARGS+=" --auth-supported=none"
    CEPH_ARGS+=" --mon-host=$CEPH_MON"
    CEPH_ARGS+=" --chdir="
    CEPH_ARGS+=" --journal-dio=false"
    CEPH_ARGS+=" --erasure-code-dir=$CEPH_LIB"
    CEPH_ARGS+=" --plugin-dir=$CEPH_LIB"
    CEPH_ARGS+=" --log-file=$dir/\$name.log"
    CEPH_ARGS+=" --pid-file=$dir/\$name.pidfile"
    CEPH_ARGS+=" --osd-class-dir=$CEPH_LIB"
    CEPH_ARGS+=" --run-dir=$dir"
    CEPH_ARGS+=" --osd-failsafe-full-ratio=.99"
    CEPH_ARGS+=" --osd-journal-size=100"
    CEPH_ARGS+=" --debug-osd=20"
    CEPH_ARGS+=" --debug-bdev=20"
    CEPH_ARGS+=" --debug-bluestore=20"
    CEPH_ARGS+=" --osd-max-object-name-len=460"
    CEPH_ARGS+=" --osd-max-object-namespace-len=64 "
    local default_actions
    default_actions+="test_path "
    default_actions+="test_no_path "
    default_actions+="test_find_cluster_by_uuid "
    default_actions+="test_prepend_to_path "
    default_actions+="test_activate_dir_magic "
    default_actions+="test_activate_dir "
    default_actions+="test_keyring_path "
    [ `uname` != FreeBSD ] && \
      default_actions+="test_mark_init "
    default_actions+="test_zap "
    [ `uname` != FreeBSD ] && \
      default_actions+="test_activate_dir_bluestore "
    default_actions+="test_ceph_osd_mkfs "
    default_actions+="test_crush_device_class "
    default_actions+="test_reuse_osd_id "
    local actions=${@:-$default_actions}
    for action in $actions  ; do
        setup $dir || return 1
        set -x
        $action $dir || return 1
        set +x
        teardown $dir || return 1
    done
}

main test-ceph-disk "$@"

# Local Variables:
# compile-command: "cd .. ; test/ceph-disk.sh # test_activate_dir"
# End:
