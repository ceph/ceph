#!/bin/bash 
#
# Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
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
set -e

export PATH=/sbin:$PATH

: ${VERBOSE:=false}
: ${EXT4:=$(which mkfs.ext4)}
: ${EXT3:=$(which mkfs.ext3)}
: ${XFS:=$(which mkfs.xfs)}
: ${BTRFS:=$(which mkfs.btrfs)}
: ${CEPH_TEST_FILESTORE:=ceph_test_objectstore}
: ${FILE_SYSTEMS:=EXT4} #  EXT3 XFS BTRFS
: ${DEBUG:=}

function EXT4_test() {
    local dir="$1"

    if [ -z "$EXT4" ] ; then
        echo "mkfs command for ext4 is missing. On Debian GNU/Linux try apt-get install e2fsprogs" >&2
        return 1
    fi

    local disk="$dir/disk.img"

    truncate --size=1G $disk || return 1
    mkfs.ext4 -q -F $disk || return 2
    mkdir -p $dir/mountpoint || return 3
    MOUNTPOINT=$dir/mountpoint DISK=$disk sudo -E $CEPH_TEST_FILESTORE --gtest_filter=EXT4StoreTest.* $DEBUG || return 4
}

function main() {
    local dir=$(mktemp --directory)

    trap "sudo umount $dir/mountpoint || true ; rm -fr $dir" EXIT QUIT INT

    for fs in $FILE_SYSTEMS ; do
        ${fs}_test $dir || return 2
    done
}

if [ "$1" = TEST ]
then
    set -x
    set -o functrace
    PS4='${BASH_SOURCE[0]}:$LINENO: ${FUNCNAME[0]}:  '

    DEBUG='--log-to-stderr=true --debug-filestore=20'

    function run_test() {
        dir=/tmp/filestore
        rm -fr $dir
        mkdir $dir
        EXT4_test $dir || return 1

        FILE_SYSTEMS=EXT4
        main || return 2
    }

    run_test
else
    main
fi
# Local Variables:
# compile-command: "CEPH_TEST_FILESTORE=../../../src/ceph_test_objectstore filestore.sh TEST"
# End:
