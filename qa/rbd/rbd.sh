#!/bin/bash -x

basedir=`echo $0 | sed 's/[^/]*$//g'`.
. $basedir/common.sh

rbd_test_init

rbd_add 0

devname=/dev/rbd$rbd0

mkfs -t ext3 $devname
mount -t ext3 $devname /mnt

dbench -D /mnt -t 30 5
sync

umount /mnt
rbd_remove $rbd0

