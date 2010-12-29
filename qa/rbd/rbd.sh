#!/bin/bash -x

basedir=`echo $0 | sed 's/[^/]*$//g'`.
. $basedir/common.sh

rbd_test_init


create_multiple() {
	for i in `seq 1 10`; do
		rbd_create_image $i
	done

	for i in `seq 1 10`; do
		rbd_add $i
	done
	for i in `seq 1 10`; do
		devname=/dev/rbd`eval echo \\$rbd$i`
		echo $devname
	done
	for i in `seq 1 10`; do
		devid=`eval echo \\$rbd$i`
		rbd_remove $devid
	done
	for i in `seq 1 10`; do
		rbd_rm_image $i
	done
}

test_dbench() {
	rbd_create_image 0
	rbd_add 0

	devname=/dev/rbd$rbd0

	mkfs -t ext3 $devname
	mount -t ext3 $devname $mnt

	dbench -D $mnt -t 30 5
	sync

	umount $mnt
	rbd_remove $rbd0
	rbd_rm_image 0
}

create_multiple
test_dbench

