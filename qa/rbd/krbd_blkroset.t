
Setup
=====

  $ RO_KEY=$(ceph auth get-or-create-key client.ro mon 'profile rbd' mgr 'profile rbd' osd 'profile rbd-read-only')
  $ rbd create --size 10 img
  $ rbd snap create --no-progress img@snap
  $ rbd snap protect img@snap
  $ rbd clone img@snap cloneimg
  $ rbd create --size 1 imgpart
  $ DEV=$(sudo rbd map imgpart)
  $ cat <<EOF | sudo sfdisk $DEV >/dev/null 2>&1
  > unit: sectors
  > /dev/rbd0p1 : start=        512, size=    512, Id=83
  > /dev/rbd0p2 : start=       1024, size=    512, Id=83
  > EOF
  $ sudo rbd unmap $DEV
  $ rbd snap create --no-progress imgpart@snap


Image HEAD
==========

R/W, unpartitioned:

  $ DEV=$(sudo rbd map img)
  $ blockdev --getro $DEV
  0
  $ dd if=/dev/urandom of=$DEV bs=1k seek=1 count=1 status=none
  $ blkdiscard $DEV
  $ blockdev --setro $DEV
  .*BLKROSET: Permission denied (re)
  [1]
  $ sudo blockdev --setro $DEV
  $ blockdev --getro $DEV
  1
  $ dd if=/dev/urandom of=$DEV bs=1k seek=1 count=1 status=none
  dd: error writing '/dev/rbd?': Operation not permitted (glob)
  [1]
  $ blkdiscard $DEV
  blkdiscard: /dev/rbd?: BLKDISCARD ioctl failed: Operation not permitted (glob)
  [1]
  $ blockdev --setrw $DEV
  .*BLKROSET: Permission denied (re)
  [1]
  $ sudo blockdev --setrw $DEV
  $ blockdev --getro $DEV
  0
  $ dd if=/dev/urandom of=$DEV bs=1k seek=1 count=1 status=none
  $ blkdiscard $DEV
  $ sudo rbd unmap $DEV

R/W, partitioned:

  $ DEV=$(sudo rbd map imgpart)
  $ udevadm settle
  $ blockdev --getro ${DEV}p1
  0
  $ blockdev --getro ${DEV}p2
  0
  $ dd if=/dev/urandom of=${DEV}p1 bs=1k seek=1 count=1 status=none
  $ blkdiscard ${DEV}p1
  $ dd if=/dev/urandom of=${DEV}p2 bs=1k seek=1 count=1 status=none
  $ blkdiscard ${DEV}p2
  $ blockdev --setro ${DEV}p1
  .*BLKROSET: Permission denied (re)
  [1]
  $ sudo blockdev --setro ${DEV}p1
  $ blockdev --getro ${DEV}p1
  1
  $ blockdev --getro ${DEV}p2
  0
  $ dd if=/dev/urandom of=${DEV}p1 bs=1k seek=1 count=1 status=none
  dd: error writing '/dev/rbd?p1': Operation not permitted (glob)
  [1]
  $ blkdiscard ${DEV}p1
  blkdiscard: /dev/rbd?p1: BLKDISCARD ioctl failed: Operation not permitted (glob)
  [1]
  $ dd if=/dev/urandom of=${DEV}p2 bs=1k seek=1 count=1 status=none
  $ blkdiscard ${DEV}p2
  $ blockdev --setrw ${DEV}p1
  .*BLKROSET: Permission denied (re)
  [1]
  $ sudo blockdev --setrw ${DEV}p1
  $ blockdev --getro ${DEV}p1
  0
  $ blockdev --getro ${DEV}p2
  0
  $ dd if=/dev/urandom of=${DEV}p1 bs=1k seek=1 count=1 status=none
  $ blkdiscard ${DEV}p1
  $ dd if=/dev/urandom of=${DEV}p2 bs=1k seek=1 count=1 status=none
  $ blkdiscard ${DEV}p2
  $ sudo rbd unmap $DEV

  $ DEV=$(sudo rbd map imgpart)
  $ udevadm settle
  $ blockdev --getro ${DEV}p1
  0
  $ blockdev --getro ${DEV}p2
  0
  $ dd if=/dev/urandom of=${DEV}p1 bs=1k seek=1 count=1 status=none
  $ blkdiscard ${DEV}p1
  $ dd if=/dev/urandom of=${DEV}p2 bs=1k seek=1 count=1 status=none
  $ blkdiscard ${DEV}p2
  $ blockdev --setro ${DEV}p2
  .*BLKROSET: Permission denied (re)
  [1]
  $ sudo blockdev --setro ${DEV}p2
  $ blockdev --getro ${DEV}p1
  0
  $ blockdev --getro ${DEV}p2
  1
  $ dd if=/dev/urandom of=${DEV}p1 bs=1k seek=1 count=1 status=none
  $ blkdiscard ${DEV}p1
  $ dd if=/dev/urandom of=${DEV}p2 bs=1k seek=1 count=1 status=none
  dd: error writing '/dev/rbd?p2': Operation not permitted (glob)
  [1]
  $ blkdiscard ${DEV}p2
  blkdiscard: /dev/rbd?p2: BLKDISCARD ioctl failed: Operation not permitted (glob)
  [1]
  $ blockdev --setrw ${DEV}p2
  .*BLKROSET: Permission denied (re)
  [1]
  $ sudo blockdev --setrw ${DEV}p2
  $ blockdev --getro ${DEV}p1
  0
  $ blockdev --getro ${DEV}p2
  0
  $ dd if=/dev/urandom of=${DEV}p1 bs=1k seek=1 count=1 status=none
  $ blkdiscard ${DEV}p1
  $ dd if=/dev/urandom of=${DEV}p2 bs=1k seek=1 count=1 status=none
  $ blkdiscard ${DEV}p2
  $ sudo rbd unmap $DEV

R/O, unpartitioned:

  $ DEV=$(sudo rbd map --read-only img)
  $ blockdev --getro $DEV
  1
  $ dd if=/dev/urandom of=$DEV bs=1k seek=1 count=1 status=none
  dd: error writing '/dev/rbd?': Operation not permitted (glob)
  [1]
  $ blkdiscard $DEV
  blkdiscard: /dev/rbd?: BLKDISCARD ioctl failed: Operation not permitted (glob)
  [1]
  $ blockdev --setrw $DEV
  .*BLKROSET: Permission denied (re)
  [1]
  $ sudo blockdev --setrw $DEV  # succeeds but effectively ignored
  $ blockdev --getro $DEV
  1
  $ dd if=/dev/urandom of=$DEV bs=1k seek=1 count=1 status=none
  dd: error writing '/dev/rbd?': Operation not permitted (glob)
  [1]
  $ blkdiscard $DEV
  blkdiscard: /dev/rbd?: BLKDISCARD ioctl failed: Operation not permitted (glob)
  [1]
  $ sudo rbd unmap $DEV

R/O, partitioned:

  $ DEV=$(sudo rbd map --read-only imgpart)
  $ udevadm settle
  $ blockdev --getro ${DEV}p1
  1
  $ blockdev --getro ${DEV}p2
  1
  $ dd if=/dev/urandom of=${DEV}p1 bs=1k seek=1 count=1 status=none
  dd: error writing '/dev/rbd?p1': Operation not permitted (glob)
  [1]
  $ blkdiscard ${DEV}p1
  blkdiscard: /dev/rbd?p1: BLKDISCARD ioctl failed: Operation not permitted (glob)
  [1]
  $ dd if=/dev/urandom of=${DEV}p2 bs=1k seek=1 count=1 status=none
  dd: error writing '/dev/rbd?p2': Operation not permitted (glob)
  [1]
  $ blkdiscard ${DEV}p2
  blkdiscard: /dev/rbd?p2: BLKDISCARD ioctl failed: Operation not permitted (glob)
  [1]
  $ blockdev --setrw ${DEV}p1
  .*BLKROSET: Permission denied (re)
  [1]
  $ sudo blockdev --setrw ${DEV}p1  # succeeds but effectively ignored
  $ blockdev --setrw ${DEV}p2
  .*BLKROSET: Permission denied (re)
  [1]
  $ sudo blockdev --setrw ${DEV}p2  # succeeds but effectively ignored
  $ blockdev --getro ${DEV}p1
  1
  $ blockdev --getro ${DEV}p2
  1
  $ dd if=/dev/urandom of=${DEV}p1 bs=1k seek=1 count=1 status=none
  dd: error writing '/dev/rbd?p1': Operation not permitted (glob)
  [1]
  $ blkdiscard ${DEV}p1
  blkdiscard: /dev/rbd?p1: BLKDISCARD ioctl failed: Operation not permitted (glob)
  [1]
  $ dd if=/dev/urandom of=${DEV}p2 bs=1k seek=1 count=1 status=none
  dd: error writing '/dev/rbd?p2': Operation not permitted (glob)
  [1]
  $ blkdiscard ${DEV}p2
  blkdiscard: /dev/rbd?p2: BLKDISCARD ioctl failed: Operation not permitted (glob)
  [1]
  $ sudo rbd unmap $DEV


Image snapshot
==============

Unpartitioned:

  $ DEV=$(sudo rbd map img@snap)
  $ blockdev --getro $DEV
  1
  $ dd if=/dev/urandom of=$DEV bs=1k seek=1 count=1 status=none
  dd: error writing '/dev/rbd?': Operation not permitted (glob)
  [1]
  $ blkdiscard $DEV
  blkdiscard: /dev/rbd?: BLKDISCARD ioctl failed: Operation not permitted (glob)
  [1]
  $ blockdev --setrw $DEV
  .*BLKROSET: Permission denied (re)
  [1]
  $ sudo blockdev --setrw $DEV  # succeeds but effectively ignored
  $ blockdev --getro $DEV
  1
  $ dd if=/dev/urandom of=$DEV bs=1k seek=1 count=1 status=none
  dd: error writing '/dev/rbd?': Operation not permitted (glob)
  [1]
  $ blkdiscard $DEV
  blkdiscard: /dev/rbd?: BLKDISCARD ioctl failed: Operation not permitted (glob)
  [1]
  $ sudo rbd unmap $DEV

Partitioned:

  $ DEV=$(sudo rbd map imgpart@snap)
  $ udevadm settle
  $ blockdev --getro ${DEV}p1
  1
  $ blockdev --getro ${DEV}p2
  1
  $ dd if=/dev/urandom of=${DEV}p1 bs=1k seek=1 count=1 status=none
  dd: error writing '/dev/rbd?p1': Operation not permitted (glob)
  [1]
  $ blkdiscard ${DEV}p1
  blkdiscard: /dev/rbd?p1: BLKDISCARD ioctl failed: Operation not permitted (glob)
  [1]
  $ dd if=/dev/urandom of=${DEV}p2 bs=1k seek=1 count=1 status=none
  dd: error writing '/dev/rbd?p2': Operation not permitted (glob)
  [1]
  $ blkdiscard ${DEV}p2
  blkdiscard: /dev/rbd?p2: BLKDISCARD ioctl failed: Operation not permitted (glob)
  [1]
  $ blockdev --setrw ${DEV}p1
  .*BLKROSET: Permission denied (re)
  [1]
  $ sudo blockdev --setrw ${DEV}p1  # succeeds but effectively ignored
  $ blockdev --setrw ${DEV}p2
  .*BLKROSET: Permission denied (re)
  [1]
  $ sudo blockdev --setrw ${DEV}p2  # succeeds but effectively ignored
  $ blockdev --getro ${DEV}p1
  1
  $ blockdev --getro ${DEV}p2
  1
  $ dd if=/dev/urandom of=${DEV}p1 bs=1k seek=1 count=1 status=none
  dd: error writing '/dev/rbd?p1': Operation not permitted (glob)
  [1]
  $ blkdiscard ${DEV}p1
  blkdiscard: /dev/rbd?p1: BLKDISCARD ioctl failed: Operation not permitted (glob)
  [1]
  $ dd if=/dev/urandom of=${DEV}p2 bs=1k seek=1 count=1 status=none
  dd: error writing '/dev/rbd?p2': Operation not permitted (glob)
  [1]
  $ blkdiscard ${DEV}p2
  blkdiscard: /dev/rbd?p2: BLKDISCARD ioctl failed: Operation not permitted (glob)
  [1]
  $ sudo rbd unmap $DEV


read-only OSD caps
==================

R/W:

  $ DEV=$(sudo rbd map --id ro --key $(echo $RO_KEY) img)
  rbd: sysfs write failed
  rbd: map failed: (1) Operation not permitted
  [1]

R/O:

  $ DEV=$(sudo rbd map --id ro --key $(echo $RO_KEY) --read-only img)
  $ blockdev --getro $DEV
  1
  $ sudo rbd unmap $DEV

Snapshot:

  $ DEV=$(sudo rbd map --id ro --key $(echo $RO_KEY) img@snap)
  $ blockdev --getro $DEV
  1
  $ sudo rbd unmap $DEV

R/W, clone:

  $ DEV=$(sudo rbd map --id ro --key $(echo $RO_KEY) cloneimg)
  rbd: sysfs write failed
  rbd: map failed: (1) Operation not permitted
  [1]

R/O, clone:

  $ DEV=$(sudo rbd map --id ro --key $(echo $RO_KEY) --read-only cloneimg)
  $ blockdev --getro $DEV
  1
  $ sudo rbd unmap $DEV


rw -> ro with open_count > 0
============================

  $ DEV=$(sudo rbd map img)
  $ { sleep 10; sudo blockdev --setro $DEV; } &
  $ dd if=/dev/urandom of=$DEV bs=1k oflag=direct status=noxfer
  dd: error writing '/dev/rbd?': Operation not permitted (glob)
  [1-9]\d*\+0 records in (re)
  [1-9]\d*\+0 records out (re)
  [1]
  $ sudo rbd unmap $DEV


"-o rw --read-only" should result in read-only mapping
======================================================

  $ DEV=$(sudo rbd map -o rw --read-only img)
  $ blockdev --getro $DEV
  1
  $ sudo rbd unmap $DEV


Teardown
========

  $ rbd snap purge imgpart >/dev/null 2>&1
  $ rbd rm imgpart >/dev/null 2>&1
  $ rbd rm cloneimg >/dev/null 2>&1
  $ rbd snap unprotect img@snap
  $ rbd snap purge img >/dev/null 2>&1
  $ rbd rm img >/dev/null 2>&1

