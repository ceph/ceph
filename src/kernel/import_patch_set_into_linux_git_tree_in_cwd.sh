#!/bin/sh

# run me from the root of a _linux_ git tree, and pass ceph tree root.
cephtree=$1
echo ceph tree at $cephtree.
test -e include/linux/mm.h || exit 0
test -e $cephtree/src/kernel/super.h || exit 0

# copy into the tree
mkdir fs/ceph
cp $cephtree/src/kernel/*.[ch] fs/ceph
cp $cephtree/src/kernel/Makefile fs/ceph
git apply $cephtree/src/kernel/kbuild.patch

# build the patch sequence
git add fs/ceph/ceph_fs.h
git commit -m 'ceph: on-wire types'

git add fs/ceph/types.h
git add fs/ceph/super.h
git commit -m 'ceph: client types'

git add fs/ceph/super.c
git commit -m 'ceph: super.c'

git add fs/ceph/inode.c
git commit -m 'ceph: inode operations'

git add fs/ceph/dir.c
git commit -m 'ceph: directory operations'

git add fs/ceph/file.c
git commit -m 'ceph: file operations'

git add fs/ceph/addr.c
git commit -m 'ceph: address space operations'

git add fs/ceph/mds_client.h
git add fs/ceph/mds_client.c
git add fs/ceph/mdsmap.h
git add fs/ceph/mdsmap.c
git commit -m 'ceph: MDS client'

git add fs/ceph/osd_client.h
git add fs/ceph/osd_client.c
git add fs/ceph/osdmap.h
git add fs/ceph/osdmap.c
git commit -m 'ceph: OSD client'

git add fs/ceph/crush/crush.h
git add fs/ceph/crush/crush.c
git add fs/ceph/crush/mapper.h
git add fs/ceph/crush/mapper.c
git add fs/ceph/crush/hash.h
git commit -m 'ceph: CRUSH mapping algorithm'

git add fs/ceph/mon_client.h
git add fs/ceph/mon_client.c
git commit -m 'ceph: monitor client'

git add fs/ceph/caps.c
git commit -m 'ceph: capability management'

git add fs/ceph/snap.c
git commit -m 'ceph: snapshot management'

git add fs/ceph/decode.h
git add fs/ceph/messenger.h
git add fs/ceph/messenger.c
git commit -m 'ceph: messenger library'

git add fs/ceph/export.c
git commit -m 'ceph: nfs re-export support'

git add fs/ceph/ioctl.h
git add fs/ceph/ioctl.c
git commit -m 'ceph: ioctls'

git add fs/ceph/ceph_debug.h
git add fs/ceph/proc.c
git add fs/ceph/sysfs.c
git add fs/ceph/debugfs.c
git commit -m 'ceph: debugging'

git add fs/Kconfig
git add fs/Makefile
git commit -m 'ceph: Kconfig, Makefile'
