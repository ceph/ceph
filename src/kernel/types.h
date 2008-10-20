#ifndef _FS_CEPH_TYPES_H
#define _FS_CEPH_TYPES_H

/*
 * Identify inodes by both their ino and snapshot id (a u64).
 */
struct ceph_vino {
	u64 ino;
	u64 snap;
};

#endif
