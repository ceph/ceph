#ifndef _FS_CEPH_TYPES_H
#define _FS_CEPH_TYPES_H

struct ceph_vino {
	u64 ino;
	u64 snap;
};

#endif
