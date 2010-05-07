#ifndef FS_CEPH_IOCTL_H
#define FS_CEPH_IOCTL_H

#include <linux/ioctl.h>
#include <linux/types.h>

#define CEPH_IOCTL_MAGIC 0x97

/* just use u64 to align sanely on all archs */
struct ceph_ioctl_layout {
	uint64_t stripe_unit, stripe_count, object_size;
	uint64_t data_pool;
	int64_t preferred_osd;
};

#define CEPH_IOC_GET_LAYOUT _IOR(CEPH_IOCTL_MAGIC, 1,		\
				   struct ceph_ioctl_layout)
#define CEPH_IOC_SET_LAYOUT _IOW(CEPH_IOCTL_MAGIC, 2,		\
				   struct ceph_ioctl_layout)

/*
 * Extract identity, address of the OSD and object storing a given
 * file offset.
 */
struct ceph_ioctl_dataloc {
	uint64_t file_offset;           /* in+out: file offset */
	uint64_t object_offset;         /* out: offset in object */
	uint64_t object_no;             /* out: object # */
	uint64_t object_size;           /* out: object size */
	char object_name[64];        /* out: object name */
	uint64_t block_offset;          /* out: offset in block */
	uint64_t block_size;            /* out: block length */
	int64_t osd;                   /* out: osd # */
	struct sockaddr_storage osd_addr; /* out: osd address */
};

#define CEPH_IOC_GET_DATALOC _IOWR(CEPH_IOCTL_MAGIC, 3,	\
				   struct ceph_ioctl_dataloc)

#endif
