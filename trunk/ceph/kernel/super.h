#ifndef _FS_CEPH_CEPH_H
#define _FS_CEPH_CEPH_H

/* #include <linux/ceph_fs.h> */

#include "kmsg.h"
#include "monmap.h"
#include "mds_client.h"
#include "osd_client.h"



/*
 * CEPH file system in-core superblock info
 */
struct ceph_sb_info {
	__u64 s_fsid;
	__u32 s_whoami;               /* client number */
	struct ceph_kmsg   *s_kmsg;    /* messenger instance */

	struct ceph_monmap *s_monmap;  /* monitor map */

	struct ceph_mds_client *s_mds_client;
	struct ceph_osd_client *s_osd_client;
	
};

/*
 * CEPH file system in-core inode info
 */
struct ceph_inode_info {
	struct ceph_file_layout i_layout;
	struct inode vfs_inode;
};

static inline struct ceph_inode_info *CEPH_I(struct inode *inode)
{
	return list_entry(inode, struct ceph_inode_info, vfs_inode);
}


/* file.c */
extern const struct inode_operations ceph_file_inops;
extern const struct file_operations ceph_file_operations;
extern const struct address_space_operations ceph_aops;

/* dir.c */
extern const struct inode_operations ceph_dir_inops;
extern const struct file_operations ceph_dir_operations;

#endif /* _FS_CEPH_CEPH_H */
