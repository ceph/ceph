#ifndef _FS_CEPH_CEPH_H
#define _FS_CEPH_CEPH_H

/* #include <linux/ceph_fs.h> */

/*
 * CEPH file system in-core superblock info
 * taken from OSDSuperblock from osd_types.h ??  check with sage
 */
struct ceph_sb_info {
	uint64_t magic;
	uint64_t fsid;
	int32_t  whoami;
	uint32_t current_epoch;
	uint32_t oldest_map;
	uint32_t newest_map;
};

/*
 * CEPH file system in-core inode info
 */
struct ceph_inode_info {
	unsigned long val;  /* inode from types.h is uint64_t */
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
