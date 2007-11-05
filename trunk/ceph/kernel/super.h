#ifndef _FS_CEPH_CEPH_H
#define _FS_CEPH_CEPH_H

/* #include <linux/ceph_fs.h> */

#include "kmsg.h"
#include "monmap.h"
#include "mds_client.h"
#include "osd_client.h"



/* 
 * CEPH per-filesystem client state
 * 
 * possibly shared by multiple mount points, if they are 
 * mounting the same ceph filesystem/cluster.
 */
struct ceph_client {
	__u64 s_fsid;  /* hmm this should be part of the monmap? */

	__u32 s_whoami;                /* my client number */
	struct ceph_kmsgr  *s_kmsgr;   /* messenger instance */

	struct ceph_monmap *s_monmap;  /* monitor map */

	struct ceph_mds_client *s_mds_client;
	struct ceph_osd_client *s_osd_client;

	int s_ref;    /* reference count (for each sb_info that points to me) */
};

/*
 * directory of filesystems mounted by this host
 *
 *   key: fsid?  ipquad of monitor?  hmm!
 * value: struct ceph_fs_client*
 */
extern struct radix_tree ceph_fs_clients; 


/*
 * CEPH per-mount superblock info
 */
struct ceph_sb_info {
	struct ceph_client *sb_client;
	
	/* FIXME: add my relative offset into the filesystem,
	   so we can appropriately mangle/adjust path names in requests, etc. */
};

/*
 * CEPH file system in-core inode info
 */
struct ceph_inode_info {
	struct ceph_file_layout i_layout;
	int i_dir_auth;
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
