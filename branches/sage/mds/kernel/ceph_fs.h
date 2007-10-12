/* -*- mode:C++; tab-width:8; c-basic-offset:8; indent-tabs-mode:t -*- 
 * vim: ts=8 sw=8 smarttab
 */

#ifndef _FS_CEPH_CEPH_H
#define _FS_CEPH_CEPH_H

/* #include <linux/ceph_fs.h> */

#include "kmsg.h"

/* do these later
#include "osdmap.h"
#include "mdsmap.h"
#include "monmap.h"
*/
struct ceph_monmap;
struct ceph_osdmap;
struct ceph_mdsmap;



/*
 * state associated with an individual MDS<->client session
 */
struct ceph_mds_session {
	__u64 s_push_seq;  
	/* wait queue? */
};


/*
 * CEPH file system in-core superblock info
 */
struct ceph_sb_info {
	__u32  s_whoami;               /* client number */
	struct ceph_kmsg   *s_kmsg;    /* messenger instance */

	struct ceph_monmap *s_monmap;  /* monitor map */
	struct ceph_mdsmap *s_mdsmap;  /* mds map */
	struct ceph_osdmap *s_osdmap;  /* osd map */

	/* mds sessions */
	struct ceph_mds_session **s_mds_sessions;     /* sparse array; elements NULL if no session */
	int                      s_max_mds_sessions;  /* size of s_mds_sessions array */

	/* current requests */
	/* ... */
	__u64 last_tid;
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
