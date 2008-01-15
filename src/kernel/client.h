#ifndef __FS_CEPH_CLIENT_H
#define __FS_CEPH_CLIENT_H

/*
 * client.h
 *
 * ceph client instance.  may be shared by multiple superblocks,
 * if we are mounting the same cluster multiple times (e.g. at 
 * different relative server paths)
 */

#include <linux/ceph_fs.h>
#include <linux/wait.h>
#include <linux/completion.h>

#include "messenger.h"

#include "mon_client.h"
#include "mds_client.h"
#include "osd_client.h"

struct ceph_mount_args;

enum {
	MOUNTING,
	MOUNTED,
	UNMOUNTING,
	UNMOUNTED
};

/* 
 * per-filesystem client state
 * 
 * possibly shared by multiple mount points, if they are 
 * mounting the same ceph filesystem/cluster.
 */
struct ceph_client {
	__u32 whoami;                   /* my client number */
	struct ceph_fsid fsid;
	atomic_t nref;

	struct super_block *sb;

	unsigned long mounting;   /* map bitset; 4=mon, 2=mds, 1=osd map */
	struct completion mount_completion;

	struct ceph_messenger *msgr;   /* messenger instance */
	struct ceph_mon_client monc;
	struct ceph_mds_client mdsc;
	struct ceph_osd_client osdc;

	/* lets ignore all this until later */
	spinlock_t sb_lock;
	int num_sb;      /* reference count (for each sb_info that points to me) */
	struct list_head sb_list;
};

extern struct ceph_client *ceph_create_client(struct ceph_mount_args *args, struct super_block *sb);
extern void ceph_put_client(struct ceph_client *cl);

#endif
