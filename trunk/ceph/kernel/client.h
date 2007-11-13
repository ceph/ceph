#ifndef __FS_CEPH_CLIENT_H
#define __FS_CEPH_CLIENT_H

/*
 * client.h
 *
 * ceph client instance.  may be shared by multiple superblocks,
 * if we are mounting the same cluster multiple times (e.g. at 
 * different relative server paths)
 */

#include <include/ceph_fs.h>

#include "messenger.h"
#include "monmap.h"

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
	atomic_t nref;

	atomic_t mounting;
	struct wait_queue mount_wq;

	struct ceph_messenger *msgr;   /* messenger instance */
	struct ceph_mon_client monc;
	struct ceph_mds_client mdss;
	struct ceph_osd_client osdc;

	/* lets ignore all this until later */
	spinlock_t sb_lock;
	int num_sb;      /* reference count (for each sb_info that points to me) */
	struct list_head sb_list;
};

extern struct ceph_client *ceph_get_client(ceph_mount_args *args);
extern void ceph_put_client(struct ceph_client *cl);

#endif
