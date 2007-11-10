/*
 * client.h
 *
 * ceph client instance.  may be shared by multiple supers (mount points),
 * if we are mounting the same cluster multiple times (e.g. at different
 * offsets)
 */

#include <include/ceph_fs.h>

#include "messenger.h"
#include "monmap.h"

#include "mon_client.h"
#include "mds_client.h"
#include "osd_client.h"

struct ceph_mount_args;

/* 
 * CEPH per-filesystem client state
 * 
 * possibly shared by multiple mount points, if they are 
 * mounting the same ceph filesystem/cluster.
 */
struct ceph_client {
	__u32 s_whoami;                 /* my client number */
	struct ceph_messenger  *msgr;   /* messenger instance */

	struct ceph_monmap *monmap;  /* monitor map */

	struct ceph_mon_client mon_client;
	struct ceph_mds_client mds_client;
	struct ceph_osd_client osd_client;

	int s_ref;    /* reference count (for each sb_info that points to me) */
	struct list_head fsid_item;
};

extern struct ceph_client *ceph_get_client(ceph_mount_args *args);
extern void ceph_put_client(struct ceph_client *cl);
