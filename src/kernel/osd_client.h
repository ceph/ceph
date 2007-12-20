#ifndef _FS_CEPH_OSD_CLIENT_H
#define _FS_CEPH_OSD_CLIENT_H

/* this will be equivalent to osdc/Objecter.h */

#include <linux/ceph_fs.h>
#include <linux/radix-tree.h>
#include <linux/completion.h>

#include "osdmap.h"

struct ceph_msg;

enum {
	REQUEST_ACK, REQUEST_SAFE
};

struct ceph_osd_request {
	__u64             r_tid;
	ceph_pg_t         r_pgid;
	int               r_flags;
	struct ceph_msg  *r_request;
	struct ceph_msg  *r_reply;
	atomic_t          r_ref;
	struct completion r_completion;
};

struct ceph_osd_client {
	spinlock_t             lock;
	struct ceph_osdmap     *osdmap;       /* current map */
	__u64                  last_tid;      /* tid of last request */
	struct radix_tree_root request_tree;  /* pending requests, by tid */
	__u64                  last_requested_map;
	struct completion      map_waiters;
};

extern void ceph_osdc_init(struct ceph_osd_client *osdc);
extern void ceph_osdc_handle_reply(struct ceph_osd_client *osdc, struct ceph_msg *msg);
extern void ceph_osdc_handle_map(struct ceph_osd_client *osdc, struct ceph_msg *msg);

#endif

