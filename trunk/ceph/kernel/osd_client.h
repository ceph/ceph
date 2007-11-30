#ifndef _FS_CEPH_OSD_CLIENT_H
#define _FS_CEPH_OSD_CLIENT_H

/* this will be equivalent to osdc/Objecter.h */

#include <linux/ceph_fs.h>
#include <linux/radix-tree.h>
#include <linux/completion.h>

struct ceph_msg;

struct ceph_osdmap {
	struct ceph_fsid fsid;
	ceph_epoch_t epoch;
	ceph_epoch_t mon_epoch;
	struct ceph_timeval ctime, mtime;
	
	__u32 pg_num, pg_num_mask;
	__u32 localized_pg_num, localized_pg_num_mask;
	
	__u32 max_osd;
	__u8 *osd_state;
	__u32 *osd_offload;  /* 0 = normal, 0x10000 = 100% offload (failed) */
	struct ceph_entity_addr *osd_addr;
	struct crush_map *crush;

	__u32 num_pg_swap_primary;
	struct {
		ceph_pg_t pg;
		__u32 osd;
	} *pg_swap_primary;
};

enum {
	REQUEST_ACK, REQUEST_SAFE
};

struct ceph_osd_request {
	__u64 r_tid;
	ceph_pg_t r_pgid;
	int r_flags;

	atomic_t r_ref;
	struct ceph_msg *r_request;
	struct completion r_completion;
};

struct ceph_osd_client {
	struct ceph_osdmap *osdmap;  /* current osd map */

	__u64 last_tid;              /* id of last mds request */
	struct radix_tree_root request_tree;  /* pending mds requests */

	__u64 last_requested_map;
	struct completion map_waiters;
};

extern void ceph_osdc_init(struct ceph_osd_client *osdc);
extern void ceph_osdc_handle_reply(struct ceph_osd_client *osdc, struct ceph_msg *msg);
extern void ceph_osdc_handle_map(struct ceph_osd_client *osdc, struct ceph_msg *msg);

#endif

