#ifndef _FS_CEPH_MON_CLIENT_H
#define _FS_CEPH_MON_CLIENT_H

#include "messenger.h"
#include <linux/completion.h>
#include <linux/radix-tree.h>

struct ceph_client;
struct ceph_mount_args;

struct ceph_monmap {
	ceph_epoch_t epoch;
	struct ceph_fsid fsid;
	__u32 num_mon;
	struct ceph_entity_inst mon_inst[0];
};

struct ceph_mon_statfs_request {
	u64 tid;
	struct ceph_statfs *buf;
	struct completion completion;
	u64 last_attempt; /* jiffies */
};

struct ceph_mon_client {
	struct ceph_client *client;
	int last_mon;  /* last monitor i contacted */
	struct ceph_monmap *monmap;

	spinlock_t lock;
	struct radix_tree_root statfs_request_tree;  /* statfs requests */
	u64 last_tid;

	u32 want_mdsmap;  /* protected by caller's lock */
	struct delayed_work delayed_work;  /* delayed work */
	unsigned long delay;

	struct ceph_msg *msg;
};

extern struct ceph_monmap *ceph_monmap_decode(void *p, void *end);
extern int ceph_monmap_contains(struct ceph_monmap *m, struct ceph_entity_addr *addr);

extern int ceph_monc_init(struct ceph_mon_client *monc, struct ceph_client *cl);

extern int ceph_monc_request_mdsmap(struct ceph_mon_client *monc, __u32 have);
extern int ceph_monc_got_mdsmap(struct ceph_mon_client *monc, __u32 have);


extern void ceph_monc_request_osdmap(struct ceph_mon_client *monc, __u64 have);
extern void ceph_monc_request_umount(struct ceph_mon_client *monc);
extern void ceph_monc_report_failure(struct ceph_mon_client *monc, struct ceph_entity_inst *who);

extern int ceph_monc_do_statfs(struct ceph_mon_client *monc, struct ceph_statfs *buf);
extern void ceph_monc_handle_statfs_reply(struct ceph_mon_client *monc, struct ceph_msg *msg);

#endif
