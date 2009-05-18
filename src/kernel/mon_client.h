#ifndef _FS_CEPH_MON_CLIENT_H
#define _FS_CEPH_MON_CLIENT_H

#include "messenger.h"
#include <linux/completion.h>
#include <linux/radix-tree.h>

/*
 * A small cluster of Ceph "monitors" are responsible for managing critical
 * cluster configuration and state information.  An odd number (e.g., 3, 5)
 * of cmon daemons use a modified version of the Paxos part-time parliament
 * algorithm to manage the MDS map (mds cluster membership), OSD map, and
 * list of clients who have mounted the file system.
 *
 * Communication with the monitor cluster is lossy, so requests for
 * information may have to be resent if we time out waiting for a response.
 * As long as we do not time out, we continue to send all requests to the
 * same monitor.  If there is a problem, we randomly pick a new monitor from
 * the cluster to try.
 */

struct ceph_client;
struct ceph_mount_args;

/*
 * The monitor map enumerates the set of all monitors.
 *
 * Make sure this structure size matches the encoded map size, or change
 * ceph_monmap_decode().
 */
struct ceph_monmap {
	ceph_fsid_t fsid;
	u32 epoch;
	u32 num_mon;
	struct ceph_entity_inst mon_inst[0];
};

struct ceph_mon_client;
struct ceph_mon_statfs_request;

struct ceph_mon_client_attr {
	struct attribute attr;
	ssize_t (*show)(struct ceph_mon_client *, struct ceph_mon_client_attr *,
			char *);
	ssize_t (*store)(struct ceph_mon_client *,
			 struct ceph_mon_client_attr *,
			 const char *, size_t);
};

struct ceph_mon_statfs_request_attr {
	struct attribute attr;
	ssize_t (*show)(struct ceph_mon_statfs_request *,
			struct ceph_mon_statfs_request_attr *,
			char *);
	ssize_t (*store)(struct ceph_mon_statfs_request *,
			 struct ceph_mon_statfs_request_attr *,
			const char *, size_t);
	struct ceph_entity_inst dst;
};

/*
 * Generic mechanism for resending monitor requests.
 */
typedef void (*ceph_monc_request_func_t)(struct ceph_mon_client *monc,
					 int newmon);
struct ceph_mon_request {
	struct ceph_mon_client *monc;
	struct delayed_work delayed_work;
	unsigned long delay;
	ceph_monc_request_func_t do_request;
};

/* statfs() is done a bit differently */
struct ceph_mon_statfs_request {
	u64 tid;
	struct kobject kobj;
	struct ceph_mon_statfs_request_attr k_op, k_mon;
	int result;
	struct ceph_statfs *buf;
	struct completion completion;
	unsigned long last_attempt, delay; /* jiffies */
	struct ceph_msg  *request;  /* original request */
};

struct ceph_mon_client {
	struct ceph_client *client;
	int last_mon;                       /* last monitor i contacted */
	struct ceph_monmap *monmap;

	/* pending statfs requests */
	struct mutex statfs_mutex;
	struct radix_tree_root statfs_request_tree;
	int num_statfs_requests;
	u64 last_tid;
	struct delayed_work statfs_delayed_work;

	/* mds/osd map or umount requests */
	struct mutex req_mutex;
	struct ceph_mon_request mdsreq, osdreq, umountreq;
	u32 want_mdsmap;
	u32 want_osdmap;

	struct dentry *debugfs_file;
};

extern struct ceph_monmap *ceph_monmap_decode(void *p, void *end);
extern int ceph_monmap_contains(struct ceph_monmap *m,
				struct ceph_entity_addr *addr);

extern int ceph_monc_init(struct ceph_mon_client *monc, struct ceph_client *cl);
extern void ceph_monc_stop(struct ceph_mon_client *monc);

/*
 * The model here is to indicate that we need a new map of at least epoch
 * @want, and to indicate which maps receive.  Periodically rerequest the map
 * from the monitor cluster until we get what we want.
 */
extern void ceph_monc_request_mdsmap(struct ceph_mon_client *monc, u32 want);
extern int ceph_monc_got_mdsmap(struct ceph_mon_client *monc, u32 have);

extern void ceph_monc_request_osdmap(struct ceph_mon_client *monc, u32 want);
extern int ceph_monc_got_osdmap(struct ceph_mon_client *monc, u32 have);

extern void ceph_monc_request_umount(struct ceph_mon_client *monc);

extern int ceph_monc_do_statfs(struct ceph_mon_client *monc,
			       struct ceph_statfs *buf);
extern void ceph_monc_handle_statfs_reply(struct ceph_mon_client *monc,
					  struct ceph_msg *msg);

extern void ceph_monc_request_umount(struct ceph_mon_client *monc);
extern void ceph_monc_handle_umount(struct ceph_mon_client *monc,
				    struct ceph_msg *msg);

#endif
