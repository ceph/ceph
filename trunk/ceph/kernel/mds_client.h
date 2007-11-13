#ifndef _FS_CEPH_MDS_CLIENT_H
#define _FS_CEPH_MDS_CLIENT_H

#include <linux/radix-tree.h>
#include <linux/list.h>
#include <linux/completion.h>
#include <linux/spinlock.h>

#include "messenger.h"
#include "mdsmap.h"

struct ceph_client;

/*
 * state associated with an individual MDS<->client session
 */
enum {
	CEPH_MDS_SESSION_IDLE,
	CEPH_MDS_SESSION_OPENING,
	CEPH_MDS_SESSION_OPEN,
	CEPH_MDS_SESSION_CLOSING
};
struct ceph_mds_session {
	int s_state;
	__u64 s_cap_seq;    /* cap message count from mds */
	atomic_t s_ref;
	struct completion s_completion;
};

struct ceph_mds_request {
	__u64 r_tid;
	struct ceph_message *r_request;
	struct ceph_message *r_reply;
	
	__u32 r_mds[4];      /* set of mds's with whom request may be outstanding */
        int r_num_mds;       /* items in r_mds */
	
	int r_attempts;
	int r_num_fwd;       /* number of forward attempts */
        int r_resend_mds;    /* mds to resend to next, if any*/

	atomic_t r_ref;
	struct completion r_completion;
};


struct ceph_mds_client {
	spinlock_t lock;

	struct ceph_client *client;
	struct ceph_mdsmap *mdsmap;  /* mds map */

	/* mds sessions */
	struct ceph_mds_session **sessions;  /* NULL if no session */
	int max_sessions;            /* size of s_mds_sessions array */

	__u64 last_tid;              /* id of last mds request */
	struct radix_tree_root request_tree;  /* pending mds requests */

	__u64 last_requested_map;
	struct completion map_waiters;
};

extern void ceph_mdsc_init(struct ceph_mds_client *mdsc,
			   struct ceph_client *client);
extern void ceph_mdsc_submit_request(struct ceph_mds_client *mdsc, struct ceph_message *msg, int mds);
extern void ceph_mdsc_handle_reply(struct ceph_mds_client *mdsc, struct ceph_message *msg);
extern void ceph_mdsc_handle_forward(struct ceph_mds_client *mdsc, struct ceph_message *msg);
extern void ceph_mdsc_handle_map(struct ceph_mds_client *mdsc, struct ceph_message *msg);

#endif
