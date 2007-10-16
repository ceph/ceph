#ifndef _FS_CEPH_MDS_CLIENT_H
#define _FS_CEPH_MDS_CLIENT_H

#include <linux/radix-tree.h>
#include "kmsg.h"

/*
 * state associated with an individual MDS<->client session
 */
struct ceph_mds_session {
	__u64 s_push_seq;  
	/* wait queue? */
};

struct ceph_mds_request {
	__u64 r_tid;
	struct ceph_message *r_msg;
	__u8  r_idempotent;
	
	__u32 r_mds[4];        /* set of mds's with whom request may be outstanding */
	__u32 r_num_mds;       /* items in r_mds */
	
	__u32 r_num_fwd;       /* number of forward attempts */
        __s32 r_resend_mds;    /* mds to resend to next, if any*/
	
	/* waiter/callback? */	
};


struct ceph_mds_client {
	struct ceph_mdsmap *s_mdsmap;  /* mds map */

	/* mds sessions */
	struct ceph_mds_session **s_mds_sessions;     /* sparse array; elements NULL if no session */
	int s_max_mds_sessions;            /* size of s_mds_sessions array */

	__u64 s_last_mds_tid;              /* id of last mds request */
	struct radix_tree_root s_mds_requests;  /* in-flight mds requests */

};

#endif
