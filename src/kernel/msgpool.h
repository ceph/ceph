#ifndef _FS_CEPH_MSGPOOL
#define _FS_CEPH_MSGPOOL

#include "messenger.h"

/*
 * we use memory pools for preallocating messages we may receive, to
 * avoid unexpected OOM conditions.
 */
struct ceph_msg_pool {
	spinlock_t lock;
	int front_len;          /* preallocated payload size */
	struct list_head msgs;  /* msgs in the pool; each has 1 ref */
	int num, min;           /* cur, min # msgs in the pool */
	bool blocking;
	wait_queue_head_t wait;
};

extern int ceph_msgpool_init(struct ceph_msg_pool *pool,
			     int front_len, int size, bool blocking);
extern void ceph_msgpool_destroy(struct ceph_msg_pool *pool);
extern int ceph_msgpool_resv(struct ceph_msg_pool *, int delta);
extern struct ceph_msg *ceph_msgpool_get(struct ceph_msg_pool *);
extern void ceph_msgpool_put(struct ceph_msg_pool *, struct ceph_msg *);

#endif
