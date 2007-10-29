#ifndef __FS_CEPH_KMSG_H
#define __FS_CEPH_KMSG_H

#include <linux/uio.h>
#include <linux/radix-tree.h>
#include <linux/workqueue.h>
#include <linux/ceph_fs.h>
#include "accepter.h"
#include "bufferlist.h"

/* dispatch function type */
typedef void (*ceph_kmsg_work_dispatch_t)(struct work_struct *);

extern struct workqueue_struct *rwq;		/* receive work queue (worker threads) */
extern struct workqueue_struct *swq;		/* send work queue (worker threads) */

struct ceph_kmsgr {
	void *m_parent;
	struct radix_tree_root mpipes;		/* other nodes talk to */
	struct ceph_accepter accepter;		/* listener or select thread info */
};

struct ceph_message {
	atomic_t nref;
	int mflags;
	struct ceph_message_header *msghdr;	/* header */
	struct ceph_bufferlist *payload;
	struct list_head m_list_head;
};

struct ceph_connection {
	struct socket sock;	/* connection socket */
	__u64 out_seq;		/* last message sent */
	__u64 in_seq;		/* last message received */

	/* out queue */
	struct list_head out_queue;
	spinlock_t out_queue_lock;
	struct ceph_message *out_partial;	/* partially sent message */
	struct ceph_bufferlist_iterator out_pos;
	struct list_head out_sent;  /* sent but unacked; may need resend if connection drops */

	/* partially read message contents */
	struct ceph_message *in_partial;
	struct work_struct *rwork;		/* received work */
	struct work_struct *swork;		/* send work */
/* note: work->func = dispatch func */
	int retries;
};

/* 
 * function prototypes
 */
extern struct ceph_message *ceph_read_message(void);
extern int ceph_send_message(struct ceph_message *message);

static __inline__ void ceph_put_msg(struct ceph_message *msg) {
	if (atomic_dec_and_test(&msg->nref)) {
		ceph_bl_clear(msg->payload);
		kfree(msg);
	}
}

static __inline__ void ceph_get_msg(struct ceph_message *msg) {
	atomic_inc(&msg->nref);
}

#endif
