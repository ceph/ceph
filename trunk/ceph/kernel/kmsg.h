#ifndef __FS_CEPH_KMSG_H
#define __FS_CEPH_KMSG_H

#include <linux/uio.h>
#include <linux/radix-tree.h>
#include <linux/workqueue.h>
#include <linux/ceph_fs.h>
#include "ceph_kthread.h"

/* dispatch function type */
typedef void (*ceph_kmsg_work_dispatch_t)(struct work_struct *);

struct ceph_kmsgr {
	void *m_parent;
	struct radix_tree_root mpipes;		/* other nodes talk to */
	struct ceph_client_info cthread;	/* listener or select thread info */
	struct workqueue_struct *wq;		/* work queue (worker threads) */
	struct work_struct *work;		/* received work */
/* note: work->func = dispatch func */
};

struct ceph_message {
	struct ceph_message_header *msghdr;	/* header */
	struct ceph_bufferlist payload;
	struct list_head m_list_head;
	atomic_t nref;
};

struct ceph_kmsg_pipe {
	int p_sd;         /* socket descriptor */
	__u64 p_out_seq;  /* last message sent */
	__u64 p_in_seq;   /* last message received */

	/* out queue */
	struct list_head p_out_queue;
	struct ceph_message *p_out_partial;  /* partially sent message */
	struct ceph_bufferlist_iterator p_out_pos;
	struct list_head p_out_sent;  /* sent but unacked; may need resend if connection drops */

	/* partially read message contents */
	struct ceph_message *p_in_partial;
};

/* 
 * function prototypes
 */
extern void ceph_read_message(struct ceph_message *message);
extern void ceph_write_message(struct ceph_message *message);

__inline__ void ceph_put_msg(struct ceph_message *msg) {
	if (atomic_dec_and_test(&msg->nref)) {
		ceph_bufferlist_clear(msg->payload);
		kfree(msg);
	}
}

__inline__ void ceph_get_msg(struct ceph_message *msg) {
	atomic_inc(&msg->nref);
}

#endif
