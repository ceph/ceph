#ifndef __FS_CEPH_KMSG_H
#define __FS_CEPH_KMSG_H

#include <linux/uio.h>
#include <linux/net.h>
#include <linux/radix-tree.h>
#include <linux/workqueue.h>
#include <linux/ceph_fs.h>
#include "bufferlist.h"

/* TBD:  this will be filled into ceph_kmsgr.athread during mount */
extern struct task_struct *athread;

struct ceph_kmsgr {
	void *m_parent;
	struct task_struct *athread;
	struct radix_tree_root mpipes;		/* other nodes talk to */
};

struct ceph_message {
	atomic_t nref;
	int mflags;
	struct ceph_message_header *msghdr;	/* header */
	__u32 chunklens[2];
	struct ceph_bufferlist payload;
	struct list_head m_list_head;
};

/* current state of connection, probably won't need all these.. */
enum ceph_con_state {
	ACCEPTING,
	CONNECTING,
	OPEN,

	READ_PENDING,
	READING,
	READ_DONE,
	SEND_PENDING,
	/*SENDING,*/
	SEND_DONE,
	CONNECTING,
	CONNECT_RETRY,
	CONNECTED,
	CONNECT_FAIL,
	CONNECT_KEEPALIVE,
	DISPATCH_READY,
	DISPATCH_DONE,
	CLOSE_PENDING,
	CLOSING,
	CLOSED
};

struct ceph_connection {
	struct socket *sock;	/* connection socket */
	/* TDB: may need a mutex here depending if */
	spinlock_t con_lock;

	enum ceph_con_state state;
	__u32 connect_seq;     
	__u32 out_seq;		     /* last message queued for send */
	__u32 in_seq, in_seq_acked;  /* last message received, acked */

	
	/* out queue */
/* note: need to adjust queues because we have a work queue for the message */ 
	spinlock_t out_queue_lock;
	struct list_head out_queue;
	struct ceph_bufferlist out_partial;
	struct ceph_bufferlist_iterator out_pos;
	struct list_head out_sent;   /* sending/sent but unacked; resend if connection drops */

	/* partially read message contents */
	char in_tag;  /* ack or msg */
	__u32 in_partial_ack;  
	int in_base_pos;   /* for ack seq, or msg header */
	struct ceph_message *in_partial;
	struct ceph_bufferlist_iterator in_pos;  /* for msg payload */


	struct work_struct rwork;		/* received work */
	struct work_struct swork;		/* send work */
	int retries;
};

/* 
 * function prototypes
 */

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
