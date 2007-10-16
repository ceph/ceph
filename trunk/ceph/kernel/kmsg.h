#ifndef __CEPH_KMSG_H
#define __CEPH_KMSG_H

#include <linux/uio.h>
#include <include/ceph_fs.h>
#include "ceph_kthread.h"

/* 
 * function prototypes
 */
void ceph_read_message(ceph_message *message );
void ceph_write_message(ceph_message *message );
void ceph_client_dispatch(void *fs_client, struct ceph_message *message );
void queue_message(ceph_message *);

struct ceph_kthreadpool *msg_threadpool;  	/* thread pool */

struct ceph_kmsgr {
	void *m_parent;
	struct radix_tree mpipes;		/* other nodes i talk to */
	struct client_thread_info client_thread;	/* listener thread info */
};

struct ceph_message {
	struct ceph_message_header *msghdr;	/* header */
	struct kvec *m_iov;			/* data storage */
	size_t m_iovlen;	/* is this kvec.iov_len why need it in kvec? */
	struct list_head m_list_head;
};

struct ceph_kmsg_pipe {
	int p_sd;         /* socket descriptor */
	__u64 p_out_seq;  /* last message sent */
	__u64 p_in_seq;   /* last message received */

	/* out queue */
	struct list_head p_out_queue;
	struct ceph_message *p_out_partial;  /* partially sent message */
	int p_out_partial_pos;
	struct list_head p_out_sent;  /* sent but unacked; may need resend if connection drops */

	/* partially read message contents */
	struct kvec *p_in_partial_iov;   /* hrm, this probably isn't what we want */
	size_t p_in_partial_iovlen;
	size_t p_in_parital_iovmax;  /* size of currently allocated m_iov array */
	/* .. or something like that? .. */

};

struct dispatch_queue {
};
