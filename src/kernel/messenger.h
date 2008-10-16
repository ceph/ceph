#ifndef __FS_CEPH_MESSENGER_H
#define __FS_CEPH_MESSENGER_H

#include <linux/kobject.h>
#include <linux/mutex.h>
#include <linux/net.h>
#include <linux/radix-tree.h>
#include <linux/uio.h>
#include <linux/version.h>
#include <linux/workqueue.h>

#include "ceph_fs.h"

struct ceph_msg;

extern struct workqueue_struct *ceph_msgr_wq;       /* receive work queue */

typedef void (*ceph_msgr_dispatch_t) (void *p, struct ceph_msg *m);
typedef void (*ceph_msgr_peer_reset_t) (void *p, struct ceph_entity_addr *addr,
					struct ceph_entity_name *pn);
typedef int (*ceph_msgr_prepare_pages_t) (void *p, struct ceph_msg *m,
					  int want);

static __inline__ const char *ceph_name_type_str(int t) {
	switch (t) {
	case CEPH_ENTITY_TYPE_MON: return "mon";
	case CEPH_ENTITY_TYPE_MDS: return "mds";
	case CEPH_ENTITY_TYPE_OSD: return "osd";
	case CEPH_ENTITY_TYPE_CLIENT: return "client";
	case CEPH_ENTITY_TYPE_ADMIN: return "admin";
	default: return "???";
	}
}

#define CEPH_MSGR_BACKUP 10  /* backlogged incoming connections */

/* use format string %s%d */
#define ENTITY_NAME(n)				   \
	ceph_name_type_str(le32_to_cpu((n).type)), \
		le32_to_cpu((n).num)

struct ceph_messenger {
	void *parent;
	ceph_msgr_dispatch_t dispatch;
	ceph_msgr_peer_reset_t peer_reset;
	ceph_msgr_prepare_pages_t prepare_pages;
	struct ceph_entity_inst inst;    /* my name+address */
	struct socket *listen_sock; 	 /* listening socket */
	struct work_struct awork;	 /* accept work */
	spinlock_t con_lock;
	struct list_head con_all;        /* all connections */
	struct list_head con_accepting;  /* accepting */
	struct radix_tree_root con_tree; /*  established */
	struct page *zero_page;
	u32 global_seq;
	spinlock_t global_seq_lock;
};

struct ceph_msg {
	struct ceph_msg_header hdr;	/* header */
	struct ceph_msg_footer footer;	/* footer */
	struct kvec front;              /* first bit of message */
	struct mutex page_mutex;
	struct page **pages;            /* data payload.  NOT OWNER. */
	unsigned nr_pages;              /* size of page array */
	struct list_head list_head;
	atomic_t nref;
};

struct ceph_msg_pos {
	int page, page_pos;        /* which page; -3=tag, -2=hdr, -1=front */
	int data_pos;
	int did_page_crc;
};

/* ceph connection fault delay defaults */
#define BASE_DELAY_INTERVAL	(HZ/2)
#define MAX_DELAY_INTERVAL	(5 * 60 * HZ)

/* ceph_connection state bit flags */
#define LOSSYTX         0 /* close channel on errors */
#define LOSSYRX         1 /* close channel on errors */
#define CONNECTING	2
#define ACCEPTING	3
#define WRITE_PENDING	4  /* we have data to send */
#define QUEUED          5  /* there is work to be done */
#define BUSY            6  /* work is being done */
#define BACKOFF         7  /* backing off; will retry */
#define STANDBY		8  /* standby, when socket state close, no messages */
#define WAIT		9  /* wait for peer to connect */
#define CLOSED		10  /* we've closed the connection */
#define SOCK_CLOSED	11 /* socket state changed to closed */
#define REGISTERED      12


struct ceph_connection {
	struct ceph_messenger *msgr;
	struct socket *sock;	/* connection socket */
	unsigned long state;	/* connection state */
	const char *error_msg;

	atomic_t nref;

	struct list_head list_all;   /* msgr->con_all */
	struct list_head list_bucket;  /* msgr->con_tree or con_accepting */

	struct ceph_entity_addr peer_addr; /* peer address */
	struct ceph_entity_name peer_name; /* peer name */
	__u32 connect_seq, global_seq;
	bool lossy_rx;                     /* true if sender is lossy */

	/* out queue */
	spinlock_t out_queue_lock;   /* protects out_queue, out_sent, out_seq */
	struct list_head out_queue;
	struct list_head out_sent;   /* sending/sent but unacked */

	__u32 out_seq;		     /* last message queued for send */
	__u32 in_seq, in_seq_acked;  /* last message received, acked */

	/* negotiation temps */
	char in_banner[CEPH_BANNER_MAX_LEN];
	struct ceph_msg_connect out_connect, in_connect;
	struct ceph_entity_addr actual_peer_addr;

	/* out */
	struct ceph_msg *out_msg;
	struct ceph_msg_pos out_msg_pos;
	__le32 out32;
	struct kvec out_kvec[6],
		*out_kvec_cur;
	int out_kvec_left;   /* kvec's left */
	int out_kvec_bytes;  /* bytes left */
	int out_more;        /* there is more data after this kvec */

	/* partially read message contents */
	char in_tag;
	u8 in_flags;
	int in_base_pos;   /* for ack seq, or msg headers, or handshake */
	__le32 in_partial_ack;
	struct ceph_msg *in_msg;
	struct ceph_msg_pos in_msg_pos;
	u32 in_front_crc, in_data_crc;

	struct delayed_work work;	    /* send|recv work */
	unsigned long       delay;          /* delay interval */
};

extern int ceph_msgr_init(void);
extern void ceph_msgr_exit(void);

extern struct ceph_messenger *
ceph_messenger_create(struct ceph_entity_addr *myaddr);
extern void ceph_messenger_destroy(struct ceph_messenger *);
extern void ceph_messenger_mark_down(struct ceph_messenger *msgr,
				     struct ceph_entity_addr *addr);

extern void ceph_queue_con(struct ceph_connection *con);

extern struct ceph_msg *ceph_msg_new(int type, int front_len,
				     int page_len, int page_off,
				     struct page **pages);

static __inline__ void ceph_msg_get(struct ceph_msg *msg) {
	/*printk("ceph_msg_get %p %d -> %d\n", msg, atomic_read(&msg->nref),
	  atomic_read(&msg->nref)+1);*/
	atomic_inc(&msg->nref);
}

extern void ceph_msg_put(struct ceph_msg *msg);

static inline void ceph_msg_put_list(struct list_head *head)
{
	while (!list_empty(head)) {
		struct ceph_msg *msg = list_first_entry(head, struct ceph_msg,
							list_head);
		list_del_init(&msg->list_head);
		ceph_msg_put(msg);
	}
}

extern struct ceph_msg *ceph_msg_maybe_dup(struct ceph_msg *msg);

extern int ceph_msg_send(struct ceph_messenger *msgr, struct ceph_msg *msg,
			 unsigned long timeout);

extern void ceph_ping(struct ceph_messenger *msgr, struct ceph_entity_name name,
		      struct ceph_entity_addr *addr);

#endif
