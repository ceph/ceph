#ifndef __FS_CEPH_MESSENGER_H
#define __FS_CEPH_MESSENGER_H

#include <linux/uio.h>
#include <linux/net.h>
#include <linux/radix-tree.h>
#include <linux/workqueue.h>
#include <linux/ceph_fs.h>
#include <linux/version.h>

struct ceph_msg;

typedef void (*ceph_msgr_dispatch_t) (void *p, struct ceph_msg *m);
typedef void (*ceph_msgr_peer_reset_t) (void *c);
typedef int (*ceph_msgr_prepare_pages_t) (void *p, struct ceph_msg *m, int want);

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
	struct list_head con_accepting;  /*  doing handshake, or */
	struct radix_tree_root con_open; /*  established. see get_connection() */
};

struct ceph_msg {
	struct ceph_msg_header hdr;	/* header */
	struct kvec front;              /* first bit of message */
	struct page **pages;            /* data payload.  NOT OWNER. */
	unsigned nr_pages;              /* size of page array */
	struct list_head list_head;
	atomic_t nref;
};

struct ceph_msg_pos {
	int page, page_pos;        /* which page; -3=tag, -2=hdr, -1=front */
	int data_pos;
};

/* ceph connection fault delay defaults */
#define BASE_DELAY_INTERVAL	1	
#define MAX_DELAY_INTERVAL	(5U * 60 * HZ)

/* ceph_connection state bit flags */
#define NEW           	 0
#define CONNECTING       1
#define ACCEPTING        2
#define OPEN             3
#define WRITE_PENDING    4  /* we have data to send */
#define READABLE         5  /* set when socket gets new data */
#define READING          6  /* provides mutual exclusion, protecting in_* */
#define REJECTING        7
#define CLOSING          8
#define CLOSED           9
#define SOCK_CLOSE       10 /* socket state changed to close */
#define STANDBY          11 /* standby, when socket state close, no message queued */

struct ceph_connection {
	struct ceph_messenger *msgr;
	struct socket *sock;	/* connection socket */
	unsigned long state;	/* connection state */
	
	atomic_t nref;

	struct list_head list_all;   /* msgr->con_all */
	struct list_head list_bucket;  /* msgr->con_open or con_accepting */

	struct ceph_entity_addr peer_addr; /* peer address */
	__u32 connect_seq;     
	__u32 out_seq;		     /* last message queued for send */
	__u32 in_seq, in_seq_acked;  /* last message received, acked */

	/* connect state */
	struct ceph_entity_addr actual_peer_addr;
	__u32 peer_connect_seq;

	/* out queue */
	spinlock_t out_queue_lock;   /* protects out_queue, out_sent, out_seq */
	struct list_head out_queue;
	struct list_head out_sent;   /* sending/sent but unacked; resend if connection drops */

	__le32 out32;
	struct kvec out_kvec[4],
		*out_kvec_cur;
	int out_kvec_left;   /* kvec's left */
	int out_kvec_bytes;  /* bytes left */
	struct ceph_msg *out_msg;
	struct ceph_msg_pos out_msg_pos;

	/* partially read message contents */
	char in_tag;       /* READY (accepting, or no in-progress read) or ACK or MSG */
	int in_base_pos;   /* for ack seq, or msg headers, or accept handshake */
	__u32 in_partial_ack; 
	struct ceph_msg *in_msg;
	struct ceph_msg_pos in_msg_pos;

	struct work_struct rwork;		/* receive work */
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 20)
	struct delayed_work swork;		/* send work */
#else
	struct work_struct	swork;		/* send work */
#endif
        unsigned long           delay;          /* delay interval */
        unsigned int            retries;        /* temp track of retries */
};


extern struct ceph_messenger *ceph_messenger_create(struct ceph_entity_addr *myaddr);
extern void ceph_messenger_destroy(struct ceph_messenger *);
extern void ceph_messenger_mark_down(struct ceph_messenger *msgr, struct ceph_entity_addr *addr);

extern void ceph_queue_write(struct ceph_connection *con);
extern void ceph_queue_read(struct ceph_connection *con);

extern struct ceph_msg *ceph_msg_new(int type, int front_len, int page_len, int page_off, struct page **pages);
static __inline__ void ceph_msg_get(struct ceph_msg *msg) {
	atomic_inc(&msg->nref);
}
extern void ceph_msg_put(struct ceph_msg *msg);
extern int ceph_msg_send(struct ceph_messenger *msgr, struct ceph_msg *msg, 
			 unsigned long timeout);


/* encoding/decoding helpers */
static __inline__ int ceph_decode_64(void **p, void *end, __u64 *v) {
	if (unlikely(*p + sizeof(*v) > end))
		return -EINVAL;
	*v = le64_to_cpu(*(__u64*)*p);
	*p += sizeof(*v);
	return 0;
}
static __inline__ int ceph_decode_32(void **p, void *end, __u32 *v) {
	if (unlikely(*p + sizeof(*v) > end))
		return -EINVAL;
	*v = le32_to_cpu(*(__u32*)*p);
	*p += sizeof(*v);
	return 0;
}
static __inline__ int ceph_decode_16(void **p, void *end, __u16 *v) {
	if (unlikely(*p + sizeof(*v) > end))
		return -EINVAL;
	*v = le16_to_cpu(*(__u16*)*p);
	*p += sizeof(*v);
	return 0;
}
static __inline__ int ceph_decode_copy(void **p, void *end, void *v, int len) {
	if (unlikely(*p + len > end)) 
		return -EINVAL;
	memcpy(v, *p, len);
	*p += len;
	return 0;
}

static __inline__ int ceph_encode_64(void **p, void *end, __u64 v) {
	BUG_ON(*p + sizeof(v) > end);
	*(__u64*)*p = cpu_to_le64(v);
	*p += sizeof(v);
	return 0;
}

static __inline__ int ceph_encode_32(void **p, void *end, __u32 v) {
	BUG_ON(*p + sizeof(v) > end);
	*(__u32*)*p = cpu_to_le32(v);
	*p += sizeof(v);
	return 0;
}

static __inline__ int ceph_encode_16(void **p, void *end, __u16 v) {
	BUG_ON(*p + sizeof(v) > end);
	*(__u16*)*p = cpu_to_le16(v);
	*p += sizeof(v);
	return 0;
}

static __inline__ int ceph_encode_8(void **p, void *end, __u8 v) {
	BUG_ON(*p >= end);
	*(__u8*)*p = v;
	(*p)++;
	return 0;
}

static __inline__ int ceph_encode_filepath(void **p, void *end, ceph_ino_t ino, const char *path)
{
	__u32 len = path ? strlen(path):0;
	BUG_ON(*p + sizeof(ino) + sizeof(len) + len > end);
	ceph_encode_64(p, end, ino);
	ceph_encode_32(p, end, len);
	if (len) memcpy(*p, path, len);
	*p += len;
	return 0;
}

static __inline__ int ceph_encode_string(void **p, void *end, const char *s, __u32 len)
{
	BUG_ON(*p + sizeof(len) + len > end);
	ceph_encode_32(p, end, len);
	if (len) memcpy(*p, s, len);
	*p += len;
	return 0;
}

static __inline__ void ceph_decode_timespec(struct timespec *ts, struct ceph_timeval *tv)
{
	ts->tv_sec = le32_to_cpu(tv->tv_sec);
	ts->tv_nsec = 1000*le32_to_cpu(tv->tv_usec);
}
static __inline__ int ceph_encode_timespec(struct ceph_timeval *tv, struct timespec *ts)
{
	int usec = ts->tv_nsec;
	do_div(usec, 1000);
	tv->tv_sec = cpu_to_le32(ts->tv_sec);
	tv->tv_usec = cpu_to_le32(usec);
	return 0;
}

#endif
