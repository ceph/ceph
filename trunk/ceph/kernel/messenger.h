#ifndef __FS_CEPH_MESSENGER_H
#define __FS_CEPH_MESSENGER_H

#include <linux/uio.h>
#include <linux/net.h>
#include <linux/radix-tree.h>
#include <linux/workqueue.h>
#include <linux/ceph_fs.h>

struct ceph_msg;

typedef void (*ceph_messenger_dispatch_t) (void *p, struct ceph_msg *m);

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
	ceph_messenger_dispatch_t dispatch;
	struct socket *listen_sock; 	 /* listening socket */
	struct work_struct awork;	 /* accept work */
	struct ceph_entity_inst inst;    /* my name+address */
	spinlock_t con_lock;
	struct list_head con_all;        /* all connections */
	struct list_head con_accepting;  /*  doing handshake, or */
	struct radix_tree_root con_open; /*  established. see get_connection() */
};

struct ceph_msg {
	struct ceph_msg_header hdr;	/* header */
	struct kvec front;              /* first bit of message */
	struct page **pages;            /* data payload */
	unsigned nr_pages;              /* size of page array */

	struct list_head list_head;
	atomic_t nref;
};

struct ceph_msg_pos {
	int page, page_pos;        /* which page; -3=tag, -2=hdr, -1=front */
	int data_pos;
};


/* current state of connection */
enum ceph_connection_state {
	NEW = 1,
	ACCEPTING = 2,
	CONNECTING = 4,
	OPEN = 8,
	REJECTING = 16,
	CLOSED = 32,
	READ_PEND = 64,
	WRITE_PEND = 128
};

struct ceph_connection {
	struct ceph_messenger *msgr;
	struct socket *sock;	/* connection socket */
	
	atomic_t nref;
	spinlock_t con_lock;    /* connection lock */

	struct list_head list_all;   /* msgr->con_all */
	struct list_head list_bucket;  /* msgr->con_open or con_accepting */

	struct ceph_entity_addr peer_addr; /* peer address */
	enum ceph_connection_state state;
	__u32 connect_seq;     
	__u32 out_seq;		     /* last message queued for send */
	__u32 in_seq, in_seq_acked;  /* last message received, acked */

	/* out queue */
	struct list_head out_queue;
	struct ceph_msg_header out_hdr;
	struct kvec out_kvec[4],
		*out_kvec_cur;
	int out_kvec_left;   /* kvec's left */
	int out_kvec_bytes;  /* bytes left */

	struct ceph_msg *out_msg;
	struct ceph_msg_pos out_msg_pos;

	struct list_head out_sent;   /* sending/sent but unacked; resend if connection drops */

	/* partially read message contents */
	char in_tag;       /* READY (accepting, or no in-progress read) or ACK or MSG */
	int in_base_pos;   /* for ack seq, or msg headers, or accept handshake */
	__u32 in_partial_ack;  
	struct ceph_msg *in_msg;
	struct ceph_msg_pos in_msg_pos;

	struct work_struct rwork;		/* received work */
	struct work_struct swork;		/* send work */
	int retries;
	int error;				/* error on connection */
};


extern struct ceph_msg *ceph_msg_new(int type, int front_len, int page_len, int page_off);
static __inline__ void ceph_msg_get(struct ceph_msg *msg) {
	atomic_inc(&msg->nref);
}
extern void ceph_msg_put(struct ceph_msg *msg);
extern int ceph_msg_send(struct ceph_messenger *msgr, struct ceph_msg *msg);


/* encoding/decoding helpers */
static __inline__ int ceph_decode_64(void **p, void *end, __u64 *v) {
	if (*p + sizeof(*v) > end)
		return -EINVAL;
	*v = le64_to_cpu(*(__u64*)p);
	p += sizeof(*v);
	return 0;
}
static __inline__ int ceph_decode_32(void **p, void *end, __u32 *v) {
	if (*p + sizeof(*v) > end)
		return -EINVAL;
	*v = le32_to_cpu(*(__u32*)p);
	p += sizeof(*v);
	return 0;
}
static __inline__ int ceph_decode_16(void **p, void *end, __u16 *v) {
	if (*p + sizeof(*v) > end)
		return -EINVAL;
	*v = le16_to_cpu(*(__u16*)p);
	p += sizeof(*v);
	return 0;
}
static __inline__ int ceph_decode_copy(void **p, void *end, void *v, int len) {
	if (*p + len > end) 
		return -EINVAL;
	memcpy(v, *p, len);
	*p += len;
	return 0;
}

static __inline__ int ceph_decode_addr(void **p, void *end, struct ceph_entity_addr *v) {
	int err;
	if (*p + sizeof(*v) > end) 
		return -EINVAL;
	if ((err = ceph_decode_32(p, end, &v->erank)) != 0)
		return -EINVAL;
	if ((err = ceph_decode_32(p, end, &v->nonce)) != 0)
		return -EINVAL;
	ceph_decode_copy(p, end, &v->ipaddr, sizeof(v->ipaddr));
	return 0;
}

static __inline__ int ceph_decode_name(void **p, void *end, struct ceph_entity_name *v) {
	if (*p + sizeof(*v) > end)
		return -EINVAL;
	v->type = le32_to_cpu(*(__u32*)p);
	p += sizeof(__u32);
	v->num = le32_to_cpu(*(__u32*)p);
	p += sizeof(__u32);
	return 0;
}

/* hmm, these are actually identical, yeah? */
static __inline__ void ceph_decode_inst(struct ceph_entity_inst *to)
{
	to->name.type = le32_to_cpu(to->name.type);
	to->name.num = le32_to_cpu(to->name.num);
	to->addr.erank = le32_to_cpu(to->addr.erank);
	to->addr.nonce = le32_to_cpu(to->addr.nonce);
}
static __inline__ void ceph_encode_inst(struct ceph_entity_inst *to, struct ceph_entity_inst *from)
{
	to->name.type = cpu_to_le32(from->name.type);
	to->name.num = cpu_to_le32(from->name.num);
	to->addr.erank = cpu_to_le32(from->addr.erank);
	to->addr.nonce = cpu_to_le32(from->addr.nonce);
	to->addr.ipaddr = from->addr.ipaddr;
}

static __inline__ void ceph_encode_header(struct ceph_msg_header *to, struct ceph_msg_header *from)
{
	to->seq = cpu_to_le32(from->seq);
	to->type = cpu_to_le32(from->type);
	ceph_encode_inst(&to->src, &from->src);
	ceph_encode_inst(&to->dst, &from->dst);
	to->front_len = cpu_to_le16(from->front_len);
	to->data_off = cpu_to_le16(from->data_off);
	to->data_len = cpu_to_le16(from->data_len);
}
static __inline__ void ceph_decode_header(struct ceph_msg_header *to)
{
	to->seq = cpu_to_le32(to->seq);
	to->type = cpu_to_le32(to->type);
	ceph_decode_inst(&to->src);
	ceph_decode_inst(&to->dst);
	to->front_len = cpu_to_le16(to->front_len);
	to->data_off = cpu_to_le16(to->data_off);
	to->data_len = cpu_to_le16(to->data_len);
}


#endif
