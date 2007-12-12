#include <linux/kthread.h>
#include <linux/socket.h>
#include <linux/net.h>
#include <linux/string.h>
#include <linux/highmem.h>
#include <linux/ceph_fs.h>
#include <net/tcp.h>
#include "messenger.h"
#include "ktcp.h"

int ceph_msgr_debug = 50;
#define DOUT_VAR ceph_msgr_debug
#define DOUT_PREFIX "msgr: " 
#include "super.h"

/* static tag bytes */
static char tag_ready = CEPH_MSGR_TAG_READY;
static char tag_reject = CEPH_MSGR_TAG_REJECT;
static char tag_msg = CEPH_MSGR_TAG_MSG;
static char tag_ack = CEPH_MSGR_TAG_ACK;
//static char tag_close = CEPH_MSGR_TAG_CLOSE;

static void try_read(struct work_struct *);
static void try_write(struct work_struct *);
static void try_accept(struct work_struct *);

/*
 * failure case
 * A retry mechanism is used with exponential backoff
 */
static void ceph_send_fault(struct ceph_connection *con, int error)
{
	derr(1, "connection error %d to peer %x:%d\n", error,
	     ntohl(con->peer_addr.ipaddr.sin_addr.s_addr),
	     ntohs(con->peer_addr.ipaddr.sin_port));

	if (!con->delay) {
		derr(1, "ceph_send_fault timeout not set\n");
		return;
	}

	switch (error) {
		/* no space in socket buffer, ceph_write_space will handle 
		 * requeueing */
		case -EAGAIN:
			break;
		case -ETIMEDOUT:
			derr(1, "timed out to peer %x:%d\n",
			     ntohl(con->peer_addr.ipaddr.sin_addr.s_addr),
			     ntohs(con->peer_addr.ipaddr.sin_port));
		/* peer unreachable */
		case -EHOSTDOWN:
		case -EHOSTUNREACH:
        	case -ENETUNREACH:
			derr(1, "ENETUNREACH set\n");
			spin_lock(&con->out_queue_lock);
			list_splice_init(&con->out_sent, &con->out_queue);
			spin_unlock(&con->out_queue_lock);
			/* retry with delay */
			queue_delayed_work(send_wq, &con->swork, con->delay);
			break;
        	case -EPIPE:
        	case -ECONNREFUSED:
		case -ECONNRESET:
		/* never connected socket. SOCK_DONE flag not set */
		case -ENOTCONN:
			derr(1, "ENOTCONN set\n");
			/* TBD: setup timeout here */
			if (!test_and_clear_bit(CONNECTING, &con->state)){
				derr(1, "CONNECTING bit not set\n");
				/* reset buffer */
				spin_lock(&con->out_queue_lock);
				list_splice_init(&con->out_sent, 
					         &con->out_queue);
				spin_unlock(&con->out_queue_lock);
				clear_bit(OPEN, &con->state);
			}
			set_bit(NEW, &con->state);
			/* retry with delay */
			queue_delayed_work(send_wq, &con->swork, con->delay);
			break;
		case -EIO:
			derr(1, "EIO set\n");
			/* shutdown or soft timeout */

		default:
			/* if we ever hit here ... */
			derr(1, "unrecognized error %d\n", error);
	}
}

/*
 * calculate the number of pages a given length and offset map onto,
 * if we align the data.
 */
static int calc_pages_for(int len, int off)
{
	int nr = 0;
	if (len == 0) 
		return 0;
	if (off + len < PAGE_SIZE)
		return 1;
	if (off) {
		nr++;
		len -= off;
	}
	nr += len >> PAGE_SHIFT;
	if (len & PAGE_MASK)
		nr++;
	return nr;
}



/*
 * connections
 */

/* 
 * create a new connection.  initial state is NEW.
 */
static struct ceph_connection *new_connection(struct ceph_messenger *msgr)
{
	struct ceph_connection *con;
	con = kzalloc(sizeof(struct ceph_connection), GFP_KERNEL);
	if (con == NULL) 
		return NULL;

	con->msgr = msgr;
	con->delay = BASE_RETRY_INTERVAL;
	set_bit(NEW, &con->state);
	atomic_set(&con->nref, 1);

	INIT_LIST_HEAD(&con->list_all);
	INIT_LIST_HEAD(&con->list_bucket);

	spin_lock_init(&con->out_queue_lock);
	INIT_LIST_HEAD(&con->out_queue);
	INIT_LIST_HEAD(&con->out_sent);

	INIT_WORK(&con->rwork, try_read);
	INIT_DELAYED_WORK(&con->swork, try_write);

	return con;
}

/*
 * the radix_tree has an unsigned long key and void * value.  since
 * ceph_entity_addr is bigger than that, we use a trivial hash key, and
 * point to a list_head in ceph_connection, as you would with a hash
 * table.  in the rare event that the trivial hash collides, we just
 * traverse the (short) list.
 */
static unsigned long hash_addr(struct ceph_entity_addr *addr) 
{
	unsigned long key;
	key = *(__u32*)&addr->ipaddr.sin_addr.s_addr;
	key ^= addr->ipaddr.sin_port;
	return key;
}

/* 
 * get an existing connection, if any, for given addr
 */
static struct ceph_connection *get_connection(struct ceph_messenger *msgr, struct ceph_entity_addr *addr)
{
	struct ceph_connection *con = NULL;
	struct list_head *head, *p;
	unsigned long key = hash_addr(addr);

	/* existing? */
	spin_lock(&msgr->con_lock);
	head = radix_tree_lookup(&msgr->con_open, key);
	if (head == NULL) 
		goto out;
	if (list_empty(head)) {
		con = list_entry(head, struct ceph_connection, list_bucket);
		if (memcmp(&con->peer_addr, addr, sizeof(addr)) == 0) {
			atomic_inc(&con->nref);
			goto out;
		}
	} else {
		list_for_each(p, head) {
			con = list_entry(p, struct ceph_connection, list_bucket);
			if (memcmp(&con->peer_addr, addr, sizeof(addr)) == 0) {
				atomic_inc(&con->nref);
				goto out;
			}
		}
	}
	con = NULL;
out:
	spin_unlock(&msgr->con_lock);
	return con;
}

/* 
 * drop a reference
 */
static void put_connection(struct ceph_connection *con) 
{
	if (atomic_dec_and_test(&con->nref)) {
		dout(20, "put_connection destroying %p\n", con);
		sock_release(con->sock);
		kfree(con);
	}
}

/* 
 * add to connections tree
 */
static void add_connection(struct ceph_messenger *msgr, struct ceph_connection *con)
{
	struct list_head *head;
	unsigned long key = hash_addr(&con->peer_addr);

	/* inc ref count */
	atomic_inc(&con->nref);

	spin_lock(&msgr->con_lock);
	head = radix_tree_lookup(&msgr->con_open, key);
	if (head) {
		dout(20, "add_connection %p in existing bucket %lu\n", con, key);
		list_add(&con->list_bucket, head);
	} else {
		dout(20, "add_connection %p in new bucket %lu\n", con, key);
		INIT_LIST_HEAD(&con->list_bucket); /* empty */
		radix_tree_insert(&msgr->con_open, key, &con->list_bucket);
	}
	spin_unlock(&msgr->con_lock);
}

static void add_connection_accepting(struct ceph_messenger *msgr, struct ceph_connection *con)
{
	atomic_inc(&con->nref);
	spin_lock(&msgr->con_lock);
	list_add(&con->list_all, &msgr->con_all);
	list_add(&con->list_bucket, &msgr->con_accepting);
	spin_unlock(&msgr->con_lock);
}

/*
 * remove connection from all list.
 * also, from con_open radix tree, if it should have been there
 */
static void __remove_connection(struct ceph_messenger *msgr, struct ceph_connection *con)
{
	unsigned long key;

	dout(20, "__remove_connection %p from %p\n", con, msgr);
	list_del(&con->list_all);
	if (test_bit(CONNECTING, &con->state) ||
	    test_bit(OPEN, &con->state)) {
		/* remove from con_open too */
		key = hash_addr(&con->peer_addr);
		if (list_empty(&con->list_bucket)) {
			/* last one */
			dout(20, "remove_connection %p and removing bucket %lu\n", con, key);
			radix_tree_delete(&msgr->con_open, key);
		} else {
			dout(20, "remove_connection %p from bucket %lu\n", con, key);
			list_del(&con->list_bucket);
		}
	}
	put_connection(con);
}

static void remove_connection(struct ceph_messenger *msgr, struct ceph_connection *con)
{
	spin_lock(&msgr->con_lock);
	__remove_connection(msgr, con);
	spin_unlock(&msgr->con_lock);
}


/*
 * replace another connection
 *  (old and new should be for the _same_ peer, and thus in the same pos in the radix tree)
 */
static void replace_connection(struct ceph_messenger *msgr, struct ceph_connection *old, struct ceph_connection *new)
{
	spin_lock(&msgr->con_lock);
	list_add(&new->list_bucket, &old->list_bucket);
	list_del(&old->list_bucket);
	spin_unlock(&msgr->con_lock);
	put_connection(old); /* dec reference count */
}

/*
 * non-blocking versions
 *
 * these should be called while holding con->con_lock
 */

/*
 * write as much of con->out_partial to the socket as we can.
 *  1 -> done
 *  0 -> socket full, but more to do
 * <0 -> error
 */
static int write_partial_kvec(struct ceph_connection *con)
{
	int ret;

	dout(30, "write_partial_kvec %p left %d vec %d bytes\n", con, 
	     con->out_kvec_left, con->out_kvec_bytes);
	while (con->out_kvec_bytes > 0) {
		ret = ceph_tcp_sendmsg(con->sock, con->out_kvec_cur, 
				       con->out_kvec_left, con->out_kvec_bytes);
		if (ret <= 0) goto out;
		con->out_kvec_bytes -= ret;
		if (con->out_kvec_bytes == 0)
			break;            /* done */
		while (ret > 0) {
			if (ret >= con->out_kvec_cur->iov_len) {
				ret -= con->out_kvec_cur->iov_len;
				con->out_kvec_cur++;
			} else {
				con->out_kvec_cur->iov_len -= ret;
				con->out_kvec_cur->iov_base += ret;
				ret = 0;
				break;
			}
		}
	}
	con->out_kvec_left = 0;
	ret = 1;
out:
	dout(30, "write_partial_kvec %p left %d vec %d bytes ret = %d\n", con, 
	     con->out_kvec_left, con->out_kvec_bytes, ret);
	return ret;  /* done! */
}

static int write_partial_msg_pages(struct ceph_connection *con, struct ceph_msg *msg)
{
	struct kvec kv;
	int ret;

	while (con->out_msg_pos.page < con->out_msg->nr_pages) {
		kv.iov_base = kmap(msg->pages[con->out_msg_pos.page]) + con->out_msg_pos.page_pos;
		kv.iov_len = min((int)(PAGE_SIZE - con->out_msg_pos.page_pos), 
				 (int)(msg->hdr.data_len - con->out_msg_pos.data_pos));
		ret = ceph_tcp_sendmsg(con->sock, &kv, 1, kv.iov_len);
		if (ret < 0) return ret;
		if (ret == 0) return 0;   /* socket full */
		con->out_msg_pos.data_pos += ret;
		con->out_msg_pos.page_pos += ret;
		if (ret == kv.iov_len) {
			con->out_msg_pos.page_pos = 0;
			con->out_msg_pos.page++;
		}		
	}

	/* done */
	con->out_msg = 0;
	return 1;
}


/*
 * build out_partial based on the next outgoing message in the queue.
 */
static void prepare_write_message(struct ceph_connection *con)
{
	struct ceph_msg *m = list_entry(con->out_queue.next, struct ceph_msg, list_head);

	/* move to sending/sent list */
	list_del(&m->list_head);
	list_add_tail(&m->list_head, &con->out_sent);
	con->out_msg = m;  /* FIXME: do we want to take a reference here? */

	/* encode header */
	ceph_encode_header(&con->out_hdr, &m->hdr);

	dout(20, "prepare_write_message %p seq %d type %d len %d+%d\n", 
	     m, m->hdr.seq, m->hdr.type, m->hdr.front_len, m->hdr.data_len);

	/* tag + hdr + front */
	con->out_kvec[0].iov_base = &tag_msg;
	con->out_kvec[0].iov_len = 1;
	con->out_kvec[1].iov_base = &con->out_hdr;
	con->out_kvec[1].iov_len = sizeof(con->out_hdr);
	con->out_kvec[2] = m->front;
	con->out_kvec_left = 3;
	con->out_kvec_bytes = 1 + sizeof(con->out_hdr) + m->front.iov_len;
	con->out_kvec_cur = con->out_kvec;

	/* pages */
	con->out_msg_pos.page = 0;
	con->out_msg_pos.page_pos = m->hdr.data_off & PAGE_MASK;
	con->out_msg_pos.data_pos = 0;

	set_bit(WRITE_PENDING, &con->state);
}

/* 
 * prepare an ack for send
 */
static void prepare_write_ack(struct ceph_connection *con)
{
	con->in_seq_acked = con->in_seq;

	con->out_kvec[0].iov_base = &tag_ack;
	con->out_kvec[0].iov_len = 1;
	con->out_kvec[1].iov_base = &con->in_seq_acked;
	con->out_kvec[1].iov_len = sizeof(con->in_seq_acked);
	con->out_kvec_left = 2;
	con->out_kvec_bytes = 1 + sizeof(con->in_seq_acked);
	con->out_kvec_cur = con->out_kvec;
	set_bit(WRITE_PENDING, &con->state);
}

static void prepare_write_connect(struct ceph_messenger *msgr, struct ceph_connection *con)
{
	con->out_kvec[0].iov_base = &msgr->inst.addr;
	con->out_kvec[0].iov_len = sizeof(msgr->inst.addr);
	con->out_kvec[1].iov_base = &con->connect_seq;
	con->out_kvec[1].iov_len = sizeof(con->connect_seq);
	con->out_kvec_left = 2;
	con->out_kvec_bytes = sizeof(msgr->inst.addr) + sizeof(con->connect_seq);
	con->out_kvec_cur = con->out_kvec;
	set_bit(WRITE_PENDING, &con->state);
}

static void prepare_write_accept_announce(struct ceph_messenger *msgr, struct ceph_connection *con)
{
	con->out_kvec[0].iov_base = &msgr->inst.addr;
	con->out_kvec[0].iov_len = sizeof(msgr->inst.addr);
	con->out_kvec_left = 1;
	con->out_kvec_bytes = sizeof(msgr->inst.addr);
	con->out_kvec_cur = con->out_kvec;
	set_bit(WRITE_PENDING, &con->state);
}

static void prepare_write_accept_ready(struct ceph_connection *con)
{
	con->out_kvec[0].iov_base = &tag_ready;
	con->out_kvec[0].iov_len = 1;
	con->out_kvec_left = 1;
	con->out_kvec_bytes = 1;
	con->out_kvec_cur = con->out_kvec;
	set_bit(WRITE_PENDING, &con->state);
}

static void prepare_write_accept_reject(struct ceph_connection *con)
{
	con->out_kvec[0].iov_base = &tag_reject;
	con->out_kvec[0].iov_len = 1;
	con->out_kvec[1].iov_base = &con->connect_seq;
	con->out_kvec[1].iov_len = sizeof(con->connect_seq);
	con->out_kvec_left = 2;
	con->out_kvec_bytes = 1 + sizeof(con->connect_seq);
	con->out_kvec_cur = con->out_kvec;
	set_bit(WRITE_PENDING, &con->state);
}

/*
 * worker function when socket is writeable
 */
static void try_write(struct work_struct *work)
{
	struct ceph_connection *con;
	struct ceph_messenger *msgr;
	int ret = 1;

	con = container_of(work, struct ceph_connection, swork.work);
	msgr = con->msgr;
	dout(30, "try_write start %p state %d\n", con, con->state);
more:
	dout(30, "try_write out_kvec_bytes %d\n", con->out_kvec_bytes);

	/* initiate connect? */
	if (test_and_clear_bit(NEW, &con->state)) {
		prepare_write_connect(msgr, con);
		set_bit(CONNECTING, &con->state);
		dout(5, "try_write initiating connect on %p new state %u\n", con, con->state);
		ret = ceph_tcp_connect(con);
		dout(5, "try_write initiated connect\n");
		if (ret < 0) {
			/* fault */
			derr(1, "connect error\n");
			ceph_send_fault(con, ret);
			goto done;
		}
	}

	/* kvec data queued? */
	if (con->out_kvec_left) {
		ret = write_partial_kvec(con);
		if (ret == 0)
			goto done;
		if (test_and_clear_bit(REJECTING, &con->state)) {
			dout(30, "try_write done rejecting, state %u, closing\n", con->state);
			/* FIXME do something else here, pbly? */
			remove_connection(msgr, con);
			set_bit(CLOSED, &con->state);
			put_connection(con);
		}
		if (ret < 0) {
			ceph_send_fault(con, ret);
			goto done; /* error */
		}
	}

	/* msg pages? */
	if (con->out_msg) {
		ret = write_partial_msg_pages(con, con->out_msg);
		if (ret == 0) 
			goto done;
		if (ret < 0) {
			ceph_send_fault(con, ret);
			goto done;
		}
	}
	
	/* anything else pending? */
	spin_lock(&con->out_queue_lock);
	if (con->in_seq > con->in_seq_acked) {
		prepare_write_ack(con);
	} else if (!list_empty(&con->out_queue)) {
		prepare_write_message(con);
	} else {
		clear_bit(WRITE_PENDING, &con->state);
		/* hmm, nothing to do! No more writes pending? */
		dout(30, "try_write nothing else to write.\n");
		spin_unlock(&con->out_queue_lock);
		goto done;
	}
	spin_unlock(&con->out_queue_lock);
	goto more;

done:
	dout(30, "try_write done\n");
	return;
}


/* 
 * prepare to read a message
 */
static int prepare_read_message(struct ceph_connection *con)
{
	int err;
	BUG_ON(con->in_msg != NULL);
	con->in_tag = CEPH_MSGR_TAG_MSG;
	con->in_base_pos = 0;
	con->in_msg = ceph_msg_new(0, 0, 0, 0);
	if (IS_ERR(con->in_msg)) {
		/* TBD: we don't check for error in caller, handle error here? */
		err = PTR_ERR(con->in_msg);
		con->in_msg = 0;		
		derr(1, "kmalloc failure on incoming message %d\n", err);
		return err;
	}
	return 0;
}

/*
 * read (part of) a message
 */
static int read_message_partial(struct ceph_connection *con)
{
	struct ceph_msg *m = con->in_msg;
	void *p;
	int ret;
	int want, left;

	/* header */
	while (con->in_base_pos < sizeof(struct ceph_msg_header)) {
		left = sizeof(struct ceph_msg_header) - con->in_base_pos;
		ret = ceph_tcp_recvmsg(con->sock, &m->hdr + con->in_base_pos, left);
		if (ret <= 0) return ret;
		con->in_base_pos += ret;
		if (con->in_base_pos == sizeof(struct ceph_msg_header)) {
			/* decode/swab */
			ceph_decode_header(&m->hdr);
			break;
		}
	}

	/* front */
	if (m->front.iov_len < m->hdr.front_len) {
		if (m->front.iov_base == NULL) {
			m->front.iov_base = kmalloc(m->hdr.front_len, GFP_KERNEL);
			if (m->front.iov_base == NULL)
				return -ENOMEM;
		}
		left = m->hdr.front_len - m->front.iov_len;
		ret = ceph_tcp_recvmsg(con->sock, (char*)m->front.iov_base + m->front.iov_len, left);
		if (ret <= 0) return ret;
		m->front.iov_len += ret;
	}

	/* (page) data */
	if (m->hdr.data_len == 0) 
		goto done;
	if (m->nr_pages == 0) {
		want = calc_pages_for(m->hdr.data_len, m->hdr.data_off);
		m->pages = kmalloc(want * sizeof(*m->pages), GFP_KERNEL);
		if (m->pages == NULL)
			return -ENOMEM;
		m->nr_pages = want;
		con->in_msg_pos.page = 0;
		con->in_msg_pos.page_pos = m->hdr.data_off;
		con->in_msg_pos.data_pos = 0;
	}
	while (con->in_msg_pos.data_pos < m->hdr.data_len) {
		left = min((int)(m->hdr.data_len - con->in_msg_pos.data_pos),
			   (int)(PAGE_SIZE - con->in_msg_pos.page_pos));
		p = kmap(m->pages[con->in_msg_pos.page]);
		ret = ceph_tcp_recvmsg(con->sock, p + con->in_msg_pos.page_pos, left);
		if (ret <= 0) return ret;
		con->in_msg_pos.data_pos += ret;
		con->in_msg_pos.page_pos += ret;
		if (con->in_msg_pos.page_pos == PAGE_SIZE) {
			con->in_msg_pos.page_pos = 0;
			con->in_msg_pos.page++;
		}
	}

done:
	return 1; /* done! */
}


/* 
 * prepare to read an ack
 */
static void prepare_read_ack(struct ceph_connection *con)
{
	con->in_tag = CEPH_MSGR_TAG_ACK;
	con->in_base_pos = 0;
}

/*
 * read (part of) an ack
 */
static int read_ack_partial(struct ceph_connection *con)
{
	while (con->in_base_pos < sizeof(con->in_partial_ack)) {
		int left = sizeof(con->in_partial_ack) - con->in_base_pos;
		int ret = ceph_tcp_recvmsg(con->sock, (char*)&con->in_partial_ack + con->in_base_pos, left);
		if (ret <= 0) return ret;
		con->in_base_pos += ret;
	}
	return 1; /* done */
}

static void process_ack(struct ceph_connection *con, __u32 ack)
{
	struct ceph_msg *m;
	while (!list_empty(&con->out_sent)) {
		m = list_entry(con->out_sent.next, struct ceph_msg, list_head);
		if (m->hdr.seq > ack) break;
		dout(5, "got ack for %d type %d at %p\n", m->hdr.seq, m->hdr.type, m);
		list_del(&m->list_head);
		ceph_msg_put(m);
	}
}


/* 
 * read portion of connect-side handshake on a new connection
 */
static int read_connect_partial(struct ceph_connection *con)
{
	int ret, to;
	dout(20, "read_connect_partial %p start at %d\n", con, con->in_base_pos);

	/* actual_peer_addr */
	to = sizeof(con->actual_peer_addr);
	while (con->in_base_pos < to) {
		int left = to - con->in_base_pos;
		int have = con->in_base_pos;
		ret = ceph_tcp_recvmsg(con->sock, (char*)&con->actual_peer_addr + have, left);
		if (ret <= 0) goto out;
		con->in_base_pos += ret;
	}

	/* in_tag */
	to += 1;
	if (con->in_base_pos < to) {
		ret = ceph_tcp_recvmsg(con->sock, &con->in_tag, 1);
		if (ret <= 0) goto out;
		con->in_base_pos += ret;
	}

	/* peer_connect_seq */
	to += sizeof(con->peer_connect_seq);
	if (con->in_base_pos < to) {
		int left = to - con->in_base_pos;
		int have = sizeof(con->peer_connect_seq) - left;
		ret = ceph_tcp_recvmsg(con->sock, (char*)&con->peer_connect_seq + have, left);
		if (ret <= 0) goto out;
		con->in_base_pos += ret;
	}	
	ret = 1;
out:
	dout(20, "read_connect_partial %p end at %d ret %d\n", con, con->in_base_pos, ret);
	return ret; /* done */
}

static void process_connect(struct ceph_connection *con)
{
	dout(20, "process_connect on %p tag %d\n", con, (int)con->in_tag);
	clear_bit(CONNECTING, &con->state);
	if (!ceph_entity_addr_is_local(con->peer_addr, con->actual_peer_addr)) {
		derr(1, "process_connect wrong peer, want %x:%d/%d, got %x:%d/%d, wtf\n",
		     ntohl(con->peer_addr.ipaddr.sin_addr.s_addr), 
		     ntohs(con->peer_addr.ipaddr.sin_port),
		     con->peer_addr.nonce,
		     ntohl(con->actual_peer_addr.ipaddr.sin_addr.s_addr), 
		     ntohs(con->actual_peer_addr.ipaddr.sin_port),
		     con->actual_peer_addr.nonce);
		con->in_tag = CEPH_MSGR_TAG_REJECT;
	}
	if (con->in_tag == CEPH_MSGR_TAG_REJECT) {
		dout(10, "process_connect got REJECT peer seq %u\n", con->peer_connect_seq);
		set_bit(CLOSED, &con->state);
	}
	if (con->in_tag == CEPH_MSGR_TAG_READY) {
		dout(10, "process_connect got READY, now open\n");
		set_bit(OPEN, &con->state);
	}
}



/*
 * read portion of accept-side handshake on a newly accepted connection
 */
static int read_accept_partial(struct ceph_connection *con)
{
	int ret;
	int to;

	/* peer addr */
	to = sizeof(con->peer_addr);
	while (con->in_base_pos < to) {
		int left = to - con->in_base_pos;
		int have = con->in_base_pos;
		ret = ceph_tcp_recvmsg(con->sock, (char*)&con->peer_addr + have, left);
		if (ret <= 0) return ret;
		con->in_base_pos += ret;
	}

	/* connect_seq */
	to += sizeof(con->connect_seq);
	while (con->in_base_pos < to) {
		int left = to - con->in_base_pos;
		int have = sizeof(con->peer_addr) - left;
		ret = ceph_tcp_recvmsg(con->sock, (char*)&con->connect_seq + have, left);
		if (ret <= 0) return ret;
		con->in_base_pos += ret;
	}
	return 1; /* done */
}

/*
 * call after a new connection's handshake has completed
 */
static void process_accept(struct ceph_connection *con)
{
	struct ceph_connection *existing;

	/* do we already have a connection for this peer? */
	spin_lock(&con->msgr->con_lock);
	existing = get_connection(con->msgr, &con->peer_addr);
	if (existing) {
		//spin_lock(&existing->lock);
		/* replace existing connection? */
		if ((test_bit(CONNECTING, &existing->state) && 
		     compare_addr(&con->msgr->inst.addr, &con->peer_addr)) ||
		    (test_bit(OPEN, &existing->state) && 
		     con->connect_seq == existing->connect_seq)) {
			/* replace existing with new connection */
			replace_connection(con->msgr, existing, con);
			/* steal message queue */
			list_splice_init(&con->out_queue, &existing->out_queue); /* fixme order */
			con->out_seq = existing->out_seq;
			set_bit(OPEN, &con->state);
			set_bit(CLOSED, &existing->state);
			clear_bit(OPEN, &existing->state);
		} else {
			/* reject new connection */
			set_bit(REJECTING, &con->state);
			con->connect_seq = existing->connect_seq; /* send this with the reject */
		}
		//spin_unlock(&existing->lock);
		put_connection(existing);
	} else {
		add_connection(con->msgr, con);
		set_bit(OPEN, &con->state);
	}
	spin_unlock(&con->msgr->con_lock);

	/* the result? */
	clear_bit(ACCEPTING, &con->state);
	if (test_bit(REJECTING, &con->state))
		prepare_write_accept_reject(con);
	else
		prepare_write_accept_ready(con);
	/* queue write */
	queue_work(send_wq, &con->swork.work);
}


/*
 * worker function when data is available on the socket
 */
void try_read(struct work_struct *work)
{
	int ret = -1;
	struct ceph_connection *con;
	struct ceph_messenger *msgr;

	dout(20, "Entering try_read\n");
	con = container_of(work, struct ceph_connection, rwork);
	msgr = con->msgr;

retry:
	if (test_and_set_bit(READING, &con->state)) {
		dout(20, "try_read already reading\n");
		return;
	}
	clear_bit(READABLE, &con->state);

more:
	/*
	 * TBD: maybe store error in ceph_connection
         */

	if (test_bit(CLOSED, &con->state)) {
		dout(20, "try_read closed\n");
		goto done;
	}
	if (test_bit(ACCEPTING, &con->state)) {
		dout(20, "try_read accepting\n");
		ret = read_accept_partial(con);
		if (ret <= 0) goto done;
		process_accept(con);		/* accepted */
		goto more;
	}
	if (test_bit(CONNECTING, &con->state)) {
		dout(20, "try_read connecting\n");
		ret = read_connect_partial(con);
		if (ret <= 0) goto done;
		process_connect(con);
		if (test_bit(CLOSED, &con->state)) goto done;
	}

	if (con->in_tag == CEPH_MSGR_TAG_READY) {
		ret = ceph_tcp_recvmsg(con->sock, &con->in_tag, 1);
		if (ret <= 0) goto done;
		if (con->in_tag == CEPH_MSGR_TAG_MSG) 
			prepare_read_message(con);
		else if (con->in_tag == CEPH_MSGR_TAG_ACK)
			prepare_read_ack(con);
		else {
			derr(2, "try_read got bad tag %d\n", (int)con->in_tag);
			ret = -EINVAL;
			goto bad;
		}
		goto more;
	}
	if (con->in_tag == CEPH_MSGR_TAG_MSG) {
		ret = read_message_partial(con);
		if (ret <= 0) goto done;
		msgr->dispatch(con->msgr->parent, con->in_msg); /* fixme: use a workqueue */
		con->in_msg = 0;
		con->in_tag = CEPH_MSGR_TAG_READY;
		goto more;
	}
	if (con->in_tag == CEPH_MSGR_TAG_ACK) {
		ret = read_ack_partial(con);
		if (ret <= 0) goto done;
		/* got an ack */
		process_ack(con, con->in_partial_ack);
		con->in_tag = CEPH_MSGR_TAG_READY;
		goto more;
	}
	derr(2, "try_read bad con->in_tag = %d\n", (int)con->in_tag);
bad:
	BUG_ON(1); /* shouldn't get here */
done:
	clear_bit(READING, &con->state);
	if (test_bit(READABLE, &con->state)) {
		dout(30, "try_read readable flag set again, looping\n");
		goto retry;
	}
	dout(20, "Exited try_read\n");
	return;
}


/*
 *  worker function when listener receives a connect
 */
static void try_accept(struct work_struct *work)
{
        struct ceph_connection *new_con = NULL;
	struct ceph_messenger *msgr;

	msgr = container_of(work, struct ceph_messenger, awork);

        dout(5, "Entered try_accept\n");

	/* initialize the msgr connection */
	new_con = new_connection(msgr);
	if (new_con == NULL) {
               	derr(1, "malloc failure\n");
		goto done;
       	}
	if (ceph_tcp_accept(msgr->listen_sock, new_con) < 0) {
        	derr(1, "error accepting connection\n");
		put_connection(new_con);
                goto done;
        }
	dout(5, "accepted connection \n");

	new_con->in_tag = CEPH_MSGR_TAG_READY;
	set_bit(ACCEPTING, &new_con->state);
	clear_bit(NEW,&new_con->state);
	prepare_write_accept_announce(msgr, new_con);
	add_connection_accepting(msgr, new_con);

	/*
	 * hand off to worker threads ,should be able to write, we want to 
	 * try to write right away, we may have missed socket state change
	 */
	queue_work(send_wq, &new_con->swork.work);
done:
        return;
}

/*
 * create a new messenger instance, creates listening socket
 */
struct ceph_messenger *ceph_messenger_create(struct ceph_entity_addr *myaddr)
{
        struct ceph_messenger *msgr;
	int ret = 0;

        msgr = kzalloc(sizeof(*msgr), GFP_KERNEL);
        if (msgr == NULL) 
		return ERR_PTR(-ENOMEM);
	INIT_WORK(&msgr->awork, try_accept);
	spin_lock_init(&msgr->con_lock);
	INIT_LIST_HEAD(&msgr->con_all);
	INIT_LIST_HEAD(&msgr->con_accepting);
	INIT_RADIX_TREE(&msgr->con_open, GFP_KERNEL);

	/* create listening socket */
	ret = ceph_tcp_listen(msgr, myaddr ? ntohs(myaddr->ipaddr.sin_port):0);
	if (ret < 0) {
		kfree(msgr);
		return ERR_PTR(ret);
	}
	if (myaddr) 
		msgr->inst.addr.ipaddr.sin_addr = myaddr->ipaddr.sin_addr;

	dout(1, "ceph_messenger_create %p listening on %x:%d\n", msgr,
	     ntohl(msgr->inst.addr.ipaddr.sin_addr.s_addr), 
	     ntohs(msgr->inst.addr.ipaddr.sin_port));
	return msgr;
}

void ceph_messenger_destroy(struct ceph_messenger *msgr)
{
	struct ceph_connection *con;

	dout(1, "ceph_messenger_destroy %p\n", msgr);

	/* kill off connections */
	spin_lock(&msgr->con_lock);
	while (!list_empty(&msgr->con_all)) {
		con = list_entry(msgr->con_all.next, struct ceph_connection, list_all);
		__remove_connection(msgr, con);
	}
	spin_unlock(&msgr->con_lock);

	/* stop listener */
	sock_release(msgr->listen_sock);

	kfree(msgr);
}


/*
 * queue up an outgoing message
 *
 * will take+drop msgr, then connection locks.
 */
int ceph_msg_send(struct ceph_messenger *msgr, struct ceph_msg *msg, 
		  unsigned long timeout)
{
	struct ceph_connection *con;
	int ret = 0;
	
	/* set source */
	msg->hdr.src = msgr->inst;

	/* do we have the connection? */
	con = get_connection(msgr, &msg->hdr.dst.addr);
	if (!con) {
		con = new_connection(msgr);
		if (IS_ERR(con))
			return PTR_ERR(con);
		dout(5, "ceph_msg_send new connection %p to peer %x:%d\n", con,
		     ntohl(msg->hdr.dst.addr.ipaddr.sin_addr.s_addr), 
		     ntohs(msg->hdr.dst.addr.ipaddr.sin_port));
		con->peer_addr = msg->hdr.dst.addr;
		add_connection(msgr, con);
	} else {
		dout(10, "ceph_msg_send had connection %p to peer %x:%d\n", con,
		     ntohl(msg->hdr.dst.addr.ipaddr.sin_addr.s_addr),
		     ntohs(msg->hdr.dst.addr.ipaddr.sin_port));
	}		     
	con->delay = timeout;
	dout(10, "ceph_msg_send delay = %lu\n", con->delay); 
	/* queue */
	spin_lock(&con->out_queue_lock);
	msg->hdr.seq = ++con->out_seq;
	dout(1, "ceph_msg_send queuing %p seq %u for %s%d on %p\n", msg, msg->hdr.seq,
	     ceph_name_type_str(msg->hdr.dst.name.type), msg->hdr.dst.name.num, con);
	ceph_msg_get(msg);
	list_add_tail(&msg->list_head, &con->out_queue);
	spin_unlock(&con->out_queue_lock);
		
	if (test_and_set_bit(WRITE_PENDING, &con->state) == 0) {
		dout(30, "ceph_msg_send queuing new swork on %p\n", con);
		queue_work(send_wq, &con->swork.work);
		dout(30, "ceph_msg_send queued\n");
	}

	put_connection(con);
	dout(30, "ceph_msg_send done\n");
	return ret;
}



struct ceph_msg *ceph_msg_new(int type, int front_len, int page_len, int page_off)
{
	struct ceph_msg *m;
	int i;

	m = kmalloc(sizeof(*m), GFP_KERNEL);
	if (m == NULL)
		goto out;
	atomic_set(&m->nref, 1);
	m->hdr.type = type;
	m->hdr.front_len = front_len;
	m->hdr.data_len = page_len;
	m->hdr.data_off = page_off;

	/* front */
	if (front_len) {
		m->front.iov_base = kmalloc(front_len, GFP_KERNEL);
		if (m->front.iov_base == NULL)
			goto out2;
	} else {
		m->front.iov_base = 0;
	}
	m->front.iov_len = front_len;

	/* pages */
	m->nr_pages = calc_pages_for(page_len, page_off);
	if (m->nr_pages) {
		m->pages = kzalloc(m->nr_pages*sizeof(*m->pages), GFP_KERNEL);
		for (i=0; i<m->nr_pages; i++) {
			m->pages[i] = alloc_page(GFP_KERNEL);
			if (m->pages[i] == NULL)
				goto out2;
		}
	} else {
		m->pages = 0;
	}

	INIT_LIST_HEAD(&m->list_head);
	return m;

out2:
	ceph_msg_put(m);
out:
	return ERR_PTR(-ENOMEM);
}

void ceph_msg_put(struct ceph_msg *m)
{
	int i;
	if (atomic_dec_and_test(&m->nref)) {
		dout(30, "ceph_msg_put last one on %p\n", m);
		BUG_ON(!list_empty(&m->list_head));
		if (m->pages) {
			for (i=0; i<m->nr_pages; i++)
				if (m->pages[i])
					__free_pages(m->pages[i], 0);
			kfree(m->pages);
		}
		if (m->front.iov_base) {
			kfree(m->front.iov_base);
		}
		kfree(m);
	}
}


