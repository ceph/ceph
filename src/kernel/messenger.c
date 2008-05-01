#include <linux/kthread.h>
#include <linux/socket.h>
#include <linux/net.h>
#include <linux/string.h>
#include <linux/highmem.h>
#include <net/tcp.h>

#include "ceph_fs.h"
#include "messenger.h"
#include "ktcp.h"

int ceph_debug_msgr;
#define DOUT_VAR ceph_debug_msgr
#define DOUT_PREFIX "msgr: "
#include "super.h"

/* static tag bytes */
static char tag_ready = CEPH_MSGR_TAG_READY;
static char tag_reset = CEPH_MSGR_TAG_RESETSESSION;
static char tag_retry = CEPH_MSGR_TAG_RETRY;
static char tag_wait = CEPH_MSGR_TAG_WAIT;
static char tag_msg = CEPH_MSGR_TAG_MSG;
static char tag_ack = CEPH_MSGR_TAG_ACK;

static void try_read(struct work_struct *);
static void try_write(struct work_struct *);
static void try_accept(struct work_struct *);



/*
 * connections
 */

/*
 * create a new connection.  initial state is NEW.
 */
static struct ceph_connection *new_connection(struct ceph_messenger *msgr)
{
	struct ceph_connection *con;
	con = kzalloc(sizeof(struct ceph_connection), GFP_NOFS);
	if (con == NULL)
		return NULL;

	con->msgr = msgr;
	con->delay = BASE_DELAY_INTERVAL;
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
	key = *(__u32 *)&addr->ipaddr.sin_addr.s_addr;
	key ^= addr->ipaddr.sin_port;
	return key;
}

/*
 * get an existing connection, if any, for given addr
 */
static struct ceph_connection *__get_connection(struct ceph_messenger *msgr,
						struct ceph_entity_addr *addr)
{
	struct ceph_connection *con = NULL;
	struct list_head *head, *p;
	unsigned long key = hash_addr(addr);

	/* existing? */
	head = radix_tree_lookup(&msgr->con_tree, key);
	if (head == NULL)
		goto out;
	con = list_entry(head, struct ceph_connection, list_bucket);
	if (memcmp(&con->peer_addr, addr, sizeof(addr)) == 0) {
		atomic_inc(&con->nref);
		goto out;
	}
	list_for_each(p, head) {
		con = list_entry(p, struct ceph_connection, list_bucket);
		if (memcmp(&con->peer_addr, addr, sizeof(addr)) == 0) {
			atomic_inc(&con->nref);
			goto out;
		}
	}
	con = NULL;
out:
	return con;
}



/*
 * drop a reference
 */
static void put_connection(struct ceph_connection *con)
{
	dout(20, "put_connection nref = %d\n", atomic_read(&con->nref));
	if (atomic_dec_and_test(&con->nref)) {
		dout(20, "put_connection destroying %p\n", con);
		ceph_msg_put_list(&con->out_queue);
		ceph_msg_put_list(&con->out_sent);
		ceph_sock_release(con->sock);
		kfree(con);
	}
}

/*
 * add to connections tree
 */
static void __register_connection(struct ceph_messenger *msgr,
				  struct ceph_connection *con)
{
	struct list_head *head;
	unsigned long key = hash_addr(&con->peer_addr);

	/* inc ref count */
	atomic_inc(&con->nref);

	if (test_and_clear_bit(ACCEPTING, &con->state)) {
		list_del_init(&con->list_bucket);
		put_connection(con);
	} else
		list_add(&con->list_all, &msgr->con_all);

	head = radix_tree_lookup(&msgr->con_tree, key);
	if (head) {
		dout(20, "add_connection %p in existing bucket %lu head %p\n",
		     con, key, head);
		list_add(&con->list_bucket, head);
	} else {
		dout(20, "add_connection %p in new bucket %lu head %p\n", con,
		     key, &con->list_bucket);
		INIT_LIST_HEAD(&con->list_bucket); /* empty */
		radix_tree_insert(&msgr->con_tree, key, &con->list_bucket);
	}
	set_bit(REGISTERED, &con->state);
}

static void add_connection_accepting(struct ceph_messenger *msgr,
				     struct ceph_connection *con)
{
	atomic_inc(&con->nref);
	spin_lock(&msgr->con_lock);
	list_add(&con->list_all, &msgr->con_all);
	spin_unlock(&msgr->con_lock);
}

/*
 * remove connection from all list.
 * also, from con_tree radix tree, if it should have been there
 */
static void __remove_connection(struct ceph_messenger *msgr,
				struct ceph_connection *con)
{
	unsigned long key;
	void **slot, *val;

	dout(20, "__remove_connection %p\n", con);
	if (list_empty(&con->list_all)) {
		dout(20, "__remove_connection %p not registered\n", con);
		return;
	}
	list_del_init(&con->list_all);
	if (test_bit(REGISTERED, &con->state)) {
		/* remove from con_tree too */
		key = hash_addr(&con->peer_addr);
		if (list_empty(&con->list_bucket)) {
			/* last one */
			dout(20, "__remove_connection %p and bucket %lu\n",
			     con, key);
			radix_tree_delete(&msgr->con_tree, key);
		} else {
			slot = radix_tree_lookup_slot(&msgr->con_tree, key);
			val = radix_tree_deref_slot(slot);
			dout(20, "__remove_connection %p from bucket %lu "
			     "head %p\n", con, key, val);
			if (val == &con->list_bucket) {
				dout(20, "__remove_connection adjusting bucket"
				     " for %lu to next item, %p\n", key,
				     con->list_bucket.next);
				radix_tree_replace_slot(slot,
							con->list_bucket.next);
			}
			list_del_init(&con->list_bucket);
		}
	}
	if (test_bit(ACCEPTING, &con->state))
		list_del_init(&con->list_bucket);
	put_connection(con);
}

static void remove_connection(struct ceph_messenger *msgr,
			      struct ceph_connection *con)
{
	spin_lock(&msgr->con_lock);
	__remove_connection(msgr, con);
	spin_unlock(&msgr->con_lock);
}


/*
 * atomically queue read or write work on a connection.
 * bump reference to avoid races.
 */
void ceph_queue_write(struct ceph_connection *con)
{
	dout(40, "ceph_queue_write %p\n", con);
	atomic_inc(&con->nref);
	if (!queue_work(send_wq, &con->swork.work)) {
		dout(40, "ceph_queue_write %p - already queued\n", con);
		put_connection(con);
	}
}

void ceph_queue_delayed_write(struct ceph_connection *con)
{
	dout(40, "ceph_queue_delayed_write %p delay %lu\n", con, con->delay);
	atomic_inc(&con->nref);
	if (!queue_delayed_work(send_wq, &con->swork,
				round_jiffies_relative(con->delay))) {
		dout(40, "ceph_queue_delayed_write %p - already queued\n", con);
		put_connection(con);
	}
}

void ceph_queue_read(struct ceph_connection *con)
{
	if (test_and_set_bit(READABLE, &con->state) != 0) {
		dout(40, "ceph_queue_read %p - already READABLE\n", con);
		return;
	}
	dout(40, "ceph_queue_read %p\n", con);
	atomic_inc(&con->nref);
	queue_work(recv_wq, &con->rwork);
}




/*
 * failure case
 * A retry mechanism is used with exponential backoff
 */
static void ceph_fault(struct ceph_connection *con)
{
	derr(1, "%s%d %u.%u.%u.%u:%u %s\n", ENTITY_NAME(con->peer_name),
	     IPQUADPORT(con->peer_addr.ipaddr), con->error_msg);
	dout(10, "fault %p state %lu to peer %u.%u.%u.%u:%u\n",
	     con, con->state, IPQUADPORT(con->peer_addr.ipaddr));

	/* PW if never get here remove */
	if (test_bit(WAIT, &con->state)) {
		derr(30, "fault socket close during WAIT state\n");
		return;
	}

	if (con->delay) {
		dout(30, "fault tcp_close delay != 0\n");
		ceph_sock_release(con->sock);
		con->sock = NULL;
		set_bit(NEW, &con->state);

		/*
		 * If there are no messages in the queue, place the
		 * connection in a STANDBY state otherwise retry with
		 * delay
		 */
		spin_lock(&con->out_queue_lock);
		if (list_empty(&con->out_queue)) {
			dout(10, "setting STANDBY bit\n");
			set_bit(STANDBY, &con->state);
			spin_unlock(&con->out_queue_lock);
			return;
		}

		if (!test_and_clear_bit(CONNECTING, &con->state)) {
			derr(1, "CONNECTING bit not set\n");
			/* TBD: reset buffer correctly */
			/* reset buffer */
			list_splice_init(&con->out_sent, &con->out_queue);
			clear_bit(OPEN, &con->state);
		}
		spin_unlock(&con->out_queue_lock);

		/* retry with delay */
		ceph_queue_delayed_write(con);

		if (con->delay < MAX_DELAY_INTERVAL)
			con->delay *= 2;
		else
			con->delay = MAX_DELAY_INTERVAL;
	} else {
		dout(30, "fault tcp_close delay = 0\n");
		remove_connection(con->msgr, con);
	}
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

	dout(10, "write_partial_kvec have %d left\n", con->out_kvec_bytes);
	while (con->out_kvec_bytes > 0) {
		ret = ceph_tcp_sendmsg(con->sock, con->out_kvec_cur,
				       con->out_kvec_left, con->out_kvec_bytes,
				       con->out_more);
		if (ret <= 0)
			goto out;
		con->out_kvec_bytes -= ret;
		if (con->out_kvec_bytes == 0)
			break;            /* done */
		while (ret > 0) {
			if (ret >= con->out_kvec_cur->iov_len) {
				ret -= con->out_kvec_cur->iov_len;
				con->out_kvec_cur++;
				con->out_kvec_left--;
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

static int write_partial_msg_pages(struct ceph_connection *con,
				   struct ceph_msg *msg)
{
	struct kvec kv;
	int ret;
	unsigned data_len = le32_to_cpu(msg->hdr.data_len);

	dout(30, "write_partial_msg_pages con %p msg %p on %d/%d offset %d\n",
	     con, con->out_msg, con->out_msg_pos.page, con->out_msg->nr_pages,
	     con->out_msg_pos.page_pos);

	while (con->out_msg_pos.page < con->out_msg->nr_pages) {
		struct page *page;
		void *kaddr;

		mutex_lock(&msg->page_mutex);
		if (msg->pages) {
			page = msg->pages[con->out_msg_pos.page];
			kaddr = kmap(page);
		} else {
			/*dout(60, "using zero page\n");*/
			kaddr = page_address(con->msgr->zero_page);
		}
		kv.iov_base = kaddr + con->out_msg_pos.page_pos;
		kv.iov_len = min((int)(PAGE_SIZE - con->out_msg_pos.page_pos),
				 (int)(data_len - con->out_msg_pos.data_pos));
		ret = ceph_tcp_sendmsg(con->sock, &kv, 1, kv.iov_len, 1);
		if (msg->pages)
			kunmap(page);
		mutex_unlock(&msg->page_mutex);
		if (ret <= 0)
			goto out;
		con->out_msg_pos.data_pos += ret;
		con->out_msg_pos.page_pos += ret;
		if (ret == kv.iov_len) {
			con->out_msg_pos.page_pos = 0;
			con->out_msg_pos.page++;
		}
	}

	/* done */
	dout(30, "write_partial_msg_pages wrote all pages on %p\n", con);

	con->out_footer.aborted = cpu_to_le32(con->out_msg->pages == 0);
	con->out_kvec[0].iov_base = &con->out_footer;
	con->out_kvec_bytes = con->out_kvec[0].iov_len = 
		sizeof(con->out_footer);
	con->out_kvec_left = 1;
	con->out_kvec_cur = con->out_kvec;
	con->out_msg = 0;
	con->out_more = 0;  /* end of message */
	
	ret = 1;
out:
	return ret;
}


/*
 * build out_partial based on the next outgoing message in the queue.
 */
static void prepare_write_message(struct ceph_connection *con)
{
	struct ceph_msg *m;
	int v = 0;
	con->out_kvec_bytes = 0;

	/* ack? */
	if (con->in_seq > con->in_seq_acked) {
		con->in_seq_acked = con->in_seq;
		con->out_kvec[v].iov_base = &tag_ack;
		con->out_kvec[v++].iov_len = 1;
		con->out32 = cpu_to_le32(con->in_seq_acked);
		con->out_kvec[v].iov_base = &con->out32;
		con->out_kvec[v++].iov_len = 4;
		con->out_kvec_bytes = 1 + 4;
	}

	/* move to sending/sent list */
	m = list_entry(con->out_queue.next,
		       struct ceph_msg, list_head);
	list_del_init(&m->list_head);
	list_add_tail(&m->list_head, &con->out_sent);
	con->out_msg = m;  /* we dont bother taking a reference here. */

	/* encode header */
	dout(20, "prepare_write_message %p seq %lld type %d len %d+%d %d pgs\n",
	     m, le64_to_cpu(m->hdr.seq), le32_to_cpu(m->hdr.type),
	     le32_to_cpu(m->hdr.front_len), le32_to_cpu(m->hdr.data_len),
	     m->nr_pages);
	BUG_ON(le32_to_cpu(m->hdr.front_len) != m->front.iov_len);

	/* tag + hdr + front */
	con->out_kvec[v].iov_base = &tag_msg;
	con->out_kvec[v++].iov_len = 1;
	con->out_kvec[v].iov_base = &m->hdr;
	con->out_kvec[v++].iov_len = sizeof(m->hdr);
	con->out_kvec[v++] = m->front;
	con->out_kvec_left = v;
	con->out_kvec_bytes += 1 + sizeof(m->hdr) + m->front.iov_len;
	con->out_kvec_cur = con->out_kvec;
	con->out_more = le32_to_cpu(m->hdr.data_len);  /* data? */

	/* pages */
	con->out_msg_pos.page = 0;
	con->out_msg_pos.page_pos = le32_to_cpu(m->hdr.data_off) & ~PAGE_MASK;
	con->out_msg_pos.data_pos = 0;

	set_bit(WRITE_PENDING, &con->state);
}

/*
 * prepare an ack for send
 */
static void prepare_write_ack(struct ceph_connection *con)
{
	dout(20, "prepare_write_ack %p %u -> %u\n", con, 
	     con->in_seq_acked, con->in_seq);
	con->in_seq_acked = con->in_seq;

	con->out_kvec[0].iov_base = &tag_ack;
	con->out_kvec[0].iov_len = 1;
	con->out32 = cpu_to_le32(con->in_seq_acked);
	con->out_kvec[1].iov_base = &con->out32;
	con->out_kvec[1].iov_len = 4;
	con->out_kvec_left = 2;
	con->out_kvec_bytes = 1 + 4;
	con->out_kvec_cur = con->out_kvec;
	con->out_more = 1;  /* more will follow.. eventually.. */
	set_bit(WRITE_PENDING, &con->state);
}

static void prepare_read_connect(struct ceph_connection *con)
{
	con->in_base_pos = 0;
}

static void prepare_write_connect(struct ceph_messenger *msgr,
				  struct ceph_connection *con)
{
	con->out_kvec[0].iov_base = &msgr->inst.addr;
	con->out_kvec[0].iov_len = sizeof(msgr->inst.addr);
	con->out_connect_seq = cpu_to_le32(con->connect_seq);
	con->out_kvec[1].iov_base = &con->out_connect_seq;
	con->out_kvec[1].iov_len = 4;
	con->out_kvec_left = 2;
	con->out_kvec_bytes = sizeof(msgr->inst.addr) + 4;
	con->out_kvec_cur = con->out_kvec;
	con->out_more = 0;
	set_bit(WRITE_PENDING, &con->state);
}

static void prepare_write_connect_retry(struct ceph_messenger *msgr,
					struct ceph_connection *con)
{
	con->out_connect_seq = cpu_to_le32(con->connect_seq);
	con->out_kvec[0].iov_base = &con->out_connect_seq;
	con->out_kvec[0].iov_len = 4;
	con->out_kvec_left = 1;
	con->out_kvec_bytes = 4;
	con->out_kvec_cur = con->out_kvec;
	con->out_more = 0;
	set_bit(WRITE_PENDING, &con->state);
}

static void prepare_write_accept_announce(struct ceph_messenger *msgr,
					  struct ceph_connection *con)
{
	con->out_kvec[0].iov_base = &msgr->inst.addr;
	con->out_kvec[0].iov_len = sizeof(msgr->inst.addr);
	con->out_kvec_left = 1;
	con->out_kvec_bytes = sizeof(msgr->inst.addr);
	con->out_kvec_cur = con->out_kvec;
	con->out_more = 0;
	set_bit(WRITE_PENDING, &con->state);
}

static void prepare_write_accept_reply(struct ceph_connection *con, char *ptag)
{
	con->out_kvec[0].iov_base = ptag;
	con->out_kvec[0].iov_len = 1;
	con->out_kvec_left = 1;
	con->out_kvec_bytes = 1;
	con->out_kvec_cur = con->out_kvec;
	con->out_more = 0;
	set_bit(WRITE_PENDING, &con->state);
}

static void prepare_write_accept_retry(struct ceph_connection *con, char *ptag)
{
	con->out_kvec[0].iov_base = ptag;
	con->out_kvec[0].iov_len = 1;
	con->out_connect_seq = cpu_to_le32(con->connect_seq);
	con->out_kvec[1].iov_base = &con->out_connect_seq;
	con->out_kvec[1].iov_len = 4;
	con->out_kvec_left = 2;
	con->out_kvec_bytes = 1 + 4;
	con->out_kvec_cur = con->out_kvec;
	con->out_more = 0;
	set_bit(WRITE_PENDING, &con->state);
}

/*
 * worker function when socket is writeable
 */
static void try_write(struct work_struct *work)
{
	struct ceph_connection *con =
		container_of(work, struct ceph_connection, swork.work);
	struct ceph_messenger *msgr = con->msgr;
	int ret = 1;

	dout(30, "try_write start %p state %lu nref %d\n", con, con->state,
	     atomic_read(&con->nref));

	if (test_bit(CLOSED, &con->state)) {
		dout(5, "try_write closed\n");
		goto done;
	}

	if (test_and_clear_bit(SOCK_CLOSE, &con->state)) {
		ceph_fault(con);
		goto done;
	}
more:
	dout(30, "try_write out_kvec_bytes %d\n", con->out_kvec_bytes);

	/* initiate connect? */
	if (test_and_clear_bit(NEW, &con->state)) {
		if (test_and_clear_bit(STANDBY, &con->state))
			con->connect_seq++;
		prepare_write_connect(msgr, con);
		prepare_read_connect(con);
		set_bit(CONNECTING, &con->state);
		con->in_tag = CEPH_MSGR_TAG_READY;
		dout(5, "try_write initiating connect on %p new state %lu\n",
		     con, con->state);
		ret = ceph_tcp_connect(con);
		if (ret < 0) {
			con->error_msg = "connect error";
			ceph_fault(con);
			goto done;
		}
	}

	/* kvec data queued? */
more_kvec:
	if (con->out_kvec_left) {
		ret = write_partial_kvec(con);
		if (ret == 0)
			goto done;
		if (ret < 0) {
			dout(30, "try_write write_partial_kvec err %d\n", ret);
			goto done;
		}
	}

	/* msg pages? */
	if (con->out_msg && con->out_msg->nr_pages) {
		ret = write_partial_msg_pages(con, con->out_msg);
		if (ret == 1)
			goto more_kvec;
		if (ret == 0)
			goto done;
		if (ret < 0) {
			dout(30, "try_write write_partial_msg_pages err %d\n",
			     ret);
			goto done;
		}
	}
	con->out_msg = 0; /* done with this message. */

	/* anything else pending? */
	spin_lock(&con->out_queue_lock);
	if (!list_empty(&con->out_queue)) {
		prepare_write_message(con);
	} else if (con->in_seq > con->in_seq_acked) {
		prepare_write_ack(con);
	} else {
		clear_bit(WRITE_PENDING, &con->state);
		/* hmm, nothing to do! No more writes pending? */
		dout(30, "try_write nothing else to write.\n");
		spin_unlock(&con->out_queue_lock);
		if (con->delay > 0)
			con->delay = BASE_DELAY_INTERVAL;
		goto done;
	}
	spin_unlock(&con->out_queue_lock);
	goto more;

done:
	dout(30, "try_write done on %p\n", con);
	put_connection(con);
	return;
}

/*
 * prepare to read a message
 */
static int prepare_read_message(struct ceph_connection *con)
{
	int err;
	BUG_ON(con->in_msg != NULL);
	con->in_base_pos = 0;
	con->in_msg = ceph_msg_new(0, 0, 0, 0, 0);
	if (IS_ERR(con->in_msg)) {
		/* TBD: we don't check for error in caller, handle it here? */
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
	unsigned front_len, data_len, data_off;

	dout(20, "read_message_partial con %p msg %p\n", con, m);

	/* header */
	while (con->in_base_pos < sizeof(m->hdr)) {
		left = sizeof(m->hdr) - con->in_base_pos;
		ret = ceph_tcp_recvmsg(con->sock, &m->hdr + con->in_base_pos,
				       left);
		if (ret <= 0)
			return ret;
		con->in_base_pos += ret;
	}

	/* front */
	front_len = le32_to_cpu(m->hdr.front_len);
	while (m->front.iov_len < front_len) {
		if (m->front.iov_base == NULL) {
			m->front.iov_base = kmalloc(front_len, GFP_NOFS);
			if (m->front.iov_base == NULL)
				return -ENOMEM;
		}
		left = front_len - m->front.iov_len;
		ret = ceph_tcp_recvmsg(con->sock, (char *)m->front.iov_base +
				       m->front.iov_len, left);
		if (ret <= 0)
			return ret;
		m->front.iov_len += ret;
	}

	/* (page) data */
	data_len = le32_to_cpu(m->hdr.data_len);
	data_off = le32_to_cpu(m->hdr.data_off);
	if (data_len == 0)
		goto no_data;
	if (m->nr_pages == 0) {
		con->in_msg_pos.page = 0;
		con->in_msg_pos.page_pos = data_off & ~PAGE_MASK;
		con->in_msg_pos.data_pos = 0;
		/* find pages for data payload */
		want = calc_pages_for(data_off & ~PAGE_MASK, data_len);
		ret = 0;
		BUG_ON(!con->msgr->prepare_pages);
		ret = con->msgr->prepare_pages(con->msgr->parent, m, want);
		if (ret < 0) {
			dout(10, "prepare_pages failed, skipping payload\n");
			con->in_base_pos = -data_len; /* skip payload */
			ceph_msg_put(con->in_msg);
			con->in_msg = 0;
			con->in_tag = CEPH_MSGR_TAG_READY;
			return 0;
		} else {
			BUG_ON(m->nr_pages < want);
		}
	}
	while (con->in_msg_pos.data_pos < data_len) {
		left = min((int)(data_len - con->in_msg_pos.data_pos),
			   (int)(PAGE_SIZE - con->in_msg_pos.page_pos));
		//p = kmap_atomic(m->pages[con->in_msg_pos.page], KM_USER1);
		p = page_address(m->pages[con->in_msg_pos.page]);
		ret = ceph_tcp_recvmsg(con->sock, p + con->in_msg_pos.page_pos,
				       left);
		//kunmap_atomic(p, KM_USER1);
		if (ret <= 0)
			return ret;
		con->in_msg_pos.data_pos += ret;
		con->in_msg_pos.page_pos += ret;
		if (con->in_msg_pos.page_pos == PAGE_SIZE) {
			con->in_msg_pos.page_pos = 0;
			con->in_msg_pos.page++;
		}
	}

	/* footer */
	while (con->in_base_pos < sizeof(m->hdr) + sizeof(m->footer)) {
		left = sizeof(m->hdr) + sizeof(m->footer) - con->in_base_pos;
		ret = ceph_tcp_recvmsg(con->sock, &m->footer +
				       (con->in_base_pos - sizeof(m->hdr)),
				       left);
		if (ret <= 0)
			return ret;
		con->in_base_pos += ret;
	}

no_data:
	dout(20, "read_message_partial got msg %p\n", m);

	/* did i learn my ip? */
	if (con->msgr->inst.addr.ipaddr.sin_addr.s_addr == htonl(INADDR_ANY)) {
		/*
		 * in practice, we learn our ip from the first incoming mon
		 * message, before anyone else knows we exist, so this is
		 * safe.
		 */
		con->msgr->inst.addr.ipaddr = con->in_msg->hdr.dst.addr.ipaddr;
		dout(10, "read_message_partial learned my addr is "
		     "%u.%u.%u.%u:%u\n",
		     IPQUADPORT(con->msgr->inst.addr.ipaddr));
	}

	return 1; /* done! */
}

static void process_message(struct ceph_connection *con)
{
	/* if first message, set peer_name */
	if (con->peer_name.type == 0)
		con->peer_name = con->in_msg->hdr.src.name;
	
	spin_lock(&con->out_queue_lock);
	con->in_seq++;
	spin_unlock(&con->out_queue_lock);
	ceph_queue_write(con);
	
	dout(1, "===== %p %u from %s%d %d=%s len %d+%d =====\n",
	     con->in_msg, le32_to_cpu(con->in_msg->hdr.seq),
	     ENTITY_NAME(con->in_msg->hdr.src.name),
	     le32_to_cpu(con->in_msg->hdr.type),
	     ceph_msg_type_name(le32_to_cpu(con->in_msg->hdr.type)),
	     le32_to_cpu(con->in_msg->hdr.front_len),
	     le32_to_cpu(con->in_msg->hdr.data_len));
	con->msgr->dispatch(con->msgr->parent, con->in_msg);
	con->in_msg = 0;
	con->in_tag = CEPH_MSGR_TAG_READY;
}

/*
 * prepare to read an ack
 */
static void prepare_read_ack(struct ceph_connection *con)
{
	con->in_base_pos = 0;
}

/*
 * read (part of) an ack
 */
static int read_ack_partial(struct ceph_connection *con)
{
	while (con->in_base_pos < sizeof(con->in_partial_ack)) {
		int left = sizeof(con->in_partial_ack) - con->in_base_pos;
		int ret = ceph_tcp_recvmsg(con->sock,
					   (char *)&con->in_partial_ack +
					   con->in_base_pos, left);
		if (ret <= 0)
			return ret;
		con->in_base_pos += ret;
	}
	return 1; /* done */
}

static void process_ack(struct ceph_connection *con)
{
	struct ceph_msg *m;
	u32 ack = le32_to_cpu(con->in_partial_ack);
	u64 seq;

	spin_lock(&con->out_queue_lock);
	while (!list_empty(&con->out_sent)) {
		m = list_entry(con->out_sent.next, struct ceph_msg, list_head);
		seq = le64_to_cpu(m->hdr.seq);
		if (seq > ack)
			break;
		dout(5, "got ack for seq %llu type %d at %p\n", seq,
		     le32_to_cpu(m->hdr.type), m);
		list_del_init(&m->list_head);
		ceph_msg_put(m);
	}
	spin_unlock(&con->out_queue_lock);
	con->in_tag = CEPH_MSGR_TAG_READY;
}


/*
 * read portion of connect-side handshake on a new connection
 */
static int read_connect_partial(struct ceph_connection *con)
{
	int ret, to;
	dout(20, "read_connect_partial %p at %d\n", con, con->in_base_pos);

	/* actual_peer_addr */
	to = sizeof(con->actual_peer_addr);
	while (con->in_base_pos < to) {
		int left = to - con->in_base_pos;
		int have = con->in_base_pos;
		ret = ceph_tcp_recvmsg(con->sock,
				       (char *)&con->actual_peer_addr + have,
				       left);
		if (ret <= 0)
			goto out;
		con->in_base_pos += ret;
	}

	/* in_tag */
	to += 1;
	if (con->in_base_pos < to) {
		ret = ceph_tcp_recvmsg(con->sock, &con->in_tag, 1);
		if (ret <= 0)
			goto out;
		con->in_base_pos += ret;
	}

	if (con->in_tag == CEPH_MSGR_TAG_RETRY) {
		/* peers connect_seq */
		to += sizeof(con->in_connect_seq);
		if (con->in_base_pos < to) {
			int left = to - con->in_base_pos;
			int have = sizeof(con->in_connect_seq) - left;
			ret = ceph_tcp_recvmsg(con->sock,
					       (char *)&con->in_connect_seq +
					       have, left);
			if (ret <= 0)
				goto out;
			con->in_base_pos += ret;
		}
	}
	ret = 1;
out:
	dout(20, "read_connect_partial %p end at %d ret %d\n", con,
	     con->in_base_pos, ret);
	dout(20, "read_connect_partial peer in connect_seq = %u\n",
	     le32_to_cpu(con->in_connect_seq));
	return ret; /* done */
}

/*
 * Reset a connection
 */
static void reset_connection(struct ceph_connection *con)
{
	derr(1, "%s%d %u.%u.%u.%u:%u connection reset\n", 
	     ENTITY_NAME(con->peer_name),
	     IPQUADPORT(con->peer_addr.ipaddr));

	/* reset connection, out_queue, msg_ and connect_seq */
	/* discard existing out_queue and msg_seq */
	spin_lock(&con->out_queue_lock);
	ceph_msg_put_list(&con->out_queue);
	ceph_msg_put_list(&con->out_sent);

	con->connect_seq = 0;
	con->out_seq = 0;
	con->out_msg = 0;
	con->in_seq = 0;
	con->in_msg = 0;
	spin_unlock(&con->out_queue_lock);
}

static void process_connect(struct ceph_connection *con)
{
	dout(20, "process_connect on %p tag %d\n", con, (int)con->in_tag);
	if (!ceph_entity_addr_is_local(con->peer_addr, con->actual_peer_addr)) {
		derr(1, "process_connect wrong peer, want %u.%u.%u.%u:%u/%d, "
		     "got %u.%u.%u.%u:%u/%d, wtf\n",
		     IPQUADPORT(con->peer_addr.ipaddr),
		     con->peer_addr.nonce,
		     IPQUADPORT(con->actual_peer_addr.ipaddr),
		     con->actual_peer_addr.nonce);
		set_bit(CLOSED, &con->state);
		remove_connection(con->msgr, con);
		return;
	}
	switch (con->in_tag) {
	case CEPH_MSGR_TAG_RESETSESSION:
		dout(10, "process_connect got RESET peer seq %u\n",
		     le32_to_cpu(con->in_connect_seq));
		reset_connection(con);
		prepare_write_connect_retry(con->msgr, con);
		con->msgr->peer_reset(con->msgr->parent, &con->peer_name);
		break;
	case CEPH_MSGR_TAG_RETRY:
		dout(10,
		     "process_connect got RETRY my seq = %u, peer_seq = %u\n",
		     le32_to_cpu(con->out_connect_seq),
		     le32_to_cpu(con->in_connect_seq));
		con->connect_seq = le32_to_cpu(con->in_connect_seq);
		prepare_write_connect_retry(con->msgr, con);
		break;
	case CEPH_MSGR_TAG_WAIT:
		dout(10, "process_connect peer connecting WAIT\n");
		set_bit(WAIT, &con->state);
		ceph_sock_release(con->sock);
		con->sock = NULL;
		break;
	case CEPH_MSGR_TAG_READY:
		dout(10, "process_connect got READY, now open\n");
		clear_bit(CONNECTING, &con->state);
		set_bit(OPEN, &con->state);
		break;
	default:
		derr(1, "process_connect protocol error, will retry\n");
		con->delay = BASE_DELAY_INTERVAL;
		con->error_msg = "protocol error, garbage tag during connect";
		ceph_fault(con);
	}
	if (test_bit(WRITE_PENDING, &con->state))
		ceph_queue_write(con);
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
		ret = ceph_tcp_recvmsg(con->sock,
				       (char *)&con->peer_addr + have, left);
		if (ret <= 0)
			return ret;
		con->in_base_pos += ret;
	}

	/* connect_seq */
	to += sizeof(con->in_connect_seq);
	while (con->in_base_pos < to) {
		int left = to - con->in_base_pos;
		int have = sizeof(con->peer_addr) - left;
		ret = ceph_tcp_recvmsg(con->sock,
				       (char *)&con->in_connect_seq + have,
				       left);
		if (ret <= 0)
			return ret;
		con->in_base_pos += ret;
	}
	return 1; /* done */
}

/*
 * replace another connection
 *  (old and new should be for the _same_ peer,
 *   and thus in the same pos in the radix tree)
 */
static void __replace_connection(struct ceph_messenger *msgr,
				 struct ceph_connection *old,
				 struct ceph_connection *new)
{
	clear_bit(OPEN, &old->state);

	/* take old connections message queue */
	spin_lock(&old->out_queue_lock);
	if (!list_empty(&old->out_queue))
		list_splice_init(&new->out_queue, &old->out_queue);
	spin_unlock(&old->out_queue_lock);

	new->connect_seq =  le32_to_cpu(new->in_connect_seq);
	new->out_seq = old->out_seq;

	/* replace list entry */
	list_add(&new->list_bucket, &old->list_bucket);
	list_del_init(&old->list_bucket);

	set_bit(CLOSED, &old->state);
	put_connection(old); /* dec reference count */

	set_bit(OPEN, &new->state);
	clear_bit(ACCEPTING, &new->state);
	prepare_write_accept_reply(new, &tag_ready);
}

/*
 * call after a new connection's handshake has completed
 */
static void process_accept(struct ceph_connection *con)
{
	struct ceph_connection *existing;
	struct ceph_messenger *msgr = con->msgr;
	__u32 peer_cseq = le32_to_cpu(con->in_connect_seq);

	/* do we have an existing connection for this peer? */
	radix_tree_preload(GFP_NOFS);
	spin_lock(&msgr->con_lock);
	existing = __get_connection(msgr, &con->peer_addr);
	if (existing) {
		if (peer_cseq < existing->connect_seq) {
			if (peer_cseq == 0) {
				/* reset existing connection */
				reset_connection(existing);
				/* replace connection */
				__replace_connection(msgr, existing, con);
				con->msgr->peer_reset(con->msgr->parent,
						      &con->peer_name);
			} else {
				/* old attempt or peer didn't get the READY */
				/* send retry with peers connect seq */
				con->connect_seq = existing->connect_seq;
				prepare_write_accept_retry(con, &tag_retry);
			}
		} else if (peer_cseq == existing->connect_seq &&
			   (test_bit(CONNECTING, &existing->state) ||
			    test_bit(WAIT, &existing->state))) {
			/* connection race */
			dout(20, "process_accept connection race state = %lu\n",
			     con->state);
			if (ceph_entity_addr_equal(&msgr->inst.addr,
						   &con->peer_addr)) {
				/* incoming connection wins.. */
				/* replace existing with new connection */
				__replace_connection(msgr, existing, con);
			} else {
				/* our existing outgoing connection wins..
				   tell peer to wait for our outgoing
				   connection to go through */
				prepare_write_accept_reply(con, &tag_wait);
			}
		} else if (existing->connect_seq == 0 &&
			  (peer_cseq > existing->connect_seq)) {
			/* we reset and already reconnecting */
			prepare_write_accept_reply(con, &tag_reset);
		} else {
			/* reconnect case, replace connection */
			__replace_connection(msgr, existing, con);
		}
		put_connection(existing);
	} else if (peer_cseq > 0) {
		dout(20, "process_accept no existing connection, we reset\n");
		prepare_write_accept_reply(con, &tag_reset);
	} else {
		dout(20, "process_accept no existing connection, opening\n");
		__register_connection(msgr, con);
		set_bit(OPEN, &con->state);
		con->connect_seq = peer_cseq + 1;
		prepare_write_accept_reply(con, &tag_ready);
	}
	spin_unlock(&msgr->con_lock);

	ceph_queue_write(con);
	put_connection(con);
}

/*
 * worker function when data is available on the socket
 */
static void try_read(struct work_struct *work)
{
	int ret = -1;
	struct ceph_connection *con;
	struct ceph_messenger *msgr;

	con = container_of(work, struct ceph_connection, rwork);
	dout(20, "try_read start on %p\n", con);
	msgr = con->msgr;

retry:
	if (test_and_set_bit(READING, &con->state)) {
		dout(20, "try_read already reading\n");
		goto out;
	}
	clear_bit(READABLE, &con->state);

more:
	if (test_bit(CLOSED, &con->state)) {
		dout(20, "try_read closed\n");
		goto done;
	}
	if (test_bit(ACCEPTING, &con->state)) {
		dout(20, "try_read accepting\n");
		ret = read_accept_partial(con);
		if (ret <= 0)
			goto done;
		process_accept(con);		/* accepted */
		goto more;
	}
	if (test_bit(CONNECTING, &con->state)) {
		dout(20, "try_read connecting\n");
		ret = read_connect_partial(con);
		if (ret <= 0)
			goto done;
		process_connect(con);
		goto more;
	}

	if (con->in_base_pos < 0) {
		/* skipping + discarding content */
		static char buf[1024];
		ret = ceph_tcp_recvmsg(con->sock, buf,
				       min(1024, -con->in_base_pos));
		if (ret <= 0)
			goto done;
		con->in_base_pos += ret;
		goto more;
	}
	if (con->in_tag == CEPH_MSGR_TAG_READY) {
		ret = ceph_tcp_recvmsg(con->sock, &con->in_tag, 1);
		if (ret <= 0)
			goto done;
		dout(30, "try_read got tag %d\n", (int)con->in_tag);
		switch (con->in_tag) {
		case CEPH_MSGR_TAG_MSG:
			prepare_read_message(con);
			break;
		case CEPH_MSGR_TAG_ACK:
			prepare_read_ack(con);
			break;
		case CEPH_MSGR_TAG_CLOSE:
			set_bit(CLOSED, &con->state);   /* fixme */
			goto done;
		default:
			goto bad_tag;
		}
	}
	if (con->in_tag == CEPH_MSGR_TAG_MSG) {
		ret = read_message_partial(con);
		if (ret <= 0)
			goto done;
		process_message(con);
		goto more;
	}
	if (con->in_tag == CEPH_MSGR_TAG_ACK) {
		ret = read_ack_partial(con);
		if (ret <= 0)
			goto done;
		process_ack(con);
		goto more;
	}

bad_tag:
	derr(2, "try_read bad con->in_tag = %d\n", (int)con->in_tag);
	con->error_msg = "protocol error, garbage tag";
	ceph_fault(con);

done:
	clear_bit(READING, &con->state);
	if (test_bit(READABLE, &con->state)) {
		dout(30, "try_read readable flag set again, looping\n");
		goto retry;
	}

out:
	dout(20, "try_read done on %p\n", con);
	put_connection(con);
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
		derr(1, "kmalloc failure accepting new connection\n");
		goto done;
	}

	new_con->connect_seq = 1;
	set_bit(ACCEPTING, &new_con->state);
	clear_bit(NEW, &new_con->state);
	new_con->in_tag = CEPH_MSGR_TAG_READY;  /* eventually, hopefully */

	if (ceph_tcp_accept(msgr->listen_sock, new_con) < 0) {
		derr(1, "error accepting connection\n");
		put_connection(new_con);
		goto done;
	}
	dout(5, "accepted connection \n");

	prepare_write_accept_announce(msgr, new_con);
	add_connection_accepting(msgr, new_con);

	/*
	 * hand off to worker threads ,should be able to write, we want to
	 * try to write right away, we may have missed socket state change
	 */
	ceph_queue_write(new_con);
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
	INIT_RADIX_TREE(&msgr->con_tree, GFP_KERNEL);

	msgr->zero_page = alloc_page(GFP_KERNEL | __GFP_ZERO);
	if (!msgr->zero_page) {
		kfree(msgr);
		return ERR_PTR(-ENOMEM);
	}
	kmap(msgr->zero_page);

	/* pick listening address */
	if (myaddr) {
		msgr->inst.addr = *myaddr;
	} else {
		dout(10, "create my ip not specified, binding to INADDR_ANY\n");
		msgr->inst.addr.ipaddr.sin_addr.s_addr = htonl(INADDR_ANY);
		msgr->inst.addr.ipaddr.sin_port = htons(0);  /* any port */
	}
	msgr->inst.addr.ipaddr.sin_family = AF_INET;

	/* create listening socket */
	ret = ceph_tcp_listen(msgr);
	if (ret < 0) {
		kfree(msgr);
		return ERR_PTR(ret);
	}
	if (myaddr)
		msgr->inst.addr.ipaddr.sin_addr = myaddr->ipaddr.sin_addr;

	dout(1, "messenger %p listening on %u.%u.%u.%u:%u\n", msgr,
	     IPQUADPORT(msgr->inst.addr.ipaddr));
	return msgr;
}

void ceph_messenger_destroy(struct ceph_messenger *msgr)
{
	struct ceph_connection *con;

	dout(2, "destroy %p\n", msgr);
	
	/* stop listener */
	ceph_cancel_sock_callbacks(msgr->listen_sock);
	cancel_work_sync(&msgr->awork);
	ceph_sock_release(msgr->listen_sock);

	/* kill off connections */
	spin_lock(&msgr->con_lock);
	while (!list_empty(&msgr->con_all)) {
		con = list_entry(msgr->con_all.next, struct ceph_connection,
				 list_all);
		dout(10, "destroy removing connection %p\n", con);
		set_bit(CLOSED, &con->state);
		atomic_inc(&con->nref);
		__remove_connection(msgr, con);
		
		/* in case there's queued work... */
		spin_unlock(&msgr->con_lock);
		ceph_cancel_sock_callbacks(con->sock);
		cancel_work_sync(&con->rwork);
		cancel_delayed_work_sync(&con->swork);
		put_connection(con);
		dout(10, "destroy removed connection %p\n", con);

		spin_lock(&msgr->con_lock);
	}
	spin_unlock(&msgr->con_lock);

	kunmap(msgr->zero_page);
	__free_page(msgr->zero_page);

	kfree(msgr);

	dout(10, "destroyed messenger %p\n", msgr);
}

/*
 * mark a peer down.  drop any open connection.
 */
void ceph_messenger_mark_down(struct ceph_messenger *msgr,
			      struct ceph_entity_addr *addr)
{
	struct ceph_connection *con;

	dout(2, "mark_down peer %u.%u.%u.%u:%u\n",
	     IPQUADPORT(addr->ipaddr));

	spin_lock(&msgr->con_lock);
	con = __get_connection(msgr, addr);
	if (con) {
		dout(1, "mark_down %s%d %u.%u.%u.%u:%u (%p)\n",
		     ENTITY_NAME(con->peer_name),
		     IPQUADPORT(con->peer_addr.ipaddr), con);
		set_bit(CLOSED, &con->state);  /* in case there's queued work */
		__remove_connection(msgr, con);
		put_connection(con);
	}
	spin_unlock(&msgr->con_lock);
}


/*
 * a single ceph_msg can't be queued for send twice, unless it's
 * already been delivered (i.e. we have the only remaining reference).
 * so, dup the message if there is more than once reference.
 */
struct ceph_msg *ceph_msg_maybe_dup(struct ceph_msg *old)
{
	struct ceph_msg *dup;

	if (atomic_read(&old->nref) == 1)
		return old;  /* we have only ref, all is well */
	
	dup = ceph_msg_new(le32_to_cpu(old->hdr.type), 
			   le32_to_cpu(old->hdr.front_len),
			   le32_to_cpu(old->hdr.data_len), 
			   le32_to_cpu(old->hdr.data_off),
			   old->pages);
	BUG_ON(!dup);
	memcpy(dup->front.iov_base, old->front.iov_base,
	       le32_to_cpu(old->hdr.front_len));
	
	/* revoke old message's pages */
	mutex_lock(&old->page_mutex);
	old->pages = 0;
	mutex_unlock(&old->page_mutex);

	ceph_msg_put(old);
	return dup;
}


/*
 * queue up an outgoing message.
 *
 * this consumes a msg reference.  that is, if the caller wants to
 * keep @msg around, it had better call ceph_msg_get first.
 */
int ceph_msg_send(struct ceph_messenger *msgr, struct ceph_msg *msg,
		  unsigned long timeout)
{
	struct ceph_connection *con, *newcon;
	int ret = 0;

	/* set source */
	msg->hdr.src = msgr->inst;

	/* do we have the connection? */
	spin_lock(&msgr->con_lock);
	con = __get_connection(msgr, &msg->hdr.dst.addr);
	if (!con) {
		/* drop lock while we allocate a new connection */
		spin_unlock(&msgr->con_lock);
		newcon = new_connection(msgr);
		if (IS_ERR(newcon))
			return PTR_ERR(con);
		spin_lock(&msgr->con_lock);
		con = __get_connection(msgr, &msg->hdr.dst.addr);
		if (con) {
			put_connection(newcon);
			dout(10, "ceph_msg_send (lost race and) had connection "
			     "%p to peer %u.%u.%u.%u:%u\n", con,
			     IPQUADPORT(msg->hdr.dst.addr.ipaddr));
		} else {
			con = newcon;
			con->peer_addr = msg->hdr.dst.addr;
			con->peer_name = msg->hdr.dst.name;
			__register_connection(msgr, con);
			dout(5, "ceph_msg_send new connection %p to peer "
			     "%u.%u.%u.%u:%u\n", con,
			     IPQUADPORT(msg->hdr.dst.addr.ipaddr));
		}
	} else {
		dout(10, "ceph_msg_send had connection %p to peer "
		     "%u.%u.%u.%u:%u\n", con,
		     IPQUADPORT(msg->hdr.dst.addr.ipaddr));
	}
	spin_unlock(&msgr->con_lock);
	con->delay = timeout;
	dout(10, "ceph_msg_send delay = %lu\n", con->delay);

	/* queue */
	spin_lock(&con->out_queue_lock);
	msg->hdr.seq = cpu_to_le64(++con->out_seq);
	dout(1, "----- %p %u to %s%d %d=%s len %d+%d -----\n", msg,
	     (unsigned)con->out_seq,
	     ENTITY_NAME(msg->hdr.dst.name), le32_to_cpu(msg->hdr.type),
	     ceph_msg_type_name(le32_to_cpu(msg->hdr.type)),
	     le32_to_cpu(msg->hdr.front_len),
	     le32_to_cpu(msg->hdr.data_len));
	dout(2, "ceph_msg_send queuing %p seq %llu for %s%d on %p pgs %d\n", msg,
	     le64_to_cpu(msg->hdr.seq), ENTITY_NAME(msg->hdr.dst.name), con, msg->nr_pages);
	list_add_tail(&msg->list_head, &con->out_queue);
	spin_unlock(&con->out_queue_lock);

	if (test_and_set_bit(WRITE_PENDING, &con->state) == 0)
		ceph_queue_write(con);

	put_connection(con);
	dout(30, "ceph_msg_send done\n");
	return ret;
}


/*
 * construct a new message with given type, size
 * the new msg has a ref count of 1.
 */
struct ceph_msg *ceph_msg_new(int type, int front_len,
			      int page_len, int page_off, struct page **pages)
{
	struct ceph_msg *m;

	m = kmalloc(sizeof(*m), GFP_NOFS);
	if (m == NULL)
		goto out;
	atomic_set(&m->nref, 1);
	mutex_init(&m->page_mutex);
	INIT_LIST_HEAD(&m->list_head);

	m->hdr.type = cpu_to_le32(type);
	m->hdr.front_len = cpu_to_le32(front_len);
	m->hdr.data_len = cpu_to_le32(page_len);
	m->hdr.data_off = cpu_to_le32(page_off);

	/* front */
	if (front_len) {
		m->front.iov_base = kmalloc(front_len, GFP_NOFS);
		if (m->front.iov_base == NULL) {
			derr(0, "ceph_msg_new can't allocate %d bytes\n",
			     front_len);
			goto out2;
		}
	} else {
		m->front.iov_base = 0;
	}
	m->front.iov_len = front_len;

	/* pages */
	m->nr_pages = calc_pages_for(page_off, page_len);
	m->pages = pages;

	dout(20, "ceph_msg_new %p page %d~%d -> %d\n", m, page_off, page_len, m->nr_pages);
	return m;

out2:
	ceph_msg_put(m);
out:
	derr(0, "msg_new can't create msg type %d len %d\n", type, front_len);
	return ERR_PTR(-ENOMEM);
}

void ceph_msg_put(struct ceph_msg *m)
{
	BUG_ON(atomic_read(&m->nref) <= 0);
	dout(20, "ceph_msg_put %p %d -> %d\n", m, atomic_read(&m->nref),
	     atomic_read(&m->nref)-1);
	if (atomic_dec_and_test(&m->nref)) {
		dout(20, "ceph_msg_put last one on %p\n", m);
		WARN_ON(!list_empty(&m->list_head));
		kfree(m->front.iov_base);
		kfree(m);
	}
}


