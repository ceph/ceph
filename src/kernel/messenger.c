#include <linux/crc32c.h>
#include <linux/kthread.h>
#include <linux/socket.h>
#include <linux/net.h>
#include <linux/string.h>
#include <linux/highmem.h>
#include <net/tcp.h>

#include "ceph_debug.h"

#include "ceph_fs.h"
#include "messenger.h"

int ceph_debug_msgr;
#define DOUT_MASK DOUT_MASK_MSGR
#define DOUT_VAR ceph_debug_msgr
#define DOUT_PREFIX "msgr: "
#include "super.h"


/* static tag bytes */
static char tag_ready = CEPH_MSGR_TAG_READY;
static char tag_reset = CEPH_MSGR_TAG_RESETSESSION;
static char tag_retry_session = CEPH_MSGR_TAG_RETRY_SESSION;
static char tag_retry_global = CEPH_MSGR_TAG_RETRY_GLOBAL;
static char tag_wait = CEPH_MSGR_TAG_WAIT;
static char tag_msg = CEPH_MSGR_TAG_MSG;
static char tag_ack = CEPH_MSGR_TAG_ACK;

static void con_work(struct work_struct *);
static void try_accept(struct work_struct *);


/*
 *  workqueue
 */
struct workqueue_struct *ceph_msgr_wq;

int ceph_msgr_init(void)
{
	ceph_msgr_wq = create_workqueue("ceph-msgr");
	if (IS_ERR(ceph_msgr_wq)) {
		int ret = PTR_ERR(ceph_msgr_wq);
		derr(0, "failed to create workqueue: %d\n", ret);
		ceph_msgr_wq = 0;
		return ret;
	}
	return 0;
}

void ceph_msgr_exit(void)
{
	destroy_workqueue(ceph_msgr_wq);
}


/*
 * socket callback functions
 */

/* listen socket received a connect */
static void ceph_accept_ready(struct sock *sk, int count_unused)
{
	struct ceph_messenger *msgr = (struct ceph_messenger *)sk->sk_user_data;

	dout(30, "ceph_accept_ready messenger %p sk_state = %u\n",
	     msgr, sk->sk_state);
	if (sk->sk_state == TCP_LISTEN)
		queue_work(ceph_msgr_wq, &msgr->awork);
}

/* Data available on socket or listen socket received a connect */
static void ceph_data_ready(struct sock *sk, int count_unused)
{
	struct ceph_connection *con =
		(struct ceph_connection *)sk->sk_user_data;
	if (sk->sk_state != TCP_CLOSE_WAIT) {
		dout(30, "ceph_data_ready on %p state = %lu, queuing rwork\n",
		     con, con->state);
		ceph_queue_con(con);
	}
}

/* socket has bufferspace for writing */
static void ceph_write_space(struct sock *sk)
{
	struct ceph_connection *con =
		(struct ceph_connection *)sk->sk_user_data;

	dout(30, "ceph_write_space %p state = %lu\n", con, con->state);

	/* only queue to workqueue if a WRITE is pending */
	if (test_bit(WRITE_PENDING, &con->state)) {
		dout(30, "ceph_write_space %p queuing write work\n", con);
		ceph_queue_con(con);
	}

	/* Since we have our own write_space, Clear the SOCK_NOSPACE flag */
	clear_bit(SOCK_NOSPACE, &sk->sk_socket->flags);
}

/* sockets state has change */
static void ceph_state_change(struct sock *sk)
{
	struct ceph_connection *con =
		(struct ceph_connection *)sk->sk_user_data;

	dout(30, "ceph_state_change %p state = %lu sk_state = %u\n",
	     con, con->state, sk->sk_state);

	if (test_bit(CLOSED, &con->state))
		return;

	switch (sk->sk_state) {
	case TCP_CLOSE:
		dout(30, "ceph_state_change TCP_CLOSE\n");
	case TCP_CLOSE_WAIT:
		dout(30, "ceph_state_change TCP_CLOSE_WAIT\n");
		set_bit(SOCK_CLOSED, &con->state);
		if (test_bit(CONNECTING, &con->state))
			con->error_msg = "connection failed";
		else
			con->error_msg = "socket closed";
		ceph_queue_con(con);
		break;
	case TCP_ESTABLISHED:
		dout(30, "ceph_state_change TCP_ESTABLISHED\n");
		ceph_write_space(sk);
		break;
	}
}

/* make a listening socket active by setting up the data ready call back */
static void listen_sock_callbacks(struct socket *sock,
				  struct ceph_messenger *msgr)
{
	struct sock *sk = sock->sk;
	sk->sk_user_data = (void *)msgr;
	sk->sk_data_ready = ceph_accept_ready;
}

/* make a socket active by setting up the call back functions */
static void set_sock_callbacks(struct socket *sock,
			       struct ceph_connection *con)
{
	struct sock *sk = sock->sk;
	sk->sk_user_data = (void *)con;
	sk->sk_data_ready = ceph_data_ready;
	sk->sk_write_space = ceph_write_space;
	sk->sk_state_change = ceph_state_change;
}


/*
 * socket helpers
 */

/*
 * initiate connection to a remote socket.
 */
struct socket *ceph_tcp_connect(struct ceph_connection *con)
{
	int ret;
	struct sockaddr *paddr = (struct sockaddr *)&con->peer_addr.ipaddr;
	struct socket *sock;

	ret = sock_create_kern(AF_INET, SOCK_STREAM, IPPROTO_TCP, &sock);
	if (ret)
		return ERR_PTR(ret);

	con->sock = sock;
	sock->sk->sk_allocation = GFP_NOFS;

	set_sock_callbacks(sock, con);

	dout (20, "connect %u.%u.%u.%u:%u\n",
	             IPQUADPORT(*(struct sockaddr_in *)paddr));

	ret = sock->ops->connect(sock, paddr,
				 sizeof(struct sockaddr_in), O_NONBLOCK);
	if (ret == -EINPROGRESS) {
		dout(20, "connect EINPROGRESS sk_state = = %u\n",
		     sock->sk->sk_state);
		ret = 0;
	}
	if (ret < 0) {
		/* TBD check for fatal errors, retry if not fatal.. */
		derr(1, "connect %u.%u.%u.%u:%u error: %d\n",
		     IPQUADPORT(*(struct sockaddr_in *)paddr), ret);
		sock_release(sock);
		con->sock = 0;
	}

	if (ret < 0)
		return ERR_PTR(ret);
	return sock;
}

/*
 * setup listening socket
 */
int ceph_tcp_listen(struct ceph_messenger *msgr)
{
	int ret;
	int optval = 1;
	struct sockaddr_in *myaddr = &msgr->inst.addr.ipaddr;
	int nlen;
	struct socket *sock;

	ret = sock_create_kern(AF_INET, SOCK_STREAM, IPPROTO_TCP, &sock);
	if (ret)
		return ret;

	sock->sk->sk_allocation = GFP_NOFS;

	ret = kernel_setsockopt(sock, SOL_SOCKET, SO_REUSEADDR,
				(char *)&optval, sizeof(optval));
	if (ret < 0) {
		derr(0, "Failed to set SO_REUSEADDR: %d\n", ret);
		goto err;
	}

	ret = sock->ops->bind(sock, (struct sockaddr *)myaddr,
			      sizeof(*myaddr));
	if (ret < 0) {
		derr(0, "Failed to bind: %d\n", ret);
		goto err;
	}

	/* what port did we bind to? */
	nlen = sizeof(*myaddr);
	ret = sock->ops->getname(sock, (struct sockaddr *)myaddr, &nlen,
				 0);
	if (ret < 0) {
		derr(0, "failed to getsockname: %d\n", ret);
		goto err;
	}
	dout(10, "listen on port %d\n", ntohs(myaddr->sin_port));

	ret = kernel_setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE,
				(char *)&optval, sizeof(optval));
	if (ret < 0) {
		derr(0, "Failed to set SO_KEEPALIVE: %d\n", ret);
		goto err;
	}

	/* TBD: probaby want to tune the backlog queue .. */
	ret = sock->ops->listen(sock, CEPH_MSGR_BACKUP);
	if (ret < 0) {
		derr(0, "kernel_listen error: %d\n", ret);
		goto err;
	}

	/* ok! */
	msgr->listen_sock = sock;
	listen_sock_callbacks(sock, msgr);
	return ret;

err:
	sock_release(sock);
	return ret;
}

/*
 *  accept a connection
 */
int ceph_tcp_accept(struct socket *lsock, struct ceph_connection *con)
{
	int ret;
	struct sockaddr *paddr = (struct sockaddr *)&con->peer_addr.ipaddr;
	int len;
	struct socket *sock;

	ret = sock_create_kern(AF_INET, SOCK_STREAM, IPPROTO_TCP, &sock);
	if (ret)
		return ret;
	con->sock = sock;

	sock->sk->sk_allocation = GFP_NOFS;

	ret = lsock->ops->accept(lsock, sock, O_NONBLOCK);
	if (ret < 0) {
		derr(0, "accept error: %d\n", ret);
		goto err;
	}

	sock->ops = lsock->ops;
	sock->type = lsock->type;
	ret = sock->ops->getname(sock, paddr, &len, 2);
	if (ret < 0) {
		derr(0, "getname error: %d\n", ret);
		goto err;
	}

	/* setup callbacks */
	set_sock_callbacks(sock, con);

	return ret;

err:
	sock->ops->shutdown(sock, SHUT_RDWR);
	sock_release(sock);
	return ret;
}

/*
 * receive a message this may return after partial send
 */
int ceph_tcp_recvmsg(struct socket *sock, void *buf, size_t len)
{
	struct kvec iov = {buf, len};
	struct msghdr msg = {.msg_flags = 0};
	int rlen = 0;		/* length read */

	msg.msg_flags |= MSG_DONTWAIT | MSG_NOSIGNAL;
	rlen = kernel_recvmsg(sock, &msg, &iov, 1, len, msg.msg_flags);
	return(rlen);
}


/*
 * Send a message this may return after partial send
 */
int ceph_tcp_sendmsg(struct socket *sock, struct kvec *iov,
		     size_t kvlen, size_t len, int more)
{
	struct msghdr msg = {.msg_flags = 0};
	int rlen = 0;

	msg.msg_flags |= MSG_DONTWAIT | MSG_NOSIGNAL;
	if (more)
		msg.msg_flags |= MSG_MORE;
	else
		msg.msg_flags |= MSG_EOR;  /* superfluous, but what the hell */

	rlen = kernel_sendmsg(sock, &msg, iov, kvlen, len);

	return(rlen);
}



/*
 * create a new connection.
 */
static struct ceph_connection *new_connection(struct ceph_messenger *msgr)
{
	struct ceph_connection *con;
	con = kzalloc(sizeof(struct ceph_connection), GFP_NOFS);
	if (con == NULL)
		return NULL;

	con->msgr = msgr;
	atomic_set(&con->nref, 1);

	INIT_LIST_HEAD(&con->list_all);
	INIT_LIST_HEAD(&con->list_bucket);

	spin_lock_init(&con->out_queue_lock);
	INIT_LIST_HEAD(&con->out_queue);
	INIT_LIST_HEAD(&con->out_sent);

	INIT_DELAYED_WORK(&con->work, con_work);

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
		return NULL;
	con = list_entry(head, struct ceph_connection, list_bucket);
	if (memcmp(&con->peer_addr, addr, sizeof(addr)) == 0)
		goto yes;
	list_for_each(p, head) {
		con = list_entry(p, struct ceph_connection, list_bucket);
		if (memcmp(&con->peer_addr, addr, sizeof(addr)) == 0)
			goto yes;
	}

	return NULL;

yes:
	atomic_inc(&con->nref);
	dout(20, "get_connection %p nref = %d -> %d\n", con,
	     atomic_read(&con->nref) - 1, atomic_read(&con->nref));
	return con;
}


/*
 * close connection socket
 */
static int con_close_socket(struct ceph_connection *con)
{
	int rc;
	dout(10, "con_close_socket on %p sock %p\n", con, con->sock);
	if (!con->sock)
		return 0;
	rc = con->sock->ops->shutdown(con->sock, SHUT_RDWR);
	sock_release(con->sock);
	con->sock = 0;

	return rc;
}

/*
 * drop a reference
 */
static void put_connection(struct ceph_connection *con)
{
	dout(20, "put_connection %p nref = %d -> %d\n", con,
	     atomic_read(&con->nref), atomic_read(&con->nref) - 1);
	if (atomic_dec_and_test(&con->nref)) {
		dout(20, "put_connection %p destroying\n", con);
		ceph_msg_put_list(&con->out_queue);
		ceph_msg_put_list(&con->out_sent);
		set_bit(CLOSED, &con->state);
		con_close_socket(con); /* silently ignore possible errors */
		kfree(con);
	}
}

/*
 * add to connections tree
 */
static int __register_connection(struct ceph_messenger *msgr,
				  struct ceph_connection *con)
{
	struct list_head *head;
	unsigned long key = hash_addr(&con->peer_addr);
	int rc = 0;

	/* inc ref count */
	atomic_inc(&con->nref);
	dout(20, "add_connection %p %d -> %d\n", con,
	     atomic_read(&con->nref) - 1, atomic_read(&con->nref));

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
		rc = radix_tree_insert(&msgr->con_tree, key, &con->list_bucket);

		if (rc < 0) {
			list_del(&con->list_all);
			return rc;
		}
	}
	set_bit(REGISTERED, &con->state);

	return 0;
}

static void add_connection_accepting(struct ceph_messenger *msgr,
				     struct ceph_connection *con)
{
	atomic_inc(&con->nref);
	dout(20, "add_connection_accepting %p nref = %d -> %d\n", con,
	     atomic_read(&con->nref) - 1, atomic_read(&con->nref));
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
 * atomically queue work on a connection.  bump reference to avoid
 * races with connection teardown.
 */
void ceph_queue_con(struct ceph_connection *con)
{
	if (test_bit(WAIT, &con->state) ||
	    test_bit(CLOSED, &con->state) ||
	    test_bit(BACKOFF, &con->state)) {
		dout(40, "ceph_queue_con %p ignoring: WAIT|CLOSED|BACKOFF\n",
		     con);
		return;
	}

	atomic_inc(&con->nref);
	dout(40, "ceph_queue_con %p %d -> %d\n", con,
	     atomic_read(&con->nref) - 1, atomic_read(&con->nref));

	set_bit(QUEUED, &con->state);
	if (test_bit(BUSY, &con->state) ||
	    !queue_work(ceph_msgr_wq, &con->work.work)) {
		dout(40, "ceph_queue_con %p - already BUSY or queued\n", con);
		put_connection(con);
	}
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

	if (test_bit(LOSSYTX, &con->state)) {
		dout(30, "fault on LOSSYTX channel\n");
		remove_connection(con->msgr, con);
		return;
	}

	con_close_socket(con);

	/* hmm? */
	BUG_ON(test_bit(WAIT, &con->state));

	/*
	 * If there are no messages in the queue, place the
	 * connection in a STANDBY state.  otherwise, retry with
	 * delay
	 */
	spin_lock(&con->out_queue_lock);
	if (list_empty(&con->out_queue)) {
		dout(10, "fault setting STANDBY\n");
		set_bit(STANDBY, &con->state);
		spin_unlock(&con->out_queue_lock);
		return;
	}

	dout(10, "fault setting BACKOFF\n");
	set_bit(BACKOFF, &con->state);

	if (con->delay == 0)
		con->delay = BASE_DELAY_INTERVAL;
	else if (con->delay < MAX_DELAY_INTERVAL)
		con->delay *= 2;

	atomic_inc(&con->nref);
	dout(40, "fault queueing %p %d -> %d delay %lu\n", con,
	     atomic_read(&con->nref) - 1, atomic_read(&con->nref),
	     con->delay);
	queue_delayed_work(ceph_msgr_wq, &con->work,
			   round_jiffies_relative(con->delay));

	list_splice_init(&con->out_sent, &con->out_queue);
	spin_unlock(&con->out_queue_lock);
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
	int ret;
	unsigned data_len = le32_to_cpu(msg->hdr.data_len);
	struct ceph_client *client = con->msgr->parent;
	int crc = !(client->mount_args.flags & CEPH_MOUNT_NOCRC);
	size_t len;

	dout(30, "write_partial_msg_pages con %p msg %p on %d/%d offset %d\n",
	     con, con->out_msg, con->out_msg_pos.page, con->out_msg->nr_pages,
	     con->out_msg_pos.page_pos);

	while (con->out_msg_pos.page < con->out_msg->nr_pages) {
		struct page *page = NULL;
		void *kaddr = 0;

		mutex_lock(&msg->page_mutex);
		if (msg->pages) {
			page = msg->pages[con->out_msg_pos.page];
			if (crc)
				kaddr = kmap(page);
		} else {
			/*dout(60, "using zero page\n");*/
			if (crc)
				kaddr = page_address(con->msgr->zero_page);
		}
		len = min((int)(PAGE_SIZE - con->out_msg_pos.page_pos),
			  (int)(data_len - con->out_msg_pos.data_pos));
		if (crc && !con->out_msg_pos.did_page_crc) {
			void *base = kaddr + con->out_msg_pos.page_pos;

			con->out_msg->footer.data_crc =
				crc32c_le(con->out_msg->footer.data_crc,
					  base, len);
			con->out_msg_pos.did_page_crc = 1;
		}

		if (msg->pages)
			ret = kernel_sendpage(con->sock, page,
					      con->out_msg_pos.page_pos, len,
					      MSG_DONTWAIT | MSG_NOSIGNAL |
					      MSG_MORE);
		else
			ret = kernel_sendpage(con->sock, con->msgr->zero_page,
					      con->out_msg_pos.page_pos, len,
					      MSG_DONTWAIT | MSG_NOSIGNAL |
					      MSG_MORE);

		if (crc && msg->pages)
			kunmap(page);

		mutex_unlock(&msg->page_mutex);
		if (ret <= 0)
			goto out;
		con->out_msg_pos.data_pos += ret;
		con->out_msg_pos.page_pos += ret;

		if (ret == len) {
			con->out_msg_pos.page_pos = 0;
			con->out_msg_pos.page++;
			con->out_msg_pos.did_page_crc = 0;
		}
	}

	/* done with data pages */
	dout(30, "write_partial_msg_pages wrote all pages on %p\n", con);

	/* queue up footer, too */
	if (!crc)
		con->out_msg->footer.flags |= CEPH_MSG_FOOTER_NOCRC;
	con->out_kvec[0].iov_base = &con->out_msg->footer;
	con->out_kvec_bytes = con->out_kvec[0].iov_len =
		sizeof(con->out_msg->footer);
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
	con->out_more = 1;  /* data? */

	/* pages */
	con->out_msg_pos.page = 0;
	con->out_msg_pos.page_pos = le32_to_cpu(m->hdr.data_off) & ~PAGE_MASK;
	con->out_msg_pos.data_pos = 0;
	con->out_msg_pos.did_page_crc = 0;

	/* fill in crc (except data pages), footer */
	con->out_msg->hdr.crc = crc32c_le(0, (void *)&m->hdr,
					  sizeof(m->hdr) - sizeof(m->hdr.crc));
	con->out_msg->footer.flags = 0;
	con->out_msg->footer.front_crc = crc32c_le(0, m->front.iov_base,
						   m->front.iov_len);
	con->out_msg->footer.data_crc = 0;

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

static u32 get_global_seq(struct ceph_messenger *msgr, u32 gt)
{
	u32 ret;
	spin_lock(&msgr->global_seq_lock);
	if (msgr->global_seq < gt)
		msgr->global_seq = gt;
	ret = ++msgr->global_seq;
	spin_unlock(&msgr->global_seq_lock);
	return ret;
}

static void prepare_write_connect(struct ceph_messenger *msgr,
				  struct ceph_connection *con)
{
	con->out_connect.connect_seq = cpu_to_le32(con->connect_seq);
	con->out_connect.global_seq = cpu_to_le32(con->global_seq);
	con->out_connect.flags = 0;
	if (test_bit(LOSSYTX, &con->state))
		con->out_connect.flags = CEPH_MSG_CONNECT_LOSSYTX;

	con->out_kvec[0].iov_base = CEPH_BANNER;
	con->out_kvec[0].iov_len = strlen(CEPH_BANNER);
	con->out_kvec[1].iov_base = &msgr->inst.addr;
	con->out_kvec[1].iov_len = sizeof(msgr->inst.addr);
	con->out_kvec[2].iov_base = &con->out_connect;
	con->out_kvec[2].iov_len = sizeof(con->out_connect);
	con->out_kvec_left = 3;
	con->out_kvec_bytes = strlen(CEPH_BANNER) +
		sizeof(msgr->inst.addr) +
		sizeof(con->out_connect);
	con->out_kvec_cur = con->out_kvec;
	con->out_more = 0;
	set_bit(WRITE_PENDING, &con->state);
}

static void prepare_write_connect_retry(struct ceph_messenger *msgr,
					struct ceph_connection *con)
{
	con->out_connect.connect_seq = cpu_to_le32(con->connect_seq);
	con->out_connect.global_seq = cpu_to_le32(con->global_seq);

	con->out_kvec[0].iov_base = &con->out_connect;
	con->out_kvec[0].iov_len = sizeof(con->out_connect);
	con->out_kvec_left = 1;
	con->out_kvec_bytes = sizeof(con->out_connect);
	con->out_kvec_cur = con->out_kvec;
	con->out_more = 0;
	set_bit(WRITE_PENDING, &con->state);
}

static void prepare_write_accept_hello(struct ceph_messenger *msgr,
				       struct ceph_connection *con)
{
	int len = strlen(CEPH_BANNER);

	con->out_kvec[0].iov_base = CEPH_BANNER;
	con->out_kvec[0].iov_len = len;
	con->out_kvec[1].iov_base = &msgr->inst.addr;
	con->out_kvec[1].iov_len = sizeof(msgr->inst.addr);
	con->out_kvec_left = 2;
	con->out_kvec_bytes = len + sizeof(msgr->inst.addr);
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

static void prepare_write_accept_ready(struct ceph_connection *con)
{
	con->out_connect.flags = 0;
	if (test_bit(LOSSYTX, &con->state))
		con->out_connect.flags = CEPH_MSG_CONNECT_LOSSYTX;

	con->out_kvec[0].iov_base = &tag_ready;
	con->out_kvec[0].iov_len = 1;
	con->out_kvec[1].iov_base = &con->out_connect.flags;
	con->out_kvec[1].iov_len = 1;
	con->out_kvec_left = 2;
	con->out_kvec_bytes = 2;
	con->out_kvec_cur = con->out_kvec;
	con->out_more = 0;
	set_bit(WRITE_PENDING, &con->state);
}

static void prepare_write_accept_retry(struct ceph_connection *con, char *ptag,
				       u32 *pseq)
{
	con->out_kvec[0].iov_base = ptag;
	con->out_kvec[0].iov_len = 1;
	con->out_kvec[1].iov_base = pseq;
	con->out_kvec[1].iov_len = 4;
	con->out_kvec_left = 2;
	con->out_kvec_bytes = 1 + 4;
	con->out_kvec_cur = con->out_kvec;
	con->out_more = 0;
	set_bit(WRITE_PENDING, &con->state);

	/* we'll re-read the connect request, but not the hello */
	con->in_base_pos = strlen(CEPH_BANNER) + sizeof(con->msgr->inst.addr);
}

/*
 * worker function when socket is writeable
 */
static int try_write(struct ceph_connection *con)
{
	struct ceph_messenger *msgr = con->msgr;
	int ret = 1;

	dout(30, "try_write start %p state %lu nref %d\n", con, con->state,
	     atomic_read(&con->nref));

more:
	dout(30, "try_write out_kvec_bytes %d\n", con->out_kvec_bytes);

	/* initiate connect? */
	if (con->sock == 0) {
		if (test_and_clear_bit(STANDBY, &con->state))
			con->connect_seq++;
		con->global_seq = get_global_seq(msgr, 0);
		prepare_write_connect(msgr, con);
		prepare_read_connect(con);
		set_bit(CONNECTING, &con->state);
		con->in_tag = CEPH_MSGR_TAG_READY;
		dout(5, "try_write initiating connect on %p new state %lu\n",
		     con, con->state);
		BUG_ON(con->sock);
		con->sock = ceph_tcp_connect(con);
		dout(10, "tcp_connect returned %p\n", con->sock);
		if (IS_ERR(con->sock)) {
			con->sock = 0;
			con->error_msg = "connect error";
			ret = -1;
			goto out;
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
	if (con->out_msg) {
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
		goto done;
	}
	spin_unlock(&con->out_queue_lock);
	goto more;

done:
	ret = 0;
out:
	dout(30, "try_write done on %p\n", con);
	return ret;
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
	con->in_front_crc = con->in_data_crc = 0;
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
	int to, want, left;
	unsigned front_len, data_len, data_off;

	dout(20, "read_message_partial con %p msg %p\n", con, m);

	/* header */
	while (con->in_base_pos < sizeof(m->hdr)) {
		left = sizeof(m->hdr) - con->in_base_pos;
		ret = ceph_tcp_recvmsg(con->sock,
				       (char *)&m->hdr + con->in_base_pos,
				       left);
		if (ret <= 0)
			return ret;
		con->in_base_pos += ret;
		if (con->in_base_pos == sizeof(m->hdr)) {
			u32 crc = crc32c_le(0, (void *)&m->hdr,
				    sizeof(m->hdr) - sizeof(m->hdr.crc));
			if (crc != m->hdr.crc) {
				derr(0, "read_message_partial %p bad hdr crc"
				     " %u != expected %u\n",
				     m, crc, m->hdr.crc);
				return -EIO;
			}
		}
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
		if (m->front.iov_len == front_len)
			con->in_front_crc = crc32c_le(0, m->front.iov_base,
						      m->front.iov_len);
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
			con->in_base_pos = -data_len - sizeof(m->footer);
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
		mutex_lock(&m->page_mutex);
		if (!m->pages) {
			dout(10, "pages revoked during msg read\n");
			mutex_unlock(&m->page_mutex);
			con->in_base_pos = con->in_msg_pos.data_pos - data_len -
				sizeof(m->footer);
			ceph_msg_put(m);
			con->in_msg = 0;
			con->in_tag = CEPH_MSGR_TAG_READY;
			return 0;
		}
		p = kmap(m->pages[con->in_msg_pos.page]);
		ret = ceph_tcp_recvmsg(con->sock, p + con->in_msg_pos.page_pos,
				       left);
		if (ret > 0)
			con->in_data_crc =
				crc32c_le(con->in_data_crc,
					  p + con->in_msg_pos.page_pos, ret);
		kunmap(m->pages[con->in_msg_pos.page]);
		mutex_unlock(&m->page_mutex);
		if (ret <= 0)
			return ret;
		con->in_msg_pos.data_pos += ret;
		con->in_msg_pos.page_pos += ret;
		if (con->in_msg_pos.page_pos == PAGE_SIZE) {
			con->in_msg_pos.page_pos = 0;
			con->in_msg_pos.page++;
		}
	}

no_data:
	/* footer */
	to = sizeof(m->hdr) + sizeof(m->footer);
	while (con->in_base_pos < to) {
		left = to - con->in_base_pos;
		ret = ceph_tcp_recvmsg(con->sock, (char *)&m->footer +
				       (con->in_base_pos - sizeof(m->hdr)),
				       left);
		if (ret <= 0)
			return ret;
		con->in_base_pos += ret;
	}
	dout(20, "read_message_partial got msg %p\n", m);

	/* crc ok? */
	if (con->in_front_crc != m->footer.front_crc) {
		derr(0, "read_message_partial %p front crc %u != expected %u\n",
		     con->in_msg,
		     con->in_front_crc, m->footer.front_crc);
		return -EIO;
	}
	if (con->in_data_crc != m->footer.data_crc) {
		derr(0, "read_message_partial %p data crc %u != expected %u\n",
		     con->in_msg,
		     con->in_data_crc, m->footer.data_crc);
		return -EIO;
	}

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

	dout(1, "===== %p %u from %s%d %d=%s len %d+%d (%u %u) =====\n",
	     con->in_msg, le32_to_cpu(con->in_msg->hdr.seq),
	     ENTITY_NAME(con->in_msg->hdr.src.name),
	     le32_to_cpu(con->in_msg->hdr.type),
	     ceph_msg_type_name(le32_to_cpu(con->in_msg->hdr.type)),
	     le32_to_cpu(con->in_msg->hdr.front_len),
	     le32_to_cpu(con->in_msg->hdr.data_len),
	     con->in_front_crc, con->in_data_crc);
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

	/* peer's banner */
	to = strlen(CEPH_BANNER);
	while (con->in_base_pos < to) {
		int left = to - con->in_base_pos;
		int have = con->in_base_pos;
		ret = ceph_tcp_recvmsg(con->sock,
				       (char *)&con->in_banner + have,
				       left);
		if (ret <= 0)
			goto out;
		con->in_base_pos += ret;
	}

	/* peer's addr */
	to += sizeof(con->actual_peer_addr);
	while (con->in_base_pos < to) {
		int left = to - con->in_base_pos;
		int have = sizeof(con->actual_peer_addr) - left;
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

	if (con->in_tag == CEPH_MSGR_TAG_READY) {
		to++;
		if (con->in_base_pos < to) {
			ret = ceph_tcp_recvmsg(con->sock,
					       (char *)&con->in_flags, 1);
			if (ret <= 0)
				goto out;
			con->in_base_pos += ret;
		}
	}

	if (con->in_tag == CEPH_MSGR_TAG_RETRY_SESSION) {
		/* peer's connect_seq */
		to += sizeof(con->in_connect.connect_seq);
		if (con->in_base_pos < to) {
			int left = to - con->in_base_pos;
			int have = sizeof(con->in_connect.connect_seq) - left;
			ret = ceph_tcp_recvmsg(con->sock,
			       (char *)&con->in_connect.connect_seq +
			       have, left);
			if (ret <= 0)
				goto out;
			con->in_base_pos += ret;
		}
	}
	if (con->in_tag == CEPH_MSGR_TAG_RETRY_GLOBAL) {
		/* peer's global_seq */
		to += sizeof(con->in_connect.global_seq);
		if (con->in_base_pos < to) {
			int left = to - con->in_base_pos;
			int have = sizeof(con->in_connect.global_seq) - left;
			ret = ceph_tcp_recvmsg(con->sock,
			       (char *)&con->in_connect.global_seq +
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
	dout(20, "read_connect_partial connect_seq = %u, global_seq = %u\n",
	     le32_to_cpu(con->in_connect.connect_seq),
	     le32_to_cpu(con->in_connect.global_seq));
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

static int verify_hello(struct ceph_connection *con)
{
	if (memcmp(con->in_banner, CEPH_BANNER, strlen(CEPH_BANNER))) {
		derr(10, "connection from %u.%u.%u.%u:%u with bad banner\n",
		     IPQUADPORT(con->peer_addr.ipaddr));
		con->error_msg = "protocol error, bad banner";
		return -1;
	}
	return 0;
}

static int process_connect(struct ceph_connection *con)
{
	dout(20, "process_connect on %p tag %d\n", con, (int)con->in_tag);

	if (verify_hello(con) < 0)
		return -1;

	/* verify peer addr */
	if (!ceph_entity_addr_is_local(&con->peer_addr,
				       &con->actual_peer_addr) &&
	    con->actual_peer_addr.ipaddr.sin_addr.s_addr != 0) {
		derr(1, "process_connect wrong peer, want %u.%u.%u.%u:%u/%d, "
		     "got %u.%u.%u.%u:%u/%d, wtf\n",
		     IPQUADPORT(con->peer_addr.ipaddr),
		     con->peer_addr.nonce,
		     IPQUADPORT(con->actual_peer_addr.ipaddr),
		     con->actual_peer_addr.nonce);
		con->error_msg = "protocol error, wrong peer";
		return -1;
	}

	switch (con->in_tag) {
	case CEPH_MSGR_TAG_RESETSESSION:
		dout(10, "process_connect got RESET peer seq %u\n",
		     le32_to_cpu(con->in_connect.connect_seq));
		reset_connection(con);
		prepare_write_connect_retry(con->msgr, con);
		prepare_read_connect(con);
		con->msgr->peer_reset(con->msgr->parent, &con->peer_addr,
				      &con->peer_name);
		break;
	case CEPH_MSGR_TAG_RETRY_SESSION:
		dout(10,
		     "process_connect got RETRY my seq = %u, peer_seq = %u\n",
		     le32_to_cpu(con->out_connect.connect_seq),
		     le32_to_cpu(con->in_connect.connect_seq));
		con->connect_seq = le32_to_cpu(con->in_connect.connect_seq);
		prepare_write_connect_retry(con->msgr, con);
		prepare_read_connect(con);
		break;
	case CEPH_MSGR_TAG_RETRY_GLOBAL:
		dout(10,
		     "process_connect got RETRY_GLOBAL my %u, peer_gseq = %u\n",
		     con->global_seq, le32_to_cpu(con->in_connect.global_seq));
		con->global_seq =
			get_global_seq(con->msgr,
				       le32_to_cpu(con->in_connect.global_seq));
		prepare_write_connect_retry(con->msgr, con);
		prepare_read_connect(con);
		break;
	case CEPH_MSGR_TAG_WAIT:
		dout(10, "process_connect peer connecting WAIT\n");
		set_bit(WAIT, &con->state);
		con_close_socket(con);
		break;
	case CEPH_MSGR_TAG_READY:
		dout(10, "process_connect got READY, now open\n");
		clear_bit(CONNECTING, &con->state);
		con->lossy_rx = con->in_flags & CEPH_MSG_CONNECT_LOSSYTX;
		con->delay = 0;  /* reset backoffmemory */
		break;
	default:
		derr(1, "process_connect protocol error, will retry\n");
		con->error_msg = "protocol error, garbage tag during connect";
		return -1;
	}
	return 0;
}


/*
 * read portion of accept-side handshake on a newly accepted connection
 */
static int read_accept_partial(struct ceph_connection *con)
{
	int ret;
	int to;

	/* banner */
	to = strlen(CEPH_BANNER);
	while (con->in_base_pos < to) {
		int left = to - con->in_base_pos;
		int have = con->in_base_pos;
		ret = ceph_tcp_recvmsg(con->sock,
				       (char *)&con->in_banner + have, left);
		if (ret <= 0)
			return ret;
		con->in_base_pos += ret;
	}

	/* peer_addr */
	to += sizeof(con->peer_addr);
	while (con->in_base_pos < to) {
		int left = to - con->in_base_pos;
		int have = sizeof(con->peer_addr) - left;
		ret = ceph_tcp_recvmsg(con->sock,
				       (char *)&con->peer_addr + have,
				       left);
		if (ret <= 0)
			return ret;
		con->in_base_pos += ret;
	}

       /* connect */
       to += sizeof(con->in_connect);
       while (con->in_base_pos < to) {
	       int left = to - con->in_base_pos;
	       int have = sizeof(u32) - left;
	       ret = ceph_tcp_recvmsg(con->sock,
	                              (char *)&con->in_connect + have, left);
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
	/* take old connections message queue */
	spin_lock(&old->out_queue_lock);
	if (!list_empty(&old->out_queue))
		list_splice_init(&new->out_queue, &old->out_queue);
	spin_unlock(&old->out_queue_lock);

	new->connect_seq = le32_to_cpu(new->in_connect.connect_seq);
	new->out_seq = old->out_seq;

	/* replace list entry */
	list_add(&new->list_bucket, &old->list_bucket);
	list_del_init(&old->list_bucket);

	set_bit(CLOSED, &old->state);
	put_connection(old); /* dec reference count */

	clear_bit(ACCEPTING, &new->state);
	prepare_write_accept_ready(new);
}

/*
 * call after a new connection's handshake has completed
 */
static int process_accept(struct ceph_connection *con)
{
	struct ceph_connection *existing;
	struct ceph_messenger *msgr = con->msgr;
	u32 peer_gseq = le32_to_cpu(con->in_connect.global_seq);
	u32 peer_cseq = le32_to_cpu(con->in_connect.connect_seq);

	if (verify_hello(con) < 0)
		return -1;

	/* note flags */
	con->lossy_rx = con->in_flags & CEPH_MSG_CONNECT_LOSSYTX;

	/* connect */
	/* do we have an existing connection for this peer? */
	if (radix_tree_preload(GFP_NOFS) < 0) {
		derr(10, "ENOMEM in process_accept\n");
		con->error_msg = "out of memory";
		return -1;
	}
	spin_lock(&msgr->con_lock);
	existing = __get_connection(msgr, &con->peer_addr);
	if (existing) {
		if (peer_gseq < existing->global_seq) {
			/* retry_global */
			con->global_seq = existing->global_seq;
			con->out_connect.global_seq =
				cpu_to_le32(con->global_seq);
			prepare_write_accept_retry(con,
					   &tag_retry_global,
					   &con->out_connect.global_seq);
		} else if (test_bit(LOSSYTX, &existing->state)) {
			dout(20, "process_accept replacing existing LOSSYTX %p\n",
			     existing);
			reset_connection(existing);
			__replace_connection(msgr, existing, con);
		} else if (peer_cseq < existing->connect_seq) {
			if (peer_cseq == 0) {
				/* reset existing connection */
				reset_connection(existing);
				/* replace connection */
				__replace_connection(msgr, existing, con);
				con->msgr->peer_reset(con->msgr->parent,
						      &con->peer_addr,
						      &con->peer_name);
			} else {
				/* old attempt or peer didn't get the READY */
				/* send retry with peers connect seq */
				con->connect_seq = existing->connect_seq;
				con->out_connect.connect_seq =
					cpu_to_le32(con->connect_seq);
				prepare_write_accept_retry(con,
					&tag_retry_session,
					&con->out_connect.connect_seq);
			}
		} else if (peer_cseq == existing->connect_seq &&
			   (test_bit(CONNECTING, &existing->state) ||
			    test_bit(STANDBY, &existing->state) ||
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
			   peer_cseq > existing->connect_seq) {
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
		con->global_seq = peer_gseq;
		con->connect_seq = peer_cseq + 1;
		prepare_write_accept_ready(con);
	}
	spin_unlock(&msgr->con_lock);
	radix_tree_preload_end();

	ceph_queue_con(con);
	put_connection(con);
	return 0;
}


/*
 * worker function when data is available on the socket
 */
static int try_read(struct ceph_connection *con)
{
	struct ceph_messenger *msgr;
	int ret = -1;

	if (!con->sock)
		return 0;

	dout(20, "try_read start on %p\n", con);
	msgr = con->msgr;

more:
	if (test_bit(ACCEPTING, &con->state)) {
		dout(20, "try_read accepting\n");
		ret = read_accept_partial(con);
		if (ret <= 0)
			goto done;
		if (process_accept(con) < 0) {
			ret = -1;
			goto out;
		}
		goto more;
	}
	if (test_bit(CONNECTING, &con->state)) {
		dout(20, "try_read connecting\n");
		ret = read_connect_partial(con);
		if (ret <= 0)
			goto done;
		if (process_connect(con) < 0) {
			ret = -1;
			goto out;
		}
		goto more;
	}

	if (con->in_base_pos < 0) {
		/* skipping + discarding content */
		static char buf[1024];
		int skip = min(1024, -con->in_base_pos);
		dout(20, "skipping %d / %d bytes\n", skip, -con->in_base_pos);
		ret = ceph_tcp_recvmsg(con->sock, buf, skip);
		if (ret <= 0)
			goto done;
		con->in_base_pos += ret;
		if (con->in_base_pos)
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
		if (ret == -EIO) {
			con->error_msg = "bad crc";
			goto out;
		}
		if (ret <= 0)
			goto done;
		if (con->in_tag == CEPH_MSGR_TAG_READY)
			goto more;
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

done:
	ret = 0;
out:
	dout(20, "try_read done on %p\n", con);
	return ret;

bad_tag:
	derr(2, "try_read bad con->in_tag = %d\n", (int)con->in_tag);
	con->error_msg = "protocol error, garbage tag";
	ret = -1;
	goto out;
}


static void con_work(struct work_struct *work)
{
	struct ceph_connection *con = container_of(work, struct ceph_connection,
						   work.work);

more:
	if (test_and_set_bit(BUSY, &con->state) != 0) {
		dout(10, "con_work %p BUSY already set\n", con);
		goto out;
	}
	dout(10, "con_work %p start, clearing QUEUED\n", con);
	clear_bit(QUEUED, &con->state);

	if (test_bit(CLOSED, &con->state)) { /* e.g. if we are replaced */
		dout(5, "con_work CLOSED\n");
		goto done;
	}
	if (test_bit(WAIT, &con->state)) {
		dout(5, "con_work WAIT\n");
		goto done;
	}
	if (test_and_clear_bit(BACKOFF, &con->state))
		dout(5, "con_work cleared BACKOFF\n");

	if (test_and_clear_bit(SOCK_CLOSED, &con->state) ||
	    try_read(con) < 0 ||
	    try_write(con) < 0)
		ceph_fault(con);

done:
	clear_bit(BUSY, &con->state);
	if (test_bit(QUEUED, &con->state)) {
		if (!test_bit(BACKOFF, &con->state)) {
			dout(10, "con_work %p QUEUED reset, looping\n", con);
			goto more;
		}
		dout(10, "con_work %p QUEUED reset, but BACKOFF\n", con);
		clear_bit(QUEUED, &con->state);
	}
	dout(10, "con_work %p done\n", con);

out:
	put_connection(con);
}



/*
 *  worker function when listener receives a connect
 */
static void try_accept(struct work_struct *work)
{
	struct ceph_connection *newcon = NULL;
	struct ceph_messenger *msgr;

	msgr = container_of(work, struct ceph_messenger, awork);

	dout(5, "Entered try_accept\n");

	/* initialize the msgr connection */
	newcon = new_connection(msgr);
	if (newcon == NULL) {
		derr(1, "kmalloc failure accepting new connection\n");
		goto done;
	}

	newcon->connect_seq = 1;
	set_bit(ACCEPTING, &newcon->state);
	newcon->in_tag = CEPH_MSGR_TAG_READY;  /* eventually, hopefully */

	if (ceph_tcp_accept(msgr->listen_sock, newcon) < 0) {
		derr(1, "error accepting connection\n");
		put_connection(newcon);
		goto done;
	}
	dout(5, "accepted connection \n");

	prepare_write_accept_hello(msgr, newcon);
	add_connection_accepting(msgr, newcon);

	/* hand off to work queue; we may have missed socket state change */
	ceph_queue_con(newcon);
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
	INIT_RADIX_TREE(&msgr->con_tree, GFP_ATOMIC);
	spin_lock_init(&msgr->global_seq_lock);

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
	msgr->listen_sock->ops->shutdown(msgr->listen_sock, SHUT_RDWR);
	sock_release(msgr->listen_sock);
	cancel_work_sync(&msgr->awork);

	/* kill off connections */
	spin_lock(&msgr->con_lock);
	while (!list_empty(&msgr->con_all)) {
		con = list_entry(msgr->con_all.next, struct ceph_connection,
				 list_all);
		dout(10, "destroy removing connection %p\n", con);
		set_bit(CLOSED, &con->state);
		atomic_inc(&con->nref);
		dout(40, " get %p %d -> %d\n", con,
		     atomic_read(&con->nref) - 1, atomic_read(&con->nref));
		__remove_connection(msgr, con);

		/* in case there's queued work... */
		spin_unlock(&msgr->con_lock);
		if (cancel_delayed_work_sync(&con->work))
			put_connection(con);
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
	}
	spin_unlock(&msgr->con_lock);
	if (con)
		put_connection(con);
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
	old->footer.flags |= CEPH_MSG_FOOTER_ABORTED;
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
	msg->hdr.orig_src = msgr->inst;

	/* do we have the connection? */
	spin_lock(&msgr->con_lock);
	con = __get_connection(msgr, &msg->hdr.dst.addr);
	if (!con) {
		/* drop lock while we allocate a new connection */
		spin_unlock(&msgr->con_lock);
		newcon = new_connection(msgr);
		if (IS_ERR(newcon))
			return PTR_ERR(con);

		newcon->out_connect.flags = 0;
		if (!timeout)
			newcon->out_connect.flags |= CEPH_MSG_CONNECT_LOSSYTX;

		ret = radix_tree_preload(GFP_NOFS);
		if (ret < 0) {
			derr(10, "ENOMEM in ceph_msg_send\n");
			return ret;
		}

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
		spin_unlock(&msgr->con_lock);
		radix_tree_preload_end();
	} else {
		dout(10, "ceph_msg_send had connection %p to peer "
		     "%u.%u.%u.%u:%u\n", con,
		     IPQUADPORT(msg->hdr.dst.addr.ipaddr));
		spin_unlock(&msgr->con_lock);
	}

	if (!timeout) {
		dout(10, "ceph_msg_send setting LOSSYTX\n");
		set_bit(LOSSYTX, &con->state);
	}

	/* queue */
	spin_lock(&con->out_queue_lock);
	if (unlikely(msg->hdr.type == CEPH_MSG_PING &&
		     !list_empty(&con->out_queue) &&
		     list_entry(con->out_queue.prev, struct ceph_msg,
				list_head)->hdr.type == CEPH_MSG_PING)) {
		/* don't queue multiple pings in a row */
		dout(2, "ceph_msg_send dropping dup ping\n");
		ceph_msg_put(msg);
	} else {
		msg->hdr.seq = cpu_to_le64(++con->out_seq);
		dout(1, "----- %p %u to %s%d %d=%s len %d+%d -----\n", msg,
		     (unsigned)con->out_seq,
		     ENTITY_NAME(msg->hdr.dst.name), le32_to_cpu(msg->hdr.type),
		     ceph_msg_type_name(le32_to_cpu(msg->hdr.type)),
		     le32_to_cpu(msg->hdr.front_len),
		     le32_to_cpu(msg->hdr.data_len));
		dout(2, "ceph_msg_send %p seq %llu for %s%d on %p pgs %d\n",
		     msg, le64_to_cpu(msg->hdr.seq),
		     ENTITY_NAME(msg->hdr.dst.name), con, msg->nr_pages);
		list_add_tail(&msg->list_head, &con->out_queue);
	}
	spin_unlock(&con->out_queue_lock);

	if (test_and_set_bit(WRITE_PENDING, &con->state) == 0)
		ceph_queue_con(con);

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
	m->footer.front_crc = 0;
	m->footer.data_crc = 0;

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
	dout(20, "ceph_msg_put %p %d -> %d\n", m, atomic_read(&m->nref),
	     atomic_read(&m->nref)-1);
	if (atomic_read(&m->nref) <= 0) {
		derr(0, "bad ceph_msg_put on %p %u from %s%d %d=%s len %d+%d\n",
		     m, le32_to_cpu(m->hdr.seq),
		     ENTITY_NAME(m->hdr.src.name),
		     le32_to_cpu(m->hdr.type),
		     ceph_msg_type_name(le32_to_cpu(m->hdr.type)),
		     le32_to_cpu(m->hdr.front_len),
		     le32_to_cpu(m->hdr.data_len));
		WARN_ON(1);
	}
	if (atomic_dec_and_test(&m->nref)) {
		dout(20, "ceph_msg_put last one on %p\n", m);
		WARN_ON(!list_empty(&m->list_head));
		kfree(m->front.iov_base);
		kfree(m);
	}
}

void ceph_ping(struct ceph_messenger *msgr, struct ceph_entity_name name,
	       struct ceph_entity_addr *addr)
{
	struct ceph_msg *m;

	m = ceph_msg_new(CEPH_MSG_PING, 0, 0, 0, 0);
	if (!m)
		return;
	memset(m->front.iov_base, 0, m->front.iov_len);
	m->hdr.dst.name = name;
	m->hdr.dst.addr = *addr;
	ceph_msg_send(msgr, m, 0);
}
