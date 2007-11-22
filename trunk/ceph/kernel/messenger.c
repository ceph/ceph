#include <linux/kthread.h>
#include <linux/socket.h>
#include <linux/net.h>
#include <linux/string.h>
#include <linux/highmem.h>
#include <linux/ceph_fs.h>
#include <net/tcp.h>
#include "messenger.h"
#include "ktcp.h"

/* static tag bytes */
static char tag_ready = CEPH_MSGR_TAG_READY;
static char tag_reject = CEPH_MSGR_TAG_REJECT;
static char tag_msg = CEPH_MSGR_TAG_MSG;
static char tag_ack = CEPH_MSGR_TAG_ACK;
static char tag_close = CEPH_MSGR_TAG_CLOSE;

static void try_read(struct work_struct *);
static void try_write(struct work_struct *);
static void try_accept(struct work_struct *);



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

	spin_lock_init(&con->con_lock);
	set_bit(NEW, &con->state);
	INIT_WORK(&con->rwork, try_read);	/* setup work structure */
	INIT_WORK(&con->swork, try_write);	/* setup work structure */

	atomic_inc(&con->nref);
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
	key = *(unsigned long*)&addr->ipaddr.sin_addr.s_addr;
	key ^= addr->ipaddr.sin_port;
	return key;
}

/* 
 * get an existing connection, if any, for given addr
 */
static struct ceph_connection *get_connection(struct ceph_messenger *msgr, struct ceph_entity_addr *addr)
{
	struct ceph_connection *con;
	struct list_head *head, *p;
	unsigned long key = hash_addr(addr);

	/* existing? */
	spin_lock(&msgr->con_lock);
	head = radix_tree_lookup(&msgr->con_open, key);
	if (head) {
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
		list_add(head, &con->list_bucket);
	} else {
		INIT_LIST_HEAD(&con->list_bucket); /* empty */
		radix_tree_insert(&msgr->con_open, key, &con->list_bucket);
	}
	spin_unlock(&msgr->con_lock);
}

static void add_connection_accepting(struct ceph_messenger *msgr, struct ceph_connection *con)
{
	atomic_inc(&con->nref);
	spin_lock(&msgr->con_lock);
	list_add(&msgr->con_all, &con->list_all);
	list_add(&msgr->con_accepting, &con->list_bucket);
	spin_unlock(&msgr->con_lock);
}

/*
 * remove connection from all list.
 * also, from con_open radix tree, if it should have been there
 */
static void remove_connection(struct ceph_messenger *msgr, struct ceph_connection *con)
{
	unsigned long key = hash_addr(&con->peer_addr);

	spin_lock(&msgr->con_lock);
	list_del(&con->list_all);
	if (test_bit(CONNECTING, &con->state) || 
	    test_bit(OPEN, &con->state)) {
		/* remove from con_open too */
		if (list_empty(&con->list_bucket)) {
			/* last one */
			radix_tree_delete(&msgr->con_open, key);
		} else {
			list_del(&con->list_bucket);
		}
	}
	spin_unlock(&msgr->con_lock);

	put_connection(con);
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

	while (con->out_kvec_bytes > 0) {
		ret = _ksendmsg(con->sock, con->out_kvec_cur, con->out_kvec_left, con->out_kvec_bytes);
		if (ret < 0) return ret;  /* error */
		if (ret == 0) return 0;   /* socket full */
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
	return 1;  /* done! */
}

static int write_partial_msg_pages(struct ceph_connection *con, struct ceph_msg *msg)
{
	struct kvec kv;
	int ret;

	while (con->out_msg_pos.page < con->out_msg->nr_pages) {
		kv.iov_base = kmap(msg->pages[con->out_msg_pos.page]) + con->out_msg_pos.page_pos;
		kv.iov_len = min((int)(PAGE_SIZE - con->out_msg_pos.page_pos), 
				 (int)(msg->hdr.data_len - con->out_msg_pos.data_pos));
		ret = _ksendmsg(con->sock, &kv, 1, kv.iov_len);
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
	list_add(&m->list_head, &con->out_sent);
	con->out_msg = m;

	/* encode header */
	ceph_encode_header(&con->out_hdr, &m->hdr);

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
}

static void prepare_write_accept_announce(struct ceph_messenger *msgr, struct ceph_connection *con)
{
	con->out_kvec[0].iov_base = &msgr->inst.addr;
	con->out_kvec[0].iov_len = sizeof(msgr->inst.addr);
	con->out_kvec_left = 1;
	con->out_kvec_bytes = sizeof(msgr->inst.addr);
	con->out_kvec_cur = con->out_kvec;
}

static void prepare_write_accept_ready(struct ceph_connection *con)
{
	con->out_kvec[0].iov_base = &tag_ready;
	con->out_kvec[0].iov_len = 1;
	con->out_kvec_left = 1;
	con->out_kvec_bytes = 1;
	con->out_kvec_cur = con->out_kvec;
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
}

/*
 * call when socket is writeable
 */
static void try_write(struct work_struct *work)
{
	struct ceph_connection *con;
	struct ceph_messenger *msgr;
	int ret = 1;

	con = container_of(work, struct ceph_connection, swork);
	msgr = con->msgr;

more:
	/* kvec data queued? */
	if (con->out_kvec_left) {
		ret = write_partial_kvec(con);
		if (ret == 0) 
			goto done;
		
		if (test_bit(REJECTING, &con->state)) {
			/* FIXME do something else here, pbly? */
			remove_connection(msgr, con);
			set_bit(CLOSED, &con->state);
			put_connection(con);
		}
		
		/* TBD: handle error; return for now */
		if (ret < 0) {
			con->error = ret;
			goto done; /* error */
		}
	}

	/* msg pages? */
	if (con->out_msg) {
		ret = write_partial_msg_pages(con, con->out_msg);
		if (ret == 0) 
			goto done;
	}
	
	/* anything else pending? */
	if (con->in_seq > con->in_seq_acked) {
		prepare_write_ack(con);
		goto more;
	}
	if (!list_empty(&con->out_queue)) {
		prepare_write_message(con);
		goto more;
	}
	
	/* hmm, nothing to do! No more writes pending? */
	if (ret)
		clear_bit(WRITE_PEND, &con->state);
done:
	return;
}


/* 
 * prepare to read a message
 */
static int prepare_read_message(struct ceph_connection *con)
{
	con->in_tag = CEPH_MSGR_TAG_MSG;
	con->in_base_pos = 0;
	con->in_msg = kzalloc(sizeof(*con->in_msg), GFP_KERNEL);
	if (con->in_msg == NULL) {
		/* TBD: we don't check for error in caller, handle error */
		derr(1, "kmalloc failure on incoming message\n");
		return -ENOMEM;
	}
	
	ceph_msg_get(con->in_msg);
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
		ret = _krecvmsg(con->sock, &m->hdr + con->in_base_pos, left);
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
		ret = _krecvmsg(con->sock, (char*)m->front.iov_base + m->front.iov_len, left);
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
		ret = _krecvmsg(con->sock, p + con->in_msg_pos.page_pos, left);
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
		int ret = _krecvmsg(con->sock, (char*)&con->in_partial_ack + con->in_base_pos, left);
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
 * read portion of handshake on a newly accepted connection
 */
static int read_accept_partial(struct ceph_connection *con)
{
	int ret;

	/* peer addr */
	while (con->in_base_pos < sizeof(con->peer_addr)) {
		int left = sizeof(con->peer_addr) - con->in_base_pos;
		ret = _krecvmsg(con->sock, (char*)&con->peer_addr + con->in_base_pos, left);
		if (ret <= 0) return ret;
		con->in_base_pos += ret;
	}

	/* connect_seq */
	while (con->in_base_pos < sizeof(con->peer_addr) + sizeof(con->connect_seq)) {
		int off = con->in_base_pos - sizeof(con->peer_addr);
		int left = sizeof(con->peer_addr) + sizeof(con->connect_seq) - con->in_base_pos;
		ret = _krecvmsg(con->sock, (char*)&con->connect_seq + off, left);
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
		spin_lock(&existing->con_lock);
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
		} else {
			/* reject new connection */
			set_bit(REJECTING, &con->state);
			con->connect_seq = existing->connect_seq; /* send this with the reject */
		}
		spin_unlock(&existing->con_lock);
		put_connection(existing);
	} else {
		add_connection(con->msgr, con);
		set_bit(OPEN, &con->state);
	}
	spin_unlock(&con->msgr->con_lock);

	/* the result? */
	if (test_bit(REJECTING, &con->state))
		prepare_write_accept_reject(con);
	else
		prepare_write_accept_ready(con);
}


/*
 * call when data is available on the socket
 */
static void try_read(struct work_struct *work)
{
	int ret = -1;
	struct ceph_connection *con;
	struct ceph_messenger *msgr;

	con = container_of(work, struct ceph_connection, rwork);
	msgr = con->msgr;

more:
	/*
	 * TBD: maybe store error in ceph_connection
         */

	if (test_bit(CLOSED, &con->state)) goto done;
	if (test_bit(ACCEPTING, &con->state)) {
		ret = read_accept_partial(con);
		if (ret <= 0) goto done;
		/* accepted */
		process_accept(con);
		goto more;
	}

	if (con->in_tag == CEPH_MSGR_TAG_READY) {
		ret = _krecvmsg(con->sock, &con->in_tag, 1);
		if (ret <= 0) goto done;
		if (con->in_tag == CEPH_MSGR_TAG_MSG) 
			prepare_read_message(con);
		else if (con->in_tag == CEPH_MSGR_TAG_ACK)
			prepare_read_ack(con);
		else {
			printk(KERN_INFO "bad tag %d\n", (int)con->in_tag);
			goto bad;
		}
		goto more;
	}
	if (con->in_tag == CEPH_MSGR_TAG_MSG) {
		ret = read_message_partial(con);
		if (ret <= 0) goto done;
		/* got a full message! */
		msgr->dispatch(con->msgr->parent, con->in_msg);
		ceph_msg_put(con->in_msg);
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
bad:
	BUG_ON(1); /* shouldn't get here */
done:
	con->error = ret;
	return;
}


/*
 * Accepter thread
 */
static void try_accept(struct work_struct *work)
{
	struct socket *sock, *new_sock;
	struct sockaddr_in saddr;
        struct ceph_connection *new_con = NULL;
	struct ceph_messenger *msgr;
	int len;

	msgr = container_of(work, struct ceph_messenger, awork);
	sock = msgr->listen_sock;

        dout(5, "Entered try_accept\n");


        if (kernel_accept(sock, &new_sock, sock->file->f_flags) < 0) {
        	derr(1, "error accepting connection\n");
                goto done;
        }
	dout(5, "accepted connection \n");

        /* get the address at the other end */
        memset(&saddr, 0, sizeof(saddr));
        if (new_sock->ops->getname(new_sock, (struct sockaddr *)&saddr, &len, 2)) {
                derr(1, "getname error connection aborted\n");
                sock_release(new_sock);
                goto done;
        }

	/* initialize the msgr connection */
	new_con = new_connection(msgr);
	if (new_con == NULL) {
               	derr(1, "malloc failure\n");
		sock_release(new_sock);
		goto done;
       	}
	new_con->sock = new_sock;
	set_bit(ACCEPTING, &new_con->state);
	new_con->in_tag = CEPH_MSGR_TAG_READY;
	/* fill in part of peers address */
	new_con->peer_addr.ipaddr = saddr;

	prepare_write_accept_announce(msgr, new_con);

	add_connection_accepting(msgr, new_con);

	/* hand off to worker threads , send pending */
	/*?? queue_work(send_wq, &new_con->swork);*/
done:
        return;
}

/*
 * create a new messenger instance, saddr is address specified from mount arg.
 * If null, will get created by _klisten()
 */
struct ceph_messenger *ceph_messenger_create()
{
        struct ceph_messenger *msgr;
	int ret = 0;

        msgr = kzalloc(sizeof(*msgr), GFP_KERNEL);
        if (msgr == NULL) 
		return ERR_PTR(-ENOMEM);

	spin_lock_init(&msgr->con_lock);

	/* create listening socket */
	ret = _klisten(msgr);
	if(ret < 0) {
		kfree(msgr);
		return  ERR_PTR(ret);
	}

	/* TBD: setup callback for accept */
	INIT_WORK(&msgr->awork, try_accept);       /* setup work structure */

	dout(1, "ceph_messenger_create listening on %x:%d\n", 
	     ntohl(msgr->inst.addr.ipaddr.sin_addr.s_addr), 
	     ntohl(msgr->inst.addr.ipaddr.sin_port));
	return msgr;
}


/*
 * queue up an outgoing message
 *
 * will take+drop msgr, then connection locks.
 */
int ceph_msg_send(struct ceph_messenger *msgr, struct ceph_msg *msg)
{
	struct ceph_connection *con;
	int ret = 0;
	
	/* set source */
	msg->hdr.src = msgr->inst;

	/* do we have the connection? */
	spin_lock(&msgr->con_lock);
	con = get_connection(msgr, &msg->hdr.dst.addr);
	if (!con) {
		con = new_connection(msgr);
		if (IS_ERR(con))
			return PTR_ERR(con);
		dout(5, "opening new connection to peer %x:%d\n",
		     ntohl(msg->hdr.dst.addr.ipaddr.sin_addr.s_addr), 
		     ntohl(msg->hdr.dst.addr.ipaddr.sin_port));
		con->peer_addr = msg->hdr.dst.addr;
		add_connection(msgr, con);
	} else {
		dout(5, "had connection to peer %x:%d\n",
		     ntohl(msg->hdr.dst.addr.ipaddr.sin_addr.s_addr),
		     ntohl(msg->hdr.dst.addr.ipaddr.sin_port));
	}		     
	spin_unlock(&msgr->con_lock);

	spin_lock(&con->con_lock);

	/* initiate connect? */
	if (test_bit(NEW, &con->state)) {
		ret = _kconnect(con);
		if (ret < 0){
			derr(1, "connection failure to peer %x:%d\n",
			     ntohl(msg->hdr.dst.addr.ipaddr.sin_addr.s_addr),
			     ntohl(msg->hdr.dst.addr.ipaddr.sin_port));
			remove_connection(msgr, con);
			kfree(con);
			return(ret);

		}
	}
	
	/* queue */
	dout(1, "queuing outgoing message for %s.%d\n",
	     ceph_name_type_str(msg->hdr.dst.name.type), msg->hdr.dst.name.num);
	ceph_msg_get(msg);

	list_add(&con->out_queue, &msg->list_head);
	set_bit(WRITE_PEND, &con->state);
	spin_unlock(&con->con_lock);
	put_connection(con);
	return ret;
}



struct ceph_msg *ceph_msg_new(int type, int front_len, int page_len, int page_off)
{
	struct ceph_msg *m;
	int i;

	m = kzalloc(sizeof(*m), GFP_KERNEL);
	if (m == NULL)
		goto out;
	atomic_set(&m->nref, 1);
	m->hdr.type = type;
	m->hdr.front_len = front_len;
	m->hdr.data_len = page_len;
	m->hdr.data_off = page_off;

	/* front */
	m->front.iov_base = kmalloc(front_len, GFP_KERNEL);
	if (m->front.iov_base == NULL)
		goto out2;
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
	}
	return m;

out2:
	ceph_msg_put(m);
out:
	return ERR_PTR(-ENOMEM);
}

void ceph_msg_put(struct ceph_msg *m)
{
	if (atomic_dec_and_test(&m->nref)) {
		int i;
		if (m->pages) {
			for (i=0; i<m->nr_pages; i++)
				if (m->pages[i])
					kfree(m->pages[i]);
			kfree(m->pages);
		}
		if (m->front.iov_base)
			kfree(m->front.iov_base);
		kfree(m);
	}
}


