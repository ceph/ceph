#include <linux/kthread.h>
#include <linux/socket.h>
#include <linux/net.h>
#include <linux/ceph_fs.h>
#include <linux/string.h>
#include <net/tcp.h>
#include "kmsg.h"
#include "ktcp.h"

static struct workqueue_struct *recv_wq;        /* receive work queue ) */
static struct workqueue_struct *send_wq;        /* send work queue */

static void ceph_reader(struct work_struct *);
static void ceph_writer(struct work_struct *);

struct task_struct *athread;  /* accepter thread, TBD: fill into kmsgr */

/* static tag bytes */
static char tag_ready = CEPH_MSGR_TAG_READY;
static char tag_reject = CEPH_MSGR_TAG_REJECT;
static char tag_msg = CEPH_MSGR_TAG_MSG;
static char tag_ack = CEPH_MSGR_TAG_ACK;
static char tag_close = CEPH_MSGR_TAG_CLOSE;



/*
 * blocking versions
 */

static struct ceph_message *ceph_read_message(struct socket *sd)
{
	int ret;
	int received = 0;
	struct ceph_message *message;
	struct kvec *iov;
	int i;

	message = kmalloc(sizeof(struct ceph_message), GFP_KERNEL);
	if (message == NULL){
		printk(KERN_INFO "malloc failure\n");
		return NULL;
	}

	ceph_bl_init(&message->payload);
	iov = message->payload.b_kv;

	/* first read in the message header */
	if (!_krecvmsg(sd, (char*)&message->env, sizeof(message->env), 0 )) {
    		return(NULL);
	}
	printk(KERN_INFO "reader got envelope type = %d \n" , message->env.type);
	printk(KERN_INFO "num chunks = %d \n" , message->env.nchunks);
/* TBD: print to info file rest of env */

	/* receive request in chunks */
	for (i = 0; i < message->env.nchunks; i++) {
		u32 size = 0;
		void *iov_base = NULL;
		ret = _krecvmsg(sd, (char*)&size, sizeof(size), 0);
		if (ret <= 0) {
			return(NULL);
		}
		/* try to allocate enough contiguous pages for this chunk */
		iov_base = ceph_buffer_create(size);
		if (iov_base == NULL) {
			printk(KERN_INFO "memory allocation error\n" );
			/* TBD: cleanup */
		}
		ret = _krecvmsg(sd, iov_base, size, 0);
		/* TBD:  place in bufferlist (payload) */
		received += ret;  /* keep track of complete size?? */
	}
	message->payload.b_kvlen = i;
	/* unmarshall message */

	return(message);
}

/*
 *  TBD: Not finished Still needs a lot of work....
 */
static int ceph_send_message(struct ceph_message *message, struct socket *sd)
{
	int ret;
	int sent = 0;
	__u32 chunklen;
	struct kvec *iov = message->payload.b_kv;
	int len = message->payload.b_kvlen;
	int i;

	/* add error handling */

	/* header */
	message->env.nchunks = 1;  /* for now */

	_ksendmsg(sd, (char*)&message->env, sizeof(message->env));

	/* send in in single large chunk */
	chunklen = message->payload.b_len;
	_ksendmsg(sd, (char*)&chunklen, sizeof(chunklen));
	for (i=0; i<len; i++) {
		_ksendmsg(sd, (char&)iov[i].iov_base, iov[i].iov_len);
	}

	return 0;
}



/*
 * non-blocking versions
 *
 * these are called while holding a lock on the connection
 */

/*
 * write as much of con->out_partial to the socket as we can.
 *  1 -> done; and cleaned up out_partial
 *  0 -> socket full, but more to do
 * <0 -> error
 */
static int write_partial(struct ceph_connection *con)
{
	struct ceph_bufferlist *bl = &con->out_partial;
	struct ceph_bufferlist_iterator *p = &con->out_pos;
	int len, ret;

more:
	len = bl->b_kv[p->i_kv].iov_len - p->i_off;
	/* FIXME */
	ret = kernel_send(con->socket, bl->b_kv[p->i_kv].iov_base + p->i_off, len);
	if (ret < 0) return sent;
	if (ret == 0) return 0;   /* socket full */
	if (ret + p->i_off == bl->b_kv[p->i_kv].iov_len) {
		p->i_kv++;
		p->i_off = 0;
		if (p->i_kv == bl->b_kvlen) 
			return 1;
	} else {
		p->i_off += ret;
	}
	goto more;
}

/*
 * build out_partial based on the next outgoing message in the queue.
 */
static void prepare_write_message(struct ceph_connection *con)
{
	struct ceph_message *m = con->out_queue.next;
	
	/* move to sending/sent list */
	list_del(&m->list_head);
	list_add(&m->list_head, &con->out_sent);
	
	ceph_bl_init(&con->out_partial);  
	ceph_bl_iterator_init(&con->out_pos);

	/* always one chunk, for now */
	m->hdr.nchunks = 1;  
	m->chunklen[0] = m->payload.b_len;

	/* tag + header */
	ceph_bl_append_ref(&con->out_partial, &tag_msg, 1);
	ceph_bl_append_ref(&con->out_partial, &m->hdr, sizeof(m->hdr));
	
	/* payload */
	ceph_bl_append_ref(&con->out_partial, &m->chunklen[0], sizeof(__u32));
	for (int i=0; i<m->payload.b_kvlen; i++) 
		ceph_bl_append_ref(&con->out_partial, m->payload.b_kv[i].iov_base, 
				   m->payload.b_kv[i].iov_len);
}

/* 
 * prepare an ack for send
 */
static void prepare_write_ack(struct ceph_connection *con)
{
	con->in_seq_acked = con->in_seq;
	
	ceph_bl_init(&con->out_partial);  
	ceph_bl_iterator_init(&con->out_pos);
	ceph_bl_append_copy(&con->out_partial, &tag_ack, 1);
	ceph_bl_append_copy(&con->out_partial, &con->in_seq_acked, sizeof(con->in_seq_acked));
}

/*
 * call when socket is writeable
 */
static int try_write(struct ceph_connection *con)
{
	int ret;

more:
	if (con->out_partial.b_kvlen) {
		ret = write_partial(con);
		if (ret == 0) return 0;

		/* clean up */
		ceph_bl_init(&con->out_partial);  
		ceph_bl_iterator_init(&con->out_pos);

		if (ret < 0) return ret; /* error */
	}
	
	/* what next? */
	if (con->in_seq > con->in_seq_acked) {
		prepare_write_ack(con);
		goto more;
	}
	if (!list_empty(&con->out_queue)) {
		prepare_write_message(con);
		goto more;
	}
	
	/* hmm, nothing to do! */
	return 0;
}


/*
 * read (part of) a message
 */
static int read_message_partial(struct ceph_connection *con)
{
	struct ceph_message *m = con->in_partial;
	int left, ret, s, chunkbytes, c, did;

	while (con->in_base_pos < sizeof(struct ceph_message_header)) {
		left = sizeof(struct ceph_message_header) - con->in_base_pos;
		ret = _read(socket, &m->hdr + con->in_base_pos, left);
		if (ret <= 0) return ret;
		con->in_base_pos += ret;
	}
	if (m->hdr.nchunks == 0) return 1; /* done */

	chunkbytes = sizeof(__u32)*m->hdr.nchunks;
	while (con->in_base_pos < sizeof(struct ceph_message_header) + chunkbytes) {
		int off = in_base_pos - sizeof(struct ceph_message_header);
		left = chunkbytes + sizeof(struct ceph_message_header) - in_base_pos;
		ret = _read(socket, (char*)m->hdr.chunklen + off, left);
		if (ret <= 0) return ret;
		con->in_base_pos += ret;
	}
	
	did = 0;
	for (c = 0; c<m->hdr.nchunks; c++) {
	more:
		left = did + m->hdr.chunklen[c] - m->payload.b_len;
		if (left <= 0) {
			did += m->hdr.chunklen[c];
			continue;
		}
		ceph_bl_prepare_append(&m->payload, left);
		s = min(m->payload.b_append.iov_len, left);
		ret = _read(socket, m->payload.b_append.iov_base, s);
		if (ret <= 0) return ret;
		ceph_bl_append_copied(&m->payload, s);
		goto more;
	}
	return 1; /* done! */
}

/*
 * read (part of) an ack
 */
static int read_ack_partial(struct ceph_connection *con)
{
	while (con->in_base_pos < sizeof(con->in_partial_ack)) {
		int left = sizeof(con->in_partial_ack) - con->in_base_pos;
		ret = _read(socket, (char*)&con->in_partial_ack + con->in_base_pos, left);
		if (ret <= 0) return ret;
		con->in_base_pos += ret;
	}
	return 1; /* done */
}

/* 
 * prepare to read a message
 */
static int prepare_read_message(struct ceph_connection *con)
{
	con->in_tag = CEPH_MSGR_TAG_MSG;
	con->in_base_pos = 0;
	con->in_partial = kmalloc(sizeof(struct ceph_message));
	if (!con->in_partial) return -1;  /* crap */
	ceph_get_msg(con->in_partial);
	ceph_bl_init(&con->payload);
	ceph_bl_iterator_init(&con->in_pos);
}

/* 
 * prepare to read an ack
 */
static int prepare_read_ack(struct ceph_connection *con)
{
	con->in_tag = CEPH_MSGR_TAG_ACK;
	con->in_base_pos = 0;
}

static void process_ack(struct ceph_connection *con, __u32 ack)
{
	struct ceph_message *m;
	while (!list_empty(&con->out_sent)) {
		m = con->out_sent.next;
		if (m->hdr.seq > ack) break;
		printk(KERN_INFO "got ack for %d type %d at %lx\n", m->hdr.seq, m->hdr.type, m);
		list_del(&m->list_head);
		ceph_put_msg(m);
	}
}

static int try_read(struct ceph_connection *con)
{
	int ret = -1;

more:
	if (con->in_tag == CEPH_MSGR_TAG_READY) {
		ret = _read(socket, &con->in_tag, 1);
		if (ret <= 0) return ret;
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
		if (ret <= 0) return ret;
		/* got a full message! */
		ceph_dispatch(con->msgr, con->in_partial);
		cphe_put_msg(con->in_partial);
		con->in_partial = 0;
		con->in_tag = CEPH_MSGR_TAG_READY;
		goto more;
	}
	if (con->in_tag == CEPH_MSGR_TAG_ACK) {
		ret = read_ack_partial(con);
		if (ret <= 0) return ret;
		/* got an ack */
		process_ack(con, con->in_partial_ack);
		con->in_tag = CEPH_MSGR_TAG_READY;
		goto more;
	}
bad:
	BUG_ON(1); /* shouldn't get here */
	return ret;
}





/*
 * Accepter thread
 */
static int ceph_accepter(void *unusedfornow)
{
	struct socket *sd, *new_sd;
	struct sockaddr saddr;
	struct ceph_connection *con = NULL;

	memset(&saddr, 0, sizeof(saddr));

        printk(KERN_INFO "starting kernel thread\n");
        set_current_state(TASK_INTERRUPTIBLE);

	/* TBD: if address specified by mount */
		/* make my address from user specified address, fill in saddr */

	sd = _klisten(&saddr);

        /* an endless loop in which we are accepting connections */
        while (!kthread_should_stop()) {
		/* TBD: should we schedule? or will accept take care of this? */
		new_sd = _kaccept(sd);
                printk(KERN_INFO "accepted connection \n");
                set_current_state(TASK_INTERRUPTIBLE);
		/* initialize the ceph connection */
		con = kmalloc(sizeof(struct ceph_connection), GFP_KERNEL);
		if (con == NULL) {
                	printk(KERN_INFO "malloc failure\n");
			sock_release(new_sd);
			break;
        	}
		con->sock = new_sd;
		con->state = READ_PENDING;
		/* setup work structure */
		INIT_WORK(&con->rwork, ceph_reader);
	        /* hand off to worker threads , read pending */
		queue_work(recv_wq, &con->rwork);
        }
        set_current_state(TASK_RUNNING);
        printk(KERN_INFO "kernel thread exiting\n");
	sock_release(sd);
        return(0);
}

void ceph_dispatch(struct ceph_message *msg)
{
	/* probably change the state and function of the recv worker *
         * and requeue the work */ 
	/* also maybe keep connection alive with timeout for further
         * communication with server... not sure if we should use connection
         * then for dispatching ?? */

	ceph_get_msg(msg);  /* grab a reference */
	/*add_to_work_queue(...);*/

	/* or, we can just do this from the connection worker threads.. in general, message
	 * processing will be fast (never block), and we're already threaded per proc... */
}


int ceph_work_init(void)
{
        int ret = 0;

	/*
	 * Create a num CPU threads to handle receive requests
	 * note: we can create more threads if needed to even out
	 * the scheduling of multiple requests.. 
	 */
        recv_wq = create_workqueue("ceph recv");
        ret = IS_ERR(recv_wq);
        if (ret) {
		printk(KERN_INFO "receive worker failed to start: %d\n", ret);
                destroy_workqueue(recv_wq);
                return ret;
        }

	/*
	 * Create a single thread to handle send requests 
	 * note: may use same thread pool as receive workers later...
	 */
        send_wq = create_singlethread_workqueue("ceph send");
        ret = IS_ERR(send_wq);
        if (ret) {
		printk(KERN_INFO "send worker failed to start: %d\n", ret);
                destroy_workqueue(send_wq);
                return ret;
        }
/* TBD: need to do this during mount, one per kmsgr */
	athread = kthread_run(ceph_accepter, NULL, "ceph accepter thread");

        return(ret);
}

void ceph_work_shutdown(void)
{
/* TBD: need to do this during unmount*/
/*
	kthread_stop(msgr->athread);
	wake_up_process(msgr->athread);
*/
	kthread_stop(athread);
	wake_up_process(athread);
	destroy_workqueue(send_wq);
	destroy_workqueue(recv_wq);
}

/*
static void make_addr(struct sockaddr *saddr, struct ceph_entity_addr *v)
{
	struct sockaddr_in *in_addr = (struct sockaddr_in *)saddr;

	memset(in_addr,0,sizeof(struct sockaddr_in));
	in_addr->sin_family = AF_INET;
	in_addr->sin_addr.s_addr = 
		htonl(create_address(v.ipq[0],v.ipq[1],v.ipq[2],v.ipq[3]));
	memcpy((char*)in_addr->sin_addr.s_addr, (char*)v.ipq, 4);
	in_addr->sin_port = htons(v.port);
}
static void set_addr()
{
}
*/

static void ceph_reader(struct work_struct *work)
{
	struct ceph_connection *con = 
		container_of(work, struct ceph_connection, rwork);
	/* char *reply = kmalloc(RCVBUF, GFP_KERNEL); */
	char *response = NULL;

/*	send_reply(con->socket, reply); */
	return;
}
static void ceph_writer(struct work_struct *work)
{
	struct ceph_connection *con = 
		container_of(work, struct ceph_connection, swork);
	/* char *reply = kmalloc(RCVBUF, GFP_KERNEL); */
	char *response = NULL;

/*	response = read_response(con->socket); */
	return;
}
