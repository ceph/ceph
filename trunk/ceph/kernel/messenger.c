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
static char tag_ready = CEPH_MSGR_TAG_READY;
static char tag_reject = CEPH_MSGR_TAG_REJECT;
static char tag_msg = CEPH_MSGR_TAG_MSG;
static char tag_ack = CEPH_MSGR_TAG_ACK;
static char tag_close = CEPH_MSGR_TAG_CLOSE;

/*
 * write as much of con->out_partial to the socket as we can.
 *  1 -> done; and cleaned up out_partial
 *  0 -> socket full, but more to do
 * <0 -> error
 */
static int ceph_write_partial(struct ceph_connection *con)
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
static void ceph_prepare_write_message(struct ceph_connection *con)
{
	struct ceph_message *m = con->out_queue.next;
	
	/* move to sending/sent list */
	list_del(&m->list_head);
	list_add(&m->list_head, &con->out_sent);
	
	ceph_bl_clear(&con->out_partial);  
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
		ceph_bl_append(&con->out_partial, m->payload.b_kv[i].iov_base, 
			       m->payload.b_kv[i].iov_len);
}

/* 
 * prepare an ack for send
 */
static void ceph_prepare_write_ack(struct ceph_connection *con)
{
	con->in_seq_acked = con->in_seq;
	
	ceph_bl_clear(&con->out_partial);  
	ceph_bl_iterator_init(&con->out_pos);
	ceph_bl_append_ref(&con->out_partial, &tag_ack, 1);
	ceph_bl_append_ref(&con->out_partial, &con->in_seq_acked, sizeof(con->in_seq_acked));
}

/*
 * call when socket is writeable
 */
static int ceph_try_write(struct ceph_connection *con)
{
	int ret;

more:
	if (con->out_partial.b_kvlen) {
		ret = ceph_write_partial(con);
		if (ret == 0) return 0;

		/* clean up */
		ceph_bl_clear(&con->out_partial);  
		ceph_bl_iterator_init(&con->out_pos);

		if (ret < 0) return ret; /* error */
	}
	
	/* what next? */
	if (con->in_seq > con->in_seq_acked) {
		ceph_prepare_write_ack(con);
		goto more;
	}
	if (!list_empty(&con->out_queue)) {
		ceph_prepare_write_message(con);
		goto more;
	}
	
	/* hmm, nothing to do! */
	return 0;
}





/*
 * The following functions are just for testing the comms stuff...
 */
static char *read_response(struct socket *sd)
{
	char *response;
	/* response = kmalloc(RECBUF, GFP_KERNEL); */

	return (response);
}
static void send_reply(struct socket *sd, char *reply)
{
	/* char *reply = kmalloc(SENDBUF, GFP_KERNEL); */

	return;
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
