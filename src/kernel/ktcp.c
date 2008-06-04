#include <linux/socket.h>
#include <linux/net.h>
#include <net/tcp.h>
#include <linux/string.h>
#include "messenger.h"
#include "ktcp.h"

int ceph_debug_tcp;
#define DOUT_VAR ceph_debug_tcp
#define DOUT_PREFIX "tcp: "
#include "super.h"

struct workqueue_struct *recv_wq;	/* receive work queue */
struct workqueue_struct *send_wq;	/* send work queue */
struct workqueue_struct *accept_wq;	/* accept work queue */

struct kobject *ceph_sockets_kobj;

/*
 * sockets
 */
void ceph_socket_destroy(struct kobject *kobj)
{
	struct ceph_socket *s = container_of(kobj, struct ceph_socket, kobj);
	dout(10, "socket_destroy %p\n", s);
	sock_release(s->sock);
	kfree(s);
}

struct kobj_type ceph_socket_type = {
	.release = ceph_socket_destroy,
};

static struct ceph_socket *ceph_socket_create(void)
{
	struct ceph_socket *s;
	int err = -ENOMEM;

	s = kzalloc(sizeof(*s), GFP_NOFS);
	if (!s)
		goto out;

	err = sock_create_kern(AF_INET, SOCK_STREAM, IPPROTO_TCP, &s->sock);
	if (err)
		goto out_free;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,6,25)
	err = kobject_init_and_add(&s->kobj, &ceph_socket_type,
				   ceph_sockets_kobj,
				   "socket %p", s);
	if (err)
		goto out_release;
#else
	kobject_init(&s->kobj);
	kobject_set_name(&s->kobj, "socket %p", s);
	s->kobj.ktype = &ceph_socket_type;
#endif
	return s;

out_release:
	sock_release(s->sock);
out_free:
	kfree(s);
out:
	return ERR_PTR(err);
}

void ceph_socket_get(struct ceph_socket *s)
{
	struct kobject *r;

	dout(10, "socket_get %p\n", s);
	r = kobject_get(&s->kobj);
	BUG_ON(!r);
}

void ceph_socket_put(struct ceph_socket *s)
{
	dout(10, "socket_put %p\n", s);
	if (!s)
		return;
	kobject_put(&s->kobj);
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
	if (msgr && (sk->sk_state == TCP_LISTEN))
		queue_work(accept_wq, &msgr->awork);
}

/* Data available on socket or listen socket received a connect */
static void ceph_data_ready(struct sock *sk, int count_unused)
{
	struct ceph_connection *con =
		(struct ceph_connection *)sk->sk_user_data;
	if (con && (sk->sk_state != TCP_CLOSE_WAIT)) {
		dout(30, "ceph_data_ready on %p state = %lu, queuing rwork\n",
		     con, con->state);
		ceph_queue_read(con);
	}
}

/* socket has bufferspace for writing */
static void ceph_write_space(struct sock *sk)
{
	struct ceph_connection *con =
		(struct ceph_connection *)sk->sk_user_data;

	dout(30, "ceph_write_space %p state = %lu\n", con, con->state);

	/* only queue to workqueue if a WRITE is pending */
	if (con && test_bit(WRITE_PENDING, &con->state)) {
		dout(30, "ceph_write_space %p queuing write work\n", con);
		ceph_queue_write(con);
	}
	/* Since we have our own write_space, Clear the SOCK_NOSPACE flag */
	clear_bit(SOCK_NOSPACE, &sk->sk_socket->flags);
}

/* sockets state has change */
static void ceph_state_change(struct sock *sk)
{
	struct ceph_connection *con =
		(struct ceph_connection *)sk->sk_user_data;
	if (con == NULL)
		return;

	dout(30, "ceph_state_change %p state = %lu sk_state = %u\n",
	     con, con->state, sk->sk_state);

	switch (sk->sk_state) {
	case TCP_CLOSE:
		dout(30, "ceph_state_change TCP_CLOSE\n");
	case TCP_CLOSE_WAIT:
		dout(30, "ceph_state_change TCP_CLOSE_WAIT\n");
		set_bit(SOCK_CLOSE, &con->state);
		if (test_bit(CONNECTING, &con->state))
			con->error_msg = "connection refused";
		else
			con->error_msg = "socket closed";
		ceph_queue_write(con);
		break;
	case TCP_ESTABLISHED:
		dout(30, "ceph_state_change TCP_ESTABLISHED\n");
		ceph_write_space(sk);
		break;
	}
}

/* make a listening socket active by setting up the data ready call back */
static void listen_sock_callbacks(struct ceph_socket *s,
				  struct ceph_messenger *msgr)
{
	struct sock *sk = s->sock->sk;
	sk->sk_user_data = (void *)msgr;
	sk->sk_data_ready = ceph_accept_ready;
}

/* make a socket active by setting up the call back functions */
static void set_sock_callbacks(struct ceph_socket *s,
			       struct ceph_connection *con)
{
	struct sock *sk = s->sock->sk;
	sk->sk_user_data = (void *)con;
	sk->sk_data_ready = ceph_data_ready;
	sk->sk_write_space = ceph_write_space;
	sk->sk_state_change = ceph_state_change;
}

void ceph_cancel_sock_callbacks(struct ceph_socket *s)
{
	struct sock *sk;
	if (!s)
		return;
	sk = s->sock->sk;
	sk->sk_user_data = 0;
	sk->sk_data_ready = 0;
	sk->sk_write_space = 0;
	sk->sk_state_change = 0;
}

/*
 * initiate connection to a remote socket.
 */
struct ceph_socket *ceph_tcp_connect(struct ceph_connection *con)
{
	int ret;
	struct sockaddr *paddr = (struct sockaddr *)&con->peer_addr.ipaddr;
	struct ceph_socket *s;

	s = ceph_socket_create();
	if (IS_ERR(s))
		return s;

	con->s = s;
	s->sock->sk->sk_allocation = GFP_NOFS;

	set_sock_callbacks(s, con);

	ret = s->sock->ops->connect(s->sock, paddr,
				    sizeof(struct sockaddr_in), O_NONBLOCK);
	if (ret == -EINPROGRESS) {
		dout(20, "connect EINPROGRESS sk_state = = %u\n",
		     s->sock->sk->sk_state);
		ret = 0;
	}
	if (ret < 0) {
		/* TBD check for fatal errors, retry if not fatal.. */
		derr(1, "connect %u.%u.%u.%u:%u error: %d\n",
		     IPQUADPORT(*(struct sockaddr_in *)paddr), ret);
		ceph_cancel_sock_callbacks(s);
		ceph_socket_put(s);
		con->s = 0;
	}

	if (ret < 0)
		return ERR_PTR(ret);
	ceph_socket_get(s); /* for caller */
	return s;
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
	struct ceph_socket *s;

	s = ceph_socket_create();
	if (IS_ERR(s))
		return PTR_ERR(s);

	s->sock->sk->sk_allocation = GFP_NOFS;

	ret = kernel_setsockopt(s->sock, SOL_SOCKET, SO_REUSEADDR,
				(char *)&optval, sizeof(optval));
	if (ret < 0) {
		derr(0, "Failed to set SO_REUSEADDR: %d\n", ret);
		goto err;
	}

	ret = s->sock->ops->bind(s->sock, (struct sockaddr *)myaddr,
				 sizeof(*myaddr));
	if (ret < 0) {
		derr(0, "Failed to bind: %d\n", ret);
		goto err;
	}

	/* what port did we bind to? */
	nlen = sizeof(*myaddr);
	ret = s->sock->ops->getname(s->sock, (struct sockaddr *)myaddr, &nlen,
				    0);
	if (ret < 0) {
		derr(0, "failed to getsockname: %d\n", ret);
		goto err;
	}
	dout(10, "listen on port %d\n", ntohs(myaddr->sin_port));

	ret = kernel_setsockopt(s->sock, SOL_SOCKET, SO_KEEPALIVE,
				(char *)&optval, sizeof(optval));
	if (ret < 0) {
		derr(0, "Failed to set SO_KEEPALIVE: %d\n", ret);
		goto err;
	}

	/* TBD: probaby want to tune the backlog queue .. */
	ret = s->sock->ops->listen(s->sock, NUM_BACKUP);
	if (ret < 0) {
		derr(0, "kernel_listen error: %d\n", ret);
		goto err;
	}

	/* ok! */
	msgr->listen_s = s;
	listen_sock_callbacks(s, msgr);
	return ret;

err:
	ceph_socket_put(s);
	return ret;
}

/*
 *  accept a connection
 */
int ceph_tcp_accept(struct ceph_socket *ls, struct ceph_connection *con)
{
	int ret;
	struct sockaddr *paddr = (struct sockaddr *)&con->peer_addr.ipaddr;
	int len;
	struct ceph_socket *s;
	
	s = ceph_socket_create();
	if (IS_ERR(s))
		return PTR_ERR(s);
	con->s = s;

	s->sock->sk->sk_allocation = GFP_NOFS;

	ret = ls->sock->ops->accept(ls->sock, s->sock, O_NONBLOCK);
	if (ret < 0) {
		derr(0, "accept error: %d\n", ret);
		goto err;
	}

	/* setup callbacks */
	set_sock_callbacks(s, con);

	s->sock->ops = ls->sock->ops;
	s->sock->type = ls->sock->type;
	ret = s->sock->ops->getname(s->sock, paddr, &len, 2);
	if (ret < 0) {
		derr(0, "getname error: %d\n", ret);
		goto err;
	}
	return ret;

err:
	ceph_socket_put(s);
	con->s = 0;
	return ret;
}

/*
 * receive a message this may return after partial send
 */
int ceph_tcp_recvmsg(struct ceph_socket *s, void *buf, size_t len)
{
	struct kvec iov = {buf, len};
	struct msghdr msg = {.msg_flags = 0};
	int rlen = 0;		/* length read */

	msg.msg_flags |= MSG_DONTWAIT | MSG_NOSIGNAL;
	rlen = kernel_recvmsg(s->sock, &msg, &iov, 1, len, msg.msg_flags);
	return(rlen);
}


/*
 * Send a message this may return after partial send
 */
int ceph_tcp_sendmsg(struct ceph_socket *s, struct kvec *iov,
		     size_t kvlen, size_t len, int more)
{
	struct msghdr msg = {.msg_flags = 0};
	int rlen = 0;

	msg.msg_flags |= MSG_DONTWAIT | MSG_NOSIGNAL;
	if (more)
		msg.msg_flags |= MSG_MORE;
	else
		msg.msg_flags |= MSG_EOR;  /* superfluous, but what the hell */

	/*printk(KERN_DEBUG "before sendmsg %d\n", len);*/
	rlen = kernel_sendmsg(s->sock, &msg, iov, kvlen, len);
	/*printk(KERN_DEBUG "after sendmsg %d\n", rlen);*/
	return(rlen);
}

/*
 *  workqueue initialization
 */

int ceph_workqueue_init(void)
{
	int ret = 0;

	dout(20, "entered work_init\n");
	/*
	 * Create a num CPU threads to handle receive requests
	 * note: we can create more threads if needed to even out
	 * the scheduling of multiple requests..
	 */
	recv_wq = create_workqueue("ceph-recv");
	ret = IS_ERR(recv_wq);
	if (ret) {
		derr(0, "receive worker failed to start: %d\n", ret);
		destroy_workqueue(recv_wq);
		return ret;
	}

	/*
	 * Create a single thread to handle send requests
	 * note: may use same thread pool as receive workers later...
	 */
	send_wq = create_singlethread_workqueue("ceph-send");
	ret = IS_ERR(send_wq);
	if (ret) {
		derr(0, "send worker failed to start: %d\n", ret);
		destroy_workqueue(send_wq);
		return ret;
	}

	/*
	 * Create a single thread to handle send requests
	 * note: may use same thread pool as receive workers later...
	 */
	accept_wq = create_singlethread_workqueue("ceph-accept");
	ret = IS_ERR(accept_wq);
	if (ret) {
		derr(0, "accept worker failed to start: %d\n", ret);
		destroy_workqueue(accept_wq);
		return ret;
	}
	dout(20, "successfully created wrkqueues\n");

	return(ret);
}

/*
 *  workqueue shutdown
 */
void ceph_workqueue_shutdown(void)
{
	destroy_workqueue(accept_wq);
	destroy_workqueue(send_wq);
	destroy_workqueue(recv_wq);
}
