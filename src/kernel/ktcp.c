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
static void listen_sock_callbacks(struct socket *sock,
				  struct ceph_messenger *msgr)
{
	struct sock *sk = sock->sk;
	sk->sk_user_data = (void *)msgr;
	sk->sk_data_ready = ceph_accept_ready;
}

/* make a socket active by setting up the call back functions */
static void set_sock_callbacks(struct socket *sock, struct ceph_connection *con)
{
	struct sock *sk = sock->sk;
	sk->sk_user_data = (void *)con;
	sk->sk_data_ready = ceph_data_ready;
	sk->sk_write_space = ceph_write_space;
	sk->sk_state_change = ceph_state_change;
}

void ceph_sock_release(struct socket *sock)
{
	struct sock *sk;
	if (!sock)
		return;
	sk = sock->sk;
	sk->sk_user_data = 0;
	sk->sk_data_ready = 0;
	sk->sk_write_space = 0;
	sk->sk_state_change = 0;
	sock_release(sock);
}

/*
 * initiate connection to a remote socket.
 */
int ceph_tcp_connect(struct ceph_connection *con)
{
	int ret;
	struct sockaddr *paddr = (struct sockaddr *)&con->peer_addr.ipaddr;

	ret = sock_create_kern(AF_INET, SOCK_STREAM, IPPROTO_TCP, &con->sock);
	if (ret < 0) {
		derr(1, "connect sock_create_kern error: %d\n", ret);
		goto done;
	}
	
	con->sock->sk->sk_allocation = GFP_NOFS;

	set_sock_callbacks(con->sock, con);

	ret = con->sock->ops->connect(con->sock, paddr,
				      sizeof(struct sockaddr_in), O_NONBLOCK);
	if (ret == -EINPROGRESS) {
		dout(20, "connect EINPROGRESS sk_state = = %u\n",
		     con->sock->sk->sk_state);
		return 0;
	}
	if (ret < 0) {
		/* TBD check for fatal errors, retry if not fatal.. */
		derr(1, "connect %u.%u.%u.%u:%u error: %d\n",
		     IPQUADPORT(*(struct sockaddr_in *)paddr), ret);
		sock_release(con->sock);
		con->sock = NULL;
	}
done:
	return ret;
}

/*
 * setup listening socket
 */
int ceph_tcp_listen(struct ceph_messenger *msgr)
{
	int ret;
	struct socket *sock = NULL;
	int optval = 1;
	struct sockaddr_in *myaddr = &msgr->inst.addr.ipaddr;
	int nlen;

	ret = sock_create_kern(AF_INET, SOCK_STREAM, IPPROTO_TCP, &sock);
	if (ret < 0) {
		derr(0, "sock_create_kern error: %d\n", ret);
		return ret;
	}

	sock->sk->sk_allocation = GFP_NOFS;

	ret = kernel_setsockopt(sock, SOL_SOCKET, SO_REUSEADDR,
				(char *)&optval, sizeof(optval));
	if (ret < 0) {
		derr(0, "Failed to set SO_REUSEADDR: %d\n", ret);
		goto err;
	}

	ret = sock->ops->bind(sock, (struct sockaddr *)myaddr, sizeof(*myaddr));
	if (ret < 0) {
		derr(0, "Failed to bind: %d\n", ret);
		goto err;
	}

	/* what port did we bind to? */
	nlen = sizeof(*myaddr);
	ret = sock->ops->getname(sock, (struct sockaddr *)myaddr, &nlen, 0);
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

	msgr->listen_sock = sock;

	/* TBD: probaby want to tune the backlog queue .. */
	ret = sock->ops->listen(sock, NUM_BACKUP);
	if (ret < 0) {
		derr(0, "kernel_listen error: %d\n", ret);
		msgr->listen_sock = NULL;
		goto err;
	}

	/* setup callbacks */
	listen_sock_callbacks(msgr->listen_sock, msgr);

	return ret;
err:
	sock_release(sock);
	return ret;
}

/*
 *  accept a connection
 */
int ceph_tcp_accept(struct socket *sock, struct ceph_connection *con)
{
	int ret;
	struct sockaddr *paddr = (struct sockaddr *)&con->peer_addr.ipaddr;
	int len;


	ret = sock_create_kern(AF_INET, SOCK_STREAM, IPPROTO_TCP, &con->sock);
	if (ret < 0) {
		derr(0, "sock_create_kern error: %d\n", ret);
		goto done;
	}

	con->sock->sk->sk_allocation = GFP_NOFS;

	ret = sock->ops->accept(sock, con->sock, O_NONBLOCK);
	/* ret = kernel_accept(sock, &new_sock, sock->file->f_flags); */
	if (ret < 0) {
		derr(0, "accept error: %d\n", ret);
		goto err;
	}

	/* setup callbacks */
	set_sock_callbacks(con->sock, con);

	con->sock->ops = sock->ops;
	con->sock->type = sock->type;
	ret = con->sock->ops->getname(con->sock, paddr, &len, 2);
	if (ret < 0) {
		derr(0, "getname error: %d\n", ret);
		goto err;
	}

done:
	return ret;
err:
	sock_release(con->sock);
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

	/*printk(KERN_DEBUG "before sendmsg %d\n", len);*/
	rlen = kernel_sendmsg(sock, &msg, iov, kvlen, len);
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
