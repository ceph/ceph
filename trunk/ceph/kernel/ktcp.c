#include <linux/socket.h>
#include <linux/net.h>
#include <net/tcp.h>
#include <linux/string.h>
#include "messenger.h"
#include "ktcp.h"

struct workqueue_struct *recv_wq = NULL;	/* receive work queue */
struct workqueue_struct *send_wq = NULL;	/* send work queue */

/*
 * socket callback functions 
 */
/* Data available on socket or listen socket received a connect */
static void ceph_data_ready(struct sock *sk, int count_unused)
{
        struct ceph_connection *con;
        struct ceph_messenger *msgr;

	if (sk->sk_state == TCP_LISTEN) {
		msgr = (struct ceph_messenger *)sk->sk_user_data;
		dout(30, "ceph_data_ready listener %p\n", msgr);
		queue_work(recv_wq, &msgr->awork);
	} else {
        	con = (struct ceph_connection *)sk->sk_user_data;
		dout(30, "ceph_data_ready connection %p state = %u, queuing rwork\n",
		     con, con->state);
		queue_work(recv_wq, &con->rwork);
	}
}

/* socket has bufferspace for writing */
static void ceph_write_space(struct sock *sk)
{
        struct ceph_connection *con = (struct ceph_connection *)sk->sk_user_data;

        dout(30, "ceph_write_space %p state = %u\n", con, con->state);
        if (test_bit(WRITE_PENDING, &con->state)) {
                dout(30, "ceph_write_space %p queueing write work\n", con);
                queue_work(send_wq, &con->swork);
        }
}

/* sockets state has change */
static void ceph_state_change(struct sock *sk)
{
        struct ceph_connection *con = (struct ceph_connection *)sk->sk_user_data;

        dout(30, "ceph_state_change %p state = %u\n", con, con->state);
        if (sk->sk_state == TCP_ESTABLISHED) {
		if (test_bit(CONNECTING, &con->state) ||
		    test_bit(ACCEPTING, &con->state)) {
			dout(30, "ceph_state_change %p socket established, queuing swork\n", con);
			queue_work(send_wq, &con->swork);
		}
        }
}

/* make a socket active by setting up the call back functions */
static int set_sock_callbacks(struct socket *sock, void *user_data)
{
        struct sock *sk = sock->sk;
        sk->sk_user_data = user_data;
        dout(20, "set_sock_callbacks\n");

        /* Install callbacks */
        sk->sk_data_ready = ceph_data_ready;
        sk->sk_write_space = ceph_write_space;
        sk->sk_state_change = ceph_state_change;

        return 0;
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
                derr(1, "ceph_tcp_connect sock_create_kern error: %d\n", ret);
                goto done;
        }

        set_sock_callbacks(con->sock, (void *)con);
        
	ret = con->sock->ops->connect(con->sock, paddr,
                                      sizeof(struct sockaddr_in), O_NONBLOCK);
        if (ret == -EINPROGRESS) 
		return 0;
        if (ret < 0) {
                /* TBD check for fatal errors, retry if not fatal.. */
                derr(1, "ceph_tcp_connect kernel_connect error: %d\n", ret);
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
	ret = kernel_setsockopt(sock, SOL_SOCKET, SO_REUSEADDR,
				(char *)&optval, sizeof(optval)); 
	if (ret < 0) {
		derr(0, "Failed to set SO_REUSEADDR: %d\n", ret);
		goto err;
	}

	/* set user_data to be the messenger */
	sock->sk->sk_user_data = msgr;

	/* currently no user specified address given so create */
	/* if (!*myaddr) */
	myaddr->sin_family = AF_INET;
	myaddr->sin_addr.s_addr = htonl(INADDR_ANY);
	myaddr->sin_port = htons(0);  /* any port */
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
	dout(0, "ceph_tcp_listen on port %d\n", ntohs(myaddr->sin_port));

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
        set_sock_callbacks(msgr->listen_sock, (void *)msgr);

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
                printk(KERN_INFO "sock_create_kern error: %d\n", ret);
                goto done;
        }

        ret = sock->ops->accept(sock, con->sock, O_NONBLOCK);
	/* ret = kernel_accept(sock, &new_sock, sock->file->f_flags); */
        if (ret < 0) {
		printk(KERN_INFO "accept error: %d\n", ret);
		goto err;
	}

        /* setup callbacks */
        set_sock_callbacks(con->sock, (void *)con);

	con->sock->ops = sock->ops;
	con->sock->type = sock->type;
	ret = con->sock->ops->getname(con->sock, paddr, &len, 2);
        if (ret < 0) {
		printk(KERN_INFO "getname error: %d\n", ret);
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

	//dout(30, "ceph_tcp_recvmsg %p len %d %p-%p\n", sock, (int)len, buf, buf+len);
	msg.msg_flags |= MSG_DONTWAIT | MSG_NOSIGNAL;
	/* receive one kvec for now...  */
	rlen = kernel_recvmsg(sock, &msg, &iov, 1, len, msg.msg_flags);
	//dout(30, "ceph_tcp_recvmsg %p len %d ret = %d\n", sock, (int)len, rlen);
	return(rlen);
}

/*
 * Send a message this may return after partial send
 */
int ceph_tcp_sendmsg(struct socket *sock, struct kvec *iov, size_t kvlen, size_t len)
{
	struct msghdr msg = {.msg_flags = 0};
	int rlen = 0;

	//dout(30, "ceph_tcp_sendmsg %p len %d\n", sock, (int)len);
	msg.msg_flags |=  MSG_DONTWAIT | MSG_NOSIGNAL;
	rlen = kernel_sendmsg(sock, &msg, iov, kvlen, len);
	//dout(30, "ceph_tcp_sendmsg %p len %d ret = %d\n", sock, (int)len, rlen);
	return(rlen);
}

/*
 *  workqueue initialization
 */

int ceph_workqueue_init(void)
{
        int ret = 0;

        printk(KERN_INFO "entered work_init\n");
        /*
         * Create a num CPU threads to handle receive requests
         * note: we can create more threads if needed to even out
         * the scheduling of multiple requests..
         */
        recv_wq = create_workqueue("ceph-recv");
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
        send_wq = create_singlethread_workqueue("ceph-send");
        ret = IS_ERR(send_wq);
        if (ret) {
                printk(KERN_INFO "send worker failed to start: %d\n", ret);
                destroy_workqueue(send_wq);
                return ret;
        }
        printk(KERN_INFO "successfully created wrkqueues\n");

        return(ret);
}

/*
 *  workqueue shutdown
 */
void ceph_workqueue_shutdown(void)
{
        destroy_workqueue(send_wq);
        destroy_workqueue(recv_wq);
}
