#include <linux/socket.h>
#include <linux/net.h>
#include <net/tcp.h>
#include <linux/string.h>
#include "messenger.h"
#include "ktcp.h"

struct workqueue_struct *recv_wq;       /* receive work queue */
struct workqueue_struct *send_wq;      	/* send work queue */

/*
 * socket callback functions 
 */
/* listen socket received a connect */
static void ceph_accept_ready(struct sock *sk, int count_unused)
{
        struct ceph_messenger *msgr = (struct ceph_messenger *)sk->sk_user_data;

        printk(KERN_INFO "Entered ceph_accept_ready \n");
	if (msgr && (sk->sk_state == TCP_LISTEN))
		queue_work(recv_wq, &msgr->awork); 
}

/* Data available on socket or listen socket received a connect */
static void ceph_data_ready(struct sock *sk, int count_unused)
{
        struct ceph_connection *con = (struct ceph_connection *)sk->sk_user_data;

        printk(KERN_INFO "Entered ceph_data_ready \n");

	if (con && (sk->sk_state != TCP_CLOSE_WAIT)) {
		queue_work(recv_wq, &con->rwork);
	}
}

/* socket has bufferspace for writing */
static void ceph_write_space(struct sock *sk)
{
        struct ceph_connection *con = (struct ceph_connection *)sk->sk_user_data;

        printk(KERN_INFO "Entered ceph_write_space state = %u\n",con->state);
        if (con && test_bit(WRITE_PEND, &con->state)) {
                printk(KERN_INFO "WRITE_PEND set in connection\n");
                queue_work(send_wq, &con->swork);
        }
}

/* sockets state has change */
static void ceph_state_change(struct sock *sk)
{
        struct ceph_connection *con = (struct ceph_connection *)sk->sk_user_data;

	printk(KERN_INFO "Entered ceph_state_change state = %u\n", con->state);
        printk(KERN_INFO "Entered ceph_state_change sk_state = %u\n", sk->sk_state);

        if (sk->sk_state == TCP_ESTABLISHED) {
                ceph_write_space(sk);
        }
	/* TBD: maybe set connection state to close if TCP_CLOSE
	 * simple case for now,                                  
	 */
}

/* make a listening socket active by setting up the call back functions */
int listen_sock_callbacks(struct socket *sock, void *user_data)
{
        struct sock *sk = sock->sk;
        sk->sk_user_data = user_data;
        printk(KERN_INFO "Entered listen_sock_callbacks\n");

        /* Install callbacks */
        sk->sk_data_ready = ceph_accept_ready;
        return 0;
}

/* make a socket active by setting up the call back functions */
int add_sock_callbacks(struct socket *sock, void *user_data)
{
        struct sock *sk = sock->sk;
        sk->sk_user_data = user_data;
        printk(KERN_INFO "Entered add_sock_callbacks\n");

        /* Install callbacks */
        sk->sk_data_ready = ceph_data_ready;
	sk->sk_state_change = ceph_state_change;
	sk->sk_write_space = ceph_write_space;

        return 0;
}
/*
 * initiate connection to a remote socket.
 */
int _kconnect(struct ceph_connection *con)
{
	int ret;
	struct sockaddr *paddr = (struct sockaddr *)&con->peer_addr.ipaddr;

        set_bit(CONNECTING, &con->state);

        ret = sock_create_kern(AF_INET, SOCK_STREAM, IPPROTO_TCP, &con->sock);
        if (ret < 0) {
                printk(KERN_INFO "sock_create_kern error: %d\n", ret);
                goto done;
        }

        /* setup callbacks */
        add_sock_callbacks(con->sock, (void *)con);


        ret = con->sock->ops->connect(con->sock, paddr,
                                      sizeof(struct sockaddr_in), O_NONBLOCK);
        if (ret == -EINPROGRESS) return 0;
        if (ret < 0) {
                /* TBD check for fatal errors, retry if not fatal.. */
                printk(KERN_INFO "kernel_connect error: %d\n", ret);
                sock_release(con->sock);
                con->sock = NULL;
        }
done:
        return ret;
}

/*
 * setup listening socket
 */
int _klisten(struct ceph_messenger *msgr)
{
	int ret;
	struct socket *sock = NULL;
	int optval = 1;
	struct sockaddr_in *myaddr = &msgr->inst.addr.ipaddr;


	ret = sock_create_kern(AF_INET, SOCK_STREAM, IPPROTO_TCP, &sock);
        if (ret < 0) {
		printk(KERN_INFO "sock_create_kern error: %d\n", ret);
		return ret;
	}
	ret = kernel_setsockopt(sock, SOL_SOCKET, SO_REUSEADDR,
				(char *)&optval, sizeof(optval)); 
	if (ret < 0) {
		printk("Failed to set SO_REUSEADDR: %d\n", ret);
		goto err;
	}

	/* set user_data to be the messenger */
	sock->sk->sk_user_data = msgr;

	/* no user specified address given so create, will allow arg to mount */
	myaddr->sin_family = AF_INET;
	myaddr->sin_addr.s_addr = htonl(INADDR_ANY);
	myaddr->sin_port = htons(CEPH_PORT);  /* known port for now */
	/* myaddr->sin_port = htons(0); */  /* any port */
	ret = sock->ops->bind(sock, (struct sockaddr *)myaddr, 
				sizeof(struct sockaddr_in));
	if (ret < 0) {
		printk("Failed to bind to port %d\n", ret);
		goto err;
	}

	ret = kernel_setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE,
				(char *)&optval, sizeof(optval)); 
	if (ret < 0) {
		printk("Failed to set SO_KEEPALIVE: %d\n", ret);
		goto err;
	}

	msgr->listen_sock = sock;

	/* TBD: probaby want to tune the backlog queue .. */
	ret = sock->ops->listen(sock, NUM_BACKUP);
	if (ret < 0) {
		printk(KERN_INFO "kernel_listen error: %d\n", ret);
		msgr->listen_sock = NULL;
		goto err;
	}
        listen_sock_callbacks(msgr->listen_sock, (void *)msgr);
	return ret;
err:
	sock_release(sock);
	return ret;
}

/*
 *  accept a connection
 */
int _kaccept(struct socket *sock, struct ceph_connection *con)
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
        if (ret < 0) {
		printk(KERN_INFO "accept error: %d\n", ret);
		goto err;
	}

        /* setup callbacks */
        add_sock_callbacks(con->sock, (void *)con);

	con->sock->ops = sock->ops;
	con->sock->type = sock->type;
	ret = con->sock->ops->getname(con->sock, paddr, &len, 2);
        if (ret < 0) {
		printk(KERN_INFO "getname error: %d\n", ret);
		goto err;
	}

        set_bit(ACCEPTING, &con->state);
done:
	return ret;
err:
	sock_release(con->sock);
	return ret;
}

/*
 * receive a message this may return after partial send
 */
int _krecvmsg(struct socket *sock, void *buf, size_t len)
{
	struct kvec iov = {buf, len};
	struct msghdr msg = {.msg_flags = 0};
	int rlen = 0;		/* length read */

	printk(KERN_INFO "entered krevmsg\n");
	msg.msg_flags |= MSG_DONTWAIT | MSG_NOSIGNAL;

	/* receive one kvec for now...  */
	rlen = kernel_recvmsg(sock, &msg, &iov, 1, len, msg.msg_flags);
        if (rlen < 0) {
		printk(KERN_INFO "kernel_recvmsg error: %d\n", rlen);
        }
	/* TBD: kernel_recvmsg doesn't fill in the name and namelen
         */
	return(rlen);

}

/*
 * Send a message this may return after partial send
 */
int _ksendmsg(struct socket *sock, struct kvec *iov, size_t kvlen, size_t len)
{
	struct msghdr msg = {.msg_flags = 0};
	int rlen = 0;

	printk(KERN_INFO "entered ksendmsg\n");
	msg.msg_flags |=  MSG_DONTWAIT | MSG_NOSIGNAL;

	rlen = kernel_sendmsg(sock, &msg, iov, kvlen, len);
        if (rlen < 0) {
		printk(KERN_INFO "kernel_sendmsg error: %d\n", rlen);
        }
	return(rlen);
}

/*
 *  workqueue initialization
 */

int work_init(void)
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
void shutdown_workqueues(void)
{
        destroy_workqueue(send_wq);
        destroy_workqueue(recv_wq);
}
