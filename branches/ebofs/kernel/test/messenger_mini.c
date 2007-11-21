#include <linux/kthread.h>
#include <linux/socket.h>
#include <linux/net.h>
#include <linux/string.h>
#include <net/tcp.h>

#include <linux/ceph_fs.h>
#include "messenger.h"
#include "ktcp.h"

static struct workqueue_struct *recv_wq;        /* receive work queue */
static struct workqueue_struct *send_wq;        /* send work queue */

static void try_read(struct work_struct *);
static void try_write(struct work_struct *);
static void try_accept(struct work_struct *);

int add_sock_callbacks(struct socket *, struct ceph_connection *);


/*
 * connections
 */

/* 
 * create a new connection.  initial state is NEW.
 */
struct ceph_connection *new_connection(struct ceph_messenger *msgr)
{
	struct ceph_connection *con;
	con = kmalloc(sizeof(struct ceph_connection), GFP_KERNEL);
	if (con == NULL) 
		return NULL;
	memset(con, 0, sizeof(struct ceph_connection));

	/* con->msgr = msgr; */

	spin_lock_init(&con->con_lock);
	INIT_WORK(&con->rwork, try_read);	/* setup work structure */
	INIT_WORK(&con->swork, try_write);	/* setup work structure */

	atomic_inc(&con->nref);
	return con;
}

/*
 * initiate connection to a remote socket.
 *
 */
int do_connect(struct ceph_connection *con)
{
        struct sockaddr *paddr = (struct sockaddr *)&con->peer_addr.ipaddr;
	int ret = 0;


	set_bit(CONNECTING, &con->state);

	ret = sock_create_kern(AF_INET, SOCK_STREAM, IPPROTO_TCP, &con->sock);
	if (ret < 0) {
                printk(KERN_INFO "sock_create_kern error: %d\n", ret);
                goto done;
        }

        /* setup callbacks */
        add_sock_callbacks(con->sock, con);


        ret = con->sock->ops->connect(con->sock, paddr, 
				      sizeof(struct sockaddr_in),O_NONBLOCK);
	if (ret == -EINPROGRESS) return 0;
        if (ret < 0) {
		/* TBD check for fatal errors, retry if not fatal.. */
                printk(KERN_INFO "kernel_connect error: %d\n", ret);
                sock_release(con->sock);
		con->sock = NULL;
        }
done:
        printk(KERN_INFO "do_connect state = %u\n", con->state);
        return ret;
}

/*
 * call when socket is writeable
 */
static void try_write(struct work_struct *work)
{
	struct ceph_connection *con;
        struct kvec iov;
	int ret;



	con = container_of(work, struct ceph_connection, swork);

        printk(KERN_INFO "Entered try_write state = %u\n", con->state);

	clear_bit(WRITE_PEND, &con->state);
	set_bit(READ_PEND, &con->state);


        con->buffer = kzalloc(256, GFP_KERNEL);
        if (con->buffer == NULL)
                goto done;

        strcpy(con->buffer, "hello world");
        iov.iov_base = con->buffer;
        iov.iov_len = 255;
        printk(KERN_INFO "about to send message\n");
        ret = _ksendmsg(con->sock,&iov,1,iov.iov_len,0);
        if (ret < 0) goto done;
        printk(KERN_INFO "wrote %d bytes to server\n", ret);

	

	kfree(con->buffer);
done:
	return;
}


/*
 * call when data is available on the socket
 */
static void try_read(struct work_struct *work)
{
	struct ceph_connection *con;
	int len;


	con = container_of(work, struct ceph_connection, rwork);

        printk(KERN_INFO "Entered try_read state = %u\n", con->state);

        con->buffer = kzalloc(256, GFP_KERNEL);
        if (con->buffer == NULL)
                goto done;

	len = _krecvmsg(con->sock,con->buffer,255,0);
	if (len < 0){
        	printk(KERN_INFO "ERROR reading from socket\n");
		goto done;
	}
        printk(KERN_INFO "received message: %s\n", con->buffer);
        printk(KERN_INFO "message length: %d\n", len);

	kfree(con->buffer);
done:
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


        printk(KERN_INFO "Entered try_accept\n");


        if (kernel_accept(sock, &new_sock, sock->file->f_flags) < 0) {
        	printk(KERN_INFO "error accepting connection \n");
                goto done;
        }
        printk(KERN_INFO "accepted connection \n");

        /* get the address at the other end */
        memset(&saddr, 0, sizeof(saddr));
        if (new_sock->ops->getname(new_sock, (struct sockaddr *)&saddr, &len, 2)) {
                printk(KERN_INFO "getname error connection aborted\n");
                sock_release(new_sock);
                goto done;
        }

	/* initialize the msgr connection */
	new_con = new_connection(msgr);
	if (new_con == NULL) {
               	printk(KERN_INFO "malloc failure\n");
		sock_release(new_sock);
		goto done;
       	}
	new_con->sock = new_sock;
	set_bit(ACCEPTING, &new_con->state);
	/* new_con->in_tag = CEPH_MSGR_TAG_READY; */
	/* fill in part of peers address */
	new_con->peer_addr.ipaddr = saddr;

	/* hand off to worker threads , send pending */
	/*?? queue_work(send_wq, &new_con->swork);*/
done:
        return;
}

/*
 * create a new messenger instance, saddr is address specified from mount arg.
 * If null, will get created by _klisten()
 */
struct ceph_messenger *ceph_create_messenger(struct sockaddr *saddr)
{
        struct ceph_messenger *msgr;

        msgr = kmalloc(sizeof(*msgr), GFP_KERNEL);
        if (msgr == NULL)
                return NULL;
        memset(msgr, 0, sizeof(*msgr));

        spin_lock_init(&msgr->con_lock);

        /* create listening socket */
        msgr->listen_sock = _klisten(saddr);
        if (msgr->listen_sock == NULL) {
                kfree(msgr);
                return NULL;
        }
        /* TBD: setup callback for accept */
        INIT_WORK(&msgr->awork, try_accept);       /* setup work structure */
        return msgr;
}

/* Data available on socket or listen socket received a connect */
static void ceph_data_ready(struct sock *sk, int count_unused)
{
        struct ceph_connection *con = (struct ceph_connection *)sk->sk_user_data;

        printk(KERN_INFO "Entered ceph_data_ready state = %u\n", con->state);
        if (test_bit(READ_PEND, &con->state)) {
        	printk(KERN_INFO "READ_PEND set in connection\n");
                queue_work(recv_wq, &con->rwork);
	}
}

static void ceph_write_space(struct sock *sk)
{
        struct ceph_connection *con = (struct ceph_connection *)sk->sk_user_data;

        printk(KERN_INFO "Entered ceph_write_space state = %u\n",con->state);
        if (test_bit(WRITE_PEND, &con->state)) {
        	printk(KERN_INFO "WRITE_PEND set in connection\n");
                queue_work(send_wq, &con->swork);
	}
}

static void ceph_state_change(struct sock *sk)
{
        struct ceph_connection *con = (struct ceph_connection *)sk->sk_user_data;
	/* TBD: probably want to set our connection state to OPEN
	 * if state not set to READ or WRITE pending
	 */
        printk(KERN_INFO "Entered ceph_state_change state = %u\n", con->state);
        if (sk->sk_state == TCP_ESTABLISHED) {
		if (test_and_clear_bit(CONNECTING, &con->state))
			set_bit(OPEN, &con->state);
                ceph_write_space(sk);
	}
}

/* Make a socket active */
int add_sock_callbacks(struct socket *sock, struct ceph_connection *con)
{
	struct sock *sk = sock->sk;
	sk->sk_user_data = con;
        printk(KERN_INFO "Entered add_sock_callbacks\n");

        /* Install callbacks */
	sk->sk_data_ready = ceph_data_ready;
	sk->sk_write_space = ceph_write_space;
	sk->sk_state_change = ceph_state_change;

        return 0;
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
