#include <linux/kthread.h>
#include <linux/socket.h>
#include <linux/net.h>
#include <linux/string.h>
#include <net/tcp.h>

#include <linux/ceph_fs.h>
#include "messenger.h"
#include "ktcp.h"

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
        ret = _ksendmsg(con->sock,&iov,1,iov.iov_len);
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

	len = _krecvmsg(con->sock,con->buffer,255);
	if (len < 0){
        	printk(KERN_INFO "ERROR reading from socket\n");
		goto done;
	}
        printk(KERN_INFO "received message: %s\n", con->buffer);
        printk(KERN_INFO "message length: %d\n", len);

	kfree(con->buffer);
        set_bit(WRITE_PEND, &con->state);
        queue_work(send_wq, &con->swork);
done:
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

        printk(KERN_INFO "Entered try_accept\n");

        /* initialize the msgr connection */
        new_con = new_connection(msgr);
        if (new_con == NULL) {
                printk(KERN_INFO "malloc failure\n");
                goto done;
        }

        if(_kaccept(msgr->listen_sock, new_con) < 0) {
                printk(KERN_INFO "error accepting connection\n");
                kfree(new_con);
                goto done;
        }
        printk(KERN_INFO "accepted connection \n");

        /* prepare_write_accept_announce(msgr, new_con); */

        /* add_connection_accepting(msgr, new_con); */

        set_bit(WRITE_PEND, &new_con->state);
        /*
         * hand off to worker threads ,should be able to write, we want to
         * try to write right away, we may have missed socket state change
         */
        queue_work(send_wq, &new_con->swork);
done:
        return;
}

/*
 * create a new messenger instance, creates a listening socket
 */
struct ceph_messenger *ceph_messenger_create(void)
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

        INIT_WORK(&msgr->awork, try_accept);       /* setup work structure */

        printk(KERN_INFO "ceph_messenger_create listening on %x:%d\n",
		ntohl(msgr->inst.addr.ipaddr.sin_addr.s_addr),
		ntohl(msgr->inst.addr.ipaddr.sin_port));
        return msgr;
}
