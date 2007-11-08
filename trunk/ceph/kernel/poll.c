#include <linux/kthread.h>
#include <linux/socket.h>
#include <linux/net.h>
#include <linux/ceph_fs.h>
#include <linux/string.h>
#include <linux/poll.h>
#include <net/sock.h>
#include <net/tcp.h>
#include "messenger.h"
#include "ktcp.h"


static int do_ceph_pollfd(struct file *file)
{
        int mask;
	struct sock *sk = file->private_data->sk;
	/* may keep connection in poll list instead of using this field */
	struct ceph_connection *con = 
		(struct ceph_connection *)sk->sk_user_data;


	mask = file->f_op->poll(file, NULL);

        if (mask & POLLIN) {
		printk(KERN_INFO "socket read ready: %d\n", mask);
                /* if (sk->sk_state == TCP_LISTEN) */
		if (test_bit(LISTENING, &con->state) {
			set_bit(ACCEPTING, &con->state);
                        queue_work(recvwq, &con->awork)
			return(0); /* don't want to delete.. */
		} else {
			/* set_bit(READ_SCHED, &con->state); */
			queue_work(recvwq, &con->rwork);
		}
        }

        if (mask & POLLOUT) {
		printk(KERN_INFO "socket read ready: %d\n", mask);
		/* set_bit(WRITE_SCHED, &con>state); */
		queue_work(sendwq, &con->swork);
        }

        if (mask & (POLLERR | POLLHUP)) {
		printk(KERN_INFO "poll error: %d\n", mask);
		/* TBD:  handle error need may need reconnect */
        }
	return mask;
}

/*
 * Poll thread function, start after creating listener connection
 */
int ceph_poll(struct ceph_messenger *msgr)
{
	struct ceph_pollable *pos, *tmp;
	struct ceph_pollable *plist = msgr->poll_task->pfiles->poll_list;

        printk(KERN_INFO "starting kernel poll thread\n");

        set_current_state(TASK_INTERRUPTIBLE);

        /* an endless loop in which we are polling sockets */
        while (!kthread_should_stop()) {

        /*
         * quickly go through the list and then sleep, so each socket
         * doesn't have to wait for the timeout of the previous socket
         * this will work better for a large number of file descriptors
         */
        	list_for_each_entry_safe(pos, tmp, plist, ceph_pollable) {
                	if (do_ceph_pollfd(pos->file, NULL)) {
                        	/* remove file from poll_list */
				spin_lock(&pos->plock);
				list_del(pos->poll_list);
				spin_unlock(&pos->plock);
				/* TBD: free list entry or reuse..Need reuse list */
				/* double check not freeing out from undermyself*/
				kfree(pos);
                	}
        	}
        	schedule_timeout(timeout);
        }
        set_current_state(TASK_RUNNING);
        printk(KERN_INFO "kernel thread exiting\n");
        return(0);
}
