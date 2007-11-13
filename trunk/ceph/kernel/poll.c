#include <linux/kthread.h>
#include <linux/socket.h>
#include <linux/net.h>
#include <linux/string.h>
#include <linux/fs.h>
#include <linux/poll.h>
#include <net/sock.h>
#include <net/tcp.h>
#include <linux/ceph_fs.h>
#include "messenger.h"
#include "ktcp.h"

static struct workqueue_struct *recv_wq;        /* receive work queue */
static struct workqueue_struct *send_wq;        /* send work queue */

/* TBD: probably remove pwait, but may play around with it some.. 
 * null for now.. No timeout, timeout maybe ignored if O_NONBLOCK anyway..
*/
static int do_ceph_poll(struct ceph_connection *con, poll_table *pwait)
{
        int mask;
	struct file *file = con->sock->file;


	mask = file->f_op->poll(file, pwait);

        if (mask & POLLIN) {
                /* if (sk->sk_state == TCP_LISTEN) */
		printk(KERN_INFO "socket read ready: %d\n", mask);
		if (test_bit(LISTENING, &con->state)) {
			set_bit(ACCEPTING, &con->state);
                        queue_work(recv_wq, &con->awork);
			return(0); /* don't want to delete.. */
		} else {
			/* set_bit(READ_SCHED, &con->state); */
			queue_work(recv_wq, &con->rwork);
		}
        }

        if (mask & POLLOUT) {
		printk(KERN_INFO "socket read ready: %d\n", mask);
		/* set_bit(WRITE_SCHED, &con>state); */
		queue_work(send_wq, &con->swork);
        }

        if (mask & (POLLERR | POLLHUP)) {
		printk(KERN_INFO "poll hangup or error: %d\n", mask);
		set_bit(CLOSED, &con->state);
        }
	return mask;
}

/*
 * Poll thread function, start after creating listener connection
 */
int start_polling(void *arg)
{
	struct ceph_connection *pos, *next;
	struct ceph_messenger *pollables = arg;

        printk(KERN_INFO "starting kernel poll thread\n");

        set_current_state(TASK_INTERRUPTIBLE);

        /* an endless loop in which we are polling sockets */
        while (!kthread_should_stop()) {

        /*
         * quickly go through the list and then sleep, so each socket
         * doesn't have to wait for the timeout of the previous socket
         * this will work better for a large number of file descriptors
         */
        	list_for_each_entry_safe(pos, next, &pollables->poll_list, poll_list) {
                	if (do_ceph_poll(pos, NULL)) {
				spin_lock(&pos->con_lock);
                        	/* remove file from poll_list */
				list_del(&pos->poll_list);
				spin_unlock(&pos->con_lock);
				/* TBD: free list entry or reuse..Need reuse list */
				/* double check not freeing out from undermyself*/
				/* kfree(pos); */
                	}
        	}
        	schedule_timeout(5*HZ);  /* TBD: make configurable */
        }
        set_current_state(TASK_RUNNING);
        printk(KERN_INFO "kernel thread exiting\n");
        return(0);
}

void stop_polling(struct ceph_messenger *msgr)
{
        kthread_stop(msgr->poll_task);
        wake_up_process(msgr->poll_task);
}

/*
 * Initialize the work queues
 */

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

        return(ret);
}

void ceph_work_shutdown(void)
{
	destroy_workqueue(send_wq);
	destroy_workqueue(recv_wq);
}
