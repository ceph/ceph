#include <linux/module.h>
#include <linux/kthread.h>
#include <linux/socket.h>
#include <linux/net.h>
#include <linux/string.h>
#include <net/tcp.h>

#include <linux/ceph_fs.h>
#include "messenger.h"
#include "ktcp.h"


#define PORT 9009
#define HOST cranium
#define cranium 192.168.1.3

MODULE_AUTHOR("Patience Warnick <patience@newdream.net>");
MODULE_DESCRIPTION("kernel thread test for Linux");
MODULE_LICENSE("GPL");

struct ceph_messenger *ceph_messenger_create(void);
int inet_aton (const char *cp, struct in_addr *addr);
struct task_struct *thread;


/*
 *  Wake up a thread when there is work to do
 */
/*  void thread_wake_up(void)
{
}
*/

/*
 * Client connect thread
 */
static int listen_thread(void *unusedfornow)
{
	struct ceph_messenger *msgr = NULL;

	printk(KERN_INFO "starting kernel listen thread\n");

	msgr = ceph_messenger_create();
	if (msgr == NULL) return 1;

	printk(KERN_INFO "about to listen\n");

	set_current_state(TASK_INTERRUPTIBLE);
        /* an endless loop in which we are doing our work */
	while (!kthread_should_stop())
        {
		set_current_state(TASK_INTERRUPTIBLE);
		schedule();
		printk(KERN_INFO "sock thread has been interrupted\n");
	}

	set_current_state(TASK_RUNNING);
        sock_release(msgr->listen_sock);
        kfree(msgr);
	printk(KERN_INFO "kernel thread exiting\n");
	return 0;
}
/*
 * Client connect thread
 */
static int connect_thread(void *unusedfornow)
{
	struct ceph_messenger *msgr = NULL;
	struct ceph_connection *con;
	struct sockaddr_in saddr;
	int ret;

	printk(KERN_INFO "starting kernel thread\n");

	con = new_connection(msgr);

	/* inet_aton("192.168.1.3", &saddr.sin_addr); */
	/* con->peer_addr.ipaddr.sin_addr = saddr.sin_addr; */

        saddr.sin_family = AF_INET;
        saddr.sin_addr.s_addr = htonl(INADDR_ANY);
        saddr.sin_port = htons(PORT);
	printk(KERN_INFO "saddr info %p\n", &saddr);
	con->peer_addr.ipaddr = saddr;


	set_bit(WRITE_PEND, &con->state);

	printk(KERN_INFO "about to connect to server\n");
	/* connect to server */
	if ((ret = _kconnect(con))) {
		printk(KERN_INFO "error connecting %d\n", ret);
		goto done;
	}
	printk(KERN_INFO "connect succeeded\n");

	set_current_state(TASK_INTERRUPTIBLE);
        /* an endless loop in which we are doing our work */
	while (!kthread_should_stop())
        {
		set_current_state(TASK_INTERRUPTIBLE);
		schedule();
		printk(KERN_INFO "sock thread has been interrupted\n");
	}

	set_current_state(TASK_RUNNING);
        sock_release(con->sock);
done:
        kfree(con);
	printk(KERN_INFO "kernel thread exiting\n");
	return 0;
}

static int __init init_kst(void)
{
	int ret;

	printk(KERN_INFO "kernel thread test init\n");
	/* create new kernel threads */
	ret = work_init();
	thread = kthread_run(listen_thread, NULL, "listen-thread");
       	if (IS_ERR(thread))
       	{
               	printk(KERN_INFO "failured to start kernel thread\n");
               	return -ENOMEM;
       	}
	return 0;
}

static void __exit exit_kst(void)
{
	printk(KERN_INFO "kernel thread test exit\n");
	shutdown_workqueues();
	kthread_stop(thread);
	wake_up_process(thread);

	return;
}


module_init(init_kst);
module_exit(exit_kst);
