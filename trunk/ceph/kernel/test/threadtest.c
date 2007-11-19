#include <linux/module.h>
#include <linux/kthread.h>

MODULE_AUTHOR("Patience Warnick <patience@newdream.net>");
MODULE_DESCRIPTION("kernel thread test for Linux");
MODULE_LICENSE("GPL");

#define NTHREADS 3
struct task_struct *thread[3];
/*  static DECLARE_WAIT_QUEUE_HEAD(dispatch_wait); */

/*
 *  Wake up a thread when there is work to do
 */
/*  void thread_wake_up(void)
{
}
*/

static int dispatch_thread(void *unusedfornow)
{
	printk(KERN_INFO "starting kernel thread\n");
	set_current_state(TASK_INTERRUPTIBLE);
        /* an endless loop in which we are doing our work */
	while (!kthread_should_stop())
        {
		schedule();
		printk(KERN_INFO "Dispatch thread has been interrupted\n");
		set_current_state(TASK_INTERRUPTIBLE);
	}
	set_current_state(TASK_RUNNING);
	printk(KERN_INFO "kernel thread exiting\n");
	return 0;
}

static int __init init_kst(void)
{
	int i;

	printk(KERN_INFO "kernel thread test init\n");
	/* create new kernel threads */
	for (i=0; i < NTHREADS; i++) {
		thread[i] = kthread_run(dispatch_thread, NULL, "dispatcher thread");
        	if (IS_ERR(thread[i]))
        	{
                	printk(KERN_INFO "failured to start kernel thread\n");
                	return -ENOMEM;
        	}
	}
	return 0;
}

static void __exit exit_kst(void)
{
	int i;

	printk(KERN_INFO "kernel thread test exit\n");
	for (i=0; i <NTHREADS; i++) {
		kthread_stop(thread[i]);
		wake_up_process(thread[i]);
	}

	return;
}


module_init(init_kst);
module_exit(exit_kst);
