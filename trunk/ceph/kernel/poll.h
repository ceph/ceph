#ifndef __FS_CEPH_POLL_H
#define __FS_CEPH_POLL_H

int start_poll(void *);

/*
 *  May use, probably not..
 */
struct ceph_poll_task {
        struct task_struct *poll_task;
        struct list_head poll_list;
	u64 timeout;
};

#endif
