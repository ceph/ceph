#ifndef __FS_CEPH_POLL_H
#define __FS_CEPH_POLL_H

struct ceph_poll_task *start_poll(void);

/* list of pollable files */
struct ceph_pollable {
	spinlock_t plock;
        struct list_head poll_list;
	struct file *file;
	struct ceph_connection *con;
};

struct ceph_poll_task {
        struct task_struct *poll_task;
	struct ceph_pollable *pfiles;
	u64 timeout;
};

#endif
