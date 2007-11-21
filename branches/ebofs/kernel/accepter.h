#ifndef __FS_CEPH_ACCEPTER_H
#define __FS_CEPH_ACCEPTOR_H

#include <linux/kthread.h>
#include <linux/net.h>

/*
 *  Information about client thread
 */
struct ceph_accepter {
	struct task_struct accepter_thread;	/* thread */
	struct socket sock;			/* Socket */
};

/*
 * Prototypes definitions
 */
int ceph_accepter_start(void);
void ceph_accepter_shutdown(struct ceph_accepter *accepter);

#endif
