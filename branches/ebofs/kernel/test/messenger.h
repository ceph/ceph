#ifndef __FS_CEPH_MESSENGER_H
#define __FS_CEPH_MESSENGER_H

#include <linux/uio.h>
#include <linux/net.h>
#include <linux/radix-tree.h>
#include <linux/workqueue.h>
#include <linux/ceph_fs.h>

struct ceph_messenger {
        struct socket *listen_sock;      /* listening socket */
        struct work_struct awork;        /* accept work */
        struct ceph_entity_inst inst;    /* my name+address */
        spinlock_t con_lock;
        struct list_head con_all;        /* all connections */
};

#define	NEW 1
#define	CONNECTING 2
#define	ACCEPTING 3
#define	OPEN 4
#define READ_PEND 5
#define WRITE_PEND 6
#define	REJECTING 7
#define	CLOSED 8


struct ceph_connection {
	struct socket *sock;	/* connection socket */
	__u32 state;
	
	atomic_t nref;
	spinlock_t con_lock;    /* connection lock */

	struct list_head list_all;   /* msgr->con_all */
	struct ceph_entity_addr peer_addr; /* peer address */

	char *buffer;

	struct work_struct rwork;		/* received work */
	struct work_struct swork;		/* send work */
	int retries;
	int error;				/* error on connection */
};


struct ceph_connection *new_connection(struct ceph_messenger *);
int do_connect(struct ceph_connection *con);
#endif
