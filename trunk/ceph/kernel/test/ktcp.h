#ifndef _FS_CEPH_TCP_H
#define _FS_CEPH_TCP_H

extern struct workqueue_struct *recv_wq;       /* receive work queue */
extern struct workqueue_struct *send_wq;       /* send work queue */

/* prototype definitions */
int _kconnect(struct ceph_connection *);
int _klisten(struct ceph_messenger *);
int _kaccept(struct socket *, struct ceph_connection *);
int _krecvmsg(struct socket *, void *, size_t );
int _ksendmsg(struct socket *, struct kvec *, size_t, size_t);
int work_init(void);
void shutdown_workqueues(void);

/* Well known port for ceph client listener.. */
#define CEPH_PORT 2002
/* Max number of outstanding connections in listener queueu */
#define NUM_BACKUP 10
#endif
