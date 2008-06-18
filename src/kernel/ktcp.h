#ifndef _FS_CEPH_TCP_H
#define _FS_CEPH_TCP_H

extern struct workqueue_struct *con_wq;       /* receive work queue */

/* prototype definitions */
struct socket *ceph_tcp_connect(struct ceph_connection *);
int ceph_tcp_listen(struct ceph_messenger *);
int ceph_tcp_accept(struct socket *, struct ceph_connection *);
int ceph_tcp_recvmsg(struct socket *, void *, size_t );
int ceph_tcp_sendmsg(struct socket *, struct kvec *, size_t, size_t, int more);
int ceph_workqueue_init(void);
void ceph_workqueue_shutdown(void);

/* Max number of outstanding connections in listener queueu */
#define NUM_BACKUP 10
#endif
