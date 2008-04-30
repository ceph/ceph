#ifndef _FS_CEPH_TCP_H
#define _FS_CEPH_TCP_H

extern struct workqueue_struct *recv_wq;       /* receive work queue */
extern struct workqueue_struct *send_wq;       /* send work queue */

/* prototype definitions */
int ceph_tcp_connect(struct ceph_connection *);
int ceph_tcp_listen(struct ceph_messenger *);
int ceph_tcp_accept(struct socket *, struct ceph_connection *);
int ceph_tcp_recvmsg(struct socket *, void *, size_t );
int ceph_tcp_sendmsg(struct socket *, struct kvec *, size_t, size_t, int more);
void ceph_sock_release(struct socket *);
int ceph_workqueue_init(void);
void ceph_workqueue_shutdown(void);

/* Well known port for ceph client listener.. */
#define CEPH_PORT 2002
/* Max number of outstanding connections in listener queueu */
#define NUM_BACKUP 10
#endif
