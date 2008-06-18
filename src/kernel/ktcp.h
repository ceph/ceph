#ifndef _FS_CEPH_TCP_H
#define _FS_CEPH_TCP_H

extern struct workqueue_struct *con_wq;       /* receive work queue */

/* wrap socket, since we use/drop it from multiple threads */
extern struct kobject *ceoh_sockets_kobj;

struct ceph_socket {
	struct kobject kobj;
	struct socket *sock;
};

/* prototype definitions */
struct ceph_socket *ceph_tcp_connect(struct ceph_connection *);
int ceph_tcp_listen(struct ceph_messenger *);
int ceph_tcp_accept(struct ceph_socket *, struct ceph_connection *);
int ceph_tcp_recvmsg(struct ceph_socket *, void *, size_t );
int ceph_tcp_sendmsg(struct ceph_socket *, struct kvec *, size_t, size_t, int more);
void ceph_cancel_sock_callbacks(struct ceph_socket *);
int ceph_workqueue_init(void);
void ceph_workqueue_shutdown(void);

extern void ceph_socket_get(struct ceph_socket *s);
extern void ceph_socket_put(struct ceph_socket *s);

/* Max number of outstanding connections in listener queueu */
#define NUM_BACKUP 10
#endif
