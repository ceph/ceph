#ifndef _FS_CEPH_TCP_H
#define _FS_CEPH_TCP_H

/* prototype definitions */
int _kconnect(struct ceph_connection *);
int _klisten(struct ceph_messenger *);
struct socket *_kaccept(struct socket *);
int _krecvmsg(struct socket *, void *, size_t );
int _ksendmsg(struct socket *, struct kvec *, size_t, size_t);

/* Well known port for ceph client listener.. */
#define CEPH_PORT 2002
/* Max number of outstanding connections in listener queueu */
#define NUM_BACKUP 10
#endif
