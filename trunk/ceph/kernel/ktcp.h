#ifndef _FS_CEPH_TCP_H
#define _FS_CEPH_TCP_H

/* prototype definitions */
struct socket * _kconnect(struct sockaddr *);
struct socket * _klisten(struct sockaddr_in *);
struct socket *_kaccept(struct socket *);
int _krecvmsg(struct socket *, void *, size_t , unsigned);
int _ksendmsg(struct socket *, struct kvec *, size_t, size_t, unsigned);

/* Well known port for ceph client listener.. */
#define CEPH_PORT 2002
/* Max number of outstanding connections in listener queueu */
#define NUM_BACKUP 10
#endif
