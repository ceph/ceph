#ifndef _FS_CEPH_AUTH_H
#define _FS_CEPH_AUTH_H

#include "buffer.h"


/*
 * A ticket allows us to open a secure session to a service and prove
 * authorization.
 */
struct ceph_ticket {
	struct ceph_buffer *session_key;     /* a ceph_secret */
	struct ceph_buffer *enc_ticket;      /* opaque to us */
	unsigned long renew_after, expires;
};



struct ceph_auth_data {
	void *private_data;
};

struct ceph_client;

struct ceph_auth_ops {
	int (*init)(struct ceph_auth_data *data);
	int (*handle_response)(struct ceph_client *client,
			       struct ceph_auth_data *data,
			       const char *blob,
			       int err,
			       int len);
	int (*create_request)(struct ceph_client *client,
			      struct ceph_auth_data *data,
			      char **blob,
			      int *len);
	void (*finalize)(struct ceph_auth_data *data);
};


extern struct ceph_auth_ops *ceph_auth_get_generic_ops(void);

extern struct ceph_auth_ops *ceph_x_auth_get_ops(void);



#endif
