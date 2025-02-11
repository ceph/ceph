
#ifndef __LIBCEPHFS_PROXY_LINK_H__
#define __LIBCEPHFS_PROXY_LINK_H__

#include <sys/socket.h>
#include <sys/un.h>

#include "proxy.h"

#define PROXY_LINK_DISCONNECTED { NULL, -1 }

/* To define newer versions of the negotiate structure, look for comments
 * starting with "NEG_VERSION" and do the appropriate changes in each place. */

/* NEG_VERSION: Update this value to the latest implemented version. */
#define PROXY_LINK_NEGOTIATE_VERSION 1

/* Version 0 structure will be used to handle legacy clients that don't support
 * negotiation. */
typedef struct _proxy_link_negotiate_v0 {
	/* Size of the entire structure. Don't use sizeof() to compute it, use
	 * NEG_VERSION_SIZE() to avoid alignement issues. */
	uint16_t size;

	/* Version of the negotiation structure. */
	uint16_t version;

	/* Minimum version that the peer needs to support to proceed. */
	uint16_t min_version;

	/* Reserved. Must be 0. */
	uint16_t flags;
} proxy_link_negotiate_v0_t;

typedef struct _proxy_link_negotiate_v1 {
	uint32_t supported;
	uint32_t required;
	uint32_t enabled;
} proxy_link_negotiate_v1_t;

/* NEG_VERSION: Add typedefs for new negotiate extensions above this comment. */

struct _proxy_link_negotiate {
	proxy_link_negotiate_v0_t v0;
	proxy_link_negotiate_v1_t v1;

	/* NEG_VERSION: Add newly defined typedefs above this comment. */
};

typedef int32_t (*proxy_link_negotiate_cbk_t)(proxy_link_negotiate_t *);

struct _proxy_link {
	proxy_link_stop_t stop;
	int32_t sd;
};

typedef struct _proxy_link_req {
	uint16_t header_len;
	uint16_t op;
	uint32_t data_len;
} proxy_link_req_t;

typedef struct _proxy_link_ans {
	uint16_t header_len;
	uint16_t flags;
	int32_t result;
	uint32_t data_len;
} proxy_link_ans_t;

#define NEG_VERSION_SIZE_1(_ver) \
	(offset_of(proxy_link_negotiate_t, v##_ver) + \
	 sizeof(proxy_link_negotiate_v##_ver##_t))
#define NEG_VERSION_SIZE(_ver) NEG_VERSION_SIZE_1(_ver)

#define proxy_link_negotiate_init_v0(_neg, _ver, _min) \
	do { \
		(_neg)->v0.size = NEG_VERSION_SIZE(_ver); \
		(_neg)->v0.version = (_ver); \
		(_neg)->v0.min_version = (_min); \
		(_neg)->v0.flags = 0; \
	} while (0)

/* NEG_VERSION: Add new arguments and initialize the link->neg.vX with them. */
static inline void proxy_link_negotiate_init(proxy_link_negotiate_t *neg,
					     uint32_t min_version,
					     uint32_t supported,
					     uint32_t required,
					     uint32_t enabled)
{
	proxy_link_negotiate_init_v0(neg, PROXY_LINK_NEGOTIATE_VERSION,
				     min_version);
	neg->v1.supported = supported;
	neg->v1.required = required;
	neg->v1.enabled = enabled;
}

static inline bool proxy_link_is_connected(proxy_link_t *link)
{
	return link->sd >= 0;
}

int32_t proxy_link_client(proxy_link_t *link, const char *path,
			  proxy_link_stop_t stop);

void proxy_link_close(proxy_link_t *link);

int32_t proxy_link_server(proxy_link_t *link, const char *path,
			  proxy_link_start_t start, proxy_link_stop_t stop);

int32_t proxy_link_send(int32_t sd, struct iovec *iov, int32_t count);

int32_t proxy_link_recv(int32_t sd, struct iovec *iov, int32_t count);

int32_t proxy_link_req_send(int32_t sd, int32_t op, struct iovec *iov,
			    int32_t count);

int32_t proxy_link_req_recv(int32_t sd, struct iovec *iov, int32_t count);

int32_t proxy_link_ans_send(int32_t sd, int32_t result, struct iovec *iov,
			    int32_t count);

int32_t proxy_link_ans_recv(int32_t sd, struct iovec *iov, int32_t count);

int32_t proxy_link_request(int32_t sd, int32_t op, struct iovec *req_iov,
			   int32_t req_count, struct iovec *ans_iov,
			   int32_t ans_count);

int32_t proxy_link_ctrl_send(int32_t sd, void *data, int32_t size, int32_t type,
			     void *ctrl, int32_t ctrl_size);

int32_t proxy_link_ctrl_recv(int32_t sd, void *data, int32_t size, int32_t type,
			     void *ctrl, int32_t *ctrl_size);

int32_t proxy_link_handshake_server(proxy_link_t *link, int32_t sd,
				    proxy_link_negotiate_t *neg,
				    proxy_link_negotiate_cbk_t cbk);

int32_t proxy_link_handshake_client(proxy_link_t *link, int32_t sd,
				    proxy_link_negotiate_t *neg,
				    proxy_link_negotiate_cbk_t cbk);

#endif
