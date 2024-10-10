
#ifndef __LIBCEPHFS_PROXY_LINK_H__
#define __LIBCEPHFS_PROXY_LINK_H__

#include <sys/socket.h>
#include <sys/un.h>

#include "proxy.h"

#define PROXY_LINK_DISCONNECTED { NULL, -1 }

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

static inline bool proxy_link_is_connected(proxy_link_t *link)
{
	return link->sd >= 0;
}

int32_t proxy_link_client(proxy_link_t *link, const char *path,
			  proxy_link_stop_t stop);

void proxy_link_close(proxy_link_t *link);

int32_t proxy_link_server(proxy_link_t *link, const char *path,
			  proxy_link_start_t start, proxy_link_stop_t stop);

int32_t proxy_link_read(proxy_link_t *link, int32_t sd, void *buffer,
			int32_t size);

int32_t proxy_link_write(proxy_link_t *link, int32_t sd, void *buffer,
			 int32_t size);

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

#endif
