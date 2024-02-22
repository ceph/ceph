
#ifndef __LIBCEPHFS_PROXY_ASYNC_H__
#define __LIBCEPHFS_PROXY_ASYNC_H__

#include "proxy.h"
#include "proxy_link.h"
#include "proxy_helpers.h"

#include <pthread.h>

struct _proxy_async {
	pthread_t thread;
	proxy_random_t random;
	proxy_link_t *link;
	int32_t fd;
};

int32_t proxy_async_client(proxy_async_t *async, proxy_link_t *link,
			   int32_t sd);

int32_t proxy_async_server(proxy_async_t *async, int32_t sd);

#endif /* __LIBCEPHFS_PROXY_ASYNC_H__ */
