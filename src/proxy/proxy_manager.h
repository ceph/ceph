
#ifndef __LIBCEPHFSD_PROXY_MANAGER_H__
#define __LIBCEPHFSD_PROXY_MANAGER_H__

#include <pthread.h>

#include "proxy.h"

struct _proxy_worker {
	list_t list;
	pthread_t tid;
	proxy_manager_t *manager;
	proxy_worker_start_t start;
	proxy_worker_destroy_t destroy;
	bool stop;
};

struct _proxy_manager {
	list_t workers;
	list_t finished;
	pthread_t main_tid;
	pthread_t tid;
	pthread_mutex_t mutex;
	pthread_cond_t condition;
	bool stop;
	bool done;
};

int32_t proxy_manager_run(proxy_manager_t *manager,
			  proxy_manager_start_t start);

void proxy_manager_shutdown(proxy_manager_t *manager);

int32_t proxy_manager_launch(proxy_manager_t *manager, proxy_worker_t *worker,
			     proxy_worker_start_t start,
			     proxy_worker_destroy_t destroy);

static inline bool proxy_manager_stop(proxy_manager_t *manager)
{
	return manager->stop;
}

#endif
