
#ifndef __LIBCEPHFSD_PROXY_LOG_H__
#define __LIBCEPHFSD_PROXY_LOG_H__

#include "proxy.h"

enum { LOG_CRIT, LOG_ERR, LOG_WARN, LOG_INFO, LOG_DBG };

struct _proxy_log_handler {
	list_t list;
	proxy_log_callback_t callback;
};

int32_t proxy_log_args(int32_t level, int32_t err, const char *fmt,
		       va_list args);

int32_t proxy_log(int32_t level, int32_t err, const char *fmt, ...);

void proxy_abort_args(int32_t err, const char *fmt, va_list args);

void proxy_abort(int32_t err, const char *fmt, ...);

void proxy_log_register(proxy_log_handler_t *handler,
			proxy_log_callback_t callback);

void proxy_log_deregister(proxy_log_handler_t *handler);

#endif
