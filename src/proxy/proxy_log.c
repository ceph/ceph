
#include <stdio.h>
#include <stdarg.h>

#include "proxy_log.h"
#include "proxy_helpers.h"
#include "proxy_list.h"

#define PROXY_LOG_BUFFER_SIZE 4096

static __thread char proxy_log_buffer[PROXY_LOG_BUFFER_SIZE];

static pthread_rwlock_t proxy_log_mutex = PTHREAD_RWLOCK_INITIALIZER;
static list_t proxy_log_handlers = LIST_INIT(&proxy_log_handlers);

static void proxy_log_write(int32_t level, int32_t err, const char *msg)
{
	proxy_log_handler_t *handler;

	proxy_rwmutex_rdlock(&proxy_log_mutex);

	list_for_each_entry(handler, &proxy_log_handlers, list) {
		handler->callback(handler, level, err, msg);
	}

	proxy_rwmutex_unlock(&proxy_log_mutex);
}

__public void proxy_log_register(proxy_log_handler_t *handler,
				 proxy_log_callback_t callback)
{
	handler->callback = callback;

	proxy_rwmutex_wrlock(&proxy_log_mutex);

	list_add_tail(&handler->list, &proxy_log_handlers);

	proxy_rwmutex_unlock(&proxy_log_mutex);
}

__public void proxy_log_deregister(proxy_log_handler_t *handler)
{
	proxy_rwmutex_wrlock(&proxy_log_mutex);

	list_del_init(&handler->list);

	proxy_rwmutex_unlock(&proxy_log_mutex);
}

static void proxy_log_msg(char *buffer, const char *text)
{
	int32_t len;

	len = strlen(text) + 1;

	memcpy(buffer, text, len);
}

int32_t proxy_log_args(int32_t level, int32_t err, const char *fmt,
		       va_list args)
{
	static __thread bool busy = false;
	int32_t len;

	if (busy) {
		return -err;
	}
	busy = true;

	len = vsnprintf(proxy_log_buffer, sizeof(proxy_log_buffer), fmt, args);
	if (len < 0) {
		proxy_log_msg(proxy_log_buffer,
			      "<log message formatting failed>");
	} else if (len >= sizeof(proxy_log_buffer)) {
		proxy_log_msg(proxy_log_buffer + sizeof(proxy_log_buffer) - 6,
			      "[...]");
	}

	proxy_log_write(level, err, proxy_log_buffer);

	busy = false;

	return -err;
}

int32_t proxy_log(int32_t level, int32_t err, const char *fmt, ...)
{
	va_list args;

	va_start(args, fmt);
	err = proxy_log_args(level, err, fmt, args);
	va_end(args);

	return err;
}

void proxy_abort_args(int32_t err, const char *fmt, va_list args)
{
	proxy_log_args(LOG_CRIT, err, fmt, args);
	abort();
}

void proxy_abort(int32_t err, const char *fmt, ...)
{
	va_list args;

	va_start(args, fmt);
	proxy_abort_args(err, fmt, args);
	va_end(args);
}
