
#ifndef __LIBCEPHFSD_PROXY_H__
#define __LIBCEPHFSD_PROXY_H__

#include <string.h>
#include <errno.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdbool.h>

#define LIBCEPHFSD_MAJOR 0
#define LIBCEPHFSD_MINOR 2

#define LIBCEPHFS_LIB_CLIENT 0xe3e5f0e8 // 'ceph' xor 0x80808080

#define PROXY_SOCKET "/run/libcephfsd.sock"
#define PROXY_SOCKET_ENV "LIBCEPHFSD_SOCKET"

#define offset_of(_type, _field) ((uintptr_t) & ((_type *)0)->_field)

#define container_of(_ptr, _type, _field) \
	((_type *)((uintptr_t)(_ptr) - offset_of(_type, _field)))

struct _list;
typedef struct _list list_t;

struct _proxy_buffer_ops;
typedef struct _proxy_buffer_ops proxy_buffer_ops_t;

struct _proxy_buffer;
typedef struct _proxy_buffer proxy_buffer_t;

struct _proxy_output;
typedef struct _proxy_output proxy_output_t;

struct _proxy_log_handler;
typedef struct _proxy_log_handler proxy_log_handler_t;

struct _proxy_worker;
typedef struct _proxy_worker proxy_worker_t;

struct _proxy_manager;
typedef struct _proxy_manager proxy_manager_t;

struct _proxy_link;
typedef struct _proxy_link proxy_link_t;

typedef int32_t (*proxy_output_write_t)(proxy_output_t *);
typedef int32_t (*proxy_output_full_t)(proxy_output_t *);

typedef void (*proxy_log_callback_t)(proxy_log_handler_t *, int32_t, int32_t,
				     const char *);

typedef void (*proxy_worker_start_t)(proxy_worker_t *);
typedef void (*proxy_worker_destroy_t)(proxy_worker_t *);

typedef int32_t (*proxy_manager_start_t)(proxy_manager_t *);

typedef int32_t (*proxy_link_start_t)(proxy_link_t *, int32_t);
typedef bool (*proxy_link_stop_t)(proxy_link_t *);

struct _list {
	list_t *next;
	list_t *prev;
};

#endif
