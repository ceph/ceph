#ifndef __LIBRADOS_H
#define __LIBRADOS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <netinet/in.h>
#include <linux/types.h>
#include <string.h>
#include <stdbool.h>

#include "include/msgr.h"
#include "include/rados.h"

/* initialization */
int rados_initialize(int argc, const char **argv); /* arguments are optional */
void rados_deinitialize();

/* pools */
typedef int rados_pool_t;
int rados_open_pool(const char *name, rados_pool_t *pool);
int rados_close_pool(rados_pool_t pool);

/* read/write objects */
int rados_write(rados_pool_t pool, struct ceph_object *oid, const char *buf, off_t off, size_t len);
int rados_read(rados_pool_t pool, struct ceph_object *oid, char *buf, off_t off, size_t len);
int rados_exec(rados_pool_t pool, struct ceph_object *o, const char *cls, const char *method,
	       const char *in_buf, size_t in_len, char *buf, size_t out_len);

#ifdef __cplusplus
}
#endif

#endif
