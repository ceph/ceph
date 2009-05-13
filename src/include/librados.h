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

/* read/write objects */
int rados_write(struct ceph_object *oid, const char *buf, off_t off, size_t len);
int rados_read(struct ceph_object *oid, char *buf, off_t off, size_t len);
int rados_exec(struct ceph_object *o, const char *code,
                          off_t data_off, size_t data_len, char *buf, size_t out_len);

#ifdef __cplusplus
}
#endif

#endif
