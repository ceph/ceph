#ifndef __LIBRADOS_H
#define __LIBRADOS_H

#include "include/types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* initialization */
int rados_initialize(int argc, const char **argv); /* arguments are optional */
void rados_deinitialize();

/* read/write objects */
int rados_write(ceph_object *oid, const char *buf, off_t off, size_t len);
int rados_read(ceph_object *oid, char *buf, off_t off, size_t len);

#ifdef __cplusplus
}
#endif

#endif
