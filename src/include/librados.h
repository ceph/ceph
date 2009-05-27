#ifndef __LIBRADOS_H
#define __LIBRADOS_H

#ifdef __cplusplus

#include "include/types.h"

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
int rados_write(rados_pool_t pool, struct ceph_object *oid, off_t off, const char *buf, size_t len);
int rados_read(rados_pool_t pool, struct ceph_object *oid, off_t off, char *buf, size_t len);
int rados_remove(rados_pool_t pool, struct ceph_object *oid);
int rados_exec(rados_pool_t pool, struct ceph_object *o, const char *cls, const char *method,
	       const char *in_buf, size_t in_len, char *buf, size_t out_len);

/* async io */
typedef struct {
  int done;
  int rval;
} rados_completion_t;

int rados_aio_write(rados_pool_t pool, struct ceph_object *oid, off_t off, const char *buf, size_t len, rados_completion_t *completion);
int rados_aio_read(rados_pool_t pool, struct ceph_object *oid, off_t off, char *buf, size_t len, rados_completion_t *completion);

#ifdef __cplusplus
}

class RadosClient;

class Rados
{
  RadosClient *client;
public:
  Rados();
  ~Rados();
  bool initialize(int argc, const char *argv[]);

  int open_pool(const char *name, rados_pool_t *pool);
  int close_pool(rados_pool_t pool);

  int write(rados_pool_t pool, object_t& oid, off_t off, bufferlist& bl, size_t len);
  int read(rados_pool_t pool, object_t& oid, off_t off, bufferlist& bl, size_t len);
  int remove(rados_pool_t pool, object_t& oid);

  int exec(rados_pool_t pool, object_t& oid, const char *cls, const char *method,
             bufferlist& inbl, bufferlist& outbl);
};
#endif

#endif
