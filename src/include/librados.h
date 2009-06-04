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

#include "msgr.h"
#include "rados.h"

/* initialization */
int rados_initialize(int argc, const char **argv); /* arguments are optional */
void rados_deinitialize();

typedef void *rados_list_ctx_t;

/* pools */
typedef int rados_pool_t;
int rados_open_pool(const char *name, rados_pool_t *pool);
int rados_close_pool(rados_pool_t pool);
void rados_pool_init_ctx(rados_list_ctx_t *ctx);
void rados_pool_close_ctx(rados_list_ctx_t *ctx);
int rados_pool_list_next(rados_pool_t pool, const char **entry, rados_list_ctx_t *ctx);

/* read/write objects */
int rados_write(rados_pool_t pool, const char *oid, off_t off, const char *buf, size_t len);
int rados_read(rados_pool_t pool, const char *oid, off_t off, char *buf, size_t len);
int rados_remove(rados_pool_t pool, const char *oid);
int rados_exec(rados_pool_t pool, const char *oid, const char *cls, const char *method,
	       const char *in_buf, size_t in_len, char *buf, size_t out_len);

/* async io */
typedef void *rados_completion_t;

int rados_aio_wait_for_complete(rados_completion_t c);
int rados_aio_wait_for_safe(rados_completion_t c);
int rados_aio_is_complete(rados_completion_t c);
int rados_aio_is_safe(rados_completion_t c);
int rados_aio_get_return_value(rados_completion_t c);
void rados_aio_release(rados_completion_t c);

int rados_aio_write(rados_pool_t pool, const char *oid, off_t off, const char *buf, size_t len, rados_completion_t *completion);
int rados_aio_read(rados_pool_t pool, const char *oid, off_t off, char *buf, size_t len, rados_completion_t *completion);

#ifdef __cplusplus
}

class RadosClient;

class Rados
{
  RadosClient *client;
public:
  Rados();
  ~Rados();
  int initialize(int argc, const char *argv[]);

  int open_pool(const char *name, rados_pool_t *pool);
  int close_pool(rados_pool_t pool);

  int write(rados_pool_t pool, const object_t& oid, off_t off, bufferlist& bl, size_t len);
  int read(rados_pool_t pool, const object_t& oid, off_t off, bufferlist& bl, size_t len);
  int remove(rados_pool_t pool, const object_t& oid);

  int exec(rados_pool_t pool, const object_t& oid, const char *cls, const char *method,
             bufferlist& inbl, bufferlist& outbl);

 struct ListCtx {
   void *ctx;
   ListCtx() : ctx(NULL) {}
 };

  int list(rados_pool_t pool, int max, std::list<object_t>& entries, Rados::ListCtx& ctx);

  // -- aio --
  struct AioCompletion {
    void *pc;
    AioCompletion(void *_pc) : pc(_pc) {}
    int wait_for_complete();
    int wait_for_safe();
    bool is_complete();
    bool is_safe();
    int get_return_value();
    void put();
  };

  int aio_read(int pool, const object_t& oid, off_t off, bufferlist *pbl, size_t len,
	       AioCompletion **pc);
  int aio_write(int pool, const object_t& oid, off_t off, bufferlist& bl, size_t len,
		AioCompletion **pc);

};
#endif

#endif
