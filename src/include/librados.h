#ifndef __LIBRADOS_H
#define __LIBRADOS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <netinet/in.h>
#include <linux/types.h>
#include <string.h>

/* initialization */
int rados_initialize(int argc, const char **argv); /* arguments are optional */
void rados_deinitialize();

typedef void *rados_list_ctx_t;

/* pools */
typedef void *rados_pool_t;
typedef long long unsigned rados_snap_t;

struct rados_pool_stat_t {
  long long unsigned num_bytes;    // in bytes
  long long unsigned num_kb;       // in KB
  long long unsigned num_objects;
  long long unsigned num_object_clones;
  long long unsigned num_object_copies;  // num_objects * num_replicas
  long long unsigned num_objects_missing_on_primary;
  long long unsigned num_objects_degraded;
  long long unsigned num_rd, num_rd_kb,num_wr, num_wr_kb;
};

struct rados_statfs_t {
  __u64 kb, kb_used, kb_avail;
  __u64 num_objects;
};

int rados_open_pool(const char *name, rados_pool_t *pool);
int rados_close_pool(rados_pool_t pool);
void rados_set_snap(rados_pool_t pool, rados_snap_t snap);
  /* After creating a new rados_list_ctx_t, call this to initialize it*/
void rados_pool_init_ctx(rados_list_ctx_t *ctx);
  /* Once you've finished with a rados_list_ctx_t, call before you dump it*/
void rados_pool_close_ctx(rados_list_ctx_t *ctx);
  /* Given a rados_list_ctx_t and its pool, get the next object in sequence*/
int rados_pool_list_next(rados_pool_t pool, const char **entry, rados_list_ctx_t *ctx);

int rados_create_pool(const char *name);
int rados_create_pool_with_auid(const char *name, __u64 auid);
int rados_delete_pool(const rados_pool_t pool);
int rados_change_pool_auid(const rados_pool_t pool, __u64 auid);

/* snapshots */
int rados_snap_create(const rados_pool_t pool, const char *snapname);
int rados_snap_remove(const rados_pool_t pool, const char *snapname);
int rados_snap_list(rados_pool_t pool, rados_snap_t *snaps, int maxlen);
int rados_snap_lookup(rados_pool_t pool, const char *name, rados_snap_t *id);
int rados_snap_get_name(rados_pool_t pool, rados_snap_t id, char *name, int maxlen);

/* read/write objects */
int rados_write(rados_pool_t pool, const char *oid, off_t off, const char *buf, size_t len);
int rados_read(rados_pool_t pool, const char *oid, off_t off, char *buf, size_t len);
int rados_remove(rados_pool_t pool, const char *oid);
int rados_getxattr(rados_pool_t pool, const char *o, const char *name, char *buf, size_t len);
int rados_setxattr(rados_pool_t pool, const char *o, const char *name, const char *buf, size_t len);
int rados_stat(rados_pool_t pool, const char *o, __u64 *psize, time_t *pmtime);
int rados_tmap_update(rados_pool_t pool, const char *o, const char *cmdbuf, size_t cmdbuflen);
int rados_exec(rados_pool_t pool, const char *oid, const char *cls, const char *method,
	       const char *in_buf, size_t in_len, char *buf, size_t out_len);

/* async io */
typedef void *rados_completion_t;
typedef void (*rados_callback_t)(rados_completion_t cb, void *arg);

int rados_aio_set_callback(rados_completion_t c, rados_callback_t, void *arg);
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
#endif

#endif
