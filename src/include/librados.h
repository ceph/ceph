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
};

struct rados_statfs_t {
  __u64 kb, kb_used, kb_avail;
  __u64 num_objects;
};

int rados_open_pool(const char *name, rados_pool_t *pool);
int rados_close_pool(rados_pool_t pool);
void rados_set_snap(rados_pool_t pool, rados_snap_t snap);
void rados_pool_init_ctx(rados_list_ctx_t *ctx);
void rados_pool_close_ctx(rados_list_ctx_t *ctx);
int rados_pool_list_next(rados_pool_t pool, const char **entry, rados_list_ctx_t *ctx);

/* snapshots */
int rados_snap_create(const rados_pool_t pool, const char *snapname);
int rados_snap_remove(const rados_pool_t pool, const char *snapname);
int rados_snap_list(rados_pool_t pool, rados_snap_t *snaps, int maxlen);
int rados_snap_get_name(rados_pool_t pool, rados_snap_t id, char *name, int maxlen);

/* read/write objects */
int rados_write(rados_pool_t pool, const char *oid, off_t off, const char *buf, size_t len);
int rados_read(rados_pool_t pool, const char *oid, off_t off, char *buf, size_t len);
int rados_remove(rados_pool_t pool, const char *oid);
int rados_getxattr(rados_pool_t pool, const char *o, const char *name, char *buf, size_t len);
int rados_setxattr(rados_pool_t pool, const char *o, const char *name, const char *buf, size_t len);
int rados_stat(rados_pool_t pool, const char *o, __u64 *psize, time_t *pmtime);
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

class RadosClient;

class Rados
{
  RadosClient *client;
public:
  Rados();
  ~Rados();
  int initialize(int argc, const char *argv[]);
  void shutdown();

  int open_pool(const char *name, rados_pool_t *pool);
  int close_pool(rados_pool_t pool);

  void set_snap(rados_pool_t pool, snapid_t seq);

  int write(rados_pool_t pool, const object_t& oid, off_t off, bufferlist& bl, size_t len);
  int write_full(rados_pool_t pool, const object_t& oid, bufferlist& bl);
  int read(rados_pool_t pool, const object_t& oid, off_t off, bufferlist& bl, size_t len);
  int remove(rados_pool_t pool, const object_t& oid);

  int getxattr(rados_pool_t pool, const object_t& oid, const char *name, bufferlist& bl);
  int setxattr(rados_pool_t pool, const object_t& oid, const char *name, bufferlist& bl);
  int stat(rados_pool_t pool, const object_t& oid, __u64 *psize, time_t *pmtime);

  int exec(rados_pool_t pool, const object_t& oid, const char *cls, const char *method,
             bufferlist& inbl, bufferlist& outbl);

  struct ListCtx {
   void *ctx;
   ListCtx() : ctx(NULL) {}
 };

  int list(rados_pool_t pool, int max, std::list<object_t>& entries, Rados::ListCtx& ctx);
  int list_pools(std::vector<std::string>& v);
  int get_pool_stats(std::vector<std::string>& v,
		     std::map<std::string,rados_pool_stat_t>& stats);
  int get_fs_stats(rados_statfs_t& result);

  int create_pool(string& name);

  int snap_create(const rados_pool_t pool, const char *snapname);
  int snap_remove(const rados_pool_t pool, const char *snapname);
  int snap_list(rados_pool_t pool, vector<rados_snap_t> *snaps);
  int snap_get_name(rados_pool_t pool, rados_snap_t snap, std::string *name);
  int snap_get_stamp(rados_pool_t pool, rados_snap_t snap, time_t *t);
  int snap_lookup(rados_pool_t, const char *snapname, rados_snap_t *snapid);

  // -- aio --
  struct AioCompletion {
    void *pc;
    AioCompletion(void *_pc) : pc(_pc) {}
    int set_callback(rados_callback_t cb, void *cba);
    int wait_for_complete();
    int wait_for_safe();
    bool is_complete();
    bool is_safe();
    int get_return_value();
    void release();
  };

  int aio_read(rados_pool_t pool, const object_t& oid, off_t off, bufferlist *pbl, size_t len,
	       AioCompletion **pc);
  int aio_write(rados_pool_t pool, const object_t& oid, off_t off, const bufferlist& bl, size_t len,
		AioCompletion **pc);

};
#endif

#endif
