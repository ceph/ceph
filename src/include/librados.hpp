#ifndef __LIBRADOS_HPP
#define __LIBRADOS_HPP

#include <stdbool.h>
#include <string>
#include <list>
#include <map>
#include <vector>
#include "buffer.h"

namespace librados {

  using ceph::bufferlist;

  typedef void *list_ctx_t;
  typedef void *pool_t;
  typedef __u64 snap_t;
  typedef __u64 auid_t;

  struct statfs_t {
    __u64 kb, kb_used, kb_avail;
    __u64 num_objects;
  };

  struct pool_stat_t {
    __u64 num_bytes;    // in bytes
    __u64 num_kb;       // in KB
    __u64 num_objects;
    __u64 num_object_clones;
    __u64 num_object_copies;  // num_objects * num_replicas
    __u64 num_objects_missing_on_primary;
    __u64 num_objects_degraded;
    __u64 num_rd, num_rd_kb, num_wr, num_wr_kb;
  };

  typedef void *completion_t;
  typedef void (*callback_t)(completion_t cb, void *arg);


class Rados
{
  void *client;
public:
  Rados();
  ~Rados();
  int initialize(int argc, const char *argv[]);
  void shutdown();

  int open_pool(const char *name, pool_t *pool);
  int close_pool(pool_t pool);
  int lookup_pool(const char *name);

  void set_snap(pool_t pool, snap_t seq);

  int create(pool_t pool, const std::string& oid, bool exclusive);

  int write(pool_t pool, const std::string& oid, off_t off, bufferlist& bl, size_t len);
  int write_full(pool_t pool, const std::string& oid, bufferlist& bl);
  int read(pool_t pool, const std::string& oid, off_t off, bufferlist& bl, size_t len);
  int remove(pool_t pool, const std::string& oid);

  int getxattr(pool_t pool, const std::string& oid, const char *name, bufferlist& bl);
  int setxattr(pool_t pool, const std::string& oid, const char *name, bufferlist& bl);
  int getxattrs(pool_t pool, const std::string& oid, std::map<std::string, bufferlist>& attrset);
  int stat(pool_t pool, const std::string& oid, __u64 *psize, time_t *pmtime);

  int tmap_update(pool_t pool, const std::string& oid, bufferlist& cmdbl);
  
  int exec(pool_t pool, const std::string& oid, const char *cls, const char *method,
	   bufferlist& inbl, bufferlist& outbl);

  /* listing objects */
  struct ListCtx {
    void *ctx;
    ListCtx() : ctx(NULL) {}
  };
  int list_objects_open(pool_t pool, Rados::ListCtx *ctx);
  int list_objects_more(Rados::ListCtx ctx, int max, std::list<std::string>& entries);
  void list_objects_close(Rados::ListCtx ctx);

  int list_pools(std::list<std::string>& v);
  int get_pool_stats(std::list<std::string>& v,
		     std::map<std::string,pool_stat_t>& stats);
  int get_fs_stats(statfs_t& result);

  int create_pool(const char *name, __u64 auid=0);
  int delete_pool(const pool_t& pool);
  int change_pool_auid(const pool_t& pool, __u64 auid);

  int snap_create(const pool_t pool, const char *snapname);
  int selfmanaged_snap_create(const pool_t pool, __u64 *snapid);
  int snap_remove(const pool_t pool, const char *snapname);
  int selfmanaged_snap_remove(const pool_t pool, __u64 snapid);
  int snap_list(pool_t pool, std::vector<snap_t> *snaps);
  int snap_get_name(pool_t pool, snap_t snap, std::string *name);
  int snap_get_stamp(pool_t pool, snap_t snap, time_t *t);
  int snap_lookup(pool_t, const char *snapname, snap_t *snapid);

  // -- aio --
  struct AioCompletion {
    void *pc;
    AioCompletion(void *_pc) : pc(_pc) {}
    int set_callback(callback_t cb, void *cba);
    int wait_for_complete();
    int wait_for_safe();
    bool is_complete();
    bool is_safe();
    int get_return_value();
    void release();
  };

  int aio_read(pool_t pool, const std::string& oid, off_t off, bufferlist *pbl, size_t len,
	       AioCompletion *c);
  int aio_write(pool_t pool, const std::string& oid, off_t off, const bufferlist& bl, size_t len,
		AioCompletion *c);
  AioCompletion *aio_create_completion();
  AioCompletion *aio_create_completion(callback_t cb, void *cba);
};

}

#endif

