#ifndef __LIBRADOS_HPP
#define __LIBRADOS_HPP

#include <stdbool.h>
#include <string>
#include <list>
#include <map>
#include <vector>
#include "buffer.h"

#include "librados.h"

class RadosClient;
class Context;
class AioCompletionImpl;
class PoolCtx;

namespace librados
{
  class Pool;
  class PoolHandle;
  using ceph::bufferlist;

  typedef void *list_ctx_t;
  typedef void *pool_t;
  typedef uint64_t snap_t;
  typedef uint64_t auid_t;

  struct statfs_t {
    uint64_t kb, kb_used, kb_avail;
    uint64_t num_objects;
  };

  struct pool_stat_t {
    uint64_t num_bytes;    // in bytes
    uint64_t num_kb;       // in KB
    uint64_t num_objects;
    uint64_t num_object_clones;
    uint64_t num_object_copies;  // num_objects * num_replicas
    uint64_t num_objects_missing_on_primary;
    uint64_t num_objects_unfound;
    uint64_t num_objects_degraded;
    uint64_t num_rd, num_rd_kb, num_wr, num_wr_kb;
  };

  typedef void *completion_t;
  typedef void (*callback_t)(completion_t cb, void *arg);

  struct SnapContext {
    snap_t seq;
    std::vector<snap_t> snaps;
  };

  class ObjectIterator : public std::iterator <std::forward_iterator_tag, std::string> {
  public:
    static const ObjectIterator __EndObjectIterator;
    ObjectIterator(rados_list_ctx_t ctx_);
    ~ObjectIterator();
    bool operator==(const ObjectIterator& rhs) const;
    bool operator!=(const ObjectIterator& rhs) const;
    const std::string& operator*() const;
    ObjectIterator &operator++(); // Preincrement
    ObjectIterator operator++(int); // Postincrement
  private:
    void get_next();
    rados_list_ctx_t ctx;
    bufferlist *bl;
    std::string cur_obj;
  };

  class WatchCtx {
  public:
    virtual void notify(uint8_t opcode, uint64_t ver) = 0;
  };

  struct AioCompletion {
    AioCompletion(AioCompletionImpl *pc_) : pc(pc_) {}
    int set_complete_callback(void *cb_arg, callback_t cb);
    int set_safe_callback(void *cb_arg, callback_t cb);
    int wait_for_complete();
    int wait_for_safe();
    bool is_complete();
    bool is_safe();
    int get_return_value();
    int get_version();
    void release();
    AioCompletionImpl *pc;
  };

  /* PoolHandle : Represents our view of a Pool.
   *
   * Typical use (error checking omitted):
   *
   * PoolHandle *p;
   * rados.pool_open("my_pool", &pool);
   * p->stat(&stats);
   * ... etc ...
   * delete p; // close our pool handle
   */
  class PoolHandle
  {
  public:
    PoolHandle();
    static void from_rados_pool_t(rados_pool_t p, PoolHandle &pool);

    // Close our pool handle
    ~PoolHandle();
    void close();

    // Remove this Pool from the cluster.
    // If successful, the pool handle will be closed after this call
    // Returns 0 on success; error code otherwise.
    int destroy();

    // set pool auid
    int set_auid(uint64_t auid_);

    // create an object
    int create(const std::string& oid, bool exclusive);

    int write(const std::string& oid, bufferlist& bl, size_t len, off_t off);
    int write_full(const std::string& oid, bufferlist& bl);
    int read(const std::string& oid, bufferlist& bl, size_t len, off_t off);
    int remove(const std::string& oid);
    int trunc(const std::string& oid, size_t size);
    int mapext(const std::string& o, off_t off, size_t len, std::map<off_t, size_t>& m);
    int sparse_read(const std::string& o, std::map<off_t, size_t>& m, bufferlist& bl, size_t len, off_t off);
    int getxattr(const std::string& oid, const char *name, bufferlist& bl);
    int getxattrs(const std::string& oid, std::map<std::string, bufferlist>& attrset);
    int setxattr(const std::string& oid, const char *name, bufferlist& bl);
    int rmxattr(const std::string& oid, const char *name);
    int stat(const std::string& oid, uint64_t *psize, time_t *pmtime);
    int exec(const std::string& oid, const char *cls, const char *method,
	     bufferlist& inbl, bufferlist& outbl);
    int tmap_update(const std::string& oid, bufferlist& cmdbl);

    void snap_set_read(snap_t seq);
    int selfmanaged_snap_set_write_ctx(snap_t seq, std::vector<snap_t>& snaps);

    // Create a snapshot with a given name
    int snap_create(const char *snapname);

    // Look up a snapshot by name.
    // Returns 0 on success; error code otherwise
    int snap_lookup(const char *snapname, snap_t *snap);

    // Gets a timestamp for a snap
    int snap_get_stamp(snap_t snapid, time_t *t);

    // Gets the name of a snap
    int snap_get_name(snap_t snapid, std::string *s);

    // Remove a snapshot from this pool
    int snap_remove(const char *snapname);

    int snap_list(std::vector<snap_t> *snaps);

    int rollback(const std::string& oid, const char *snapname);

    int selfmanaged_snap_create(uint64_t *snapid);

    int selfmanaged_snap_remove(uint64_t snapid);

    ObjectIterator objects_begin();
    const ObjectIterator& objects_end() const;

    uint64_t get_last_version();

    int aio_read(const std::string& oid, AioCompletion *c,
		 bufferlist *pbl, size_t len, off_t off);
    int aio_sparse_read(const std::string& oid, AioCompletion *c,
			std::map<off_t,size_t> *m, bufferlist *data_bl,
			size_t len, off_t off);
    int aio_write(const std::string& oid, AioCompletion *c, const bufferlist& bl,
		  size_t len, off_t off);
    int aio_write_full(const std::string& oid, AioCompletion *c, const bufferlist& bl);

    // watch/notify
    int watch(const std::string& o, uint64_t ver, uint64_t *handle,
	      librados::WatchCtx *ctx);
    int unwatch(const std::string& o, uint64_t handle);
    int notify(const std::string& o, uint64_t ver);
    void set_notify_timeout(uint32_t timeout);

    // assert version for next sync operations
    void set_assert_version(uint64_t ver);

    const std::string& get_name() const;
  private:
    /* You can only get Pool instances from Rados */
    PoolHandle(PoolCtx *pool_ctx_);

    friend class Rados; // Only Rados can use our private constructor to create Pools.

    /* We don't allow assignment or copying */
    PoolHandle(const PoolHandle& rhs);
    const PoolHandle& operator=(const PoolHandle& rhs);
    PoolCtx *pool_ctx;
    std::string name;
  };

  class Rados
  {
  public:
    static void version(int *major, int *minor, int *extra);

    Rados();
    ~Rados();

    int init(const char * const id);
    int connect();
    void shutdown();
    int conf_read_file(const char * const path) const;
    int conf_set(const char *option, const char *value);
    void reopen_log();
    int conf_get(const char *option, std::string &val);

    int pool_create(const char *name);
    int pool_create(const char *name, uint64_t auid);
    int pool_create(const char *name, uint64_t auid, __u8 crush_rule);
    int pool_open(const char *name, PoolHandle &pool);
    int pool_lookup(const char *name);

    /* listing objects */
    int pool_list(std::list<std::string>& v);
    int get_pool_stats(std::list<std::string>& v,
		       std::map<std::string,pool_stat_t>& stats);
    int get_fs_stats(statfs_t& result);

    // -- aio --
    static AioCompletion *aio_create_completion();
    static AioCompletion *aio_create_completion(void *cb_arg, callback_t cb_complete,
						callback_t cb_safe);

    friend std::ostream& operator<<(std::ostream &oss, const Rados& r);
  private:
    // We don't allow assignment or copying
    Rados(const Rados& rhs);
    const Rados& operator=(const Rados& rhs);
    RadosClient *client;
  };
}

#endif

