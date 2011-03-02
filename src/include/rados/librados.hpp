#ifndef __LIBRADOS_HPP
#define __LIBRADOS_HPP

#include <stdbool.h>
#include <string>
#include <list>
#include <map>
#include <tr1/memory>
#include <vector>
#include "buffer.h"

#include "librados.h"

namespace librados
{
  using ceph::bufferlist;

  class AioCompletionImpl;
  class IoCtx;
  class IoCtxImpl;
  class ObjListCtx;
  class RadosClient;

  typedef void *list_ctx_t;
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
    ObjectIterator(ObjListCtx *ctx_);
    ~ObjectIterator();
    bool operator==(const ObjectIterator& rhs) const;
    bool operator!=(const ObjectIterator& rhs) const;
    const std::string& operator*() const;
    ObjectIterator &operator++(); // Preincrement
    ObjectIterator operator++(int); // Postincrement
    friend class IoCtx;
  private:
    void get_next();
    std::tr1::shared_ptr < ObjListCtx > ctx;
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

  /* IoCtx : This is a context in which we can perform I/O.
   * It includes a Pool,
   *
   * Typical use (error checking omitted):
   *
   * IoCtx *p;
   * rados.ioctx_create("my_pool", &pool);
   * p->stat(&stats);
   * ... etc ...
   * delete p; // close our pool handle
   */
  class IoCtx
  {
  public:
    IoCtx();
    static void from_rados_ioctx_t(rados_ioctx_t p, IoCtx &pool);
    IoCtx(const IoCtx& rhs);
    IoCtx& operator=(const IoCtx& rhs);

    // Close our pool handle
    ~IoCtx();

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

    const std::string& get_pool_name() const;

    void locator_set_key(const std::string& key);
  private:
    /* You can only get IoCtx instances from Rados */
    IoCtx(IoCtxImpl *io_ctx_impl_);

    friend class Rados; // Only Rados can use our private constructor to create IoCtxes.

    IoCtxImpl *io_ctx_impl;
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
    int pool_delete(const char *name);
    int pool_lookup(const char *name);

    int ioctx_create(const char *name, IoCtx &pool);

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

