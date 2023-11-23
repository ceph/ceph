#ifndef __LIBRADOS_HPP
#define __LIBRADOS_HPP

#include <string>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <vector>
#include <utility>
#include "buffer.h"

#include "librados.h"
#include "librados_fwd.hpp"
#include "rados_types.hpp"

namespace libradosstriper
{
  class RadosStriper;
}

namespace neorados { class RADOS; }

namespace librados {

using ceph::bufferlist;

struct AioCompletionImpl;
struct IoCtxImpl;
struct ListObjectImpl;
class NObjectIteratorImpl;
struct ObjListCtx;
class ObjectOperationImpl;
struct PlacementGroupImpl;
struct PoolAsyncCompletionImpl;

typedef struct rados_cluster_stat_t cluster_stat_t;
typedef struct rados_pool_stat_t pool_stat_t;

typedef void *list_ctx_t;
typedef uint64_t auid_t;
typedef void *config_t;

typedef struct {
  std::string client;
  std::string cookie;
  std::string address;
} locker_t;

typedef std::map<std::string, pool_stat_t> stats_map;

typedef void *completion_t;
typedef void (*callback_t)(completion_t cb, void *arg);

inline namespace v14_2_0 {

  class IoCtx;
  class RadosClient;

  class CEPH_RADOS_API ListObject
  {
  public:
    const std::string& get_nspace() const;
    const std::string& get_oid() const;
    const std::string& get_locator() const;

    ListObject();
    ~ListObject();
    ListObject( const ListObject&);
    ListObject& operator=(const ListObject& rhs);
  private:
    ListObject(ListObjectImpl *impl);

    friend class librados::NObjectIteratorImpl;
    friend std::ostream& operator<<(std::ostream& out, const ListObject& lop);

    ListObjectImpl *impl;
  };
  CEPH_RADOS_API std::ostream& operator<<(std::ostream& out, const librados::ListObject& lop);

  class CEPH_RADOS_API NObjectIterator;

  class CEPH_RADOS_API ObjectCursor
  {
    public:
    ObjectCursor();
    ObjectCursor(const ObjectCursor &rhs);
    explicit ObjectCursor(rados_object_list_cursor c);
    ~ObjectCursor();
    ObjectCursor& operator=(const ObjectCursor& rhs);
    bool operator<(const ObjectCursor &rhs) const;
    bool operator==(const ObjectCursor &rhs) const;
    void set(rados_object_list_cursor c);

    friend class IoCtx;
    friend class librados::NObjectIteratorImpl;
    friend std::ostream& operator<<(std::ostream& os, const librados::ObjectCursor& oc);

    std::string to_str() const;
    bool from_str(const std::string& s);

    protected:
    rados_object_list_cursor c_cursor;
  };
  CEPH_RADOS_API std::ostream& operator<<(std::ostream& os, const librados::ObjectCursor& oc);

  class CEPH_RADOS_API NObjectIterator {
  public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = ListObject;
    using difference_type = std::ptrdiff_t;
    using pointer = ListObject*;
    using reference = ListObject&;
    static const NObjectIterator __EndObjectIterator;
    NObjectIterator(): impl(NULL) {}
    ~NObjectIterator();
    NObjectIterator(const NObjectIterator &rhs);
    NObjectIterator& operator=(const NObjectIterator& rhs);

    bool operator==(const NObjectIterator& rhs) const;
    bool operator!=(const NObjectIterator& rhs) const;
    const ListObject& operator*() const;
    const ListObject* operator->() const;
    NObjectIterator &operator++(); //< Preincrement; errors are thrown as exceptions
    NObjectIterator operator++(int); //< Postincrement; errors are thrown as exceptions
    friend class IoCtx;
    friend class librados::NObjectIteratorImpl;

    /// get current hash position of the iterator, rounded to the current pg
    uint32_t get_pg_hash_position() const;

    /// move the iterator to a given hash position. this may (will!) be rounded
    /// to the nearest pg. errors are thrown as exceptions
    uint32_t seek(uint32_t pos);

    /// move the iterator to a given cursor position. errors are thrown as exceptions
    uint32_t seek(const ObjectCursor& cursor);

    /// get current cursor position
    ObjectCursor get_cursor();

    /**
     * Configure PGLS filter to be applied OSD-side (requires caller
     * to know/understand the format expected by the OSD)
     */
    void set_filter(const bufferlist &bl);

  private:
    NObjectIterator(ObjListCtx *ctx_);
    void get_next();
    NObjectIteratorImpl *impl;
  };

  class CEPH_RADOS_API ObjectItem
  {
    public:
    std::string oid;
    std::string nspace;
    std::string locator;
  };

  /// DEPRECATED; do not use
  class CEPH_RADOS_API WatchCtx {
  public:
    virtual ~WatchCtx();
    virtual void notify(uint8_t opcode, uint64_t ver, bufferlist& bl) = 0;
  };

  class CEPH_RADOS_API WatchCtx2 {
  public:
    virtual ~WatchCtx2();
    /**
     * Callback activated when we receive a notify event.
     *
     * @param notify_id unique id for this notify event
     * @param cookie the watcher we are notifying
     * @param notifier_id the unique client id of the notifier
     * @param bl opaque notify payload (from the notifier)
     */
    virtual void handle_notify(uint64_t notify_id,
			       uint64_t cookie,
			       uint64_t notifier_id,
			       bufferlist& bl) = 0;

    /**
     * Callback activated when we encounter an error with the watch.
     *
     * Errors we may see:
     *   -ENOTCONN  : our watch was disconnected
     *   -ETIMEDOUT : our watch is still valid, but we may have missed
     *                a notify event.
     *
     * @param cookie the watcher with the problem
     * @param err error
     */
    virtual void handle_error(uint64_t cookie, int err) = 0;
  };

  struct CEPH_RADOS_API AioCompletion {
    AioCompletion(AioCompletionImpl *pc_) : pc(pc_) {}
    ~AioCompletion();
    int set_complete_callback(void *cb_arg, callback_t cb);
    int set_safe_callback(void *cb_arg, callback_t cb)
      __attribute__ ((deprecated));
    int wait_for_complete();
    int wait_for_safe() __attribute__ ((deprecated));
    int wait_for_complete_and_cb();
    int wait_for_safe_and_cb() __attribute__ ((deprecated));
    bool is_complete();
    bool is_safe() __attribute__ ((deprecated));
    bool is_complete_and_cb();
    bool is_safe_and_cb() __attribute__ ((deprecated));
    int get_return_value();
    int get_version() __attribute__ ((deprecated));
    uint64_t get_version64();
    void release();
    AioCompletionImpl *pc;
  };

  struct CEPH_RADOS_API PoolAsyncCompletion {
    PoolAsyncCompletion(PoolAsyncCompletionImpl *pc_) : pc(pc_) {}
    ~PoolAsyncCompletion();
    int set_callback(void *cb_arg, callback_t cb);
    int wait();
    bool is_complete();
    int get_return_value();
    void release();
    PoolAsyncCompletionImpl *pc;
  };

  /**
   * These are per-op flags which may be different among
   * ops added to an ObjectOperation.
   */
  enum ObjectOperationFlags {
    OP_EXCL =   LIBRADOS_OP_FLAG_EXCL,
    OP_FAILOK = LIBRADOS_OP_FLAG_FAILOK,
    OP_FADVISE_RANDOM = LIBRADOS_OP_FLAG_FADVISE_RANDOM,
    OP_FADVISE_SEQUENTIAL = LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL,
    OP_FADVISE_WILLNEED = LIBRADOS_OP_FLAG_FADVISE_WILLNEED,
    OP_FADVISE_DONTNEED = LIBRADOS_OP_FLAG_FADVISE_DONTNEED,
    OP_FADVISE_NOCACHE = LIBRADOS_OP_FLAG_FADVISE_NOCACHE,
  };

  class CEPH_RADOS_API ObjectOperationCompletion {
  public:
    virtual ~ObjectOperationCompletion() {}
    virtual void handle_completion(int r, bufferlist& outbl) = 0;
  };

  /**
   * These flags apply to the ObjectOperation as a whole.
   *
   * Prior to octopus BALANCE_READS and LOCALIZE_READS should only
   * be used when reading from data you're certain won't change, like
   * a snapshot, or where eventual consistency is ok.  Since octopus
   * (get_min_compatible_osd() >= CEPH_RELEASE_OCTOPUS) both are safe
   * for general use.
   *
   * ORDER_READS_WRITES will order reads the same way writes are
   * ordered (e.g., waiting for degraded objects).  In particular, it
   * will make a write followed by a read sequence be preserved.
   *
   * IGNORE_CACHE will skip the caching logic on the OSD that normally
   * handles promotion of objects between tiers.  This allows an operation
   * to operate (or read) the cached (or uncached) object, even if it is
   * not coherent.
   *
   * IGNORE_OVERLAY will ignore the pool overlay tiering metadata and
   * process the op directly on the destination pool.  This is useful
   * for CACHE_FLUSH and CACHE_EVICT operations.
   */
  enum ObjectOperationGlobalFlags {
    OPERATION_NOFLAG             = LIBRADOS_OPERATION_NOFLAG,
    OPERATION_BALANCE_READS      = LIBRADOS_OPERATION_BALANCE_READS,
    OPERATION_LOCALIZE_READS     = LIBRADOS_OPERATION_LOCALIZE_READS,
    OPERATION_ORDER_READS_WRITES = LIBRADOS_OPERATION_ORDER_READS_WRITES,
    OPERATION_IGNORE_CACHE       = LIBRADOS_OPERATION_IGNORE_CACHE,
    OPERATION_SKIPRWLOCKS        = LIBRADOS_OPERATION_SKIPRWLOCKS,
    OPERATION_IGNORE_OVERLAY     = LIBRADOS_OPERATION_IGNORE_OVERLAY,
    // send requests to cluster despite the cluster or pool being
    // marked full; ops will either succeed (e.g., delete) or return
    // EDQUOT or ENOSPC
    OPERATION_FULL_TRY           = LIBRADOS_OPERATION_FULL_TRY,
    // mainly for delete
    OPERATION_FULL_FORCE	 = LIBRADOS_OPERATION_FULL_FORCE,
    OPERATION_IGNORE_REDIRECT	 = LIBRADOS_OPERATION_IGNORE_REDIRECT,
    OPERATION_ORDERSNAP          = LIBRADOS_OPERATION_ORDERSNAP,
    // enable/allow return value and per-op return code/buffers
    OPERATION_RETURNVEC          = LIBRADOS_OPERATION_RETURNVEC,
  };

  /*
   * Alloc hint flags for the alloc_hint operation.
   */
  enum AllocHintFlags {
    ALLOC_HINT_FLAG_SEQUENTIAL_WRITE = 1,
    ALLOC_HINT_FLAG_RANDOM_WRITE = 2,
    ALLOC_HINT_FLAG_SEQUENTIAL_READ = 4,
    ALLOC_HINT_FLAG_RANDOM_READ = 8,
    ALLOC_HINT_FLAG_APPEND_ONLY = 16,
    ALLOC_HINT_FLAG_IMMUTABLE = 32,
    ALLOC_HINT_FLAG_SHORTLIVED = 64,
    ALLOC_HINT_FLAG_LONGLIVED = 128,
    ALLOC_HINT_FLAG_COMPRESSIBLE = 256,
    ALLOC_HINT_FLAG_INCOMPRESSIBLE = 512,
  };

  /*
   * ObjectOperation : compound object operation
   * Batch multiple object operations into a single request, to be applied
   * atomically.
   */
  class CEPH_RADOS_API ObjectOperation
  {
  public:
    ObjectOperation();
    virtual ~ObjectOperation();

    ObjectOperation(const ObjectOperation&) = delete;
    ObjectOperation& operator=(const ObjectOperation&) = delete;

    /**
     * Move constructor.
     * \warning A moved from ObjectOperation is invalid and may not be used for
     *          any purpose. This is a hard contract violation and will
     *          kill your program.
     */
    ObjectOperation(ObjectOperation&&);
    ObjectOperation& operator =(ObjectOperation&&);

    size_t size();
    void set_op_flags(ObjectOperationFlags flags) __attribute__((deprecated));
    //flag mean ObjectOperationFlags
    void set_op_flags2(int flags);

    void cmpext(uint64_t off, const bufferlist& cmp_bl, int *prval);
    void cmpxattr(const char *name, uint8_t op, const bufferlist& val);
    void cmpxattr(const char *name, uint8_t op, uint64_t v);
    void exec(const char *cls, const char *method, bufferlist& inbl);
    void exec(const char *cls, const char *method, bufferlist& inbl, bufferlist *obl, int *prval);
    void exec(const char *cls, const char *method, bufferlist& inbl, ObjectOperationCompletion *completion);
    /**
     * Guard operation with a check that object version == ver
     *
     * @param ver [in] version to check
     */
    void assert_version(uint64_t ver);

    /**
     * Guard operation with a check that the object already exists
     */
    void assert_exists();

    /**
     * get key/value pairs for specified keys
     *
     * @param assertions [in] comparison assertions
     * @param prval [out] place error code in prval upon completion
     *
     * assertions has the form of mappings from keys to (comparison rval, assertion)
     * The assertion field may be CEPH_OSD_CMPXATTR_OP_[GT|LT|EQ].
     *
     * That is, to assert that the value at key 'foo' is greater than 'bar':
     *
     * ObjectReadOperation op;
     * int r;
     * map<string, pair<bufferlist, int> > assertions;
     * bufferlist bar(string('bar'));
     * assertions['foo'] = make_pair(bar, CEPH_OSD_CMP_XATTR_OP_GT);
     * op.omap_cmp(assertions, &r);
     */
    void omap_cmp(
      const std::map<std::string, std::pair<bufferlist, int> > &assertions,
      int *prval);

  protected:
    ObjectOperationImpl* impl;
    friend class IoCtx;
    friend class Rados;
  };

  /*
   * ObjectWriteOperation : compound object write operation
   * Batch multiple object operations into a single request, to be applied
   * atomically.
   */
  class CEPH_RADOS_API ObjectWriteOperation : public ObjectOperation
  {
  protected:
    time_t *unused;
  public:
    ObjectWriteOperation() : unused(NULL) {}
    ~ObjectWriteOperation() override {}

    ObjectWriteOperation(ObjectWriteOperation&&) = default;
    ObjectWriteOperation& operator =(ObjectWriteOperation&&) = default;

    void mtime(time_t *pt);
    void mtime2(struct timespec *pts);

    void create(bool exclusive);
    void create(bool exclusive,
		const std::string& category); ///< NOTE: category is unused

    void write(uint64_t off, const bufferlist& bl);
    void write_full(const bufferlist& bl);
    void writesame(uint64_t off, uint64_t write_len,
		   const bufferlist& bl);
    void append(const bufferlist& bl);
    void remove();
    void truncate(uint64_t off);
    void zero(uint64_t off, uint64_t len);
    void rmxattr(const char *name);
    void setxattr(const char *name, const bufferlist& bl);
    void setxattr(const char *name, const bufferlist&& bl);
    void tmap_update(const bufferlist& cmdbl);
    void tmap_put(const bufferlist& bl);
    void selfmanaged_snap_rollback(uint64_t snapid);

    /**
     * Rollback an object to the specified snapshot id
     *
     * Used with pool snapshots
     *
     * @param snapid [in] snopshot id specified
     */
    void snap_rollback(uint64_t snapid);

    /**
     * set keys and values according to map
     *
     * @param map [in] keys and values to set
     */
    void omap_set(const std::map<std::string, bufferlist> &map);

    /**
     * set header
     *
     * @param bl [in] header to set
     */
    void omap_set_header(const bufferlist &bl);

    /**
     * Clears omap contents
     */
    void omap_clear();

    /**
     * Clears keys in to_rm
     *
     * @param to_rm [in] keys to remove
     */
    void omap_rm_keys(const std::set<std::string> &to_rm);

    /**
     * Copy an object
     *
     * Copies an object from another location.  The operation is atomic in that
     * the copy either succeeds in its entirety or fails (e.g., because the
     * source object was modified while the copy was in progress).
     *
     * @param src source object name
     * @param src_ioctx ioctx for the source object
     * @param src_version current version of the source object
     * @param src_fadvise_flags the fadvise flags for source object
     */
    void copy_from(const std::string& src, const IoCtx& src_ioctx,
		   uint64_t src_version, uint32_t src_fadvise_flags);

    /**
     * Copy an object
     *
     * Copies an object from another location.  The operation is atomic in that
     * the copy either succeeds in its entirety or fails (e.g., because the
     * source object was modified while the copy was in progress).  Instead of
     * copying truncate_seq and truncate_size from the source object it receives
     * these values as parameters.
     *
     * @param src source object name
     * @param src_ioctx ioctx for the source object
     * @param src_version current version of the source object
     * @param truncate_seq truncate sequence for the destination object
     * @param truncate_size truncate size for the destination object
     * @param src_fadvise_flags the fadvise flags for source object
     */
    void copy_from2(const std::string& src, const IoCtx& src_ioctx,
		    uint64_t src_version, uint32_t truncate_seq,
		    uint64_t truncate_size, uint32_t src_fadvise_flags);

    /**
     * undirty an object
     *
     * Clear an objects dirty flag
     */
    void undirty();

    /**
     * Set allocation hint for an object
     *
     * @param expected_object_size expected size of the object, in bytes
     * @param expected_write_size expected size of writes to the object, in bytes
     * @param flags flags ()
     */
    void set_alloc_hint(uint64_t expected_object_size,
                        uint64_t expected_write_size);
    void set_alloc_hint2(uint64_t expected_object_size,
			 uint64_t expected_write_size,
			 uint32_t flags);

    /**
     * Pin/unpin an object in cache tier
     *
     * @returns 0 on success, negative error code on failure
     */
    void cache_pin();
    void cache_unpin();

    /**
     * Extensible tier
     *
     * Set redirect target
     */
    void set_redirect(const std::string& tgt_obj, const IoCtx& tgt_ioctx,
		      uint64_t tgt_version, int flag = 0);
    void tier_promote();
    void unset_manifest();

    friend class IoCtx;
  };

  /*
   * ObjectReadOperation : compound object operation that return value
   * Batch multiple object operations into a single request, to be applied
   * atomically.
   */
  class CEPH_RADOS_API ObjectReadOperation : public ObjectOperation
  {
  public:
    ObjectReadOperation() {}
    ~ObjectReadOperation() override {}

    ObjectReadOperation(ObjectReadOperation&&) = default;
    ObjectReadOperation& operator =(ObjectReadOperation&&) = default;

    void stat(uint64_t *psize, time_t *pmtime, int *prval);
    void stat2(uint64_t *psize, struct timespec *pts, int *prval);
    void getxattr(const char *name, bufferlist *pbl, int *prval);
    void getxattrs(std::map<std::string, bufferlist> *pattrs, int *prval);
    void read(size_t off, uint64_t len, bufferlist *pbl, int *prval);
    void checksum(rados_checksum_type_t type, const bufferlist &init_value_bl,
		  uint64_t off, size_t len, size_t chunk_size, bufferlist *pbl,
		  int *prval);

    /**
     * see aio_sparse_read()
     */
    void sparse_read(uint64_t off, uint64_t len, std::map<uint64_t,uint64_t> *m,
                     bufferlist *data_bl, int *prval,
                     uint64_t truncate_size = 0,
                     uint32_t truncate_seq = 0);

    /**
     * omap_get_vals: keys and values from the object omap
     *
     * Get up to max_return keys and values beginning after start_after
     *
     * @param start_after [in] list no keys smaller than start_after
     * @param max_return [in] list no more than max_return key/value pairs
     * @param out_vals [out] place returned values in out_vals on completion
     * @param prval [out] place error code in prval upon completion
     */
    void omap_get_vals(
      const std::string &start_after,
      uint64_t max_return,
      std::map<std::string, bufferlist> *out_vals,
      int *prval) __attribute__ ((deprecated));  // use v2

    /**
     * omap_get_vals: keys and values from the object omap
     *
     * Get up to max_return keys and values beginning after start_after
     *
     * @param start_after [in] list no keys smaller than start_after
     * @param max_return [in] list no more than max_return key/value pairs
     * @param out_vals [out] place returned values in out_vals on completion
     * @param prval [out] place error code in prval upon completion
     */
    void omap_get_vals2(
      const std::string &start_after,
      uint64_t max_return,
      std::map<std::string, bufferlist> *out_vals,
      bool *pmore,
      int *prval);

    /**
     * omap_get_vals: keys and values from the object omap
     *
     * Get up to max_return keys and values beginning after start_after
     *
     * @param start_after [in] list keys starting after start_after
     * @param filter_prefix [in] list only keys beginning with filter_prefix
     * @param max_return [in] list no more than max_return key/value pairs
     * @param out_vals [out] place returned values in out_vals on completion
     * @param prval [out] place error code in prval upon completion
     */
    void omap_get_vals(
      const std::string &start_after,
      const std::string &filter_prefix,
      uint64_t max_return,
      std::map<std::string, bufferlist> *out_vals,
      int *prval) __attribute__ ((deprecated));  // use v2

    /**
     * omap_get_vals2: keys and values from the object omap
     *
     * Get up to max_return keys and values beginning after start_after
     *
     * @param start_after [in] list keys starting after start_after
     * @param filter_prefix [in] list only keys beginning with filter_prefix
     * @param max_return [in] list no more than max_return key/value pairs
     * @param out_vals [out] place returned values in out_vals on completion
     * @param pmore [out] pointer to bool indicating whether there are more keys
     * @param prval [out] place error code in prval upon completion
     */
    void omap_get_vals2(
      const std::string &start_after,
      const std::string &filter_prefix,
      uint64_t max_return,
      std::map<std::string, bufferlist> *out_vals,
      bool *pmore,
      int *prval);


    /**
     * omap_get_keys: keys from the object omap
     *
     * Get up to max_return keys beginning after start_after
     *
     * @param start_after [in] list keys starting after start_after
     * @param max_return [in] list no more than max_return keys
     * @param out_keys [out] place returned values in out_keys on completion
     * @param prval [out] place error code in prval upon completion
     */
    void omap_get_keys(const std::string &start_after,
                       uint64_t max_return,
                       std::set<std::string> *out_keys,
                       int *prval) __attribute__ ((deprecated)); // use v2

    /**
     * omap_get_keys2: keys from the object omap
     *
     * Get up to max_return keys beginning after start_after
     *
     * @param start_after [in] list keys starting after start_after
     * @param max_return [in] list no more than max_return keys
     * @param out_keys [out] place returned values in out_keys on completion
     * @param pmore [out] pointer to bool indicating whether there are more keys
     * @param prval [out] place error code in prval upon completion
     */
    void omap_get_keys2(const std::string &start_after,
			uint64_t max_return,
			std::set<std::string> *out_keys,
			bool *pmore,
			int *prval);

    /**
     * omap_get_header: get header from object omap
     *
     * @param header [out] place header here upon completion
     * @param prval [out] place error code in prval upon completion
     */
    void omap_get_header(bufferlist *header, int *prval);

    /**
     * get key/value pairs for specified keys
     *
     * @param keys [in] keys to get
     * @param map [out] place key/value pairs found here on completion
     * @param prval [out] place error code in prval upon completion
     */
    void omap_get_vals_by_keys(const std::set<std::string> &keys,
			       std::map<std::string, bufferlist> *map,
			       int *prval);

    /**
     * list_watchers: Get list watchers of object
     *
     * @param out_watchers [out] place returned values in out_watchers on completion
     * @param prval [out] place error code in prval upon completion
     */
    void list_watchers(std::list<obj_watch_t> *out_watchers, int *prval);

    /**
     * list snapshot clones associated with a logical object
     *
     * This will include a record for each version of the object,
     * include the "HEAD" (which will have a cloneid of SNAP_HEAD).
     * Each clone includes a vector of snap ids for which it is
     * defined to exist.
     *
     * NOTE: this operation must be submitted from an IoCtx with a
     * read snapid of SNAP_DIR for reliable results.
     *
     * @param out_snaps [out] pointer to resulting snap_set_t
     * @param prval [out] place error code in prval upon completion
     */
    void list_snaps(snap_set_t *out_snaps, int *prval);

    /**
     * query dirty state of an object
     *
     * @param isdirty [out] pointer to resulting bool
     * @param prval [out] place error code in prval upon completion
     */
    void is_dirty(bool *isdirty, int *prval);

    /**
     * flush a cache tier object to backing tier; will block racing
     * updates.
     *
     * This should be used in concert with OPERATION_IGNORE_CACHE to avoid
     * triggering a promotion.
     */
    void cache_flush();

    /**
     * Flush a cache tier object to backing tier; will EAGAIN if we race
     * with an update.  Must be used with the SKIPRWLOCKS flag.
     *
     * This should be used in concert with OPERATION_IGNORE_CACHE to avoid
     * triggering a promotion.
     */
    void cache_try_flush();

    /**
     * evict a clean cache tier object
     *
     * This should be used in concert with OPERATION_IGNORE_CACHE to avoid
     * triggering a promote on the OSD (that is then evicted).
     */
    void cache_evict();

    /**
     * Extensible tier
     *
     * set_chunk: make a chunk pointing a part of the source object at the target 
     * 		  object
     *
     * @param src_offset [in] source offset to indicate the start position of 
     * 				a chunk in the source object
     * @param src_length [in] source length to set the length of the chunk
     * @param tgt_oid    [in] target object's id to set a chunk
     * @param tgt_offset [in] the start position of the target object
     * @param flag       [in] flag for the source object
     *
     */
    void set_chunk(uint64_t src_offset, uint64_t src_length, const IoCtx& tgt_ioctx,
                   std::string tgt_oid, uint64_t tgt_offset, int flag = 0);
    /**
     * flush a manifest tier object to backing tier, performing deduplication;
     * will block racing updates.
     *
     * Invoking tier_flush() implicitly makes a manifest object even if
     * the target object is not manifest. 
     */
    void tier_flush();
    /**
     * evict a manifest tier object to backing tier; will block racing
     * updates.
     */
    void tier_evict();
  };

  /* IoCtx : This is a context in which we can perform I/O.
   * It includes a Pool,
   *
   * Typical use (error checking omitted):
   *
   * IoCtx p;
   * rados.ioctx_create("my_pool", p);
   * p->stat(&stats);
   * ... etc ...
   *
   * NOTE: be sure to call watch_flush() prior to destroying any IoCtx
   * that is used for watch events to ensure that racing callbacks
   * have completed.
   */
  class CEPH_RADOS_API IoCtx
  {
  public:
    IoCtx();
    static void from_rados_ioctx_t(rados_ioctx_t p, IoCtx &pool);
    IoCtx(const IoCtx& rhs);
    IoCtx& operator=(const IoCtx& rhs);
    IoCtx(IoCtx&& rhs) noexcept;
    IoCtx& operator=(IoCtx&& rhs) noexcept;

    ~IoCtx();

    bool is_valid() const;

    // Close our pool handle
    void close();

    // deep copy
    void dup(const IoCtx& rhs);

    // set pool auid
    int set_auid(uint64_t auid_)
      __attribute__ ((deprecated));

    // set pool auid
    int set_auid_async(uint64_t auid_, PoolAsyncCompletion *c)
      __attribute__ ((deprecated));

    // get pool auid
    int get_auid(uint64_t *auid_)
      __attribute__ ((deprecated));

    uint64_t get_instance_id() const;

    std::string get_pool_name();

    bool pool_requires_alignment();
    int pool_requires_alignment2(bool * req);
    uint64_t pool_required_alignment();
    int pool_required_alignment2(uint64_t * alignment);

    // create an object
    int create(const std::string& oid, bool exclusive);
    int create(const std::string& oid, bool exclusive,
	       const std::string& category); ///< category is unused

    /**
     * write bytes to an object at a specified offset
     *
     * NOTE: this call steals the contents of @param bl.
     */
    int write(const std::string& oid, bufferlist& bl, size_t len, uint64_t off);
    /**
     * append bytes to an object
     *
     * NOTE: this call steals the contents of @param bl.
     */
    int append(const std::string& oid, bufferlist& bl, size_t len);
    /**
     * replace object contents with provided data
     *
     * NOTE: this call steals the contents of @param bl.
     */
    int write_full(const std::string& oid, bufferlist& bl);
    int writesame(const std::string& oid, bufferlist& bl,
		  size_t write_len, uint64_t off);
    int read(const std::string& oid, bufferlist& bl, size_t len, uint64_t off);
    int checksum(const std::string& o, rados_checksum_type_t type,
		 const bufferlist &init_value_bl, size_t len, uint64_t off,
		 size_t chunk_size, bufferlist *pbl);
    int remove(const std::string& oid);
    int remove(const std::string& oid, int flags);
    int trunc(const std::string& oid, uint64_t size);
    int mapext(const std::string& o, uint64_t off, size_t len, std::map<uint64_t,uint64_t>& m);
    int cmpext(const std::string& o, uint64_t off, bufferlist& cmp_bl);
    int sparse_read(const std::string& o, std::map<uint64_t,uint64_t>& m, bufferlist& bl, size_t len, uint64_t off);
    int getxattr(const std::string& oid, const char *name, bufferlist& bl);
    int getxattrs(const std::string& oid, std::map<std::string, bufferlist>& attrset);
    int setxattr(const std::string& oid, const char *name, bufferlist& bl);
    int rmxattr(const std::string& oid, const char *name);
    int stat(const std::string& oid, uint64_t *psize, time_t *pmtime);
    int stat2(const std::string& oid, uint64_t *psize, struct timespec *pts);
    int exec(const std::string& oid, const char *cls, const char *method,
	     bufferlist& inbl, bufferlist& outbl);
    /**
     * modify object tmap based on encoded update sequence
     *
     * NOTE: this call steals the contents of @param bl
     */
    int tmap_update(const std::string& oid, bufferlist& cmdbl);

    int omap_get_vals(const std::string& oid,
                      const std::string& start_after,
                      uint64_t max_return,
                      std::map<std::string, bufferlist> *out_vals);
    int omap_get_vals2(const std::string& oid,
		       const std::string& start_after,
		       uint64_t max_return,
		       std::map<std::string, bufferlist> *out_vals,
		       bool *pmore);
    int omap_get_vals(const std::string& oid,
                      const std::string& start_after,
                      const std::string& filter_prefix,
                      uint64_t max_return,
                      std::map<std::string, bufferlist> *out_vals);
    int omap_get_vals2(const std::string& oid,
		       const std::string& start_after,
		       const std::string& filter_prefix,
		       uint64_t max_return,
		       std::map<std::string, bufferlist> *out_vals,
		       bool *pmore);
    int omap_get_keys(const std::string& oid,
                      const std::string& start_after,
                      uint64_t max_return,
                      std::set<std::string> *out_keys);
    int omap_get_keys2(const std::string& oid,
		       const std::string& start_after,
		       uint64_t max_return,
		       std::set<std::string> *out_keys,
		       bool *pmore);
    int omap_get_header(const std::string& oid,
                        bufferlist *bl);
    int omap_get_vals_by_keys(const std::string& oid,
                              const std::set<std::string>& keys,
                              std::map<std::string, bufferlist> *vals);
    int omap_set(const std::string& oid,
                 const std::map<std::string, bufferlist>& map);
    int omap_set_header(const std::string& oid,
                        const bufferlist& bl);
    int omap_clear(const std::string& oid);
    int omap_rm_keys(const std::string& oid,
                     const std::set<std::string>& keys);

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

    int snap_rollback(const std::string& oid, const char *snapname);

    // Deprecated name kept for backward compatibility - same as snap_rollback()
    int rollback(const std::string& oid, const char *snapname)
      __attribute__ ((deprecated));

    int selfmanaged_snap_create(uint64_t *snapid);
    void aio_selfmanaged_snap_create(uint64_t *snapid, AioCompletion *c);

    int selfmanaged_snap_remove(uint64_t snapid);
    void aio_selfmanaged_snap_remove(uint64_t snapid, AioCompletion *c);

    int selfmanaged_snap_rollback(const std::string& oid, uint64_t snapid);

    // Advisory locking on rados objects.
    int lock_exclusive(const std::string &oid, const std::string &name,
		       const std::string &cookie,
		       const std::string &description,
		       struct timeval * duration, uint8_t flags);

    int lock_shared(const std::string &oid, const std::string &name,
		    const std::string &cookie, const std::string &tag,
		    const std::string &description,
		    struct timeval * duration, uint8_t flags);

    int unlock(const std::string &oid, const std::string &name,
	       const std::string &cookie);

    int break_lock(const std::string &oid, const std::string &name,
		   const std::string &client, const std::string &cookie);

    int list_lockers(const std::string &oid, const std::string &name,
		     int *exclusive,
		     std::string *tag,
		     std::list<librados::locker_t> *lockers);


    /// Start enumerating objects for a pool. Errors are thrown as exceptions.
    NObjectIterator nobjects_begin(const bufferlist &filter=bufferlist());
    /// Start enumerating objects for a pool starting from a hash position.
    /// Errors are thrown as exceptions.
    NObjectIterator nobjects_begin(uint32_t start_hash_position,
                                   const bufferlist &filter=bufferlist());
    /// Start enumerating objects for a pool starting from cursor. Errors are
    /// thrown as exceptions.
    NObjectIterator nobjects_begin(const librados::ObjectCursor& cursor,
                                   const bufferlist &filter=bufferlist());
    /// Iterator indicating the end of a pool
    const NObjectIterator& nobjects_end() const;

    /// Get cursor for pool beginning
    ObjectCursor object_list_begin();

    /// Get cursor for pool end
    ObjectCursor object_list_end();

    /// Check whether a cursor is at the end of a pool
    bool object_list_is_end(const ObjectCursor &oc);

    /// List some objects between two cursors
    int object_list(const ObjectCursor &start, const ObjectCursor &finish,
                    const size_t result_count,
                    const bufferlist &filter,
                    std::vector<ObjectItem> *result,
                    ObjectCursor *next);

    /// Generate cursors that include the N out of Mth slice of the pool
    void object_list_slice(
        const ObjectCursor start,
        const ObjectCursor finish,
        const size_t n,
        const size_t m,
        ObjectCursor *split_start,
        ObjectCursor *split_finish);

    /**
     * List available hit set objects
     *
     * @param uint32_t [in] hash position to query
     * @param c [in] completion
     * @param pls [out] list of available intervals
     */
    int hit_set_list(uint32_t hash, AioCompletion *c,
		     std::list< std::pair<time_t, time_t> > *pls);

    /**
     * Retrieve hit set for a given hash, and time
     *
     * @param hash [in] hash position
     * @param c [in] completion
     * @param stamp [in] time interval that falls within the hit set's interval
     * @param pbl [out] buffer to store the result in
     */
    int hit_set_get(uint32_t hash, AioCompletion *c, time_t stamp,
		    bufferlist *pbl);

    uint64_t get_last_version();

    int aio_read(const std::string& oid, AioCompletion *c,
		 bufferlist *pbl, size_t len, uint64_t off);
    /**
     * Asynchronously read from an object at a particular snapshot
     *
     * This is the same as normal aio_read, except that it chooses
     * the snapshot to read from from its arguments instead of the
     * internal IoCtx state.
     *
     * The return value of the completion will be number of bytes read on
     * success, negative error code on failure.
     *
     * @param oid the name of the object to read from
     * @param c what to do when the read is complete
     * @param pbl where to store the results
     * @param len the number of bytes to read
     * @param off the offset to start reading from in the object
     * @param snapid the id of the snapshot to read from
     * @returns 0 on success, negative error code on failure
     */
    int aio_read(const std::string& oid, AioCompletion *c,
		 bufferlist *pbl, size_t len, uint64_t off, uint64_t snapid);
    int aio_sparse_read(const std::string& oid, AioCompletion *c,
			std::map<uint64_t,uint64_t> *m, bufferlist *data_bl,
			size_t len, uint64_t off);
    /**
     * Asynchronously read existing extents from an object at a
     * particular snapshot
     *
     * This is the same as normal aio_sparse_read, except that it chooses
     * the snapshot to read from from its arguments instead of the
     * internal IoCtx state.
     *
     * m will be filled in with a map of extents in the object,
     * mapping offsets to lengths (in bytes) within the range
     * requested. The data for all of the extents are stored
     * back-to-back in offset order in data_bl.
     *
     * @param oid the name of the object to read from
     * @param c what to do when the read is complete
     * @param m where to store the map of extents
     * @param data_bl where to store the data
     * @param len the number of bytes to read
     * @param off the offset to start reading from in the object
     * @param snapid the id of the snapshot to read from
     * @returns 0 on success, negative error code on failure
     */
    int aio_sparse_read(const std::string& oid, AioCompletion *c,
			std::map<uint64_t,uint64_t> *m, bufferlist *data_bl,
			size_t len, uint64_t off, uint64_t snapid);
    /**
     * Asynchronously compare an on-disk object range with a buffer
     *
     * @param oid the name of the object to read from
     * @param c what to do when the read is complete
     * @param off object byte offset at which to start the comparison
     * @param cmp_bl buffer containing bytes to be compared with object contents
     * @returns 0 on success, negative error code on failure,
     *  (-MAX_ERRNO - mismatch_off) on mismatch
     */
    int aio_cmpext(const std::string& oid,
		   librados::AioCompletion *c,
		   uint64_t off,
		   bufferlist& cmp_bl);
    int aio_write(const std::string& oid, AioCompletion *c, const bufferlist& bl,
		  size_t len, uint64_t off);
    int aio_append(const std::string& oid, AioCompletion *c, const bufferlist& bl,
		  size_t len);
    int aio_write_full(const std::string& oid, AioCompletion *c, const bufferlist& bl);
    int aio_writesame(const std::string& oid, AioCompletion *c, const bufferlist& bl,
		      size_t write_len, uint64_t off);

    /**
     * Asynchronously remove an object
     *
     * Queues the remove and returns.
     *
     * The return value of the completion will be 0 on success, negative
     * error code on failure.
     *
     * @param oid the name of the object
     * @param c what to do when the remove is safe and complete
     * @returns 0 on success, -EROFS if the io context specifies a snap_seq
     * other than SNAP_HEAD
     */
    int aio_remove(const std::string& oid, AioCompletion *c);
    int aio_remove(const std::string& oid, AioCompletion *c, int flags);

    /**
     * Wait for all currently pending aio writes to be safe.
     *
     * @returns 0 on success, negative error code on failure
     */
    int aio_flush();

    /**
     * Schedule a callback for when all currently pending
     * aio writes are safe. This is a non-blocking version of
     * aio_flush().
     *
     * @param c what to do when the writes are safe
     * @returns 0 on success, negative error code on failure
     */
    int aio_flush_async(AioCompletion *c);
    int aio_getxattr(const std::string& oid, AioCompletion *c, const char *name, bufferlist& bl);
    int aio_getxattrs(const std::string& oid, AioCompletion *c, std::map<std::string, bufferlist>& attrset);
    int aio_setxattr(const std::string& oid, AioCompletion *c, const char *name, bufferlist& bl);
    int aio_rmxattr(const std::string& oid, AioCompletion *c, const char *name);
    int aio_stat(const std::string& oid, AioCompletion *c, uint64_t *psize, time_t *pmtime);
    int aio_stat2(const std::string& oid, AioCompletion *c, uint64_t *psize, struct timespec *pts);

    /**
     * Cancel aio operation
     *
     * @param c completion handle
     * @returns 0 on success, negative error code on failure
     */
    int aio_cancel(AioCompletion *c);

    int aio_exec(const std::string& oid, AioCompletion *c, const char *cls, const char *method,
	         bufferlist& inbl, bufferlist *outbl);

    /*
     * asynchronous version of unlock
     */
    int aio_unlock(const std::string &oid, const std::string &name,
	           const std::string &cookie, AioCompletion *c);

    // compound object operations
    int operate(const std::string& oid, ObjectWriteOperation *op);
    int operate(const std::string& oid, ObjectWriteOperation *op, int flags);
    int operate(const std::string& oid, ObjectReadOperation *op, bufferlist *pbl);
    int operate(const std::string& oid, ObjectReadOperation *op, bufferlist *pbl, int flags);
    int aio_operate(const std::string& oid, AioCompletion *c, ObjectWriteOperation *op);
    int aio_operate(const std::string& oid, AioCompletion *c, ObjectWriteOperation *op, int flags);
    /**
     * Schedule an async write operation with explicit snapshot parameters
     *
     * This is the same as the first aio_operate(), except that it
     * gets the snapshot context from its arguments instead of the
     * IoCtx internal state.
     *
     * @param oid the object to operate on
     * @param c what to do when the operation is complete and safe
     * @param op which operations to perform
     * @param seq latest selfmanaged snapshot sequence number for this object
     * @param snaps currently existing selfmanaged snapshot ids for this object
     * @returns 0 on success, negative error code on failure
     */
    int aio_operate(const std::string& oid, AioCompletion *c,
		    ObjectWriteOperation *op, snap_t seq,
		    std::vector<snap_t>& snaps);
    int aio_operate(const std::string& oid, AioCompletion *c,
        ObjectWriteOperation *op, snap_t seq,
        std::vector<snap_t>& snaps,
        const blkin_trace_info *trace_info);
    int aio_operate(const std::string& oid, AioCompletion *c,
        ObjectWriteOperation *op, snap_t seq,
        std::vector<snap_t>& snaps, int flags,
        const blkin_trace_info *trace_info);
    int aio_operate(const std::string& oid, AioCompletion *c,
		    ObjectReadOperation *op, bufferlist *pbl);

    int aio_operate(const std::string& oid, AioCompletion *c,
		    ObjectReadOperation *op, snap_t snapid, int flags,
		    bufferlist *pbl)
      __attribute__ ((deprecated));

    int aio_operate(const std::string& oid, AioCompletion *c,
		    ObjectReadOperation *op, int flags,
		    bufferlist *pbl);
    int aio_operate(const std::string& oid, AioCompletion *c,
        ObjectReadOperation *op, int flags,
        bufferlist *pbl, const blkin_trace_info *trace_info);

    // watch/notify
    int watch2(const std::string& o, uint64_t *handle,
	       librados::WatchCtx2 *ctx);
    int watch3(const std::string& o, uint64_t *handle,
	       librados::WatchCtx2 *ctx, uint32_t timeout);
    int aio_watch(const std::string& o, AioCompletion *c, uint64_t *handle,
	       librados::WatchCtx2 *ctx);
    int aio_watch2(const std::string& o, AioCompletion *c, uint64_t *handle,
	       librados::WatchCtx2 *ctx, uint32_t timeout);
    int unwatch2(uint64_t handle);
    int aio_unwatch(uint64_t handle, AioCompletion *c);
    /**
     * Send a notify event to watchers
     *
     * Upon completion the pbl bufferlist reply payload will be
     * encoded like so:
     *
     *    le32 num_acks
     *    {
     *      le64 gid     global id for the client (for client.1234 that's 1234)
     *      le64 cookie  cookie for the client
     *      le32 buflen  length of reply message buffer
     *      u8 * buflen  payload
     *    } * num_acks
     *    le32 num_timeouts
     *    {
     *      le64 gid     global id for the client
     *      le64 cookie  cookie for the client
     *    } * num_timeouts
     *
     *
     */
    int notify2(const std::string& o,   ///< object
		bufferlist& bl,         ///< optional broadcast payload
		uint64_t timeout_ms,    ///< timeout (in ms)
		bufferlist *pbl);       ///< reply buffer
    int aio_notify(const std::string& o,   ///< object
                   AioCompletion *c,       ///< completion when notify completes
                   bufferlist& bl,         ///< optional broadcast payload
                   uint64_t timeout_ms,    ///< timeout (in ms)
                   bufferlist *pbl);       ///< reply buffer
   /*
    * Decode a notify response into acks and timeout vectors.
    */
    void decode_notify_response(bufferlist &bl,
                                std::vector<librados::notify_ack_t> *acks,
                                std::vector<librados::notify_timeout_t> *timeouts);

    int list_watchers(const std::string& o, std::list<obj_watch_t> *out_watchers);
    int list_snaps(const std::string& o, snap_set_t *out_snaps);
    void set_notify_timeout(uint32_t timeout);

    /// acknowledge a notify we received.
    void notify_ack(const std::string& o, ///< watched object
		    uint64_t notify_id,   ///< notify id
		    uint64_t cookie,      ///< our watch handle
		    bufferlist& bl);      ///< optional reply payload

    /***
     * check on watch validity
     *
     * Check if a watch is valid.  If so, return the number of
     * milliseconds since we last confirmed its liveness.  If there is
     * a known error, return it.
     *
     * If there is an error, the watch is no longer valid, and should
     * be destroyed with unwatch().  The user is still interested in
     * the object, a new watch should be created with watch().
     *
     * @param cookie watch handle
     * @returns ms since last confirmed valid, or error
     */
    int watch_check(uint64_t cookie);

    // old, deprecated versions
    int watch(const std::string& o, uint64_t ver, uint64_t *cookie,
	      librados::WatchCtx *ctx) __attribute__ ((deprecated));
    int notify(const std::string& o, uint64_t ver, bufferlist& bl)
      __attribute__ ((deprecated));
    int unwatch(const std::string& o, uint64_t cookie)
      __attribute__ ((deprecated));

    /**
     * Set allocation hint for an object
     *
     * This is an advisory operation, it will always succeed (as if it
     * was submitted with a OP_FAILOK flag set) and is not guaranteed
     * to do anything on the backend.
     *
     * @param o the name of the object
     * @param expected_object_size expected size of the object, in bytes
     * @param expected_write_size expected size of writes to the object, in bytes
     * @returns 0 on success, negative error code on failure
     */
    int set_alloc_hint(const std::string& o,
                       uint64_t expected_object_size,
                       uint64_t expected_write_size);
    int set_alloc_hint2(const std::string& o,
			uint64_t expected_object_size,
			uint64_t expected_write_size,
			uint32_t flags);

    // assert version for next sync operations
    void set_assert_version(uint64_t ver);

    /**
     * Pin/unpin an object in cache tier
     *
     * @param o the name of the object
     * @returns 0 on success, negative error code on failure
     */
    int cache_pin(const std::string& o);
    int cache_unpin(const std::string& o);

    std::string get_pool_name() const;

    void locator_set_key(const std::string& key);
    void set_namespace(const std::string& nspace);
    std::string get_namespace() const;

    int64_t get_id();

    // deprecated versions
    uint32_t get_object_hash_position(const std::string& oid)
      __attribute__ ((deprecated));
    uint32_t get_object_pg_hash_position(const std::string& oid)
      __attribute__ ((deprecated));

    int get_object_hash_position2(const std::string& oid, uint32_t *hash_position);
    int get_object_pg_hash_position2(const std::string& oid, uint32_t *pg_hash_position);

    config_t cct();

    void set_osdmap_full_try()
      __attribute__ ((deprecated));
    void unset_osdmap_full_try()
      __attribute__ ((deprecated));

    bool get_pool_full_try();
    void set_pool_full_try();
    void unset_pool_full_try();

    int application_enable(const std::string& app_name, bool force);
    int application_enable_async(const std::string& app_name,
                                 bool force, PoolAsyncCompletion *c);
    int application_list(std::set<std::string> *app_names);
    int application_metadata_get(const std::string& app_name,
                                 const std::string &key,
                                 std::string *value);
    int application_metadata_set(const std::string& app_name,
                                 const std::string &key,
                                 const std::string& value);
    int application_metadata_remove(const std::string& app_name,
                                    const std::string &key);
    int application_metadata_list(const std::string& app_name,
                                  std::map<std::string, std::string> *values);

  private:
    /* You can only get IoCtx instances from Rados */
    IoCtx(IoCtxImpl *io_ctx_impl_);

    friend class Rados; // Only Rados can use our private constructor to create IoCtxes.
    friend class libradosstriper::RadosStriper; // Striper needs to see our IoCtxImpl
    friend class ObjectWriteOperation;  // copy_from needs to see our IoCtxImpl
    friend class ObjectReadOperation;  // set_chunk needs to see our IoCtxImpl

    IoCtxImpl *io_ctx_impl;
  };

  struct CEPH_RADOS_API PlacementGroup {
    PlacementGroup();
    PlacementGroup(const PlacementGroup&);
    ~PlacementGroup();
    bool parse(const char*);
    std::unique_ptr<PlacementGroupImpl> impl;
  };

  CEPH_RADOS_API std::ostream& operator<<(std::ostream&, const PlacementGroup&);

  class CEPH_RADOS_API Rados
  {
  public:
    static void version(int *major, int *minor, int *extra);

    Rados();
    explicit Rados(IoCtx& ioctx);
    ~Rados();
    static void from_rados_t(rados_t cluster, Rados &rados);

    int init(const char * const id);
    int init2(const char * const name, const char * const clustername,
	      uint64_t flags);
    int init_with_context(config_t cct_);
    config_t cct();
    int connect();
    void shutdown();
    int watch_flush();
    int aio_watch_flush(AioCompletion*);
    int conf_read_file(const char * const path) const;
    int conf_parse_argv(int argc, const char ** argv) const;
    int conf_parse_argv_remainder(int argc, const char ** argv,
				  const char ** remargv) const;
    int conf_parse_env(const char *env) const;
    int conf_set(const char *option, const char *value);
    int conf_get(const char *option, std::string &val);

    int service_daemon_register(
      const std::string& service,  ///< service name (e.g., 'rgw')
      const std::string& name,     ///< daemon name (e.g., 'gwfoo')
      const std::map<std::string,std::string>& metadata); ///< static metadata about daemon
    int service_daemon_update_status(
      std::map<std::string,std::string>&& status);

    int pool_create(const char *name);
    int pool_create(const char *name, uint64_t auid)
      __attribute__ ((deprecated));
    int pool_create(const char *name, uint64_t auid, uint8_t crush_rule)
      __attribute__ ((deprecated));
    int pool_create_with_rule(const char *name, uint8_t crush_rule);
    int pool_create_async(const char *name, PoolAsyncCompletion *c);
    int pool_create_async(const char *name, uint64_t auid, PoolAsyncCompletion *c)
      __attribute__ ((deprecated));
    int pool_create_async(const char *name, uint64_t auid, uint8_t crush_rule, PoolAsyncCompletion *c)
      __attribute__ ((deprecated));
    int pool_create_with_rule_async(const char *name, uint8_t crush_rule, PoolAsyncCompletion *c);
    int pool_get_base_tier(int64_t pool, int64_t* base_tier);
    int pool_delete(const char *name);
    int pool_delete_async(const char *name, PoolAsyncCompletion *c);
    int64_t pool_lookup(const char *name);
    int pool_reverse_lookup(int64_t id, std::string *name);

    uint64_t get_instance_id();

    int get_min_compatible_osd(int8_t* require_osd_release);
    int get_min_compatible_client(int8_t* min_compat_client,
                                  int8_t* require_min_compat_client);

    int mon_command(std::string cmd, const bufferlist& inbl,
		    bufferlist *outbl, std::string *outs);
    int mgr_command(std::string cmd, const bufferlist& inbl,
		    bufferlist *outbl, std::string *outs);
    int osd_command(int osdid, std::string cmd, const bufferlist& inbl,
                    bufferlist *outbl, std::string *outs);
    int pg_command(const char *pgstr, std::string cmd, const bufferlist& inbl,
                   bufferlist *outbl, std::string *outs);

    int ioctx_create(const char *name, IoCtx &pioctx);
    int ioctx_create2(int64_t pool_id, IoCtx &pioctx);

    // Features useful for test cases
    void test_blocklist_self(bool set);

    /* pool info */
    int pool_list(std::list<std::string>& v);
    int pool_list2(std::list<std::pair<int64_t, std::string> >& v);
    int get_pool_stats(std::list<std::string>& v,
		       stats_map& result);
    /// deprecated; use simpler form.  categories no longer supported.
    int get_pool_stats(std::list<std::string>& v,
		       std::map<std::string, stats_map>& stats);
    /// deprecated; categories no longer supported
    int get_pool_stats(std::list<std::string>& v,
                       std::string& category,
		       std::map<std::string, stats_map>& stats);
    /// check if pool has or had selfmanaged snaps
    bool get_pool_is_selfmanaged_snaps_mode(const std::string& poolname);

    int cluster_stat(cluster_stat_t& result);
    int cluster_fsid(std::string *fsid);

    /**
     * List inconsistent placement groups in the given pool
     *
     * @param pool_id the pool id
     * @param pgs [out] the inconsistent PGs
     */
    int get_inconsistent_pgs(int64_t pool_id,
                             std::vector<PlacementGroup>* pgs);
    /**
     * List the inconsistent objects found in a given PG by last scrub
     *
     * @param pg the placement group returned by @c pg_list()
     * @param start_after the first returned @c objects
     * @param max_return the max number of the returned @c objects
     * @param c what to do when the operation is complete and safe
     * @param objects [out] the objects where inconsistencies are found
     * @param interval [in,out] an epoch indicating current interval
     * @returns if a non-zero @c interval is specified, will return -EAGAIN i
     *          the current interval begin epoch is different.
     */
    int get_inconsistent_objects(const PlacementGroup& pg,
                                 const object_id_t &start_after,
                                 unsigned max_return,
                                 AioCompletion *c,
                                 std::vector<inconsistent_obj_t>* objects,
                                 uint32_t* interval);
    /**
     * List the inconsistent snapsets found in a given PG by last scrub
     *
     * @param pg the placement group returned by @c pg_list()
     * @param start_after the first returned @c objects
     * @param max_return the max number of the returned @c objects
     * @param c what to do when the operation is complete and safe
     * @param snapsets [out] the objects where inconsistencies are found
     * @param interval [in,out] an epoch indicating current interval
     * @returns if a non-zero @c interval is specified, will return -EAGAIN i
     *          the current interval begin epoch is different.
     */
    int get_inconsistent_snapsets(const PlacementGroup& pg,
                                  const object_id_t &start_after,
                                  unsigned max_return,
                                  AioCompletion *c,
                                  std::vector<inconsistent_snapset_t>* snapset,
                                  uint32_t* interval);

    /// get/wait for the most recent osdmap
    int wait_for_latest_osdmap();

    int blocklist_add(const std::string& client_address,
                      uint32_t expire_seconds);

    std::string get_addrs() const;

    /*
     * pool aio
     *
     * It is up to the caller to release the completion handler, even if the pool_create_async()
     * and/or pool_delete_async() fails and does not send the async request
     */
    static PoolAsyncCompletion *pool_async_create_completion();

   // -- aio --
    static AioCompletion *aio_create_completion();
    static AioCompletion *aio_create_completion(void *cb_arg, callback_t cb_complete,
						callback_t cb_safe)
      __attribute__ ((deprecated));
    static AioCompletion *aio_create_completion(void *cb_arg, callback_t cb_complete);

    friend std::ostream& operator<<(std::ostream &oss, const Rados& r);
  private:
    friend class neorados::RADOS;

    // We don't allow assignment or copying
    Rados(const Rados& rhs);
    const Rados& operator=(const Rados& rhs);
    RadosClient *client;
  };

} // namespace v14_2_0
} // namespace librados

#endif

