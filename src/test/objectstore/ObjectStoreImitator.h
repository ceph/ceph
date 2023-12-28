// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Fragmentation Simulator
 * Author: Tri Dao, daominhtri0503@gmail.com
 */
#pragma once

#include "include/common_fwd.h"
#include "include/intarith.h"
#include "os/ObjectStore.h"
#include "os/bluestore/Allocator.h"
#include "os/bluestore/bluestore_types.h"
#include <algorithm>
#include <boost/smart_ptr/intrusive_ptr.hpp>

#define META_POOL_ID ((uint64_t)-1ull)

/**
 * ObjectStoreImitator will simulate how BlueStore does IO (as of the time
 * the simulator is written) and assess the defragmentation levels of different
 * allocators. As the main concern of the simulator is allocators, it mainly
 * focuses on operations that triggers IOs and tries to simplify the rest as
 * much as possible(caches, memory buffers).
 *
 * The simulator inherited from ObjectStore and tries to simulate BlueStore as
 * much as possible.
 *
 * #Note: This is an allocation simulator not a data consistency simulator so
 * any object data is not stored.
 */
class ObjectStoreImitator : public ObjectStore {
private:
  class Collection;
  typedef boost::intrusive_ptr<Collection> CollectionRef;

  struct Object : public RefCountedObjectSafe {
    Collection *c;
    ghobject_t oid;
    bool exists;
    uint64_t nid;
    uint64_t size;

    uint32_t alloc_hint_flags = 0;
    uint32_t expected_object_size = 0;
    uint32_t expected_write_size = 0;

    typedef std::map<uint64_t, bluestore_pextent_t> ExtentMap;
    ExtentMap extent_map;

    Object(Collection *c_, const ghobject_t &oid_, bool exists_ = false,
           uint64_t nid_ = 0, uint64_t size_ = 0)
        : c(c_), oid(oid_), exists(exists_), nid(nid_), size(size_) {}

    void punch_hole(uint64_t offset, uint64_t length,
                    PExtentVector &old_extents);
    void verify_extents();
    void append(PExtentVector &ext, uint64_t offset);
    uint64_t ext_length();
  };
  typedef boost::intrusive_ptr<Object> ObjectRef;

  struct ReadOp {
    uint64_t offset;
    uint64_t length;
    unsigned blks;
    unsigned jmps; // # times having to stop iterating over continuous extents
    ReadOp(uint64_t offset = 0, uint64_t length = 0, unsigned blks = 0,
           unsigned jmps = 0)
        : offset(offset), length(length), blks(blks), jmps(jmps) {}
  };

  struct Collection : public CollectionImpl {
    bluestore_cnode_t cnode;
    std::map<ghobject_t, ObjectRef> objects;
    std::unordered_map<ghobject_t, std::vector<ReadOp>> read_ops;

    ceph::shared_mutex lock = ceph::make_shared_mutex(
        "ObjectStoreImitator::Collection::lock", true, false);

    // Lock for 'objects'
    ceph::recursive_mutex obj_lock = ceph::make_recursive_mutex(
        "ObjectStoreImitator::Collection::obj_lock");

    bool exists;

    // pool options
    pool_opts_t pool_opts;
    ContextQueue *commit_queue;

    bool contains(const ghobject_t &oid) {
      if (cid.is_meta())
        return oid.hobj.pool == -1;
      spg_t spgid;
      if (cid.is_pg(&spgid))
        return spgid.pgid.contains(cnode.bits, oid) &&
               oid.shard_id == spgid.shard;
      return false;
    }

    int64_t pool() const { return cid.pool(); }
    ObjectRef get_obj(const ghobject_t &oid, bool create) {
      ceph_assert(create ? ceph_mutex_is_wlocked(lock)
                         : ceph_mutex_is_locked(lock));

      spg_t pgid;
      if (cid.is_pg(&pgid) && !oid.match(cnode.bits, pgid.ps())) {
        ceph_abort();
      }

      auto o = objects.find(oid);
      if (o != objects.end())
        return o->second;

      if (!create)
        return nullptr;

      return objects[oid] = ceph::make_ref<Object>(this, oid);
    }

    bool flush_commit(Context *c) override { return false; }
    void flush() override {}

    void rename_obj(ObjectRef &oldo, const ghobject_t &old_oid,
                    const ghobject_t &new_oid) {
      std::lock_guard l(obj_lock);
      auto po = objects.find(old_oid);
      auto pn = objects.find(new_oid);

      ceph_assert(po != pn);
      ceph_assert(po != objects.end());
      if (pn != objects.end()) {
        objects.erase(pn);
      }

      ObjectRef o = po->second;

      oldo.reset(new Object(o->c, old_oid));
      po->second = oldo;
      objects.insert(std::make_pair(new_oid, o));

      o->oid = new_oid;
    }

    Collection(ObjectStoreImitator *sim_, coll_t cid_)
        : CollectionImpl(sim_->cct, cid_), exists(true), commit_queue(nullptr) {
    }
  };

  CollectionRef _get_collection(const coll_t &cid);
  int _split_collection(CollectionRef &c, CollectionRef &d, unsigned bits,
                        int rem);
  int _merge_collection(CollectionRef *c, CollectionRef &d, unsigned bits);
  int _collection_list(Collection *c, const ghobject_t &start,
                       const ghobject_t &end, int max, bool legacy,
                       std::vector<ghobject_t> *ls, ghobject_t *next);
  int _remove_collection(const coll_t &cid, CollectionRef *c);
  void _do_remove_collection(CollectionRef *c);
  int _create_collection(const coll_t &cid, unsigned bits, CollectionRef *c);

  // Transactions
  void _add_transaction(Transaction *t);

  // Object ops
  int _write(CollectionRef &c, ObjectRef &o, uint64_t offset, size_t length,
             bufferlist &bl, uint32_t fadvise_flags);
  int _set_alloc_hint(CollectionRef &c, ObjectRef &o,
                      uint64_t expected_object_size,
                      uint64_t expected_write_size, uint32_t flags);
  int _rename(CollectionRef &c, ObjectRef &oldo, ObjectRef &newo,
              const ghobject_t &new_oid);
  int _clone(CollectionRef &c, ObjectRef &oldo, ObjectRef &newo);
  int _clone_range(CollectionRef &c, ObjectRef &oldo, ObjectRef &newo,
                   uint64_t srcoff, uint64_t length, uint64_t dstoff);

  // Helpers

  void _assign_nid(ObjectRef &o);
  int _do_write(CollectionRef &c, ObjectRef &o, uint64_t offset,
                uint64_t length, ceph::buffer::list &bl,
                uint32_t fadvise_flags);
  int _do_alloc_write(CollectionRef c, ObjectRef &o, bufferlist &bl,
                      uint64_t offset, uint64_t length);
  void _do_truncate(CollectionRef &c, ObjectRef &o, uint64_t offset);
  int _do_zero(CollectionRef &c, ObjectRef &o, uint64_t offset, size_t length);
  int _do_clone_range(CollectionRef &c, ObjectRef &oldo, ObjectRef &newo,
                      uint64_t srcoff, uint64_t length, uint64_t dstoff);
  int _do_read(Collection *c, ObjectRef &o, uint64_t offset, size_t len,
               ceph::buffer::list &bl, uint32_t op_flags = 0,
               uint64_t retry_count = 0);

  void release_alloc(PExtentVector &old_extents);
  int64_t allocate_alloc(uint64_t want_size, uint64_t block_size,
                         uint64_t max_alloc_size, int64_t hint,
                         PExtentVector *extents);

  // Members

  double alloc_time = 0.0;
  uint64_t alloc_ops = 0;

  boost::scoped_ptr<Allocator> alloc;
  std::atomic<uint64_t> nid_last = {0};

  uint64_t min_alloc_size; ///< minimum allocation unit (power of 2)
  static_assert(std::numeric_limits<uint8_t>::max() >
                    std::numeric_limits<decltype(min_alloc_size)>::digits,
                "not enough bits for min_alloc_size");

  ///< rwlock to protect coll_map/new_coll_map
  ceph::shared_mutex coll_lock =
      ceph::make_shared_mutex("ObjectStoreImitator::coll_lock");
  std::unordered_map<coll_t, CollectionRef> coll_map;
  std::unordered_map<coll_t, CollectionRef>
      new_coll_map; // store collections that is opened via open_new_collection
                    // but a create txn has not executed

public:
  ObjectStoreImitator(CephContext *cct, const std::string &path_,
                      uint64_t min_alloc_size_)
      : ObjectStore(cct, path_), alloc(nullptr),
        min_alloc_size(min_alloc_size_) {}

  ~ObjectStoreImitator() = default;
  void init_alloc(const std::string &alloc_type, uint64_t size);

  void verify_objects(CollectionHandle &ch);
  void print_status();
  // Generate metrics for per-object fragmentation (how fragmented are each
  // object's extents), defined by: frag_score = 1 - sum((size proportion of
  // each extents / object size) ^ index of each extent in a vector sorted by
  // descending length). This should only be called after the  generators
  // are finished as it will attempt to change an object's extents.
  void print_per_object_fragmentation();

  // Genereate metrisc for per-access fragmentation, which is jumps/blocks read.
  // Jumps are how many times we have to stop reading continuous extents
  void print_per_access_fragmentation();

  // Print allocator average latency
  void print_allocator_profile();
  // Overrides

  // This is often not called directly but through queue_transaction
  int queue_transactions(CollectionHandle &ch, std::vector<Transaction> &tls,
                         TrackedOpRef op = TrackedOpRef(),
                         ThreadPool::TPHandle *handle = NULL) override;
  int read(CollectionHandle &c, const ghobject_t &oid, uint64_t offset,
           size_t len, ceph::buffer::list &bl, uint32_t op_flags = 0) override;
  CollectionHandle open_collection(const coll_t &cid) override;
  CollectionHandle create_new_collection(const coll_t &cid) override;
  void set_collection_commit_queue(const coll_t &cid,
                                   ContextQueue *commit_queue) override;
  bool exists(CollectionHandle &c, const ghobject_t &old) override;
  int set_collection_opts(CollectionHandle &c,
                          const pool_opts_t &opts) override;

  int list_collections(std::vector<coll_t> &ls) override;
  bool collection_exists(const coll_t &c) override;
  int collection_empty(CollectionHandle &c, bool *empty) override;
  int collection_bits(CollectionHandle &c) override;
  int collection_list(CollectionHandle &c, const ghobject_t &start,
                      const ghobject_t &end, int max,
                      std::vector<ghobject_t> *ls, ghobject_t *next) override;

  // Not used but implemented so it compiles
  std::string get_type() override { return "ObjectStoreImitator"; }
  bool test_mount_in_use() override { return false; }
  int mount() override { return 0; }
  int umount() override { return 0; }
  int validate_hobject_key(const hobject_t &obj) const override { return 0; }
  unsigned get_max_attr_name_length() override { return 256; }
  int mkfs() override { return 0; }
  int mkjournal() override { return 0; }
  bool needs_journal() override { return false; }
  bool wants_journal() override { return false; }
  bool allows_journal() override { return false; }
  int statfs(struct store_statfs_t *buf,
             osd_alert_list_t *alerts = nullptr) override {
    return 0;
  }
  int pool_statfs(uint64_t pool_id, struct store_statfs_t *buf,
                  bool *per_pool_omap) override {
    return 0;
  }
  int stat(CollectionHandle &c, const ghobject_t &oid, struct stat *st,
           bool allow_eio = false) override {
    return 0;
  }
  int fiemap(CollectionHandle &c, const ghobject_t &oid, uint64_t offset,
             size_t len, ceph::buffer::list &bl) override {
    return 0;
  }
  int fiemap(CollectionHandle &c, const ghobject_t &oid, uint64_t offset,
             size_t len, std::map<uint64_t, uint64_t> &destmap) override {
    return 0;
  }
  int getattr(CollectionHandle &c, const ghobject_t &oid, const char *name,
              ceph::buffer::ptr &value) override {
    return 0;
  }
  int getattrs(
      CollectionHandle &c, const ghobject_t &oid,
      std::map<std::string, ceph::buffer::ptr, std::less<>> &aset) override {
    return 0;
  }
  int omap_get(CollectionHandle &c,        ///< [in] Collection containing oid
               const ghobject_t &oid,      ///< [in] Object containing omap
               ceph::buffer::list *header, ///< [out] omap header
               std::map<std::string, ceph::buffer::list>
                   *out /// < [out] Key to value std::map
               ) override {
    return 0;
  }
  int omap_get_header(CollectionHandle &c,   ///< [in] Collection containing oid
                      const ghobject_t &oid, ///< [in] Object containing omap
                      ceph::buffer::list *header, ///< [out] omap header
                      bool allow_eio = false      ///< [in] don't assert on eio
                      ) override {
    return 0;
  }
  int omap_get_keys(CollectionHandle &c,   ///< [in] Collection containing oid
                    const ghobject_t &oid, ///< [in] Object containing omap
                    std::set<std::string> *keys ///< [out] Keys defined on oid
                    ) override {
    return 0;
  }
  int omap_get_values(CollectionHandle &c,   ///< [in] Collection containing oid
                      const ghobject_t &oid, ///< [in] Object containing omap
                      const std::set<std::string> &keys, ///< [in] Keys to get
                      std::map<std::string, ceph::buffer::list>
                          *out ///< [out] Returned keys and values
                      ) override {
    return 0;
  }
  int omap_check_keys(
      CollectionHandle &c,               ///< [in] Collection containing oid
      const ghobject_t &oid,             ///< [in] Object containing omap
      const std::set<std::string> &keys, ///< [in] Keys to check
      std::set<std::string> *out ///< [out] Subset of keys defined on oid
      ) override {
    return 0;
  }
  ObjectMap::ObjectMapIterator
  get_omap_iterator(CollectionHandle &c,  ///< [in] collection
                    const ghobject_t &oid ///< [in] object
                    ) override {
    return {};
  }
  void set_fsid(uuid_d u) override {}
  uuid_d get_fsid() override { return {}; }
  uint64_t estimate_objects_overhead(uint64_t num_objects) override {
    return num_objects * 300;
  }
  objectstore_perf_stat_t get_cur_stats() override { return {}; }
  const PerfCounters *get_perf_counters() const override { return nullptr; };
  void refresh_perf_counters() override {}
};
