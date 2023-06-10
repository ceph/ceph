// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Fragmentation Simulator
 * Author: Tri Dao, tri.dao@uwaterloo.ca
 */

#include "common/ceph_mutex.h"
#include "common/common_init.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "include/Context.h"
#include "include/ceph_assert.h"
#include "include/interval_set.h"
#include "include/mempool.h"
#include "os/ObjectStore.h"
#include "os/bluestore/Allocator.h"
#include <asm-generic/errno-base.h>
#include <bit>
#include <boost/scoped_ptr.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/scoped_ptr.hpp>
#include <cstdint>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <iostream>
#include <mutex>

#include "common/debug.h"
#include "common/errno.h"
#include "os/bluestore/BlueStore.h"
#include "os/bluestore/bluestore_types.h"

#define dout_context cct
#define dout_subsys ceph_subsys_

#define OBJECT_MAX_SIZE 0xffffffff // 32 bits

/**
 * FragmentationSimulator will simulate how BlueStore does IO (as of the time
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
class FragmentationSimulator : ObjectStore {
public:
private:
  class Collection;
  typedef boost::intrusive_ptr<Collection> CollectionRef;
  class OpSequencer;
  using OpSequencerRef = ceph::ref_t<OpSequencer>;
  typedef std::list<PExtentVector> old_extent_map_t;

  struct Object {
    Collection *c;
    ghobject_t id;
    bool exists;
    uint64_t nid;
    uint64_t size;

    uint32_t alloc_hint_flags = 0;
    uint32_t expected_object_size = 0;
    uint32_t expected_write_size = 0;

    // We assume these extents are sorted according by "logical" order.
    PExtentVector extents;

    Object(Collection *c_, const ghobject_t &id_, bool exists_ = false,
           uint64_t nid_ = 0, uint64_t size_ = 0)
        : c(c_), id(id_), exists(exists_), nid(nid_), size(size_) {}

    void punch_hole(uint64_t offset, uint64_t length,
                    old_extent_map_t *old_extents) {
      uint64_t l_offset{0}, punched_length{0};
      PExtentVector to_be_punched, remains;
      for (auto e : extents) {
        if (l_offset > offset && l_offset - length >= offset)
          break;

        // Found where we need to punch
        if (l_offset >= offset) {
          if (e.length + punched_length >= length) {
            uint64_t left = e.length + punched_length - length;
            e.length = length - punched_length;

            remains.emplace_back(e.offset + e.length, left);
            to_be_punched.push_back(e);
          }

          punched_length += e.length;
        } else {
          remains.push_back(e);
        }

        l_offset += e.length;
      }

      extents = remains;
      old_extents->push_back(to_be_punched);
    }
  };
  typedef boost::intrusive_ptr<Object> ObjectRef;

  struct TransContext final {
    MEMPOOL_CLASS_HELPERS();

    typedef enum {
      STATE_PREPARE,
      STATE_AIO_WAIT,
      STATE_IO_DONE,
      STATE_FINISHING,
      STATE_DONE,
    } state_t;

    inline void set_state(state_t s) { state = s; }
    inline state_t get_state() { return state; }

    CollectionRef ch;
    OpSequencerRef osr; // this should be ch->osr
    boost::intrusive::list_member_hook<> sequencer_item;

    std::list<Context *> oncommits; ///< more commit completions

    boost::intrusive::list_member_hook<> deferred_queue_item;

    interval_set<uint64_t> allocated, released;
    uint64_t osd_pool_id = META_POOL_ID; ///< osd pool id we're operating on

    uint64_t seq = 0;

    uint64_t last_nid = 0; ///< if non-zero, highest new nid we allocated

    explicit TransContext(CephContext *cct, Collection *c, OpSequencer *o,
                          std::list<Context *> *on_commits)
        : ch(c), osr(o) {
      if (on_commits) {
        oncommits.swap(*on_commits);
      }
    }
    ~TransContext() {}

  private:
    state_t state = STATE_PREPARE;
  };

  class OpSequencer : public RefCountedObject {
  public:
    ceph::mutex qlock =
        ceph::make_mutex("FragmentationSimulator::OpSequencer::qlock");
    ceph::condition_variable qcond;
    typedef boost::intrusive::list<
        TransContext, boost::intrusive::member_hook<
                          TransContext, boost::intrusive::list_member_hook<>,
                          &TransContext::sequencer_item>>
        q_list_t;
    q_list_t q; ///< transactions

    FragmentationSimulator *sim;
    coll_t cid;

    uint64_t last_seq = 0;

    std::atomic_bool zombie = {
        false}; ///< in zombie_osr std::set (collection going away)

    void queue_new(TransContext *txc) {
      std::lock_guard l(qlock);
      txc->seq = ++last_seq;
      q.push_back(*txc);
    }

    void drain() {
      std::unique_lock l(qlock);
      while (!q.empty())
        qcond.wait(l);
    }

    void drain_preceding(TransContext *txc) {
      std::unique_lock l(qlock);
      while (&q.front() != txc)
        qcond.wait(l);
    }

    void flush() {
      std::unique_lock l(qlock);
      while (true) {
        qcond.wait(l);
      }
    }

    void flush_all_but_last() {
      std::unique_lock l(qlock);
      ceph_assert(q.size() >= 1);
      while (true) {
        if (q.size() <= 1) {
          return;
        } else {
          auto it = q.rbegin();
          it++;
          if (it->get_state() >= TransContext::STATE_FINISHING) {
            return;
          }
        }

        qcond.wait(l);
      }
    }

    bool flush_commit(Context *c) {
      std::lock_guard l(qlock);
      if (q.empty()) {
        return true;
      }

      TransContext *txc = &q.back();
      if (txc->get_state() >= TransContext::STATE_FINISHING) {
        return true;
      }

      txc->oncommits.push_back(c);
      return false;
    }

  private:
    FRIEND_MAKE_REF(OpSequencer);
    OpSequencer(FragmentationSimulator *sim, uint32_t sequencer_id,
                const coll_t &c)
        : RefCountedObject(sim->cct), sim(sim), cid(c) {}
    ~OpSequencer() { ceph_assert(q.empty()); }
  };

  struct Collection : public CollectionImpl {
    FragmentationSimulator *sim;
    OpSequencerRef osr;
    bluestore_cnode_t cnode;
    std::map<ghobject_t, ObjectRef> objects;
    ceph::shared_mutex lock = ceph::make_shared_mutex(
        "FragmentationSimulator::Collection::lock", true, false);
    ceph::recursive_mutex obj_lock = ceph::make_recursive_mutex(
        "FragmentationSimulator::Collection::obj_lock");

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
      if (o->second)
        return o->second;

      auto on = new Object(this, oid);
      return objects[oid] = on;
    }

    bool flush_commit(Context *c) override { return osr->flush_commit(c); }
    void flush() override { return osr->flush(); }
    void flush_all_but_last() { return osr->flush_all_but_last(); }

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

      o->id = new_oid;
    }

    Collection(FragmentationSimulator *sim, coll_t c);
  };

  CollectionRef _get_collection(const coll_t &cid);
  int _split_collection(TransContext *txc, CollectionRef &c, CollectionRef &d,
                        unsigned bits, int rem);
  int _merge_collection(TransContext *txc, CollectionRef *c, CollectionRef &d,
                        unsigned bits);
  int _collection_list(Collection *c, const ghobject_t &start,
                       const ghobject_t &end, int max, bool legacy,
                       std::vector<ghobject_t> *ls, ghobject_t *next);
  int _remove_collection(TransContext *txc, const coll_t &cid,
                         CollectionRef *c);
  void _do_remove_collection(TransContext *txc, CollectionRef *c);
  int _create_collection(TransContext *txc, const coll_t &cid, unsigned bits,
                         CollectionRef *c);

  // Transactions
  TransContext *_txc_create(Collection *c, OpSequencer *osr,
                            std::list<Context *> *on_commits,
                            TrackedOpRef osd_op = TrackedOpRef());
  void _txc_add_transaction(TransContext *txc, Transaction *t);
  void _txc_state_proc(TransContext *txc);
  void _txc_finish_io(TransContext *txc);
  void _txc_finish(TransContext *txc);
  void _txc_release_alloc(TransContext *txc);

  // OpSequencer
  void _osr_attach(Collection *c);
  void _osr_register_zombie(OpSequencer *osr);
  void _osr_drain(OpSequencer *osr);
  void _osr_drain_preceding(TransContext *txc);

  // Object ops
  int _write(TransContext *txc, CollectionRef &c, ObjectRef &o, uint64_t offset,
             size_t length, bufferlist &bl, uint32_t fadvise_flags);
  int _set_alloc_hint(TransContext *txc, CollectionRef &c, ObjectRef &o,
                      uint64_t expected_object_size,
                      uint64_t expected_write_size, uint32_t flags);
  int _rename(TransContext *txc, CollectionRef &c, ObjectRef &oldo,
              ObjectRef &newo, const ghobject_t &new_oid);
  int _clone(TransContext *txc, CollectionRef &c, ObjectRef &oldo,
             ObjectRef &newo);
  int _clone_range(TransContext *txc, CollectionRef &c, ObjectRef &oldo,
                   ObjectRef &newo, uint64_t srcoff, uint64_t length,
                   uint64_t dstoff);

  // Write

  struct WriteContext {
    bool buffered = false;         ///< buffered write
    bool compress = false;         ///< compressed write
    uint64_t target_blob_size = 0; ///< target (max) blob size
    unsigned csum_order = 0;       ///< target checksum chunk order

    old_extent_map_t old_extents; ///< must deref these blobs

    struct write_item {
      uint64_t blob_length;
      uint64_t b_off;
      ceph::buffer::list bl;
      ceph::buffer::list compressed_bl;
      bool compressed = false;
      size_t compressed_len = 0;

      write_item(uint64_t logical_offs, uint64_t blob_len, uint64_t o,
                 ceph::buffer::list &bl)
          : blob_length(blob_len), b_off(o), bl(bl) {}
    };
    std::vector<write_item> writes; ///< blobs we're writing

    void write(uint64_t loffs, uint64_t blob_len, uint64_t o,
               ceph::buffer::list &bl, bool _new_blob) {
      writes.emplace_back(loffs, blob_len, o, bl);
    }
  };

  // Helpers

  void _assign_nid(TransContext *txc, ObjectRef &o);
  void _choose_write_options(CollectionRef &c, ObjectRef &o,
                             uint32_t fadvise_flags, WriteContext *wctx);
  int _do_write(TransContext *txc, CollectionRef &c, ObjectRef &o,
                uint64_t offset, uint64_t length, ceph::buffer::list &bl,
                uint32_t fadvise_flags);
  int _do_alloc_write(TransContext *txc, CollectionRef c, ObjectRef &o,
                      WriteContext *wctx);
  void _do_truncate(TransContext *txc, CollectionRef &c, ObjectRef &o,
                    uint64_t offset);
  int _do_zero(TransContext *txc, CollectionRef &c, ObjectRef &o,
               uint64_t offset, size_t length);
  int _do_clone_range(TransContext *txc, CollectionRef &c, ObjectRef &oldo,
                      ObjectRef &newo, uint64_t srcoff, uint64_t length,
                      uint64_t dstoff) {
    return 0;
  }
  int _do_read(Collection *c, ObjectRef &o, uint64_t offset, size_t len,
               ceph::buffer::list &bl, uint32_t op_flags = 0,
               uint64_t retry_count = 0);

  // _wctx_finish will empty out old_extents
  void _wctx_finish(TransContext *txc, CollectionRef &c, ObjectRef &o,
                    WriteContext *wctx);
  // Members

  boost::scoped_ptr<Allocator> alloc;

  ceph::mutex zombie_osr_lock =
      ceph::make_mutex("FragmentationSimulator::zombie_osr_lock");
  uint32_t next_sequencer_id = 0;
  std::map<coll_t, OpSequencerRef>
      zombie_osr_set; ///< std::set of OpSequencers for deleted collections

  std::atomic<uint64_t> nid_last = {0};
  std::atomic<int> csum_type = {Checksummer::CSUM_CRC32C};

  uint64_t block_size = 0;      ///< block size of block device (power of 2)
  uint64_t block_mask = 0;      ///< mask to get just the block offset
  size_t block_size_order = 0;  ///< bits to shift to get block size
  uint64_t optimal_io_size = 0; ///< best performance io size for block device

  uint64_t min_alloc_size;          ///< minimum allocation unit (power of 2)
  uint8_t min_alloc_size_order = 0; ///< bits to shift to get min_alloc_size
  static_assert(std::numeric_limits<uint8_t>::max() >
                    std::numeric_limits<decltype(min_alloc_size)>::digits,
                "not enough bits for min_alloc_size");

  std::atomic<Compressor::CompressionMode> comp_mode = {
      Compressor::COMP_NONE}; ///< compression mode
  CompressorRef compressor;
  std::atomic<uint64_t> comp_min_blob_size = {0};
  std::atomic<uint64_t> comp_max_blob_size = {0};

  ceph::mutex qlock = ceph::make_mutex("FragmentationSimulator::Alerts::qlock");
  std::string failed_cmode;
  std::set<std::string> failed_compressors;

  bool _set_compression_alert(bool cmode, const char *s) {
    std::lock_guard l(qlock);
    if (cmode) {
      bool ret = failed_cmode.empty();
      failed_cmode = s;
      return ret;
    }
    return failed_compressors.emplace(s).second;
  }
  void _clear_compression_alert() {
    std::lock_guard l(qlock);
    failed_compressors.clear();
    failed_cmode.clear();
  }

  std::atomic<uint64_t> max_blob_size = {0}; ///< maximum blob size
  ///
  ///< rwlock to protect coll_map/new_coll_map
  ceph::shared_mutex coll_lock =
      ceph::make_shared_mutex("FragmentationSimulator::coll_lock");
  std::unordered_map<coll_t, CollectionRef> coll_map;
  bool collections_had_errors = false;
  std::unordered_map<coll_t, CollectionRef> new_coll_map;

  Finisher finisher;

  FragmentationSimulator(CephContext *cct, const std::string &path_)
      : ObjectStore(cct, path_), alloc(nullptr),
        finisher(cct, "commit_finisher", "cfin") {}

  ~FragmentationSimulator() = default;

  void init_alloc(const std::string &alloc_type, int64_t size,
                  uint64_t min_alloc_size);

  // Overrides

  int queue_transactions(CollectionHandle &ch, std::vector<Transaction> &tls,
                         TrackedOpRef op = TrackedOpRef(),
                         ThreadPool::TPHandle *handle = NULL) override;

  std::string get_type() override { return "FragmentationSimulator"; }

  bool test_mount_in_use() override { return false; }
  int mount() override;
  int umount() override;

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

  CollectionHandle open_collection(const coll_t &cid) override;
  CollectionHandle create_new_collection(const coll_t &cid) override;
  void set_collection_commit_queue(const coll_t &cid,
                                   ContextQueue *commit_queue) override;
  bool exists(CollectionHandle &c, const ghobject_t &old) override;
  int set_collection_opts(CollectionHandle &c,
                          const pool_opts_t &opts) override;

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

  int list_collections(std::vector<coll_t> &ls) override;
  bool collection_exists(const coll_t &c) override;
  int collection_empty(CollectionHandle &c, bool *empty) override;
  int collection_bits(CollectionHandle &c) override;
  int collection_list(CollectionHandle &c, const ghobject_t &start,
                      const ghobject_t &end, int max,
                      std::vector<ghobject_t> *ls, ghobject_t *next) override;

  int omap_get(CollectionHandle &c,        ///< [in] Collection containing oid
               const ghobject_t &oid,      ///< [in] Object containing omap
               ceph::buffer::list *header, ///< [out] omap header
               std::map<std::string, ceph::buffer::list>
                   *out /// < [out] Key to value std::map
               ) override {
    return 0;
  }

  /// Get omap header
  int omap_get_header(CollectionHandle &c,   ///< [in] Collection containing oid
                      const ghobject_t &oid, ///< [in] Object containing omap
                      ceph::buffer::list *header, ///< [out] omap header
                      bool allow_eio = false      ///< [in] don't assert on eio
                      ) override {
    return 0;
  }

  /// Get keys defined on oid
  int omap_get_keys(CollectionHandle &c,   ///< [in] Collection containing oid
                    const ghobject_t &oid, ///< [in] Object containing omap
                    std::set<std::string> *keys ///< [out] Keys defined on oid
                    ) override {
    return 0;
  }

  /// Get key values
  int omap_get_values(CollectionHandle &c,   ///< [in] Collection containing oid
                      const ghobject_t &oid, ///< [in] Object containing omap
                      const std::set<std::string> &keys, ///< [in] Keys to get
                      std::map<std::string, ceph::buffer::list>
                          *out ///< [out] Returned keys and values
                      ) override {
    return 0;
  }

  /// Filters keys into out which are defined on oid
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

  void set_fsid(uuid_d u) override;
  uuid_d get_fsid() override;
  uint64_t estimate_objects_overhead(uint64_t num_objects) override;

  objectstore_perf_stat_t get_cur_stats() override { return {}; }
  const PerfCounters *get_perf_counters() const override { return nullptr; };
};

void FragmentationSimulator::init_alloc(const std::string &alloc_type,
                                        int64_t size, uint64_t min_alloc_size) {
  alloc.reset(Allocator::create(cct, alloc_type, size, min_alloc_size));
}

// ------- Collections -------

int FragmentationSimulator::_merge_collection(TransContext *txc,
                                              CollectionRef *c,
                                              CollectionRef &d, unsigned bits) {
  std::unique_lock l((*c)->lock);
  std::unique_lock l2(d->lock);
  coll_t cid = (*c)->cid;

  // flush all previous deferred writes on the source collection to ensure
  // that all deferred writes complete before we merge as the target
  // collection's sequencer may need to order new ops after those writes.

  _osr_drain((*c)->osr.get());

  // move any cached items (onodes and referenced shared blobs) that will
  // belong to the child collection post-split.  leave everything else behind.
  // this may include things that don't strictly belong to the now-smaller
  // parent split, but the OSD will always send us a split for every new
  // child.

  spg_t pgid, dest_pgid;
  bool is_pg = cid.is_pg(&pgid);
  ceph_assert(is_pg);
  is_pg = d->cid.is_pg(&dest_pgid);
  ceph_assert(is_pg);

  // adjust bits.  note that this will be redundant for all but the first
  // merge call for the parent/target.
  d->cnode.bits = bits;

  // remove source collection
  {
    std::unique_lock l3(coll_lock);
    _do_remove_collection(txc, c);
  }

  return 0;
}

int FragmentationSimulator::_split_collection(TransContext *txc,
                                              CollectionRef &c,
                                              CollectionRef &d, unsigned bits,
                                              int rem) {
  std::unique_lock l(c->lock);
  std::unique_lock l2(d->lock);

  // flush all previous deferred writes on this sequencer.  this is a bit
  // heavyweight, but we need to make sure all deferred writes complete
  // before we split as the new collection's sequencer may need to order
  // this after those writes, and we don't bother with the complexity of
  // moving those TransContexts over to the new osr.
  _osr_drain_preceding(txc);

  // move any cached items (onodes and referenced shared blobs) that will
  // belong to the child collection post-split.  leave everything else behind.
  // this may include things that don't strictly belong to the now-smaller
  // parent split, but the OSD will always send us a split for every new
  // child.

  spg_t pgid, dest_pgid;
  bool is_pg = c->cid.is_pg(&pgid);
  ceph_assert(is_pg);
  is_pg = d->cid.is_pg(&dest_pgid);
  ceph_assert(is_pg);

  ceph_assert(d->cnode.bits == bits);

  // adjust bits.  note that this will be redundant for all but the first
  // split call for this parent (first child).
  c->cnode.bits = bits;
  ceph_assert(d->cnode.bits == bits);

  return 0;
};

// ------- Transactions -------

int FragmentationSimulator::queue_transactions(CollectionHandle &ch,
                                               std::vector<Transaction> &tls,
                                               TrackedOpRef op,
                                               ThreadPool::TPHandle *handle) {
  FUNCTRACE(cct);
  std::list<Context *> on_applied, on_commit, on_applied_sync;
  ObjectStore::Transaction::collect_contexts(tls, &on_applied, &on_commit,
                                             &on_applied_sync);

  Collection *c = static_cast<Collection *>(ch.get());
  OpSequencer *osr = c->osr.get();

  // prepare
  TransContext *txc =
      _txc_create(static_cast<Collection *>(ch.get()), osr, &on_commit, op);

  for (std::vector<Transaction>::iterator p = tls.begin(); p != tls.end();
       ++p) {
    _txc_add_transaction(txc, &(*p));
  }

  if (handle)
    handle->suspend_tp_timeout();

  if (handle)
    handle->reset_tp_timeout();

  // execute (start)
  _txc_state_proc(txc);

  // we're immediately readable (unlike FileStore)
  for (auto c : on_applied_sync) {
    c->complete(0);
  }
  if (!on_applied.empty()) {
    if (c->commit_queue) {
      c->commit_queue->queue(on_applied);
    } else {
      finisher.queue(on_applied);
    }
  }

  return 0;
}

FragmentationSimulator::TransContext *
FragmentationSimulator::_txc_create(Collection *c, OpSequencer *osr,
                                    std::list<Context *> *on_commits,
                                    TrackedOpRef osd_op) {
  TransContext *txc = new TransContext(cct, c, osr, on_commits);
  osr->queue_new(txc);
  return txc;
}

FragmentationSimulator::CollectionRef
FragmentationSimulator::_get_collection(const coll_t &cid) {
  std::shared_lock l(coll_lock);
  ceph::unordered_map<coll_t, CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return CollectionRef();
  return cp->second;
}

void FragmentationSimulator::_txc_add_transaction(TransContext *txc,
                                                  Transaction *t) {
  Transaction::iterator i = t->begin();

  std::vector<CollectionRef> cvec(i.colls.size());
  unsigned j = 0;
  for (std::vector<coll_t>::iterator p = i.colls.begin(); p != i.colls.end();
       ++p, ++j) {
    cvec[j] = _get_collection(*p);
  }

  std::vector<ObjectRef> ovec(i.objects.size());

  for (int pos = 0; i.have_op(); ++pos) {
    Transaction::Op *op = i.decode_op();
    int r = 0;

    // no coll or obj
    if (op->op == Transaction::OP_NOP)
      continue;

    // collection operations
    CollectionRef &c = cvec[op->cid];

    // initialize osd_pool_id and do a smoke test that all collections belong
    // to the same pool
    spg_t pgid;
    if (!!c ? c->cid.is_pg(&pgid) : false) {
      ceph_assert(txc->osd_pool_id == META_POOL_ID ||
                  txc->osd_pool_id == pgid.pool());
      txc->osd_pool_id = pgid.pool();
    }

    switch (op->op) {
    case Transaction::OP_RMCOLL: {
      if (coll_map.find(c->cid) != coll_map.end())
        coll_map.erase(c->cid);
    } break;

    case Transaction::OP_MKCOLL: {
      ceph_assert(!c);
      const coll_t &cid = i.get_cid(op->cid);
      r = _create_collection(txc, cid, op->split_bits, &c);
      if (!r)
        continue;

    } break;

    case Transaction::OP_SPLIT_COLLECTION:
      ceph_abort_msg("deprecated");
      break;

    case Transaction::OP_SPLIT_COLLECTION2: {
      uint32_t bits = op->split_bits;
      uint32_t rem = op->split_rem;
      r = _split_collection(txc, c, cvec[op->dest_cid], bits, rem);
      if (!r)
        continue;
    } break;

    case Transaction::OP_MERGE_COLLECTION: {
      uint32_t bits = op->split_bits;
      r = _merge_collection(txc, &c, cvec[op->dest_cid], bits);
      if (!r)
        continue;
    } break;

    case Transaction::OP_COLL_HINT: {
      uint32_t type = op->hint;
      bufferlist hint;
      i.decode_bl(hint);
      auto hiter = hint.cbegin();
      if (type == Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS) {
        uint32_t pg_num;
        uint64_t num_objs;
        decode(pg_num, hiter);
        decode(num_objs, hiter);
        dout(10) << __func__ << " collection hint objects is a no-op, "
                 << " pg_num " << pg_num << " num_objects " << num_objs
                 << dendl;
      } else {
        // Ignore the hint
        dout(10) << __func__ << " unknown collection hint " << type << dendl;
      }
      continue;
    } break;

    case Transaction::OP_COLL_SETATTR:
      r = -EOPNOTSUPP;
      break;

    case Transaction::OP_COLL_RMATTR:
      r = -EOPNOTSUPP;
      break;

    case Transaction::OP_COLL_RENAME:
      ceph_abort_msg("not implemented");
      break;
    }

    // these operations implicity create the object
    bool create = false;
    if (op->op == Transaction::OP_TOUCH || op->op == Transaction::OP_CREATE ||
        op->op == Transaction::OP_WRITE || op->op == Transaction::OP_ZERO) {
      create = true;
    }

    // object operations
    std::unique_lock l(c->lock);
    ObjectRef &o = ovec[op->oid];
    if (!o) {
      ghobject_t oid = i.get_oid(op->oid);
      o = c->get_obj(oid, create);
    }

    if (!create && (!o || !o->exists)) {
      r = -ENOENT;
      goto endop;
    }

    switch (op->op) {
    case Transaction::OP_CREATE:
    case Transaction::OP_TOUCH:
      _assign_nid(txc, o);
      r = 0;
      break;

    case Transaction::OP_WRITE: {
      uint64_t off = op->off;
      uint64_t len = op->len;
      uint32_t fadvise_flags = i.get_fadvise_flags();
      bufferlist bl;
      i.decode_bl(bl);
      r = _write(txc, c, o, off, len, bl, fadvise_flags);
    } break;

    case Transaction::OP_ZERO: {
      if (op->off + op->len > OBJECT_MAX_SIZE) {
        r = -E2BIG;
      } else {
        _assign_nid(txc, o);
        r = _do_zero(txc, c, o, op->off, op->len);
      }
    } break;

    case Transaction::OP_TRIMCACHE: {
      // deprecated, no-op
    } break;

    case Transaction::OP_TRUNCATE: {
      _do_truncate(txc, c, o, op->off);
    } break;

    case Transaction::OP_REMOVE: {
      _do_truncate(txc, c, o, 0);
    } break;

    case Transaction::OP_SETATTR:
    case Transaction::OP_SETATTRS:
    case Transaction::OP_RMATTR:
    case Transaction::OP_RMATTRS:
      break;

    case Transaction::OP_CLONE: {
      ObjectRef &no = ovec[op->dest_oid];
      if (!no) {
        const ghobject_t &noid = i.get_oid(op->dest_oid);
        no = c->get_obj(noid, true);
      }
      r = _clone(txc, c, o, no);
    } break;

    case Transaction::OP_CLONERANGE:
      ceph_abort_msg("deprecated");
      break;

    case Transaction::OP_CLONERANGE2: {
      ObjectRef &no = ovec[op->dest_oid];
      if (!no) {
        const ghobject_t &noid = i.get_oid(op->dest_oid);
        no = c->get_obj(noid, true);
      }
      uint64_t srcoff = op->off;
      uint64_t len = op->len;
      uint64_t dstoff = op->dest_off;
      r = _clone_range(txc, c, o, no, srcoff, len, dstoff);
    } break;
      {
      case Transaction::OP_COLL_ADD:
        ceph_abort_msg("not implemented");
        break;

      case Transaction::OP_COLL_REMOVE:
        ceph_abort_msg("not implemented");
        break;

      case Transaction::OP_COLL_MOVE:
        ceph_abort_msg("deprecated");
        break;

      case Transaction::OP_COLL_MOVE_RENAME:
      case Transaction::OP_TRY_RENAME: {
        ceph_assert(op->cid == op->dest_cid);
        const ghobject_t &noid = i.get_oid(op->dest_oid);
        ObjectRef &no = ovec[op->dest_oid];
        if (!no) {
          no = c->get_obj(noid, false);
        }
        r = _rename(txc, c, o, no, noid);
      } break;

      case Transaction::OP_OMAP_CLEAR:
      case Transaction::OP_OMAP_SETKEYS:
      case Transaction::OP_OMAP_RMKEYS:
      case Transaction::OP_OMAP_RMKEYRANGE:
      case Transaction::OP_OMAP_SETHEADER:
        break;

      case Transaction::OP_SETALLOCHINT: {
        r = _set_alloc_hint(txc, c, o, op->expected_object_size,
                            op->expected_write_size, op->hint);
      } break;

      default:
        derr << __func__ << " bad op " << op->op << dendl;
        ceph_abort();
      }

    endop:
      if (r < 0) {
        bool ok = false;

        if (r == -ENOENT && !(op->op == Transaction::OP_CLONERANGE ||
                              op->op == Transaction::OP_CLONE ||
                              op->op == Transaction::OP_CLONERANGE2 ||
                              op->op == Transaction::OP_COLL_ADD ||
                              op->op == Transaction::OP_SETATTR ||
                              op->op == Transaction::OP_SETATTRS ||
                              op->op == Transaction::OP_RMATTR ||
                              op->op == Transaction::OP_OMAP_SETKEYS ||
                              op->op == Transaction::OP_OMAP_RMKEYS ||
                              op->op == Transaction::OP_OMAP_RMKEYRANGE ||
                              op->op == Transaction::OP_OMAP_SETHEADER))
          // -ENOENT is usually okay
          ok = true;
        if (r == -ENODATA)
          ok = true;

        if (!ok) {
          const char *msg = "unexpected error code";

          if (r == -ENOENT && (op->op == Transaction::OP_CLONERANGE ||
                               op->op == Transaction::OP_CLONE ||
                               op->op == Transaction::OP_CLONERANGE2))
            msg = "ENOENT on clone suggests osd bug";

          if (r == -ENOSPC)
            // For now, if we hit _any_ ENOSPC, crash, before we do any damage
            // by partially applying transactions.
            msg = "ENOSPC from bluestore, misconfigured cluster";

          if (r == -ENOTEMPTY) {
            msg = "ENOTEMPTY suggests garbage data in osd data dir";
          }

          derr << __func__ << " error " << cpp_strerror(r)
               << " not handled on operation " << op->op << " (op " << pos
               << ", counting from 0)" << dendl;
          derr << msg << dendl;
          ceph_abort_msg("unexpected error");
        }
      }
    }
  }
}

void FragmentationSimulator::_txc_state_proc(TransContext *txc) {
  while (true) {
    switch (txc->get_state()) {
    case TransContext::STATE_PREPARE:
      // In BLueStore this is where we submitted IO ops to BlockDevice
      // ** fall-thru **

    case TransContext::STATE_AIO_WAIT:
      _txc_finish_io(txc); // may trigger blocked txc's too
      return;

    case TransContext::STATE_IO_DONE:
      txc->set_state(TransContext::STATE_FINISHING);
      return;

    case TransContext::STATE_FINISHING:
      _txc_finish(txc);
      if (txc->ch->commit_queue) {
        txc->ch->commit_queue->queue(txc->oncommits);
      } else {
        finisher.queue(txc->oncommits);
      }

      return;

    default:
      ceph_abort_msg("unexpected txc state");
      return;
    }
  }
}

void FragmentationSimulator::_txc_finish_io(TransContext *txc) {
  OpSequencer *osr = txc->osr.get();
  std::lock_guard l(osr->qlock);
  txc->set_state(TransContext::STATE_IO_DONE);
  OpSequencer::q_list_t::iterator p = osr->q.iterator_to(*txc);
  while (p != osr->q.begin()) {
    --p;
    if (p->get_state() < TransContext::STATE_IO_DONE) {
      return;
    }
    if (p->get_state() > TransContext::STATE_IO_DONE) {
      ++p;
      break;
    }
  }

  do {
    _txc_state_proc(&*p++);
  } while (p != osr->q.end() && p->get_state() == TransContext::STATE_IO_DONE);

  osr->qcond.notify_all();
}

void FragmentationSimulator::_txc_finish(TransContext *txc) {
  ceph_assert(txc->get_state() == TransContext::STATE_FINISHING);

  OpSequencerRef osr = txc->osr;
  bool empty = false;
  OpSequencer::q_list_t releasing_txc;
  {
    std::lock_guard l(osr->qlock);
    txc->set_state(TransContext::STATE_DONE);
    bool notify = false;
    while (!osr->q.empty()) {
      TransContext *txc = &osr->q.front();
      if (txc->get_state() != TransContext::STATE_DONE) {
        if (txc->get_state() == TransContext::STATE_PREPARE) {
          // for _osr_drain_preceding()
          notify = true;
        }
        break;
      }

      osr->q.pop_front();
      releasing_txc.push_back(*txc);
    }

    if (osr->q.empty()) {
      empty = true;
    }

    // only drain()/drain_preceding() need wakeup,
    // other cases use kv_submitted_waiters
    if (notify || empty) {
      osr->qcond.notify_all();
    }
  }

  while (!releasing_txc.empty()) {
    // release to allocator only after all preceding txc's have also
    // finished any deferred writes that potentially land in these
    // blocks
    auto txc = &releasing_txc.front();
    _txc_release_alloc(txc);
    releasing_txc.pop_front();
    delete txc;
  }

  if (empty && osr->zombie) {
    std::lock_guard l(zombie_osr_lock);
    zombie_osr_set.erase(osr->cid);
  }
}

void FragmentationSimulator::_txc_release_alloc(TransContext *txc) {
  // it's expected we're called with lazy_release_lock already taken!
  if (unlikely(cct->_conf->bluestore_debug_no_reuse_blocks)) {
    goto out;
  }
  alloc->release(txc->released);

out:
  txc->allocated.clear();
  txc->released.clear();
}

// ------- Helpers -------

void FragmentationSimulator::_assign_nid(TransContext *txc, ObjectRef &o) {
  if (o->nid) {
    ceph_assert(o->exists);
  }

  o->nid = ++nid_last;
  txc->last_nid = o->nid;
  o->exists = true;
}

int FragmentationSimulator::_do_zero(TransContext *txc, CollectionRef &c,
                                     ObjectRef &o, uint64_t offset,
                                     size_t length) {
  WriteContext wctx;
  o->punch_hole(offset, length, &wctx.old_extents);
  _wctx_finish(txc, c, o, &wctx);
  return 0;
}

int FragmentationSimulator::_do_read(Collection *c, ObjectRef &o,
                                     uint64_t offset, size_t len,
                                     ceph::buffer::list &bl, uint32_t op_flags,
                                     uint64_t retry_count) {
  auto data = std::string(len, 'a');
  bl.append(data);
  return bl.length();
}

void FragmentationSimulator::_wctx_finish(TransContext *txc, CollectionRef &c,
                                          ObjectRef &o, WriteContext *wctx) {

  auto oep = wctx->old_extents.begin();
  while (oep != wctx->old_extents.end()) {
    auto &r = *oep;
    for (auto e : r) {
      txc->released.insert(e.offset, e.length);
    }

    delete &r;
  }
}

int FragmentationSimulator::_do_write(TransContext *txc, CollectionRef &c,
                                      ObjectRef &o, uint64_t offset,
                                      uint64_t length, bufferlist &bl,
                                      uint32_t fadvise_flags) {
  int r = 0;
  uint64_t end = length + offset;

  if (length == 0) {
    return 0;
  }

  WriteContext wctx;
  _choose_write_options(c, o, fadvise_flags, &wctx);
  r = _do_alloc_write(txc, c, o, &wctx);
  if (r < 0) {
    derr << __func__ << " _do_alloc_write failed with " << cpp_strerror(r)
         << dendl;
    goto out;
  }

  _wctx_finish(txc, c, o, &wctx);
  if (end > o->size) {
    o->size = end;
  }

  r = 0;

out:
  return r;
}
// ------- Operations -------

int FragmentationSimulator::_write(TransContext *txc, CollectionRef &c,
                                   ObjectRef &o, uint64_t offset, size_t length,
                                   bufferlist &bl, uint32_t fadvise_flags) {
  int r = 0;
  if (offset + length >= OBJECT_MAX_SIZE) {
    r = -E2BIG;
  } else {
    _assign_nid(txc, o);
    r = _do_write(txc, c, o, offset, length, bl, fadvise_flags);
  }

  return r;
}

template <typename T, typename F>
T select_option(const std::string &opt_name, T val1, F f) {
  // NB: opt_name reserved for future use
  std::optional<T> val2 = f();
  if (val2) {
    return *val2;
  }
  return val1;
}

void FragmentationSimulator::_choose_write_options(CollectionRef &c,
                                                   ObjectRef &o,
                                                   uint32_t fadvise_flags,
                                                   WriteContext *wctx) {
  if (fadvise_flags & CEPH_OSD_OP_FLAG_FADVISE_WILLNEED) {
    wctx->buffered = true;
  } else if (cct->_conf->bluestore_default_buffered_write &&
             (fadvise_flags & (CEPH_OSD_OP_FLAG_FADVISE_DONTNEED |
                               CEPH_OSD_OP_FLAG_FADVISE_NOCACHE)) == 0) {
    wctx->buffered = true;
  }

  // apply basic csum block size
  wctx->csum_order = block_size_order;

  // compression parameters
  unsigned alloc_hints = o->alloc_hint_flags;
  auto cm = select_option("compression_mode", comp_mode.load(), [&]() {
    std::string val;
    if (c->pool_opts.get(pool_opts_t::COMPRESSION_MODE, &val)) {
      return std::optional<Compressor::CompressionMode>(
          Compressor::get_comp_mode_type(val));
    }
    return std::optional<Compressor::CompressionMode>();
  });

  wctx->compress =
      (cm != Compressor::COMP_NONE) &&
      ((cm == Compressor::COMP_FORCE) ||
       (cm == Compressor::COMP_AGGRESSIVE &&
        (alloc_hints & CEPH_OSD_ALLOC_HINT_FLAG_INCOMPRESSIBLE) == 0) ||
       (cm == Compressor::COMP_PASSIVE &&
        (alloc_hints & CEPH_OSD_ALLOC_HINT_FLAG_COMPRESSIBLE)));

  if ((alloc_hints & CEPH_OSD_ALLOC_HINT_FLAG_SEQUENTIAL_READ) &&
      (alloc_hints & CEPH_OSD_ALLOC_HINT_FLAG_RANDOM_READ) == 0 &&
      (alloc_hints & (CEPH_OSD_ALLOC_HINT_FLAG_IMMUTABLE |
                      CEPH_OSD_ALLOC_HINT_FLAG_APPEND_ONLY)) &&
      (alloc_hints & CEPH_OSD_ALLOC_HINT_FLAG_RANDOM_WRITE) == 0) {

    wctx->csum_order = min_alloc_size_order;

    if (wctx->compress) {
      wctx->target_blob_size = select_option(
          "compression_max_blob_size", comp_max_blob_size.load(), [&]() {
            int64_t val;
            if (c->pool_opts.get(pool_opts_t::COMPRESSION_MAX_BLOB_SIZE,
                                 &val)) {
              return std::optional<uint64_t>((uint64_t)val);
            }
            return std::optional<uint64_t>();
          });
    }
  } else {
    if (wctx->compress) {
      wctx->target_blob_size = select_option(
          "compression_min_blob_size", comp_min_blob_size.load(), [&]() {
            int64_t val;
            if (c->pool_opts.get(pool_opts_t::COMPRESSION_MIN_BLOB_SIZE,
                                 &val)) {
              return std::optional<uint64_t>((uint64_t)val);
            }
            return std::optional<uint64_t>();
          });
    }
  }

  uint64_t max_bsize = max_blob_size.load();
  if (wctx->target_blob_size == 0 || wctx->target_blob_size > max_bsize) {
    wctx->target_blob_size = max_bsize;
  }

  // set the min blob size floor at 2x the min_alloc_size, or else we
  // won't be able to allocate a smaller extent for the compressed
  // data.
  if (wctx->compress && wctx->target_blob_size < min_alloc_size * 2) {
    wctx->target_blob_size = min_alloc_size * 2;
  }
}

int FragmentationSimulator::_do_alloc_write(TransContext *txc,
                                            CollectionRef coll, ObjectRef &o,
                                            WriteContext *wctx) {
  if (wctx->writes.empty()) {
    return 0;
  }

  CompressorRef c;
  double crr = 0;
  if (wctx->compress) {
    c = select_option("compression_algorithm", compressor, [&]() {
      std::string val;
      if (coll->pool_opts.get(pool_opts_t::COMPRESSION_ALGORITHM, &val)) {
        CompressorRef cp = compressor;
        if (!cp || cp->get_type_name() != val) {
          cp = Compressor::create(cct, val);
          if (!cp) {
            if (_set_compression_alert(false, val.c_str())) {
              derr << __func__ << " unable to initialize " << val.c_str()
                   << " compressor" << dendl;
            }
          }
        }
        return std::optional<CompressorRef>(cp);
      }
      return std::optional<CompressorRef>();
    });

    crr = select_option(
        "compression_required_ratio",
        cct->_conf->bluestore_compression_required_ratio, [&]() {
          double val;
          if (coll->pool_opts.get(pool_opts_t::COMPRESSION_REQUIRED_RATIO,
                                  &val)) {
            return std::optional<double>(val);
          }
          return std::optional<double>();
        });
  }

  // checksum
  int64_t csum = csum_type.load();
  csum = select_option("csum_type", csum, [&]() {
    int64_t val;
    if (coll->pool_opts.get(pool_opts_t::CSUM_TYPE, &val)) {
      return std::optional<int64_t>(val);
    }
    return std::optional<int64_t>();
  });

  // compress (as needed) and calc needed space
  uint64_t need = 0;
  uint64_t data_size = 0;
  // 'need' is amount of space that must be provided by allocator.
  // 'data_size' is a size of data that will be transferred to disk.
  // Note that data_size is always <= need. This comes from:
  // - write to blob was unaligned, and there is free space
  // - data has been compressed
  //
  // We make one decision and apply it to all blobs.
  // All blobs will be deferred or none will.
  // We assume that allocator does its best to provide contiguous space,
  // and the condition is : (data_size < deferred).

  for (auto &wi : wctx->writes) {
    if (c && wi.blob_length > min_alloc_size) {

      // compress
      ceph_assert(wi.b_off == 0);
      ceph_assert(wi.blob_length == wi.bl.length());

      // FIXME: memory alignment here is bad
      bufferlist t;
      std::optional<int32_t> compressor_message;
      int r = c->compress(wi.bl, t, compressor_message);
      uint64_t want_len_raw = wi.blob_length * crr;
      uint64_t want_len = p2roundup(want_len_raw, min_alloc_size);
      bool rejected = false;
      uint64_t compressed_len = t.length();
      // do an approximate (fast) estimation for resulting blob size
      // that doesn't take header overhead  into account
      uint64_t result_len = p2roundup(compressed_len, min_alloc_size);
      if (r == 0 && result_len <= want_len && result_len < wi.blob_length) {
        bluestore_compression_header_t chdr;
        chdr.type = c->get_type();
        chdr.length = t.length();
        chdr.compressor_message = compressor_message;
        encode(chdr, wi.compressed_bl);
        wi.compressed_bl.claim_append(t);

        compressed_len = wi.compressed_bl.length();
        result_len = p2roundup(compressed_len, min_alloc_size);
        if (result_len <= want_len && result_len < wi.blob_length) {
          // Cool. We compressed at least as much as we were hoping to.
          // pad out to min_alloc_size
          wi.compressed_bl.append_zero(result_len - compressed_len);
          wi.compressed_len = compressed_len;
          wi.compressed = true;
          need += result_len;
          data_size += result_len;
        } else {
          rejected = true;
        }
      } else if (r != 0) {
        need += wi.blob_length;
        data_size += wi.bl.length();
      } else {
        rejected = true;
      }

      if (rejected) {
        need += wi.blob_length;
        data_size += wi.bl.length();
      }
    } else {
      need += wi.blob_length;
      data_size += wi.bl.length();
    }
  }

  PExtentVector prealloc;
  prealloc.reserve(2 * wctx->writes.size());
  int64_t prealloc_left = 0;
  prealloc_left = alloc->allocate(need, min_alloc_size, need, 0, &prealloc);
  if (prealloc_left < 0 || prealloc_left < (int64_t)need) {
    derr << __func__ << " failed to allocate 0x" << std::hex << need
         << " allocated 0x " << (prealloc_left < 0 ? 0 : prealloc_left)
         << " min_alloc_size 0x" << min_alloc_size << " available 0x "
         << alloc->get_free() << std::dec << dendl;
    if (prealloc.size()) {
      alloc->release(prealloc);
    }
    return -ENOSPC;
  }

  dout(20) << __func__ << std::hex << " need=0x" << need << " data=0x"
           << data_size << " prealloc " << prealloc << dendl;
  auto prealloc_pos = prealloc.begin();
  ceph_assert(prealloc_pos != prealloc.end());

  for (auto &wi : wctx->writes) {
    uint64_t final_length = wi.blob_length;
    PExtentVector extents;
    int64_t left = final_length;

    while (left > 0) {
      ceph_assert(prealloc_left > 0);
      if (prealloc_pos->length <= left) {
        prealloc_left -= prealloc_pos->length;
        left -= prealloc_pos->length;
        extents.push_back(*prealloc_pos);
        ++prealloc_pos;
      } else {
        extents.emplace_back(prealloc_pos->offset, left);
        prealloc_pos->offset += left;
        prealloc_pos->length -= left;
        prealloc_left -= left;
        left = 0;
        break;
      }
    }

    for (auto &p : extents) {
      txc->allocated.insert(p.offset, p.length);
      o->extents.push_back(p);
    }
  }

  ceph_assert(prealloc_pos == prealloc.end());
  ceph_assert(prealloc_left == 0);
  return 0;
}

void FragmentationSimulator::_do_truncate(TransContext *txc, CollectionRef &c,
                                          ObjectRef &o, uint64_t offset) {
  if (offset == o->size)
    return;
  o->size = offset;

  WriteContext wctx;
  o->punch_hole(offset, o->size - offset, &wctx.old_extents);
  _wctx_finish(txc, c, o, &wctx);
}

int FragmentationSimulator::_rename(TransContext *txc, CollectionRef &c,
                                    ObjectRef &oldo, ObjectRef &newo,
                                    const ghobject_t &new_oid) {
  int r;
  ghobject_t old_oid = oldo->id;
  if (newo) {
    if (newo->exists) {
      r = -EEXIST;
      goto out;
    }
  }

  newo = oldo;

  c->rename_obj(oldo, old_oid, new_oid);
  r = 0;

out:
  return r;
}

int FragmentationSimulator::_set_alloc_hint(TransContext *txc, CollectionRef &c,
                                            ObjectRef &o,
                                            uint64_t expected_object_size,
                                            uint64_t expected_write_size,
                                            uint32_t flags) {
  int r = 0;
  o->expected_object_size = expected_object_size;
  o->expected_write_size = expected_write_size;
  o->alloc_hint_flags = flags;
  return r;
}

int FragmentationSimulator::_clone(TransContext *txc, CollectionRef &c,
                                   ObjectRef &oldo, ObjectRef &newo) {
  int r = 0;
  if (oldo->id.hobj.get_hash() != newo->id.hobj.get_hash()) {
    return -EINVAL;
  }

  _assign_nid(txc, newo);

  _do_truncate(txc, c, newo, 0);
  if (cct->_conf->bluestore_clone_cow) {
    _do_clone_range(txc, c, oldo, newo, 0, oldo->size, 0);
  } else {
    bufferlist bl;
    bl.clear();
    r = _do_read(c.get(), oldo, 0, oldo->size, bl, 0);
    if (r < 0)
      goto out;
    r = _do_write(txc, c, newo, 0, oldo->size, bl, 0);
    if (r < 0)
      goto out;
  }

  r = 0;

out:
  return r;
}

int FragmentationSimulator::_clone_range(TransContext *txc, CollectionRef &c,
                                         ObjectRef &oldo, ObjectRef &newo,
                                         uint64_t srcoff, uint64_t length,
                                         uint64_t dstoff) {

  int r = 0;

  if (srcoff + length >= OBJECT_MAX_SIZE ||
      dstoff + length >= OBJECT_MAX_SIZE) {
    r = -E2BIG;
    goto out;
  }
  if (srcoff + length > oldo->size) {
    r = -EINVAL;
    goto out;
  }

  _assign_nid(txc, newo);

  if (length > 0) {
    if (cct->_conf->bluestore_clone_cow) {
      _do_zero(txc, c, newo, dstoff, length);
      _do_clone_range(txc, c, oldo, newo, srcoff, length, dstoff);
    } else {
      bufferlist bl;
      r = _do_read(c.get(), oldo, srcoff, length, bl, 0);
      if (r < 0)
        goto out;
      r = _do_write(txc, c, newo, dstoff, bl.length(), bl, 0);
      if (r < 0)
        goto out;
    }
  }

  r = 0;

out:
  return r;
}

// ------- Collections -------

ObjectStore::CollectionHandle
FragmentationSimulator::open_collection(const coll_t &cid) {
  std::shared_lock l(coll_lock);
  ceph::unordered_map<coll_t, CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return CollectionRef();
  return cp->second;
}

ObjectStore::CollectionHandle
FragmentationSimulator::create_new_collection(const coll_t &cid) {
  std::unique_lock l{coll_lock};
  auto c = ceph::make_ref<Collection>(this, cid);
  new_coll_map[cid] = c;
  _osr_attach(c.get());
  return c;
}

void FragmentationSimulator::set_collection_commit_queue(
    const coll_t &cid, ContextQueue *commit_queue) {
  if (commit_queue) {
    std::shared_lock l(coll_lock);
    if (coll_map.count(cid)) {
      coll_map[cid]->commit_queue = commit_queue;
    } else if (new_coll_map.count(cid)) {
      new_coll_map[cid]->commit_queue = commit_queue;
    }
  }
}

bool FragmentationSimulator::exists(CollectionHandle &c_,
                                    const ghobject_t &oid) {

  Collection *c = static_cast<Collection *>(c_.get());
  if (!c->exists)
    return false;

  bool r = true;

  {
    std::shared_lock l(c->lock);
    ObjectRef o = c->get_obj(oid, false);
    if (!o || !o->exists)
      r = false;
  }

  return r;
}

int FragmentationSimulator::set_collection_opts(CollectionHandle &ch,
                                                const pool_opts_t &opts) {
  Collection *c = static_cast<Collection *>(ch.get());
  if (!c->exists)
    return -ENOENT;
  std::unique_lock l{c->lock};
  c->pool_opts = opts;
  return 0;
}

int FragmentationSimulator::list_collections(std::vector<coll_t> &ls) {
  std::shared_lock l(coll_lock);
  ls.reserve(coll_map.size());
  for (ceph::unordered_map<coll_t, CollectionRef>::iterator p =
           coll_map.begin();
       p != coll_map.end(); ++p)
    ls.push_back(p->first);
  return 0;
}

bool FragmentationSimulator::collection_exists(const coll_t &c) {
  std::shared_lock l(coll_lock);
  return coll_map.count(c);
}

int FragmentationSimulator::collection_empty(CollectionHandle &ch,
                                             bool *empty) {
  std::vector<ghobject_t> ls;
  ghobject_t next;
  int r =
      collection_list(ch, ghobject_t(), ghobject_t::get_max(), 1, &ls, &next);
  if (r < 0) {
    return r;
  }

  *empty = ls.empty();
  return 0;
}

int FragmentationSimulator::collection_bits(CollectionHandle &ch) {
  Collection *c = static_cast<Collection *>(ch.get());
  std::shared_lock l(c->lock);
  return c->cnode.bits;
}

int FragmentationSimulator::collection_list(CollectionHandle &c_,
                                            const ghobject_t &start,
                                            const ghobject_t &end, int max,
                                            std::vector<ghobject_t> *ls,
                                            ghobject_t *pnext) {
  Collection *c = static_cast<Collection *>(c_.get());
  c->flush();
  int r;
  {
    std::shared_lock l(c->lock);
    r = _collection_list(c, start, end, max, false, ls, pnext);
  }

  return r;
}

int FragmentationSimulator::_collection_list(
    Collection *c, const ghobject_t &start, const ghobject_t &end, int max,
    bool legacy, std::vector<ghobject_t> *ls, ghobject_t *next) {

  if (!c->exists)
    return -ENOENT;

  if (start.is_max() || start.hobj.is_max()) {
    *next = ghobject_t::get_max();
    return 0;
  }

  auto it = c->objects.find(start);
  if (it == c->objects.end()) {
    *next = ghobject_t::get_max();
    return -ENOENT;
  }

  do {
    ls->push_back((it++)->first);
    if (ls->size() >= max) {
      *next = it->first;
      return 0;
    }
  } while (it != c->objects.end() && it->first != end);

  if (it != c->objects.end())
    *next = it->first;
  else
    *next = ghobject_t::get_max();

  return 0;
}

int FragmentationSimulator::_remove_collection(TransContext *txc,
                                               const coll_t &cid,
                                               CollectionRef *c) {
  int r;

  (*c)->flush_all_but_last();
  {
    std::unique_lock l(coll_lock);
    if (!*c) {
      r = -ENOENT;
      goto out;
    }

    ceph_assert((*c)->exists);
    for (auto o : (*c)->objects) {
      if (o.second->exists) {
        r = -ENOTEMPTY;
        goto out;
      }
    }

    _do_remove_collection(txc, c);
    r = 0;
  }

out:
  return r;
}

void FragmentationSimulator::_do_remove_collection(TransContext *txc,
                                                   CollectionRef *c) {
  coll_map.erase((*c)->cid);
  (*c)->exists = false;
  _osr_register_zombie((*c)->osr.get());
  c->reset();
}

int FragmentationSimulator::_create_collection(TransContext *txc,
                                               const coll_t &cid, unsigned bits,
                                               CollectionRef *c) {
  int r;
  bufferlist bl;

  {
    std::unique_lock l(coll_lock);
    if (*c) {
      r = -EEXIST;
      goto out;
    }
    auto p = new_coll_map.find(cid);
    ceph_assert(p != new_coll_map.end());
    *c = p->second;
    (*c)->cnode.bits = bits;
    coll_map[cid] = *c;
    new_coll_map.erase(p);
  }

  encode((*c)->cnode, bl);
  r = 0;

out:
  return r;
}
// -------- OpSequencer --------

void FragmentationSimulator::_osr_attach(Collection *c) {
  // note: caller has coll_lock
  auto q = coll_map.find(c->cid);
  if (q != coll_map.end()) {
    c->osr = q->second->osr;
  } else {
    std::lock_guard l(zombie_osr_lock);
    auto p = zombie_osr_set.find(c->cid);
    if (p == zombie_osr_set.end()) {
      c->osr = ceph::make_ref<OpSequencer>(this, next_sequencer_id++, c->cid);
    } else {
      c->osr = p->second;
      zombie_osr_set.erase(p);
      c->osr->zombie = false;
    }
  }
}

void FragmentationSimulator::_osr_register_zombie(OpSequencer *osr) {
  std::lock_guard l(zombie_osr_lock);
  osr->zombie = true;
  auto i = zombie_osr_set.emplace(osr->cid, osr);
  ceph_assert(i.second || i.first->second == osr);
}
void FragmentationSimulator::_osr_drain(OpSequencer *osr) { osr->drain(); }
void FragmentationSimulator::_osr_drain_preceding(TransContext *txc) {
  OpSequencer *osr = txc->osr.get();
  osr->drain_preceding(txc);
}

// -------- Mount --------

int FragmentationSimulator::mount() {
  finisher.start();
  return 0;
}

int FragmentationSimulator::umount() {
  finisher.wait_for_empty();
  finisher.stop();
  return 0;
}
