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
class ObjectStoreImitator : public ObjectStore {
private:
  class Collection;
  typedef boost::intrusive_ptr<Collection> CollectionRef;

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
                    PExtentVector &old_extents) {
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
      old_extents = to_be_punched;
    }
  };
  typedef boost::intrusive_ptr<Object> ObjectRef;

  struct Collection : public CollectionImpl {
    ObjectStoreImitator *sim;
    bluestore_cnode_t cnode;
    std::map<ghobject_t, ObjectRef> objects;

    ceph::shared_mutex lock = ceph::make_shared_mutex(
        "FragmentationSimulator::Collection::lock", true, false);

    // Lock for 'objects'
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

      o->id = new_oid;
    }

    Collection(ObjectStoreImitator *sim, coll_t c);
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
  void _txc_add_transaction(Transaction *t);
  void _release_alloc(const PExtentVector &release_vec);

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

  void _do_write_data(CollectionRef &c, ObjectRef &o, uint64_t offset,
                      uint64_t length, bufferlist &bl,
                      std::vector<bufferlist> &bls);
  void _do_write_big(CollectionRef &c, ObjectRef &o, uint64_t offset,
                     uint64_t length, bufferlist::iterator &blp);
  void _do_write_small(CollectionRef &c, ObjectRef &o, uint64_t offset,
                       uint64_t length, bufferlist::iterator &blp);

  int _do_alloc_write(CollectionRef c, ObjectRef &o,
                      std::vector<bufferlist> &bls);

  void _do_truncate(CollectionRef &c, ObjectRef &o, uint64_t offset);
  int _do_zero(CollectionRef &c, ObjectRef &o, uint64_t offset, size_t length);
  int _do_clone_range(CollectionRef &c, ObjectRef &oldo, ObjectRef &newo,
                      uint64_t srcoff, uint64_t length, uint64_t dstoff);
  int _do_read(Collection *c, ObjectRef &o, uint64_t offset, size_t len,
               ceph::buffer::list &bl, uint32_t op_flags = 0,
               uint64_t retry_count = 0);

  // Members

  boost::scoped_ptr<Allocator> alloc;
  uint32_t next_sequencer_id = 0;

  std::atomic<uint64_t> nid_last = {0};

  uint64_t min_alloc_size; ///< minimum allocation unit (power of 2)
  static_assert(std::numeric_limits<uint8_t>::max() >
                    std::numeric_limits<decltype(min_alloc_size)>::digits,
                "not enough bits for min_alloc_size");

  ///< rwlock to protect coll_map/new_coll_map
  ceph::shared_mutex coll_lock =
      ceph::make_shared_mutex("FragmentationSimulator::coll_lock");
  std::unordered_map<coll_t, CollectionRef> coll_map;
  std::unordered_map<coll_t, CollectionRef> new_coll_map;

public:
  ObjectStoreImitator(CephContext *cct, const std::string &path_)
      : ObjectStore(cct, path_), alloc(nullptr) {}

  ~ObjectStoreImitator() = default;

  void init_alloc(const std::string &alloc_type, int64_t size,
                  uint64_t min_alloc_size);

  // Overrides

  int queue_transactions(CollectionHandle &ch, std::vector<Transaction> &tls,
                         TrackedOpRef op = TrackedOpRef(),
                         ThreadPool::TPHandle *handle = NULL) override;

  std::string get_type() override { return "FragmentationSimulator"; }

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

void ObjectStoreImitator::init_alloc(const std::string &alloc_type,
                                     int64_t size, uint64_t min_alloc_size) {
  alloc.reset(Allocator::create(cct, alloc_type, size, min_alloc_size));
}

// ------- Collections -------

int ObjectStoreImitator::_merge_collection(CollectionRef *c, CollectionRef &d,
                                           unsigned bits) {
  std::unique_lock l((*c)->lock);
  std::unique_lock l2(d->lock);
  coll_t cid = (*c)->cid;

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
    _do_remove_collection(c);
  }

  return 0;
}

int ObjectStoreImitator::_split_collection(CollectionRef &c, CollectionRef &d,
                                           unsigned bits, int rem) {
  std::unique_lock l(c->lock);
  std::unique_lock l2(d->lock);

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

int ObjectStoreImitator::queue_transactions(CollectionHandle &ch,
                                            std::vector<Transaction> &tls,
                                            TrackedOpRef op,
                                            ThreadPool::TPHandle *handle) {
  FUNCTRACE(cct);

  for (std::vector<Transaction>::iterator p = tls.begin(); p != tls.end();
       ++p) {
    _txc_add_transaction(&(*p));
  }

  if (handle)
    handle->suspend_tp_timeout();

  if (handle)
    handle->reset_tp_timeout();

  return 0;
}

ObjectStoreImitator::CollectionRef
ObjectStoreImitator::_get_collection(const coll_t &cid) {
  std::shared_lock l(coll_lock);
  ceph::unordered_map<coll_t, CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return CollectionRef();
  return cp->second;
}

void ObjectStoreImitator::_txc_add_transaction(Transaction *t) {
  Transaction::iterator i = t->begin();

  std::vector<CollectionRef> cvec(i.colls.size());
  unsigned j = 0;
  for (auto p = i.colls.begin(); p != i.colls.end(); ++p, ++j) {
    cvec[j] = _get_collection(*p);
  }

  std::vector<ObjectRef> ovec(i.objects.size());

  while (i.have_op()) {
    Transaction::Op *op = i.decode_op();
    int r = 0;

    // no coll or obj
    if (op->op == Transaction::OP_NOP)
      continue;

    // collection operations
    CollectionRef &c = cvec[op->cid];

    switch (op->op) {
    case Transaction::OP_RMCOLL: {
      if (coll_map.find(c->cid) != coll_map.end())
        coll_map.erase(c->cid);
    } break;

    case Transaction::OP_MKCOLL: {
      ceph_assert(!c);
      const coll_t &cid = i.get_cid(op->cid);
      r = _create_collection(cid, op->split_bits, &c);
      if (!r)
        continue;

    } break;

    case Transaction::OP_SPLIT_COLLECTION:
      ceph_abort_msg("deprecated");
      break;

    case Transaction::OP_SPLIT_COLLECTION2: {
      uint32_t bits = op->split_bits;
      uint32_t rem = op->split_rem;
      r = _split_collection(c, cvec[op->dest_cid], bits, rem);
      if (!r)
        continue;
    } break;

    case Transaction::OP_MERGE_COLLECTION: {
      uint32_t bits = op->split_bits;
      r = _merge_collection(&c, cvec[op->dest_cid], bits);
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
    bool create =
        (op->op == Transaction::OP_TOUCH || op->op == Transaction::OP_CREATE ||
         op->op == Transaction::OP_WRITE || op->op == Transaction::OP_ZERO);

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
      _assign_nid(o);
      r = 0;
      break;

    case Transaction::OP_WRITE: {
      uint64_t off = op->off;
      uint64_t len = op->len;
      uint32_t fadvise_flags = i.get_fadvise_flags();
      bufferlist bl;
      i.decode_bl(bl);
      r = _write(c, o, off, len, bl, fadvise_flags);
    } break;

    case Transaction::OP_ZERO: {
      if (op->off + op->len > OBJECT_MAX_SIZE) {
        r = -E2BIG;
      } else {
        _assign_nid(o);
        r = _do_zero(c, o, op->off, op->len);
      }
    } break;

    case Transaction::OP_TRIMCACHE: {
      // deprecated, no-op
    } break;

    case Transaction::OP_TRUNCATE: {
      _do_truncate(c, o, op->off);
    } break;

    case Transaction::OP_REMOVE: {
      _do_truncate(c, o, 0);
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
      r = _clone(c, o, no);
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
      r = _clone_range(c, o, no, srcoff, len, dstoff);
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
        r = _rename(c, o, no, noid);
      } break;

      case Transaction::OP_OMAP_CLEAR:
      case Transaction::OP_OMAP_SETKEYS:
      case Transaction::OP_OMAP_RMKEYS:
      case Transaction::OP_OMAP_RMKEYRANGE:
      case Transaction::OP_OMAP_SETHEADER:
        break;

      case Transaction::OP_SETALLOCHINT: {
        r = _set_alloc_hint(c, o, op->expected_object_size,
                            op->expected_write_size, op->hint);
      } break;

      default:
        derr << __func__ << " bad op " << op->op << dendl;
        ceph_abort();
      }
    endop:
      return;
    }
  }
}

void ObjectStoreImitator::_release_alloc(const PExtentVector &release_vec) {
  alloc->release(release_vec);
}

// ------- Helpers -------

void ObjectStoreImitator::_assign_nid(ObjectRef &o) {
  if (o->nid) {
    ceph_assert(o->exists);
  }

  o->nid = ++nid_last;
  o->exists = true;
}

int ObjectStoreImitator::_do_zero(CollectionRef &c, ObjectRef &o,
                                  uint64_t offset, size_t length) {
  PExtentVector old_extents;
  o->punch_hole(offset, length, old_extents);
  _release_alloc(old_extents);
  return 0;
}

int ObjectStoreImitator::_do_read(Collection *c, ObjectRef &o, uint64_t offset,
                                  size_t len, ceph::buffer::list &bl,
                                  uint32_t op_flags, uint64_t retry_count) {
  auto data = std::string(len, 'a');
  bl.append(data);
  return bl.length();
}

int ObjectStoreImitator::_do_write(CollectionRef &c, ObjectRef &o,
                                   uint64_t offset, uint64_t length,
                                   bufferlist &bl, uint32_t fadvise_flags) {
  int r = 0;
  uint64_t end = length + offset;

  if (length == 0) {
    return 0;
  }

  std::vector<bufferlist> bls;
  _do_write_data(c, o, offset, length, bl, bls);
  r = _do_alloc_write(c, o, bls);
  if (r < 0) {
    derr << __func__ << " _do_alloc_write failed with " << cpp_strerror(r)
         << dendl;
    goto out;
  }

  if (end > o->size) {
    o->size = end;
  }

  r = 0;

out:
  return r;
}

void ObjectStoreImitator::_do_write_data(CollectionRef &c, ObjectRef &o,
                                         uint64_t offset, uint64_t length,
                                         bufferlist &bl,
                                         std::vector<bufferlist> &bls) {
  uint64_t end = offset + length;
  bufferlist::iterator p = bl.begin();

  if (offset / min_alloc_size == (end - 1) / min_alloc_size &&
      (length != min_alloc_size)) {
    // we fall within the same block
    _do_write_small(c, o, offset, length, p);
  } else {
    uint64_t head_offset, head_length;
    uint64_t middle_offset, middle_length;
    uint64_t tail_offset, tail_length;

    head_offset = offset;
    head_length = p2nphase(offset, min_alloc_size);

    tail_offset = p2align(end, min_alloc_size);
    tail_length = p2phase(end, min_alloc_size);

    middle_offset = head_offset + head_length;
    middle_length = length - head_length - tail_length;

    if (head_length) {
      _do_write_small(c, o, head_offset, head_length, p);
    }

    _do_write_big(c, o, middle_offset, middle_length, p);

    if (tail_length) {
      _do_write_small(c, o, tail_offset, tail_length, p);
    }
  }
}

int ObjectStoreImitator::_do_clone_range(CollectionRef &c, ObjectRef &oldo,
                                         ObjectRef &newo, uint64_t srcoff,
                                         uint64_t length, uint64_t dstoff) {
  return 0;
}

// ------- Operations -------

int ObjectStoreImitator::_write(CollectionRef &c, ObjectRef &o, uint64_t offset,
                                size_t length, bufferlist &bl,
                                uint32_t fadvise_flags) {
  int r = 0;
  if (offset + length >= OBJECT_MAX_SIZE) {
    r = -E2BIG;
  } else {
    _assign_nid(o);
    r = _do_write(c, o, offset, length, bl, fadvise_flags);
  }

  return r;
}

int ObjectStoreImitator::_do_alloc_write(CollectionRef coll, ObjectRef &o,
                                         bufferlist &bl) {

  // No compression for now
  uint64_t need = bl.length();

  PExtentVector prealloc;

  int64_t prealloc_left =
      alloc->allocate(need, min_alloc_size, need, 0, &prealloc);
  if (prealloc_left < 0 || prealloc_left < (int64_t)need) {
    if (prealloc.size()) {
      alloc->release(prealloc);
    }
    return -ENOSPC;
  }

  auto prealloc_pos = prealloc.begin();
  ceph_assert(prealloc_pos != prealloc.end());

  PExtentVector extents;
  int64_t left = need;

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
    o->extents.push_back(p);
  }

  if (prealloc_left > 0) {
    PExtentVector old_extents;
    while (prealloc_pos != prealloc.end()) {
      old_extents.push_back(*prealloc_pos);
      prealloc_left -= prealloc_pos->length;
      ++prealloc_pos;
    }

    _release_alloc(old_extents);
  }

  ceph_assert(prealloc_pos == prealloc.end());
  ceph_assert(prealloc_left == 0);
  return 0;
}

void ObjectStoreImitator::_do_truncate(CollectionRef &c, ObjectRef &o,
                                       uint64_t offset) {
  if (offset == o->size)
    return;
  o->size = offset;

  PExtentVector old_extents;
  o->punch_hole(offset, o->size - offset, old_extents);
  _release_alloc(old_extents);
}

int ObjectStoreImitator::_rename(CollectionRef &c, ObjectRef &oldo,
                                 ObjectRef &newo, const ghobject_t &new_oid) {
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

int ObjectStoreImitator::_set_alloc_hint(CollectionRef &c, ObjectRef &o,
                                         uint64_t expected_object_size,
                                         uint64_t expected_write_size,
                                         uint32_t flags) {
  o->expected_object_size = expected_object_size;
  o->expected_write_size = expected_write_size;
  o->alloc_hint_flags = flags;
  return 0;
}

int ObjectStoreImitator::_clone(CollectionRef &c, ObjectRef &oldo,
                                ObjectRef &newo) {
  int r = 0;
  if (oldo->id.hobj.get_hash() != newo->id.hobj.get_hash()) {
    return -EINVAL;
  }

  _assign_nid(newo);

  _do_truncate(c, newo, 0);
  if (cct->_conf->bluestore_clone_cow) {
    _do_clone_range(c, oldo, newo, 0, oldo->size, 0);
  } else {
    bufferlist bl;
    bl.clear();
    r = _do_read(c.get(), oldo, 0, oldo->size, bl, 0);
    if (r < 0)
      goto out;
    r = _do_write(c, newo, 0, oldo->size, bl, 0);
    if (r < 0)
      goto out;
  }

  r = 0;

out:
  return r;
}

int ObjectStoreImitator::_clone_range(CollectionRef &c, ObjectRef &oldo,
                                      ObjectRef &newo, uint64_t srcoff,
                                      uint64_t length, uint64_t dstoff) {

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

  _assign_nid(newo);

  if (length > 0) {
    if (cct->_conf->bluestore_clone_cow) {
      _do_zero(c, newo, dstoff, length);
      _do_clone_range(c, oldo, newo, srcoff, length, dstoff);
    } else {
      bufferlist bl;
      bl.clear();

      r = _do_read(c.get(), oldo, srcoff, length, bl, 0);
      if (r < 0)
        goto out;
      r = _do_write(c, newo, dstoff, bl.length(), bl, 0);
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
ObjectStoreImitator::open_collection(const coll_t &cid) {
  std::shared_lock l(coll_lock);
  ceph::unordered_map<coll_t, CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return CollectionRef();
  return cp->second;
}

ObjectStore::CollectionHandle
ObjectStoreImitator::create_new_collection(const coll_t &cid) {
  std::unique_lock l{coll_lock};
  auto c = ceph::make_ref<Collection>(this, cid);
  new_coll_map[cid] = c;
  return c;
}

void ObjectStoreImitator::set_collection_commit_queue(
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

bool ObjectStoreImitator::exists(CollectionHandle &c_, const ghobject_t &oid) {

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

int ObjectStoreImitator::set_collection_opts(CollectionHandle &ch,
                                             const pool_opts_t &opts) {
  Collection *c = static_cast<Collection *>(ch.get());
  if (!c->exists)
    return -ENOENT;
  std::unique_lock l{c->lock};
  c->pool_opts = opts;
  return 0;
}

int ObjectStoreImitator::list_collections(std::vector<coll_t> &ls) {
  std::shared_lock l(coll_lock);
  ls.reserve(coll_map.size());
  for (ceph::unordered_map<coll_t, CollectionRef>::iterator p =
           coll_map.begin();
       p != coll_map.end(); ++p)
    ls.push_back(p->first);
  return 0;
}

bool ObjectStoreImitator::collection_exists(const coll_t &c) {
  std::shared_lock l(coll_lock);
  return coll_map.count(c);
}

int ObjectStoreImitator::collection_empty(CollectionHandle &ch, bool *empty) {
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

int ObjectStoreImitator::collection_bits(CollectionHandle &ch) {
  Collection *c = static_cast<Collection *>(ch.get());
  std::shared_lock l(c->lock);
  return c->cnode.bits;
}

int ObjectStoreImitator::collection_list(CollectionHandle &c_,
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

int ObjectStoreImitator::_collection_list(
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

int ObjectStoreImitator::_remove_collection(const coll_t &cid,
                                            CollectionRef *c) {
  int r;

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

    _do_remove_collection(c);
    r = 0;
  }

out:
  return r;
}

void ObjectStoreImitator::_do_remove_collection(CollectionRef *c) {
  coll_map.erase((*c)->cid);
  (*c)->exists = false;
  c->reset();
}

int ObjectStoreImitator::_create_collection(const coll_t &cid, unsigned bits,
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
