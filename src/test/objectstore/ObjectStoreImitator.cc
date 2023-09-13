// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Fragmentation Simulator
 * Author: Tri Dao, daominhtri0503@gmail.com
 */
#include "test/objectstore/ObjectStoreImitator.h"
#include "common/Clock.h"
#include "common/Finisher.h"
#include "common/errno.h"
#include "include/ceph_assert.h"
#include "include/intarith.h"
#include "os/bluestore/bluestore_types.h"
#include <algorithm>
#include <cmath>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_test
#define OBJECT_MAX_SIZE 0xffffffff // 32 bits

// ---------- Allocator ----------

void ObjectStoreImitator::release_alloc(PExtentVector &old_extents) {
  utime_t start = ceph_clock_now();
  alloc->release(old_extents);
  alloc_ops++;
  alloc_time += static_cast<double>(ceph_clock_now() - start);
}

int64_t ObjectStoreImitator::allocate_alloc(uint64_t want_size,
                                            uint64_t block_size,
                                            uint64_t max_alloc_size,
                                            int64_t hint,
                                            PExtentVector *extents) {
  utime_t start = ceph_clock_now();
  int64_t ret =
      alloc->allocate(want_size, block_size, max_alloc_size, hint, extents);
  alloc_time += static_cast<double>(ceph_clock_now() - start);
  return ret;
}

// ---------- Object -----------

void ObjectStoreImitator::Object::punch_hole(uint64_t offset, uint64_t length,
                                             PExtentVector &old_extents) {
  if (extent_map.empty())
    return;

  PExtentVector to_be_punched;
  std::vector<uint64_t> deleted_keys;
  uint64_t end = offset + length;

  uint64_t re_add_key{0};
  bluestore_pextent_t re_add;

  dout(20) << "current extents:" << dendl;
  for (auto &[l_off, e] : extent_map) {
    dout(20) << "l_off " << l_off << ", off " << e.offset << ", len "
             << e.length << dendl;
  }

  dout(20) << "wants to punch: off " << offset << ", len " << length << dendl;

  auto it = extent_map.lower_bound(offset);
  if ((it == extent_map.end() || it->first > offset) &&
      it != extent_map.begin()) {
    it = std::prev(it);

    // diff between where we need to punch and current position
    auto diff = offset - it->first;
    dout(20) << "diff " << diff << " , p_off " << it->first << dendl;

    // offset will be inside this extent
    // otherwise skip over this extent and assume 'offset' has been passed
    if (diff < it->second.length) {
      // the hole is bigger than the remaining of the extent
      if (end > it->first + it->second.length) {
        to_be_punched.emplace_back(it->second.offset + diff,
                                   it->second.length - diff);
      } else { // else the hole is entirely in this extent
        to_be_punched.emplace_back(it->second.offset + diff, length);

        re_add_key = end;
        re_add.offset = it->second.offset + diff + length;
        re_add.length = it->second.length - diff - length;

        dout(20) << "re_add: off " << re_add.offset << ", len " << re_add.length
                 << dendl;
      }

      // Modify the remaining extent's length
      it->second.length = diff;
    }

    it++;
  }

  // this loop is only valid when 'it' is in the hole
  while (it != extent_map.end() && it->first < end) {
    if (it->first + it->second.length > end) { // last extent to punched
      uint64_t remaining = it->first + it->second.length - end;
      uint64_t punched = it->second.length - remaining;

      to_be_punched.emplace_back(it->second.offset, punched);
      deleted_keys.push_back(it->first);

      re_add.offset = it->second.offset + punched;
      re_add.length = remaining;
      re_add_key = it->first + punched;

      it++;
      break;
    }

    deleted_keys.push_back(it->first);
    to_be_punched.push_back(it->second);
    it++;
  }

  for (auto k : deleted_keys) {
    extent_map.erase(k);
  }

  if (re_add.length > 0) {
    extent_map[re_add_key] = re_add;
  }

  old_extents = to_be_punched;
  dout(20) << "to be deleted:" << dendl;
  for (auto e : to_be_punched) {
    dout(20) << "off " << e.offset << ", len " << e.length << dendl;
  }
}

void ObjectStoreImitator::Object::append(PExtentVector &ext, uint64_t offset) {
  for (auto &e : ext) {
    ceph_assert(e.length > 0);
    dout(20) << "adding off " << offset << ", len " << e.length << dendl;
    extent_map[offset] = e;
    offset += e.length;
  }
}

void ObjectStoreImitator::Object::verify_extents() {
  dout(20) << "Verifying extents:" << dendl;
  uint64_t prev{0};
  for (auto &[l_off, ext] : extent_map) {
    dout(20) << "logical offset: " << l_off << ", extent offset: " << ext.offset
             << ", extent length: " << ext.length << dendl;

    ceph_assert(ext.is_valid());
    ceph_assert(ext.length > 0);

    // Making sure that extents don't overlap
    ceph_assert(prev <= l_off);
    prev = l_off + ext.length;
  }
}

uint64_t ObjectStoreImitator::Object::ext_length() {
  uint64_t ret{0};
  for (auto &[_, ext] : extent_map) {
    ret += ext.length;
  }
  return ret;
}

// ---------- ObjectStoreImitator ----------

void ObjectStoreImitator::init_alloc(const std::string &alloc_type,
                                     uint64_t size) {
  alloc.reset(Allocator::create(cct, alloc_type, size, min_alloc_size));
  alloc->init_add_free(0, size);
  ceph_assert(alloc->get_free() == size);
}

void ObjectStoreImitator::print_status() {
  dout(0) << std::hex
          << "Fragmentation score: " << alloc->get_fragmentation_score()
          << " , fragmentation: " << alloc->get_fragmentation()
          << ", allocator type: " << alloc->get_type() << ", capacity 0x"
          << alloc->get_capacity() << ", block size 0x"
          << alloc->get_block_size() << ", free 0x" << alloc->get_free()
          << std::dec << dendl;
}

void ObjectStoreImitator::verify_objects(CollectionHandle &ch) {
  Collection *c = static_cast<Collection *>(ch.get());
  for (auto &[_, obj] : c->objects) {
    obj->verify_extents();
  }
}

void ObjectStoreImitator::print_per_object_fragmentation() {
  for (auto &[_, coll_ref] : coll_map) {
    double coll_total{0};
    for (auto &[id, obj] : coll_ref->objects) {
      double frag_score{1};
      unsigned i{2};
      uint64_t ext_size = 0;

      PExtentVector extents;
      for (auto &[_, ext] : obj->extent_map) {
        extents.push_back(ext);
        ext_size += ext.length;
      }

      std::sort(extents.begin(), extents.end(),
                [](bluestore_pextent_t &a, bluestore_pextent_t &b) {
                  return a.length > b.length;
                });

      for (auto &ext : extents) {
        double ext_frag =
            std::pow(((double)ext.length / (double)ext_size), (double)i++);
        frag_score -= ext_frag;
      }

      coll_total += frag_score;
      dout(5) << "Object: " << id.hobj.oid.name
              << ", hash: " << id.hobj.get_hash()
              << " fragmentation score: " << frag_score << dendl;
    }
    double avg = coll_total / coll_ref->objects.size();
    dout(0) << "Collection average obj fragmentation: " << avg
            << ", coll: " << coll_ref->get_cid().to_str() << dendl;
  }
}

void ObjectStoreImitator::print_per_access_fragmentation() {
  for (auto &[_, coll_ref] : coll_map) {
    double coll_blks_read{0}, coll_jmps{0};
    for (auto &[id, read_ops] : coll_ref->read_ops) {
      unsigned blks{0}, jmps{0};
      for (auto &op : read_ops) {
        blks += op.blks;
        jmps += op.jmps;
      }

      double avg_blks_read = (double)blks / read_ops.size();
      coll_blks_read += avg_blks_read;

      double avg_jmps = (double)jmps / read_ops.size();
      coll_jmps += avg_jmps;

      double avg_jmps_per_blk = (double)jmps / (double)blks;

      dout(5) << "Object: " << id.hobj.oid.name
              << ", average blks read: " << avg_blks_read
              << ", average jumps: " << avg_jmps
              << ", average jumps per block: " << avg_jmps_per_blk << dendl;
    }

    double coll_avg_blks_read = coll_blks_read / coll_ref->objects.size();
    double coll_avg_jumps = coll_jmps / coll_ref->objects.size();
    double coll_avg_jmps_per_blk = coll_avg_jumps / coll_avg_blks_read;

    dout(0) << "Collection average total blks: " << coll_avg_blks_read
            << ", collection average jumps: " << coll_avg_jumps
            << ", collection average jumps per block: " << coll_avg_jmps_per_blk
            << ", coll: " << coll_ref->get_cid().to_str() << dendl;
  }
}

void ObjectStoreImitator::print_allocator_profile() {
  double avg = alloc_time / alloc_ops;
  dout(0) << "Total alloc ops latency: " << alloc_time
          << ", total ops: " << alloc_ops
          << ", average alloc op latency: " << avg << dendl;
}

// ------- Transactions -------

int ObjectStoreImitator::queue_transactions(CollectionHandle &ch,
                                            std::vector<Transaction> &tls,
                                            TrackedOpRef op,
                                            ThreadPool::TPHandle *handle) {
  std::list<Context *> on_applied, on_commit, on_applied_sync;
  ObjectStore::Transaction::collect_contexts(tls, &on_applied, &on_commit,
                                             &on_applied_sync);
  Collection *c = static_cast<Collection *>(ch.get());

  for (std::vector<Transaction>::iterator p = tls.begin(); p != tls.end();
       ++p) {
    _add_transaction(&(*p));
  }

  if (handle)
    handle->reset_tp_timeout();

  // Immediately complete contexts
  for (auto c : on_applied_sync) {
    c->complete(0);
  }

  if (!on_applied.empty()) {
    if (c->commit_queue) {
      c->commit_queue->queue(on_applied);
    } else {
      for (auto c : on_applied) {
        c->complete(0);
      }
    }
  }

  if (!on_commit.empty()) {
    for (auto c : on_commit) {
      c->complete(0);
    }
  }

  verify_objects(ch);
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

void ObjectStoreImitator::_add_transaction(Transaction *t) {
  Transaction::iterator i = t->begin();

  std::vector<CollectionRef> cvec(i.colls.size());
  unsigned j = 0;
  for (auto p = i.colls.begin(); p != i.colls.end(); ++p, ++j) {
    cvec[j] = _get_collection(*p);
  }

  std::vector<ObjectRef> ovec(i.objects.size());
  uint64_t prev_pool_id = META_POOL_ID;

  for (int pos = 0; i.have_op(); ++pos) {
    Transaction::Op *op = i.decode_op();
    int r = 0;

    // no coll or obj
    if (op->op == Transaction::OP_NOP)
      continue;

    // collection operations
    CollectionRef &c = cvec[op->cid];

    // validate all collections are in the same pool
    spg_t pgid;
    if (!!c && c->cid.is_pg(&pgid)) {
      ceph_assert(prev_pool_id == pgid.pool() || prev_pool_id == META_POOL_ID);
      prev_pool_id = pgid.pool();
    }

    switch (op->op) {
    case Transaction::OP_RMCOLL: {
      const coll_t &cid = i.get_cid(op->cid);
      r = _remove_collection(cid, &c);
      if (!r)
        continue;
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
    case Transaction::OP_TOUCH: {
      _assign_nid(o);
      r = 0;
    } break;

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

    case Transaction::OP_COLL_ADD:
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
    if (r < 0) {
      derr << __func__ << " error " << cpp_strerror(r)
           << " not handled on operation " << op->op << " (op " << pos
           << ", counting from 0)" << dendl;
      ceph_abort_msg("unexpected error");
    }
  }
}

int ObjectStoreImitator::read(CollectionHandle &c_, const ghobject_t &oid,
                              uint64_t offset, size_t length, bufferlist &bl,
                              uint32_t op_flags) {

  Collection *c = static_cast<Collection *>(c_.get());
  if (!c->exists)
    return -ENOENT;

  bl.clear();
  int r;
  {
    std::shared_lock l(c->lock);
    ObjectRef o = c->get_obj(oid, false);
    if (!o || !o->exists) {
      r = -ENOENT;
      goto out;
    }

    if (offset == length && offset == 0)
      length = o->size;

    r = _do_read(c, o, offset, length, bl, op_flags);
  }

out:
  return r;
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
  release_alloc(old_extents);
  return 0;
}

int ObjectStoreImitator::_do_read(Collection *c, ObjectRef &o, uint64_t offset,
                                  size_t length, ceph::buffer::list &bl,
                                  uint32_t op_flags, uint64_t retry_count) {
  auto data = std::string(length, 'a');
  bl.append(data);

  // Keeping track of read ops to evaluate per-access fragmentation
  ReadOp op(offset, length);
  bluestore_pextent_t last_ext;
  uint64_t end = length + offset;

  auto it = o->extent_map.lower_bound(offset);
  if ((it == o->extent_map.end() || it->first > offset) &&
      it != o->extent_map.begin()) {
    it = std::prev(it);

    auto diff = offset - it->first;
    if (diff < it->second.length) {
      // end not in this extent
      if (end > it->first + it->second.length) {
        op.blks += div_round_up(it->second.length - diff, min_alloc_size);
      } else { // end is within this extent so we take up the entire length
        op.blks += div_round_up(length, min_alloc_size);
      }

      last_ext = it->second;
      it++;
    }
  }

  while (it != o->extent_map.end() && it->first < end) {
    auto extent = it->second;
    if (last_ext.length > 0 &&
        last_ext.offset + last_ext.length != extent.offset) {
      op.jmps++;
    }

    if (extent.length > length) {
      op.blks += div_round_up(length, min_alloc_size);
      break;
    }

    op.blks += div_round_up(extent.length, min_alloc_size);
    length -= extent.length;
    it++;
  }

  c->read_ops[o->oid].push_back(op);
  dout(20) << "blks: " << op.blks << ", jmps: " << op.jmps
           << ", offset: " << op.offset << ", length: " << op.length << dendl;

  return bl.length();
}

int ObjectStoreImitator::_do_write(CollectionRef &c, ObjectRef &o,
                                   uint64_t offset, uint64_t length,
                                   bufferlist &bl, uint32_t fadvise_flags) {
  if (length == 0) {
    return 0;
  }
  ceph_assert(length == bl.length());

  if (offset + length > o->size) {
    o->size = offset + length;
  }

  if (length < min_alloc_size) {
    return 0;
  }

  // roundup offset, consider the beginning as deffered
  offset = p2roundup(offset, min_alloc_size);
  // align length, consider the end as deffered
  length = p2align(length, min_alloc_size);

  PExtentVector punched;
  o->punch_hole(offset, length, punched);
  release_alloc(punched);

  // all writes will trigger an allocation
  int r = _do_alloc_write(c, o, bl, offset, length);
  if (r < 0) {
    derr << __func__ << " _do_alloc_write failed with " << cpp_strerror(r)
         << dendl;
    return r;
  }

  return 0;
}

int ObjectStoreImitator::_do_clone_range(CollectionRef &c, ObjectRef &oldo,
                                         ObjectRef &newo, uint64_t srcoff,
                                         uint64_t length, uint64_t dstoff) {
  if (dstoff + length > newo->size)
    newo->size = dstoff + length;
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
                                         bufferlist &bl, uint64_t offset,
                                         uint64_t length) {

  // No compression for now
  uint64_t need = length;
  PExtentVector prealloc;

  int64_t prealloc_left =
      allocate_alloc(need, min_alloc_size, need, 0, &prealloc);
  if (prealloc_left < 0 || prealloc_left < (int64_t)need) {
    derr << __func__ << " failed to allocate 0x" << std::hex << need
         << " allocated 0x" << (prealloc_left < 0 ? 0 : prealloc_left)
         << " min_alloc_size 0x" << min_alloc_size << " available 0x "
         << alloc->get_free() << std::dec << dendl;
    if (prealloc.size())
      release_alloc(prealloc);

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

  o->append(extents, offset);

  if (prealloc_left > 0) {
    PExtentVector old_extents;
    while (prealloc_pos != prealloc.end()) {
      old_extents.push_back(*prealloc_pos);
      prealloc_left -= prealloc_pos->length;
      ++prealloc_pos;
    }

    release_alloc(old_extents);
  }

  ceph_assert(prealloc_pos == prealloc.end());
  ceph_assert(prealloc_left == 0);
  return 0;
}

void ObjectStoreImitator::_do_truncate(CollectionRef &c, ObjectRef &o,
                                       uint64_t offset) {
  // current size already satisfied
  if (offset >= o->size)
    return;

  PExtentVector old_extents;
  o->punch_hole(offset, o->size - offset, old_extents);
  o->size = offset;
  release_alloc(old_extents);
}

int ObjectStoreImitator::_rename(CollectionRef &c, ObjectRef &oldo,
                                 ObjectRef &newo, const ghobject_t &new_oid) {
  int r;
  ghobject_t old_oid = oldo->oid;
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
  if (oldo->oid.hobj.get_hash() != newo->oid.hobj.get_hash()) {
    return -EINVAL;
  }

  _assign_nid(newo);
  _do_truncate(c, newo, 0);
  if (cct->_conf->bluestore_clone_cow) {
    _do_clone_range(c, oldo, newo, 0, oldo->size, 0);
  } else {
    bufferlist bl;
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
    derr << __func__ << " collection_list returned: " << cpp_strerror(r)
         << dendl;
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
    if (ls->size() >= (unsigned)max) {
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

  r = 0;

out:
  return r;
}
