// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/dout.h"
#include "common/debug.h"

#include "BlueStore.h"
#include "BlueStore_objects.h"
#include "BlueStore_components.h"
#include "BlueStore_inlines.h"
#include "Compression.h"
#include "bluestore_common.h"
#include "os/kv.h"
#include "common/pretty_binary.h"

#include <string_view>

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore

using std::min;
using std::numeric_limits;
using std::less;
using std::list;
using std::map;
using std::max;
using std::ostream;
using std::set;
using std::string;
using std::vector;

using ceph::bufferlist;
using ceph::bufferptr;
using ceph::decode;
using ceph::encode;
using ceph::Formatter;

// Garbage Collector
#undef dout_prefix
#define dout_prefix *_dout << "bluestore.GarbageCollector "
#undef dout_context
#define dout_context cct


void bluestore::GarbageCollector::process_protrusive_extents(
  const bluestore::ExtentMap& extent_map,
  uint64_t start_offset,
  uint64_t end_offset,
  uint64_t start_touch_offset,
  uint64_t end_touch_offset,
  uint64_t min_alloc_size)
{
  ceph_assert(start_offset <= start_touch_offset && end_offset >= end_touch_offset);

  uint64_t lookup_start_offset = p2align(start_offset, min_alloc_size);
  uint64_t lookup_end_offset = round_up_to(end_offset, min_alloc_size);

  dout(30) << __func__ << " (hex): [" << std::hex
    << lookup_start_offset << ", " << lookup_end_offset
    << ")" << std::dec << dendl;

  for (auto it = extent_map.seek_lextent(lookup_start_offset);
    it != extent_map.extent_map.end() &&
    it->logical_offset < lookup_end_offset;
    ++it) {
    uint64_t alloc_unit_start = it->logical_offset / min_alloc_size;
    uint64_t alloc_unit_end = (it->logical_end() - 1) / min_alloc_size;

    dout(30) << __func__ << " " << *it
      << "alloc_units: " << alloc_unit_start << ".." << alloc_unit_end
      << dendl;

    Blob* b = it->blob.get();

    if (it->logical_offset >= start_touch_offset &&
      it->logical_end() <= end_touch_offset) {
      // Process extents within the range affected by
      // the current write request.
      // Need to take into account if existing extents
      // can be merged with them (uncompressed case)
      if (!b->get_blob().is_compressed()) {
	if (blob_info_counted && used_alloc_unit == alloc_unit_start) {
	  --blob_info_counted->expected_allocations; // don't need to allocate
	  // new AU for compressed
	  // data since another
	  // collocated uncompressed
	  // blob already exists
	  dout(30) << __func__ << " --expected:"
	    << alloc_unit_start << dendl;
	}
	used_alloc_unit = alloc_unit_end;
	blob_info_counted = nullptr;
      }
    } else if (b->get_blob().is_compressed()) {

      // additionally we take compressed blobs that were not impacted
      // by the write into account too
      BlobInfo& bi =
	affected_blobs.emplace(
	  b, BlobInfo(b->get_referenced_bytes())).first->second;

      int adjust =
	(used_alloc_unit && used_alloc_unit == alloc_unit_start) ? 0 : 1;
      bi.expected_allocations += alloc_unit_end - alloc_unit_start + adjust;
      dout(30) << __func__ << " expected_allocations="
	<< bi.expected_allocations << " end_au:"
	<< alloc_unit_end << dendl;

      blob_info_counted = &bi;
      used_alloc_unit = alloc_unit_end;

      ceph_assert(it->length <= bi.referenced_bytes);
      bi.referenced_bytes -= it->length;
      dout(30) << __func__ << " affected_blob:" << *b
	<< " unref 0x" << std::hex << it->length
	<< " referenced = 0x" << bi.referenced_bytes
	<< std::dec << dendl;
      // NOTE: we can't move specific blob to resulting GC list here
      // when reference counter == 0 since subsequent extents might
      // decrement its expected_allocation.
      // Hence need to enumerate all the extents first.
      if (!bi.collect_candidate) {
	bi.first_lextent = it;
	bi.collect_candidate = true;
      }
      bi.last_lextent = it;
    } else {
      if (blob_info_counted && used_alloc_unit == alloc_unit_start) {
	// don't need to allocate new AU for compressed data since another
	// collocated uncompressed blob already exists
	--blob_info_counted->expected_allocations;
	dout(30) << __func__ << " --expected_allocations:"
	  << alloc_unit_start << dendl;
      }
      used_alloc_unit = alloc_unit_end;
      blob_info_counted = nullptr;
    }
  }

  for (auto b_it = affected_blobs.begin();
    b_it != affected_blobs.end();
    ++b_it) {
    Blob* b = b_it->first;
    BlobInfo& bi = b_it->second;
    if (bi.referenced_bytes == 0) {
      uint64_t len_on_disk = b_it->first->get_blob().get_ondisk_capacity();
      int64_t blob_expected_for_release =
	round_up_to(len_on_disk, min_alloc_size) / min_alloc_size;

      dout(30) << __func__ << " " << *(b_it->first)
	<< " expected4release=" << blob_expected_for_release
	<< " expected_allocations=" << bi.expected_allocations
	<< dendl;
      int64_t benefit = blob_expected_for_release - bi.expected_allocations;
      if (benefit >= g_conf()->bluestore_gc_enable_blob_threshold) {
	if (bi.collect_candidate) {
	  auto it = bi.first_lextent;
	  bool bExit = false;
	  do {
	    if (it->blob.get() == b) {
	      extents_to_collect.insert(it->logical_offset, it->length);
	    }
	    bExit = it == bi.last_lextent;
	    ++it;
	  } while (!bExit);
	}
	expected_for_release += blob_expected_for_release;
	expected_allocations += bi.expected_allocations;
      }
    }
  }
}

int64_t bluestore::GarbageCollector::estimate(
  uint64_t start_offset,
  uint64_t length,
  const bluestore::ExtentMap& extent_map,
  const bluestore::old_extent_map_t& old_extents,
  uint64_t min_alloc_size)
{

  affected_blobs.clear();
  extents_to_collect.clear();
  used_alloc_unit = boost::optional<uint64_t >();
  blob_info_counted = nullptr;

  uint64_t gc_start_offset = start_offset;
  uint64_t gc_end_offset = start_offset + length;

  uint64_t end_offset = start_offset + length;

  for (auto it = old_extents.begin(); it != old_extents.end(); ++it) {
    Blob* b = it->e.blob.get();
    if (b->get_blob().is_compressed()) {

      // update gc_start_offset/gc_end_offset if needed
      gc_start_offset = min(gc_start_offset, (uint64_t)it->e.blob_start());
      gc_end_offset = std::max(gc_end_offset, (uint64_t)it->e.blob_end());

      auto o = it->e.logical_offset;
      auto l = it->e.length;

      uint64_t ref_bytes = b->get_referenced_bytes();
      // micro optimization to bypass blobs that have no more references
      if (ref_bytes != 0) {
	dout(30) << __func__ << " affected_blob:" << *b
	  << " unref 0x" << std::hex << o << "~" << l
	  << std::dec << dendl;
	affected_blobs.emplace(b, BlobInfo(ref_bytes));
      }
    }
  }
  dout(30) << __func__ << " gc range(hex): [" << std::hex
    << gc_start_offset << ", " << gc_end_offset
    << ")" << std::dec << dendl;

  // enumerate preceeding extents to check if they reference affected blobs
  if (gc_start_offset < start_offset || gc_end_offset > end_offset) {
    process_protrusive_extents(extent_map,
      gc_start_offset,
      gc_end_offset,
      start_offset,
      end_offset,
      min_alloc_size);
  }
  return expected_for_release - expected_allocations;
}

// LruOnodeCacheShard
namespace bluestore {
struct LruOnodeCacheShard : public bluestore::OnodeCacheShard {
  typedef boost::intrusive::list<
    bluestore::Onode,
    boost::intrusive::member_hook<
      bluestore::Onode,
      boost::intrusive::list_member_hook<>,
      &bluestore::Onode::lru_item> > list_t;

  list_t lru;        ///< onodes used again after their first use; front = most recent
  list_t probation;  ///< onodes used once so far; FIFO, front = newest; trimmed
                     ///< first once above its share (_probation_max), else
                     ///< after lru's tail

  explicit LruOnodeCacheShard(CephContext *cct) : bluestore::OnodeCacheShard(cct) {}

  /// With a probation ratio of 0 every onode is admitted on first use (one LRU).
  bool _admission() const
  {
    return probation_ratio.load() > 0;
  }
  /// How many onodes probation may hold; beyond that it is trimmed even when
  /// the shard as a whole is under max: a share of the shard, what came in
  /// during the last few age bins, or a floor, whichever is more. The second
  /// term gives every onode a few seconds to be used again however fast a
  /// scan brings onodes in. The floor holds a backfill scan: it reads up to
  /// osd_backfill_scan_max (512) objects of one PG at once, all in one shard,
  /// and pushes them over the next seconds. Both keep admission possible
  /// however small the tuner has made the shard (its budget follows the
  /// binned onodes, which under a scan are probation itself).
  static constexpr uint32_t PROBATION_BINS = 4;
  /// Arrivals on probation in each of the last PROBATION_BINS age bins,
  /// newest first. The age bins themselves count every onode by its last
  /// touch, the main LRU's hot set included, so a bound taken from them
  /// would grow by the hot set under a scan and the shard, once over
  /// budget, would evict the hot set to keep that much scan residue.
  std::array<uint64_t, PROBATION_BINS> probation_arrivals = {};
  void shift_bins() override
  {
    std::lock_guard l(lock);
    for (uint32_t i = PROBATION_BINS - 1; i > 0; i--) {
      probation_arrivals[i] = probation_arrivals[i - 1];
    }
    probation_arrivals[0] = 0;
    OnodeCacheShard::shift_bins();
  }
  uint64_t _probation_max() const
  {
    double r = probation_ratio.load();
    if (r <= 0) {
      return 0;
    }
    uint64_t recent = 0;
    for (uint64_t n : probation_arrivals) {
      recent += n;
    }
    return std::max<uint64_t>({probation_floor.load(), (uint64_t)(max.load() * std::min(r, 1.0)),
                               recent});
  }
  /// The keys of the onodes lru evicted most recently, as many as the
  /// shard's budget, oldest first. A miss on one of them is a hot onode the
  /// budget could not hold: it goes back to lru rather than through
  /// probation, where under a budget below the hot set it would be evicted
  /// before its second later use, so that lru never grew back. A key
  /// evicted again moves to the back. The keys are hashes: a collision
  /// promotes a stranger, which lru then ages out as any other.
  using ghost_list_t = mempool::bluestore_cache_other::list<uint64_t>;
  ghost_list_t ghosts;
  /// key -> its place in ghosts
  mempool::bluestore_cache_other::unordered_map<uint64_t, ghost_list_t::iterator>
    ghost_pos;

  /// 64 bits of the whole object id. std::hash<ghobject_t> is the 32-bit
  /// placement hash, which same-named objects of two pools share.
  static uint64_t _ghost_key(const bluestore::Onode* o)
  {
    const hobject_t& h = o->oid.hobj;
    std::hash<std::string_view> hs;
    uint64_t k = hs(h.oid.name);
    auto mix = [&k](uint64_t v) {
      k ^= v + 0x9e3779b97f4a7c15ULL + (k << 6) + (k >> 2);
    };
    mix(hs(h.nspace));
    mix(hs(h.get_key()));
    mix(h.pool);
    mix(h.snap.val);
    mix(h.get_hash());
    mix(o->oid.generation);
    mix(o->oid.shard_id.id);
    return k;
  }
  /// remember an onode lru evicts
  void _ghost_add(const bluestore::Onode* o)
  {
    auto [it, fresh] = ghost_pos.try_emplace(_ghost_key(o), ghosts.end());
    if (!fresh) {
      ghosts.erase(it->second);
    }
    ghosts.push_back(it->first);
    it->second = std::prev(ghosts.end());
    // a few at a time, as _trim_to does: a budget cut may leave many over
    uint64_t cap = std::max<uint64_t>(1, max.load());
    for (int i = 0; i < 4 && ghosts.size() > cap; i++) {
      ghost_pos.erase(ghosts.front());
      ghosts.pop_front();
    }
  }
  /// was the arriving onode evicted from lru lately? forget it either way
  bool _ghost_take(const bluestore::Onode* o)
  {
    auto it = ghost_pos.find(_ghost_key(o));
    if (it == ghost_pos.end()) {
      return false;
    }
    ghosts.erase(it->second);
    ghost_pos.erase(it);
    return true;
  }
  list_t& _list_of(bluestore::Onode* o)
  {
    return o->cache_promoted ? lru : probation;
  }
  /// stamp the onode into the newest age bin, moving its count there
  void _stamp(bluestore::Onode* o)
  {
    if (o->cache_age_bin == age_bins.front()) {
      return;  // the common case on the unpin path: touched again this bin
    }
    if (o->cache_age_bin) {
      *(o->cache_age_bin) -= 1;
    }
    o->cache_age_bin = age_bins.front();
    *(o->cache_age_bin) += 1;
  }
  void _link_front(list_t& l, bluestore::Onode* o)
  {
    l.push_front(*o);
    _stamp(o);
  }
  void _unlink(bluestore::Onode* o)
  {
    *(o->cache_age_bin) -= 1;
    o->cache_age_bin.reset();
    list_t& l = _list_of(o);
    l.erase(l.iterator_to(*o));
  }
  /// A use of the onode ended (its last pin was dropped): admit, promote or
  /// refresh it.  An onode enters probation on its first use and moves to lru
  /// only after it is used in two age bins later than the one it came in: the
  /// touches of one op sequence fall into one bin, and one pass over an
  /// object (scrub, a backfill that lists and then pushes, a write never read
  /// back) gives at most one later use.  Lock held, o->exists.
  void _touch(bluestore::Onode* o)
  {
    bool linked = o->lru_item.is_linked();
    bool popped = o->cache_popped;  // unlinked by a trim while in use
    o->cache_popped = false;
    if (!o->cache_promoted) {
      bool later = o->cache_age_bin && o->cache_age_bin != age_bins.front();
      if (_admission() && !(later && o->cache_later_hit)) {
        if (later) {
          // first use in a later bin: note it, and stamp this bin (an
          // onode with a stamp is linked: every unlink clears it)
          ceph_assert(linked);
          o->cache_later_hit = true;
          _stamp(o);
        }
        if (!linked) {
          if (!popped && _ghost_take(o)) {
            // an arrival that lru evicted lately: a hot onode the budget
            // could not hold, back to lru as a plain LRU would have it
            o->cache_promoted = true;
            _link_front(lru, o);
            dout(20) << __func__ << " " << this << " " << o->oid
                     << " promoted, evicted lately" << dendl;
            return;
          }
          _link_front(probation, o);
          if (!popped) {
            probation_arrivals[0]++;
          }
          dout(20) << __func__ << " " << this << " " << o->oid << " on probation"
                   << dendl;
        }
        return; // stays on probation
      }
      if (linked) {
        _unlink(o);
      }
      o->cache_promoted = true;
      _link_front(lru, o);
      dout(20) << __func__ << " " << this << " " << o->oid << " promoted" << dendl;
      return;
    }
    if (linked) {
      // move onode within LRU
      lru.erase(lru.iterator_to(*o));
      _link_front(lru, o);
      dout(20) << __func__ << " " << this << " " << o->oid << " touched" << dendl;
    } else {
      _link_front(lru, o);
      dout(20) << __func__ << " " << this << " " << o->oid << " unpinned" << dendl;
    }
  }

  void _add(bluestore::Onode* o) override
  {
    // every caller holds a reference besides the onode map's, so the onode
    // is pinned here; it is linked when that reference is dropped
    // (maybe_unpin), on probation or, if it does not exist, not at all
    ceph_assert(o->pin_nref > 1);
    o->set_cached();
    ++num; // we count both pinned and unpinned entries
    dout(20) << __func__ << " " << this << " " << o->oid << " added, num="
             << num << dendl;
  }
  void _rm(bluestore::Onode* o) override
  {
    o->clear_cached();
    if (o->lru_item.is_linked()) {
      _unlink(o);
    }
    // if re-added after a split, a promoted onode keeps its place in lru and a
    // non-promoted one starts over on probation
    o->cache_age_bin.reset();
    o->cache_later_hit = false;
    o->cache_popped = false;
    ceph_assert(num);
    --num;
    dout(20) << __func__ << " " << this << " " << " " << o->oid << " removed, num=" << num << dendl;
  }

  void maybe_unpin(bluestore::Onode* o) override
  {
    bluestore::OnodeCacheShard* ocs = this;
    ocs->lock.lock();
    // It is possible that during waiting split_cache moved us to different OnodeCacheShard.
    while (ocs != o->c->get_onode_cache()) {
      ocs->lock.unlock();
      ocs = o->c->get_onode_cache();
      ocs->lock.lock();
    }
    if (ocs != this) {
      // the onode is in another shard's lists now: that shard does the work
      ocs->lock.unlock();
      ocs->maybe_unpin(o);
      return;
    }
    if (o->is_cached() && o->pin_nref == 1) {
      if (o->exists) {
        _touch(o);
      } else if (!o->lru_item.is_linked()) {
	ceph_assert(num);
	--num;
	o->clear_cached();
	dout(20) << __func__ << " " << this << " " << o->oid << " removed"
                 << dendl;
        // remove will also decrement nref
        o->c->onode_space._remove(o->oid);
      }
    }
    ocs->lock.unlock();
  }

  void _trim_to(uint64_t new_size) override
  {
    if (!_admission() && !ghosts.empty()) {
      // nothing takes them with admission off
      ghosts.clear();
      ghost_pos.clear();
    }
    uint64_t linked = lru.size() + probation.size();
    uint64_t pmax = _probation_max();
    // the promoted onodes come first: over budget, probation may hold only
    // what the budget leaves after them, whatever its guarantee says, so
    // the hot set goes only when it does not fit by itself. Newcomers keep
    // half of probation's share all the same, so that a working set that
    // shifts can prove itself against a stale LRU, as it would in a plain
    // LRU; a hot set that fills the budget loses at most that much.
    if (new_size > 0) {
      uint64_t room = new_size > lru.size() ? new_size - lru.size() : 0;
      uint64_t reserve = std::max<uint64_t>(
        1, new_size * std::min(probation_ratio.load(), 1.0) / 2);
      pmax = std::min(pmax, std::max(room, reserve));
    }
    // probation beyond its share goes first, even when the shard as a whole
    // fits; a few at a time, as _trim_some does, unless this is a flush
    // with admission off (ratio 0) probation is not trimmed on its own: what
    // is on it stays until used, and is admitted then
    uint64_t over = _admission() && probation.size() > pmax ? probation.size() - pmax : 0;
    int32_t max_steps = cct->_conf->bluestore_cache_meta_evict_limit;
    if (new_size > 0 && max_steps >= 2) {
      over = std::min<uint64_t>(over, max_steps);
    }
    // only what is linked can go: a pinned onode over the budget, such as
    // the one being added when _trim_some runs, must not cost a linked one
    // its place; it is trimmed once it is linked. As before, nothing is
    // evicted while the lists fit the budget, whatever is pinned besides;
    // over budget, exactly the lists' overshoot goes, where the pinned
    // count used to be evicted on top of it.
    uint64_t n = new_size < linked ? linked - new_size : 0;
    n = std::max(n, over);
    while (n-- > 0 && (lru.size() + probation.size()) > 0) {
      list_t& l = (!probation.empty() && (probation.size() > pmax || lru.empty()))
        ? probation : lru;
      bluestore::Onode *o = &l.back();
      l.pop_back();

      dout(20) << __func__ << "  rm " << o->oid << " "
               << o->nref << " " << o->cached << dendl;

      *(o->cache_age_bin) -= 1;
      if (o->pin_nref > 1) {
        // still in use: it returns to its list when that use ends. The use
        // in progress is not a later one, so its stamp goes; a later use
        // earned before stays earned, and the return is not an arrival.
        o->cache_age_bin.reset();
        o->cache_popped = true;
        dout(20) << __func__ << " " << this << " " << " " << " " << o->oid << dendl;
      } else {
	ceph_assert(num);
        --num;
        o->clear_cached();
        o->cache_age_bin.reset();  // unlinked onodes carry no stamp
        if (&l == &lru && o->exists && _admission()) {
          _ghost_add(o);  // a deleted object's name may be reused
        }
        o->c->onode_space._remove(o->oid);
      }
    }
  }
  void _move_pinned(bluestore::OnodeCacheShard *to, bluestore::Onode *o) override
  {
    if (to == this) {
      return;
    }
    _rm(o);
    ceph_assert(o->nref > 1);
    to->_add(o);
  }
  void add_stats(uint64_t *onodes, uint64_t *pinned_onodes,
                 uint64_t *probation_onodes) override
  {
    std::lock_guard l(lock);
    *onodes += num;
    *pinned_onodes += num - lru.size() - probation.size();
    *probation_onodes += probation.size();
  }
#ifdef DEBUG_CACHE
  void _audit(const char *when) override
  {
  }
#endif
};
}

// OnodeCacheShard
bluestore::OnodeCacheShard* bluestore::OnodeCacheShard::create(
  CephContext* cct,
  std::string type,
  PerfCounters* logger)
{
  bluestore::OnodeCacheShard* c = nullptr;
  // Currently we only implement an LRU cache for onodes
  c = new LruOnodeCacheShard(cct);
  c->logger = logger;
  return c;
}

// TransContext
bool bluestore::TransContext::add_writing(Onode* o, uint32_t off, uint32_t len)
{
  std::lock_guard l(writings_lock);

  // Need to indicate non-initial observers that we're done.
  if (were_writings && writings.empty()) {
    return false;
  }
  writings.emplace_back(o, off, len);
  were_writings = true;
  return true;
}

void bluestore::TransContext::finish_writing()
{
  write_list_t finished;
  {
    std::lock_guard l(writings_lock);
    finished.swap(writings);
  }
  for (auto& e : finished) {
    e.onode->finish_write(this, e.offset, e.length);
  }
}

// WriteContext

/// Checks for writes to the same pextent within a blob
bool bluestore::WriteContext::has_conflict(
  BlobRef b,
  uint64_t loffs,
  uint64_t loffs_end,
  uint64_t min_alloc_size)
{
  ceph_assert((loffs % min_alloc_size) == 0);
  ceph_assert((loffs_end % min_alloc_size) == 0);
  for (auto w : writes) {
    if (b == w.b) {
      auto loffs2 = p2align(w.logical_offset, min_alloc_size);
      auto loffs2_end = p2roundup(w.logical_offset + w.length0, min_alloc_size);
      if ((loffs <= loffs2 && loffs_end > loffs2) ||
	(loffs >= loffs2 && loffs < loffs2_end)) {
	return true;
      }
    }
  }
  return false;
}

// DeferredBatch
#undef dout_prefix
#define dout_prefix *_dout << "bluestore.DeferredBatch(" << this << ") "
#undef dout_context
#define dout_context cct

void bluestore::DeferredBatch::prepare_write(
  CephContext* cct,
  uint64_t seq, uint64_t offset, uint64_t length,
  bufferlist::const_iterator& blp)
{
  _discard(cct, offset, length);
  auto i = iomap.insert(std::make_pair(offset, deferred_io()));
  ceph_assert(i.second);  // this should be a new insertion
  i.first->second.seq = seq;
  blp.copy(length, i.first->second.bl);
  i.first->second.bl.reassign_to_mempool(
    mempool::mempool_bluestore_writing_deferred);
  dout(20) << __func__ << " seq " << seq
    << " 0x" << std::hex << offset << "~" << length
    << " crc " << i.first->second.bl.crc32c(-1)
    << std::dec << dendl;
#ifdef DEBUG_DEFERRED
  seq_bytes[seq] += length;
  _audit(cct);
#endif
}

void bluestore::DeferredBatch::_discard(
  CephContext* cct, uint64_t offset, uint64_t length)
{
  generic_dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
    << std::dec << dendl;
  [[maybe_unused]] uint64_t delta;
  auto p = iomap.lower_bound(offset);
  if (p != iomap.begin()) {
    --p;
    auto end = p->first + p->second.bl.length();
    if (end > offset) {
      bufferlist head;
      head.substr_of(p->second.bl, 0, offset - p->first);
      dout(20) << __func__ << "  keep head " << p->second.seq
	<< " 0x" << std::hex << p->first << "~" << p->second.bl.length()
	<< " -> 0x" << head.length() << std::dec << dendl;
      if (end > offset + length) {
	bufferlist tail;
	tail.substr_of(p->second.bl, offset + length - p->first,
	  end - (offset + length));
	dout(20) << __func__ << "  keep tail " << p->second.seq
	  << " 0x" << std::hex << p->first << "~" << p->second.bl.length()
	  << " -> 0x" << tail.length() << std::dec << dendl;
	auto& n = iomap[offset + length];
	n.bl.swap(tail);
	n.seq = p->second.seq;
	delta = length;
      }
      else {
	delta = end - offset;
      }
#if defined(DEBUG_DEFERRED)
      auto i = seq_bytes.find(p->second.seq);
      ceph_assert(i != seq_bytes.end());
      i->second -= delta;
      ceph_assert(i->second >= 0);
#endif
      p->second.bl.swap(head);
    }
    ++p;
  }
  while (p != iomap.end()) {
    if (p->first >= offset + length) {
      break;
    }
    auto end = p->first + p->second.bl.length();
    if (end > offset + length) {
      unsigned drop_front = offset + length - p->first;
      unsigned keep_tail = end - (offset + length);
      dout(20) << __func__ << "  truncate front " << p->second.seq
	<< " 0x" << std::hex << p->first << "~" << p->second.bl.length()
	<< " drop_front 0x" << drop_front << " keep_tail 0x" << keep_tail
	<< " to 0x" << (offset + length) << "~" << keep_tail
	<< std::dec << dendl;
      auto& s = iomap[offset + length];
      s.seq = p->second.seq;
      s.bl.substr_of(p->second.bl, drop_front, keep_tail);
      delta = drop_front;
    }
    else {
      dout(20) << __func__ << "  drop " << p->second.seq
	<< " 0x" << std::hex << p->first << "~" << p->second.bl.length()
	<< std::dec << dendl;
      delta = p->second.bl.length();
    }
#if defined(DEBUG_DEFERRED)
    auto i = seq_bytes.find(p->second.seq);
    ceph_assert(i != seq_bytes.end());
    i->second -= delta;
    ceph_assert(i->second >= 0);
#endif
    p = iomap.erase(p);
  }
}

#if defined(DEBUG_DEFERRED)
void bluestore::DeferredBatch::_audit(CephContext* cct)
{
  map<uint64_t, int> sb;
  for (auto p : seq_bytes) {
    sb[p.first] = 0;  // make sure we have the same set of keys
  }
  uint64_t pos = 0;
  for (auto& p : iomap) {
    ceph_assert(p.first >= pos);
    sb[p.second.seq] += p.second.bl.length();
    pos = p.first + p.second.bl.length();
  }
  ceph_assert(sb == seq_bytes);
}
#endif

//BigDeferredWriteContext
bool bluestore::BigDeferredWriteContext::can_defer(
  bluestore::extent_map_t::iterator ep,
  uint64_t prefer_deferred_size,
  uint64_t block_size,
  uint64_t offset,
  uint64_t l)
{
  bool res = false;
  auto& blob = ep->blob->get_blob();
  if (offset >= ep->blob_start() &&
    blob.is_mutable()) {
    off = offset;
    b_off = offset - ep->blob_start();
    uint64_t chunk_size = blob.get_chunk_size(block_size);
    uint64_t ondisk = blob.get_ondisk_capacity();
    used = std::min(l, ondisk - b_off);

    // will read some data to fill out the chunk?
    head_read = p2phase<uint64_t>(b_off, chunk_size);
    tail_read = p2nphase<uint64_t>(b_off + used, chunk_size);
    b_off -= head_read;

    ceph_assert(b_off % chunk_size == 0);
    ceph_assert(blob_aligned_len() % chunk_size == 0);

    res = blob_aligned_len() < prefer_deferred_size &&
      blob_aligned_len() <= ondisk &&
      blob.is_allocated(b_off, blob_aligned_len());
    if (res) {
      blob_ref = ep->blob;
      blob_start = ep->blob_start();
    }
  }
  return res;
}
bool bluestore::BigDeferredWriteContext::apply_defer()
{
  int r = blob_ref->get_blob().map(
    b_off, blob_aligned_len(),
    [&](const bluestore_pextent_t& pext,
      uint64_t offset,
      uint64_t length) {
	// apply deferred if overwrite breaks blob continuity only.
	// if it totally overlaps some pextent - fallback to regular write
	if (pext.offset < offset ||
	  pext.end() > offset + length) {
	  res_extents.emplace_back(bluestore_pextent_t(offset, length));
	  return 0;
	}
	return -1;
    });
  return r >= 0;
}



