// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Btree2Allocator.h"
#include "common/debug.h"

#define dout_context (get_context())
#define dout_subsys ceph_subsys_bluestore
#undef  dout_prefix
#define dout_prefix *_dout << (std::string(this->get_type()) + "::").c_str()

/*
 * class Btree2Allocator
 *
 *
 */
MEMPOOL_DEFINE_OBJECT_FACTORY(Btree2Allocator::range_seg_t, btree2_range_seg_t, bluestore_alloc);

Btree2Allocator::Btree2Allocator(CephContext* _cct,
  int64_t device_size,
  int64_t block_size,
  uint64_t max_mem,
  double _rweight_factor,
  bool with_cache,
  std::string_view name) :
    AllocatorBase(name, device_size, block_size),
    myTraits(RANGE_SIZE_BUCKET_COUNT),
    cct(_cct),
    range_count_cap(max_mem / sizeof(range_seg_t))
{
  set_weight_factor(_rweight_factor);
  if (with_cache) {
    cache = new OpportunisticExtentCache();
  }
  range_size_set.resize(myTraits.num_buckets);
}

void Btree2Allocator::init_add_free(uint64_t offset, uint64_t length)
{
  ldout(cct, 10) << __func__ << std::hex
    << " offset 0x" << offset
    << " length 0x" << length
    << std::dec << dendl;
  if (!length)
    return;
  std::lock_guard l(lock);
  ceph_assert(offset + length <= uint64_t(device_size));
  _add_to_tree(offset, length);
}

void Btree2Allocator::init_rm_free(uint64_t offset, uint64_t length)
{
  ldout(cct, 10) << __func__ << std::hex
    << " offset 0x" << offset
    << " length 0x" << length
    << std::dec << dendl;
  if (!length)
    return;
  std::lock_guard l(lock);
  ceph_assert(offset + length <= uint64_t(device_size));
  _remove_from_tree(offset, length);
}

int64_t Btree2Allocator::allocate(
  uint64_t want,
  uint64_t unit,
  uint64_t max_alloc_size,
  int64_t  hint, // unused, for now!
  PExtentVector* extents)
{
  ldout(cct, 10) << __func__ << std::hex
    << " want 0x" << want
    << " unit 0x" << unit
    << " max_alloc_size 0x" << max_alloc_size
    << " hint 0x" << hint
    << std::dec << dendl;
  ceph_assert(std::has_single_bit(unit));
  ceph_assert(want % unit == 0);

  if (max_alloc_size == 0) {
    max_alloc_size = want;
  }
  if (constexpr auto cap = std::numeric_limits<decltype(bluestore_pextent_t::length)>::max();
    max_alloc_size >= cap) {
    max_alloc_size = p2align(uint64_t(cap), (uint64_t)block_size);
  }
  uint64_t cached_chunk_offs = 0;
  if (cache && cache->try_get(&cached_chunk_offs, want)) {
    num_free -= want;
    extents->emplace_back(cached_chunk_offs, want);
    return want;
  }
  std::lock_guard l(lock);
  return _allocate(want, unit, max_alloc_size, hint, extents);
}

void Btree2Allocator::release(const release_set_t& release_set)
{
  if (!cache || release_set.num_intervals() >= pextent_array_size) {
    std::lock_guard l(lock);
    _release(release_set);
    return;
  }
  PExtentArray to_release;
  size_t count = 0;
  auto p = release_set.begin();
  while (p != release_set.end()) {
    if (cache->try_put(p.get_start(), p.get_len())) {
      num_free += p.get_len();
    } else {
      to_release[count++] = &*p;
    }
    ++p;
  }
  if (count > 0) {
    std::lock_guard l(lock);
    _release(count, &to_release.at(0));
  }
}

void Btree2Allocator::_shutdown()
{
  if (cache) {
    delete cache;
    cache = nullptr;
  }
  for (auto& tree : range_size_set) {
    tree.clear();
  }
  range_tree.clear();
}

void Btree2Allocator::_dump(bool full) const
{
  if (full) {
    ldout(cct, 0) << " >>>range_tree: " << dendl;
    for (auto& rs : range_tree) {
      ldout(cct, 0) << std::hex
        << "0x" << rs.first << "~" << (rs.second - rs.first)
        << std::dec
        << dendl;
    }
    ldout(cct, 0) << " >>>range_size_trees: " << dendl;
    size_t i = 0;
    for (auto& rs_tree : range_size_set) {
      ldout(cct, 0) << " >>>bucket[" << i << "]" << dendl;
      ++i;
      for (auto& rs : rs_tree)
        ldout(cct, 0) << std::hex
          << "0x" << rs.start << "~" << (rs.end - rs.start)
          << std::dec << dendl;
    }
    if (cache) {
      ldout(cct, 0) << " >>>cache: " << dendl;
      auto cb = [&](uint64_t offset, uint64_t length) {
        ldout(cct, 0) << std::hex
          << "0x" << offset << "~" << length
          << std::dec << dendl;
      };
      cache->foreach(cb);
    }
    ldout(cct, 0) << " >>>>>>>>>>>" << dendl;
  }
  if (cache) {
    ldout(cct, 0) << " >>>cache stats: " << dendl;
    ldout(cct, 0) << " hits: " << cache->get_hit_count() << dendl;
  }
}

void Btree2Allocator::_foreach(
  std::function<void(uint64_t offset, uint64_t length)> notify)
{
  for (auto& rs : range_tree) {
    notify(rs.first, rs.second - rs.first);
  }
  if (cache) {
    cache->foreach(notify);
  }
}

int64_t Btree2Allocator::_allocate(
  uint64_t want,
  uint64_t unit,
  uint64_t max_alloc_size,
  int64_t  hint, // unused, for now!
  PExtentVector* extents)
{
  uint64_t allocated = 0;
  while (allocated < want) {
    auto want_now = std::min(max_alloc_size, want - allocated);
    if (cache && want_now != want) {
      uint64_t cached_chunk_offs = 0;
      if (cache->try_get(&cached_chunk_offs, want_now)) {
        num_free -= want_now;
        extents->emplace_back(cached_chunk_offs, want_now);
        allocated += want_now;
        continue;
      }
    }
    size_t bucket0 = myTraits._get_bucket(want_now);
    int64_t r = __allocate(bucket0, want_now,
      unit, extents);
    if (r < 0) {
      // Allocation failed.
      break;
    }
    allocated += r;
  }
  return allocated ? allocated : -ENOSPC;
}

void Btree2Allocator::_release(const release_set_t& release_set)
{
  for (auto p = release_set.begin(); p != release_set.end(); ++p) {
    const auto offset = p.get_start();
    const auto length = p.get_len();
    ceph_assert(offset + length <= uint64_t(device_size));
    ldout(cct, 10) << __func__ << std::hex
      << " offset 0x" << offset
      << " length 0x" << length
      << std::dec << dendl;
    _add_to_tree(offset, length);
  }
}

void Btree2Allocator::_release(const PExtentVector& release_set)
{
  for (auto& e : release_set) {
    ldout(cct, 10) << __func__ << std::hex
      << " offset 0x" << e.offset
      << " length 0x" << e.length
      << std::dec << dendl;
    _add_to_tree(e.offset, e.length);
  }
}

void Btree2Allocator::_release(size_t count, const release_set_entry_t** to_release)
{
  for (size_t i = 0; i < count; i++) {
    auto* e = to_release[i];
    ldout(cct, 10) << __func__ << std::hex
      << " offset 0x" << e->first
      << " length 0x" << e->second
      << std::dec << dendl;
    _add_to_tree(e->first, e->second);
  }
}

void Btree2Allocator::_add_to_tree(uint64_t start, uint64_t size)
{
  ceph_assert(size != 0);

  uint64_t end = start + size;

  // Make sure we don't overlap with either of our neighbors
  auto rt_p_after = range_tree.upper_bound(start);
  auto rt_p_before = range_tree.end();
  if (rt_p_after != range_tree.begin()) {
    rt_p_before = std::prev(rt_p_after);
  }
  bool merge_before = (rt_p_before != range_tree.end() && rt_p_before->second == start);
  bool merge_after = (rt_p_after != range_tree.end() && rt_p_after->first == end);

  range_seg_t rs(start, end);
  if (merge_before && merge_after) {
    rs = range_seg_t{ rt_p_before->first, rt_p_after->second};
    _range_size_tree_rm(range_seg_t{ rt_p_before->first, rt_p_before->second });
    _range_size_tree_rm(range_seg_t{ rt_p_after->first, rt_p_after->second });
    rt_p_after = range_tree.erase(rt_p_before);
    rt_p_after = range_tree.erase(rt_p_after);
  } else if (merge_before) {
    rs = range_seg_t(rt_p_before->first, end);
    _range_size_tree_rm(range_seg_t{ rt_p_before->first, rt_p_before->second });
    rt_p_after = range_tree.erase(rt_p_before);
  } else if (merge_after) {
    rs = range_seg_t(start, rt_p_after->second);
    _range_size_tree_rm(range_seg_t{ rt_p_after->first, rt_p_after->second });
    rt_p_after = range_tree.erase(rt_p_after);
  }
  // create new entry at both range_tree and range_size_set
  __try_insert_range(rs, &rt_p_after);
}

void Btree2Allocator::_try_remove_from_tree(uint64_t start, uint64_t size,
  std::function<void(uint64_t, uint64_t, bool)> cb)
{
  uint64_t end = start + size;

  ceph_assert(size != 0);

  auto rt_p = range_tree.lower_bound(start);
  if ((rt_p == range_tree.end() || rt_p->first > start) && rt_p != range_tree.begin()) {
    --rt_p;
  }

  if (rt_p == range_tree.end() || rt_p->first >= end) {
    cb(start, size, false);
    return;
  }
  do {
    if (start < rt_p->first) {
      cb(start, rt_p->first - start, false);
      start = rt_p->first;
    }
    auto range_end = std::min(rt_p->second, end);

    rt_p = _remove_from_tree(rt_p, start, range_end);

    cb(start, range_end - start, true);
    start = range_end;

  } while (rt_p != range_tree.end() && rt_p->first < end && start < end);
  if (start < end) {
    cb(start, end - start, false);
  }
}

int64_t Btree2Allocator::__allocate(
  size_t bucket0,
  uint64_t size,
  uint64_t unit,
  PExtentVector* extents)
{
  int64_t allocated = 0;

  auto rs_tree = &range_size_set[bucket0];
  auto rs_p = _pick_block(0, rs_tree, size);

  if (rs_p == rs_tree->end()) {
    auto bucket_center = myTraits._get_bucket(weight_center);

    // requested size is to the left of weight center
    // hence we try to search up toward it first
    //
    size_t bucket = bucket0; // using unsigned for bucket is crucial
                             // as bounds checking when walking downsize
                             // depends on signed->unsigned value
                             // transformation
    while (++bucket <= bucket_center) {
      rs_tree = &range_size_set[bucket];
      rs_p = _pick_block(1, rs_tree, size);
      if (rs_p != rs_tree->end()) {
        goto found;
      }
    }

    int dir = left_weight() > right_weight() ? -1 : 1; // lookup direction
    int dir0 = dir;

    // Start from the initial bucket if searching downhill to
    // run through that bucket again as we haven't check extents shorter
    // than requested size if any.
    // Start from bucket_center + 1 if walking uphill.
    bucket = dir < 0 ? bucket0 : bucket_center + 1;
    do {
      // try spilled over or different direction if bucket index is out of bounds
      if (bucket >= myTraits.num_buckets) {
        if (dir < 0) {
          // reached the bottom while going downhill,
          // time to try spilled over extents
          uint64_t r = _spillover_allocate(size,
            unit,
            size,
            0,
            extents); // 0 is returned if error were detected.
          allocated += r;
          ceph_assert(size >= (uint64_t)r);
          size -= r;
          if (size == 0) {
            return allocated;
          }
        }
        // change direction
        dir = -dir;
        bucket = dir < 0 ? bucket0 : bucket_center + 1; // See above on new bucket
                                                        // selection rationales
        ceph_assert(bucket < myTraits.num_buckets); // this should never happen
        if (dir == dir0 ) {
          // stop if both directions already attempted
          return -ENOSPC;
        }
      }
      rs_tree = &range_size_set[bucket];
      rs_p = _pick_block(dir, rs_tree, size);
      bucket += dir; // this might wrap over zero and trigger direction
                     //  change on the next loop iteration.
    } while (rs_p == rs_tree->end());
  }

found:
  if (rs_p != rs_tree->end()) {
    auto o = rs_p->start;
    auto l = std::min(size, rs_p->length());
    auto rt_p = range_tree.find(o);
    ceph_assert(rt_p != range_tree.end());
    _remove_from_tree(rs_tree, rs_p, rt_p, o, o + l);
    extents->emplace_back(o, l);
    allocated += l;
    return allocated;
  }
  // should never get here
  ceph_assert(false);
  return -EFAULT;
}

Btree2Allocator::range_size_tree_t::iterator
Btree2Allocator::_pick_block(int dir,
                              Btree2Allocator::range_size_tree_t* tree,
                              uint64_t size)
{
  if (dir < 0) {
    // use the largest available chunk from 'shorter chunks' buckets
    // given the preliminary check before the function's call
    // we should get the result in a single step
    auto p = tree->end();
    if (p != tree->begin()) {
      --p;
      return p;
    }
  } else if (dir > 0) {
    auto p = tree->begin();
    if (p != tree->end()) {
      return p;
    }
  } else if (tree->size()) {
    //For the 'best matching' bucket
    //look for a chunk with the best size matching.
    // Start checking from the last item to
    // avoid lookup if possible
    auto p = tree->end();
    --p;
    if (p->length() > size) {
      p = tree->lower_bound(range_seg_t{ 0, size });
      ceph_assert(p != tree->end());
    }
    return p;
  }
  return tree->end();
}

void Btree2Allocator::_remove_from_tree(uint64_t start, uint64_t size)
{
  ceph_assert(size != 0);
  ceph_assert(size <= num_free);
  auto end = start + size;

  // Find chunk we completely overlap with
  auto rt_p = range_tree.lower_bound(start);
  if ((rt_p == range_tree.end() || rt_p->first > start) && rt_p != range_tree.begin()) {
    --rt_p;
  }

  /// Make sure we completely overlap with someone
  ceph_assert(rt_p != range_tree.end());
  ceph_assert(rt_p->first <= start);
  ceph_assert(rt_p->second >= end);
  _remove_from_tree(rt_p, start, end);
}

Btree2Allocator::range_tree_iterator
Btree2Allocator::_remove_from_tree(
  Btree2Allocator::range_tree_iterator rt_p,
  uint64_t start,
  uint64_t end)
{
  range_seg_t rs(rt_p->first, rt_p->second);
  size_t bucket = myTraits._get_bucket(rs.length());
  range_size_tree_t* rs_tree = &range_size_set[bucket];
  auto rs_p = rs_tree->find(rs);
  ceph_assert(rs_p != rs_tree->end());

  return _remove_from_tree(rs_tree, rs_p, rt_p, start, end);
}

Btree2Allocator::range_tree_iterator
Btree2Allocator::_remove_from_tree(
  Btree2Allocator::range_size_tree_t* rs_tree,
  Btree2Allocator::range_size_tree_t::iterator rs_p,
  Btree2Allocator::range_tree_iterator rt_p,
  uint64_t start,
  uint64_t end)
{
  bool left_over = (rt_p->first != start);
  bool right_over = (rt_p->second != end);

  range_seg_t rs = *rs_p;
  _range_size_tree_rm(rs_tree, rs_p);
  rt_p = range_tree.erase(rt_p); // It's suboptimal to do the removal every time.
                                 // Some paths might avoid that but coupled with
                                 // spillover handling this looks non-trivial.
                                 // Hence leaving as-is.
  if (left_over && right_over) {
    range_seg_t rs_before(rs.start, start);
    range_seg_t rs_after(end, rs.end);
    // insert rs_after first since this updates rt_p which starts to point to
    // newly inserted item.
    __try_insert_range(rs_after, &rt_p);
    __try_insert_range(rs_before, &rt_p);
  } else if (left_over) {
    rs.end = start;
    __try_insert_range(rs, &rt_p);
  } else if (right_over) {
    rs.start = end;
    __try_insert_range(rs, &rt_p);
  }
  return rt_p;
}

void Btree2Allocator::_try_insert_range(const range_seg_t& rs)
{
  if (__try_insert_range(rs, nullptr)) {
    _range_size_tree_add(rs);
  }
  else {
    range_tree.erase(rs.start);
  }
}

bool Btree2Allocator::__try_insert_range(
  const Btree2Allocator::range_seg_t& rs,
  Btree2Allocator::range_tree_iterator* rt_p_insert)
{
  ceph_assert(rs.end > rs.start);
  // Check if amount of range_seg_t entries isn't above the threshold,
  // use doubled range_tree's size for the estimation
  bool spillover_input = range_count_cap && uint64_t(2 * range_tree.size()) >= range_count_cap;
  range_size_tree_t* lowest_rs_tree = nullptr;
  range_size_tree_t::iterator lowest_rs_p;
  if (spillover_input) {
    lowest_rs_tree = _get_lowest(&lowest_rs_p);
    if (lowest_rs_tree) {
      ceph_assert(lowest_rs_p != lowest_rs_tree->end());
      spillover_input = rs.length() <= lowest_rs_p->length();
    }
    // make sure we never spill over chunks larger than weight_center
    if (spillover_input) {
      spillover_input = rs.length() <= weight_center;
      lowest_rs_tree = nullptr;
    } else if (lowest_rs_tree && lowest_rs_p->length () > weight_center) {
      lowest_rs_tree = nullptr;
    }
  }
  if (spillover_input) {
    _spillover_range(rs.start, rs.end);
  } else {
    // NB:  we should be careful about iterators (lowest_rs_p & rt_p_insert)
    // Relevant trees' updates might invalidate any of them hence
    // a bit convoluted logic below.
    range_seg_t lowest_rs;
    range_tree_t new_rt_p;
    if (lowest_rs_tree) {
      lowest_rs = *lowest_rs_p;
      _range_size_tree_rm(lowest_rs_tree, lowest_rs_p);
      range_tree.erase(lowest_rs.start);
      // refresh the iterator to be returned
      // due to the above range_tree removal invalidated it.
      if (rt_p_insert) {
        *rt_p_insert = range_tree.lower_bound(rs.start);
      }
      _spillover_range(lowest_rs.start, lowest_rs.end);
    }
    if (rt_p_insert) {
      *rt_p_insert = range_tree.emplace_hint(*rt_p_insert, rs.start, rs.end);
      _range_size_tree_add(rs);
    }
  }
  return !spillover_input;
}

void Btree2Allocator::_range_size_tree_add(const range_seg_t& rs) {
  auto l = rs.length();
  ceph_assert(rs.end > rs.start);
  size_t bucket = myTraits._get_bucket(l);
  range_size_set[bucket].insert(rs);
  num_free += l;

  if (l > weight_center) {
    rsum += l;
  } else {
    lsum += l;
  }

}
void Btree2Allocator::_range_size_tree_rm(const range_seg_t& rs)
{
  size_t bucket = myTraits._get_bucket(rs.length());
  range_size_tree_t* rs_tree = &range_size_set[bucket];
  ceph_assert(rs_tree->size() > 0);
  auto rs_p = rs_tree->find(rs);
  ceph_assert(rs_p != rs_tree->end());
  _range_size_tree_rm(rs_tree, rs_p);
}

void Btree2Allocator::_range_size_tree_rm(
    Btree2Allocator::range_size_tree_t* rs_tree,
    Btree2Allocator::range_size_tree_t::iterator rs_p)
{
  auto l = rs_p->length();
  ceph_assert(num_free >= l);
  num_free -= l;
  if (l > weight_center) {
    ceph_assert(rsum >= l);
    rsum -= l;
  } else {
    ceph_assert(lsum >= l);
    lsum -= l;
  }
  rs_tree->erase(rs_p);
}
