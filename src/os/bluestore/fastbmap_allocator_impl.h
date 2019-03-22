// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Bitmap based in-memory allocator implementation.
 * Author: Igor Fedotov, ifedotov@suse.com
 *
 */

#ifndef __FAST_BITMAP_ALLOCATOR_IMPL_H
#define __FAST_BITMAP_ALLOCATOR_IMPL_H
#include "include/intarith.h"

#include <vector>
#include <algorithm>
#include <mutex>

typedef uint64_t slot_t;

#ifdef NON_CEPH_BUILD
#include <assert.h>
struct interval_t
{
  uint64_t offset = 0;
  uint64_t length = 0;

  interval_t() {}
  interval_t(uint64_t o, uint64_t l) : offset(o), length(l) {}
  interval_t(const interval_t &ext) :
    offset(ext.offset), length(ext.length) {}
};
typedef std::vector<interval_t> interval_vector_t;
typedef std::vector<slot_t> slot_vector_t;
#else
#include "include/ceph_assert.h"
#include "common/likely.h"
#include "os/bluestore/bluestore_types.h"
#include "include/mempool.h"
#include "common/ceph_mutex.h"

typedef bluestore_interval_t<uint64_t, uint64_t> interval_t;
typedef PExtentVector interval_vector_t;

typedef mempool::bluestore_alloc::vector<slot_t> slot_vector_t;

#endif

// fitting into cache line on x86_64
static const size_t slotset_width = 8; // 8 slots per set
static const size_t slotset_bytes = sizeof(slot_t) * slotset_width;
static const size_t bits_per_slot = sizeof(slot_t) * 8;
static const size_t bits_per_slotset = slotset_bytes * 8;
static const slot_t all_slot_set = 0xffffffffffffffff;
static const slot_t all_slot_clear = 0;

inline size_t find_next_set_bit(slot_t slot_val, size_t start_pos)
{
#ifdef __GNUC__
  if (start_pos == 0) {
    start_pos = __builtin_ffsll(slot_val);
    return start_pos ? start_pos - 1 : bits_per_slot;
  }
#endif
  slot_t mask = slot_t(1) << start_pos;
  while (start_pos < bits_per_slot && !(slot_val & mask)) {
    mask <<= 1;
    ++start_pos;
  }
  return start_pos;
}


class AllocatorLevel
{
protected:

  virtual uint64_t _children_per_slot() const = 0;
  virtual uint64_t _level_granularity() const = 0;

public:
  static uint64_t l0_dives;
  static uint64_t l0_iterations;
  static uint64_t l0_inner_iterations;
  static uint64_t alloc_fragments;
  static uint64_t alloc_fragments_fast;
  static uint64_t l2_allocs;

  virtual ~AllocatorLevel()
  {}

  virtual void collect_stats(
    std::map<size_t, size_t>& bins_overall) = 0;

};

class AllocatorLevel01 : public AllocatorLevel
{
protected:
  slot_vector_t l0; // set bit means free entry
  slot_vector_t l1;
  uint64_t l0_granularity = 0; // space per entry
  uint64_t l1_granularity = 0; // space per entry

  size_t partial_l1_count = 0;
  size_t unalloc_l1_count = 0;

  double get_fragmentation() const {
    double res = 0.0;
    auto total = unalloc_l1_count + partial_l1_count;
    if (total) {
      res = double(partial_l1_count) / double(total);
    }
    return res;
  }

  uint64_t _level_granularity() const override
  {
    return l1_granularity;
  }

  inline bool _is_slot_fully_allocated(uint64_t idx) const {
    return l1[idx] == all_slot_clear;
  }
public:
  inline uint64_t get_min_alloc_size() const
  {
    return l0_granularity;
  }

};

template <class T>
class AllocatorLevel02;

class AllocatorLevel01Loose : public AllocatorLevel01
{
  enum {
    L1_ENTRY_WIDTH = 2,
    L1_ENTRY_MASK = (1 << L1_ENTRY_WIDTH) - 1,
    L1_ENTRY_FULL = 0x00,
    L1_ENTRY_PARTIAL = 0x01,
    L1_ENTRY_NOT_USED = 0x02,
    L1_ENTRY_FREE = 0x03,
    CHILD_PER_SLOT = bits_per_slot / L1_ENTRY_WIDTH, // 32
    CHILD_PER_SLOT_L0 = bits_per_slot, // 64
  };
  uint64_t _children_per_slot() const override
  {
    return CHILD_PER_SLOT;
  }

  interval_t _get_longest_from_l0(uint64_t pos0, uint64_t pos1,
    uint64_t min_length, interval_t* tail) const;

  inline void _fragment_and_emplace(uint64_t max_length, uint64_t offset,
    uint64_t len,
    interval_vector_t* res)
  {
    auto it = res->rbegin();
    if (max_length) {
      if (it != res->rend() && it->offset + it->length == offset) {
	auto l = max_length - it->length;
	if (l >= len) {
	  it->length += len;
	  return;
	} else {
	  offset += l;
	  len -= l;
	  it->length += l;
	}
      }

      while (len > max_length) {
	res->emplace_back(offset, max_length);
	offset += max_length;
	len -= max_length;
      }
      res->emplace_back(offset, len);
      return;
    }

    if (it != res->rend() && it->offset + it->length == offset) {
      it->length += len;
    } else {
      res->emplace_back(offset, len);
    }
  }

  bool _allocate_l0(uint64_t length,
    uint64_t max_length,
    uint64_t l0_pos0, uint64_t l0_pos1,
    uint64_t* allocated,
    interval_vector_t* res)
  {
    uint64_t d0 = CHILD_PER_SLOT_L0;

    ++l0_dives;

    ceph_assert(l0_pos0 < l0_pos1);
    ceph_assert(length > *allocated);
    ceph_assert(0 == (l0_pos0 % (slotset_width * d0)));
    ceph_assert(0 == (l0_pos1 % (slotset_width * d0)));
    ceph_assert(((length - *allocated) % l0_granularity) == 0);

    uint64_t need_entries = (length - *allocated) / l0_granularity;

    for (auto idx = l0_pos0 / d0; (idx < l0_pos1 / d0) && (length > *allocated);
      ++idx) {
      ++l0_iterations;
      slot_t& slot_val = l0[idx];
      auto base = idx * d0;
      if (slot_val == all_slot_clear) {
        continue;
      } else if (slot_val == all_slot_set) {
        uint64_t to_alloc = std::min(need_entries, d0);
        *allocated += to_alloc * l0_granularity;
	++alloc_fragments;
        need_entries -= to_alloc;

	_fragment_and_emplace(max_length, base * l0_granularity,
          to_alloc * l0_granularity, res);

        if (to_alloc == d0) {
          slot_val = all_slot_clear;
        } else {
          _mark_alloc_l0(base, base + to_alloc);
        }
        continue;
      }

      auto free_pos = find_next_set_bit(slot_val, 0);
      ceph_assert(free_pos < bits_per_slot);
      auto next_pos = free_pos + 1;
      while (next_pos < bits_per_slot &&
        (next_pos - free_pos) < need_entries) {
	++l0_inner_iterations;

        if (0 == (slot_val & (slot_t(1) << next_pos))) {
          auto to_alloc = (next_pos - free_pos);
          *allocated += to_alloc * l0_granularity;
	  ++alloc_fragments;
          need_entries -= to_alloc;
	  _fragment_and_emplace(max_length, (base + free_pos) * l0_granularity,
	    to_alloc * l0_granularity, res);
          _mark_alloc_l0(base + free_pos, base + next_pos);
          free_pos = find_next_set_bit(slot_val, next_pos + 1);
          next_pos = free_pos + 1;
        } else {
          ++next_pos;
        }
      }
      if (need_entries && free_pos < bits_per_slot) {
        auto to_alloc = std::min(need_entries, d0 - free_pos);
        *allocated += to_alloc * l0_granularity;
	++alloc_fragments;
	need_entries -= to_alloc;
	_fragment_and_emplace(max_length, (base + free_pos) * l0_granularity,
	  to_alloc * l0_granularity, res);
        _mark_alloc_l0(base + free_pos, base + free_pos + to_alloc);
      }
    }
    return _is_empty_l0(l0_pos0, l0_pos1);
  }

protected:

  friend class AllocatorLevel02<AllocatorLevel01Loose>;

  void _init(uint64_t capacity, uint64_t _alloc_unit, bool mark_as_free = true)
  {
    l0_granularity = _alloc_unit;
    // 512 bits at L0 mapped to L1 entry
    l1_granularity = l0_granularity * bits_per_slotset;

    // capacity to have slot alignment at l1
    auto aligned_capacity =
      p2roundup((int64_t)capacity,
        int64_t(l1_granularity * slotset_width * _children_per_slot()));
    size_t slot_count =
      aligned_capacity / l1_granularity / _children_per_slot();
    // we use set bit(s) as a marker for (partially) free entry
    l1.resize(slot_count, mark_as_free ? all_slot_set : all_slot_clear);

    // l0 slot count
    size_t slot_count_l0 = aligned_capacity / _alloc_unit / bits_per_slot;
    // we use set bit(s) as a marker for (partially) free entry
    l0.resize(slot_count_l0, mark_as_free ? all_slot_set : all_slot_clear);

    partial_l1_count = unalloc_l1_count = 0;
    if (mark_as_free) {
      unalloc_l1_count = slot_count * _children_per_slot();
      auto l0_pos_no_use = p2roundup((int64_t)capacity, (int64_t)l0_granularity) / l0_granularity;
      _mark_alloc_l1_l0(l0_pos_no_use, aligned_capacity / l0_granularity);
    }
  }

  struct search_ctx_t
  {
    size_t partial_count = 0;
    size_t free_count = 0;
    uint64_t free_l1_pos = 0;

    uint64_t min_affordable_len = 0;
    uint64_t min_affordable_offs = 0;
    uint64_t affordable_len = 0;
    uint64_t affordable_offs = 0;

    bool fully_processed = false;

    void reset()
    {
      *this = search_ctx_t();
    }
  };
  enum {
    NO_STOP,
    STOP_ON_EMPTY,
    STOP_ON_PARTIAL,
  };
  void _analyze_partials(uint64_t pos_start, uint64_t pos_end,
    uint64_t length, uint64_t min_length, int mode,
    search_ctx_t* ctx);

  void _mark_l1_on_l0(int64_t l0_pos, int64_t l0_pos_end);
  void _mark_alloc_l0(int64_t l0_pos_start, int64_t l0_pos_end);

  void _mark_alloc_l1_l0(int64_t l0_pos_start, int64_t l0_pos_end)
  {
    _mark_alloc_l0(l0_pos_start, l0_pos_end);
    l0_pos_start = p2align(l0_pos_start, int64_t(bits_per_slotset));
    l0_pos_end = p2roundup(l0_pos_end, int64_t(bits_per_slotset));
    _mark_l1_on_l0(l0_pos_start, l0_pos_end);
  }

  void _mark_free_l0(int64_t l0_pos_start, int64_t l0_pos_end)
  {
    auto d0 = CHILD_PER_SLOT_L0;

    auto pos = l0_pos_start;
    slot_t bits = (slot_t)1 << (l0_pos_start % d0);
    slot_t& val_s = l0[pos / d0];
    int64_t pos_e = std::min(l0_pos_end,
                             p2roundup<int64_t>(l0_pos_start + 1, d0));
    while (pos < pos_e) {
      val_s |= bits;
      bits <<= 1;
      pos++;
    }
    pos_e = std::min(l0_pos_end, p2align<int64_t>(l0_pos_end, d0));
    auto idx = pos / d0;
    while (pos < pos_e) {
      l0[idx++] = all_slot_set;
      pos += d0;
    }
    bits = 1;
    uint64_t& val_e = l0[pos / d0];
    while (pos < l0_pos_end) {
      val_e |= bits;
      bits <<= 1;
      pos++;
    }
  }

  void _mark_free_l1_l0(int64_t l0_pos_start, int64_t l0_pos_end)
  {
    _mark_free_l0(l0_pos_start, l0_pos_end);
    l0_pos_start = p2align(l0_pos_start, int64_t(bits_per_slotset));
    l0_pos_end = p2roundup(l0_pos_end, int64_t(bits_per_slotset));
    _mark_l1_on_l0(l0_pos_start, l0_pos_end);
  }

  bool _is_empty_l0(uint64_t l0_pos, uint64_t l0_pos_end)
  {
    bool no_free = true;
    uint64_t d = slotset_width * CHILD_PER_SLOT_L0;
    ceph_assert(0 == (l0_pos % d));
    ceph_assert(0 == (l0_pos_end % d));

    auto idx = l0_pos / CHILD_PER_SLOT_L0;
    auto idx_end = l0_pos_end / CHILD_PER_SLOT_L0;
    while (idx < idx_end && no_free) {
      no_free = l0[idx] == all_slot_clear;
      ++idx;
    }
    return no_free;
  }
  bool _is_empty_l1(uint64_t l1_pos, uint64_t l1_pos_end)
  {
    bool no_free = true;
    uint64_t d = slotset_width * _children_per_slot();
    ceph_assert(0 == (l1_pos % d));
    ceph_assert(0 == (l1_pos_end % d));

    auto idx = l1_pos / CHILD_PER_SLOT;
    auto idx_end = l1_pos_end / CHILD_PER_SLOT;
    while (idx < idx_end && no_free) {
      no_free = _is_slot_fully_allocated(idx);
      ++idx;
    }
    return no_free;
  }

  interval_t _allocate_l1_contiguous(uint64_t length,
    uint64_t min_length, uint64_t max_length,
    uint64_t pos_start, uint64_t pos_end);

  bool _allocate_l1(uint64_t length,
    uint64_t min_length, uint64_t max_length,
    uint64_t l1_pos_start, uint64_t l1_pos_end,
    uint64_t* allocated,
    interval_vector_t* res);

  uint64_t _mark_alloc_l1(uint64_t offset, uint64_t length)
  {
    uint64_t l0_pos_start = offset / l0_granularity;
    uint64_t l0_pos_end = p2roundup(offset + length, l0_granularity) / l0_granularity;
    _mark_alloc_l1_l0(l0_pos_start, l0_pos_end);
    return l0_granularity * (l0_pos_end - l0_pos_start);
  }

  uint64_t _free_l1(uint64_t offs, uint64_t len)
  {
    uint64_t l0_pos_start = offs / l0_granularity;
    uint64_t l0_pos_end = p2roundup(offs + len, l0_granularity) / l0_granularity;
    _mark_free_l1_l0(l0_pos_start, l0_pos_end);
    return l0_granularity * (l0_pos_end - l0_pos_start);
  }

public:
  uint64_t debug_get_allocated(uint64_t pos0 = 0, uint64_t pos1 = 0)
  {
    if (pos1 == 0) {
      pos1 = l1.size() * CHILD_PER_SLOT;
    }
    auto avail = debug_get_free(pos0, pos1);
    return (pos1 - pos0) * l1_granularity - avail;
  }

  uint64_t debug_get_free(uint64_t l1_pos0 = 0, uint64_t l1_pos1 = 0)
  {
    ceph_assert(0 == (l1_pos0 % CHILD_PER_SLOT));
    ceph_assert(0 == (l1_pos1 % CHILD_PER_SLOT));

    auto idx0 = l1_pos0 * slotset_width;
    auto idx1 = l1_pos1 * slotset_width;

    if (idx1 == 0) {
      idx1 = l0.size();
    }

    uint64_t res = 0;
    for (uint64_t i = idx0; i < idx1; ++i) {
      auto v = l0[i];
      if (v == all_slot_set) {
        res += CHILD_PER_SLOT_L0;
      } else if (v != all_slot_clear) {
        size_t cnt = 0;
#ifdef __GNUC__
        cnt = __builtin_popcountll(v);
#else
        // Kernighan's Alg to count set bits
        while (v) {
          v &= (v - 1);
          cnt++;
        }
#endif
        res += cnt;
      }
    }
    return res * l0_granularity;
  }
  void collect_stats(
    std::map<size_t, size_t>& bins_overall) override;
};

class AllocatorLevel01Compact : public AllocatorLevel01
{
  uint64_t _children_per_slot() const override
  {
    return 8;
  }
public:
  void collect_stats(
    std::map<size_t, size_t>& bins_overall) override
  {
    // not implemented
  }
};

template <class L1>
class AllocatorLevel02 : public AllocatorLevel
{
public:
  uint64_t debug_get_free(uint64_t pos0 = 0, uint64_t pos1 = 0)
  {
    std::lock_guard l(lock);
    return l1.debug_get_free(pos0 * l1._children_per_slot() * bits_per_slot,
      pos1 * l1._children_per_slot() * bits_per_slot);
  }
  uint64_t debug_get_allocated(uint64_t pos0 = 0, uint64_t pos1 = 0)
  {
    std::lock_guard l(lock);
    return l1.debug_get_allocated(pos0 * l1._children_per_slot() * bits_per_slot,
      pos1 * l1._children_per_slot() * bits_per_slot);
  }

  uint64_t get_available()
  {
    std::lock_guard l(lock);
    return available;
  }
  inline uint64_t get_min_alloc_size() const
  {
    return l1.get_min_alloc_size();
  }
  void collect_stats(
    std::map<size_t, size_t>& bins_overall) override {

      std::lock_guard l(lock);
      l1.collect_stats(bins_overall);
  }

protected:
  ceph::mutex lock = ceph::make_mutex("AllocatorLevel02::lock");
  L1 l1;
  slot_vector_t l2;
  uint64_t l2_granularity = 0; // space per entry
  uint64_t available = 0;
  uint64_t last_pos = 0;

  enum {
    CHILD_PER_SLOT = bits_per_slot, // 64
  };

  uint64_t _children_per_slot() const override
  {
    return CHILD_PER_SLOT;
  }
  uint64_t _level_granularity() const override
  {
    return l2_granularity;
  }

  void _init(uint64_t capacity, uint64_t _alloc_unit, bool mark_as_free = true)
  {
    ceph_assert(isp2(_alloc_unit));
    l1._init(capacity, _alloc_unit, mark_as_free);

    l2_granularity =
      l1._level_granularity() * l1._children_per_slot() * slotset_width;

    // capacity to have slot alignment at l2
    auto aligned_capacity =
      p2roundup((int64_t)capacity, (int64_t)l2_granularity * CHILD_PER_SLOT);
    size_t elem_count = aligned_capacity / l2_granularity / CHILD_PER_SLOT;
    // we use set bit(s) as a marker for (partially) free entry
    l2.resize(elem_count, mark_as_free ? all_slot_set : all_slot_clear);

    if (mark_as_free) {
      // capacity to have slotset alignment at l1
      auto l2_pos_no_use =
	p2roundup((int64_t)capacity, (int64_t)l2_granularity) / l2_granularity;
      _mark_l2_allocated(l2_pos_no_use, aligned_capacity / l2_granularity);
      available = p2align(capacity, _alloc_unit);
    } else {
      available = 0;
    }
  }

  void _mark_l2_allocated(int64_t l2_pos, int64_t l2_pos_end)
  {
    auto d = CHILD_PER_SLOT;
    ceph_assert(0 <= l2_pos_end);
    ceph_assert((int64_t)l2.size() >= (l2_pos_end / d));

    while (l2_pos < l2_pos_end) {
      l2[l2_pos / d] &= ~(slot_t(1) << (l2_pos % d));
      ++l2_pos;
    }
  }

  void _mark_l2_free(int64_t l2_pos, int64_t l2_pos_end)
  {
    auto d = CHILD_PER_SLOT;
    ceph_assert(0 <= l2_pos_end);
    ceph_assert((int64_t)l2.size() >= (l2_pos_end / d));

    while (l2_pos < l2_pos_end) {
        l2[l2_pos / d] |= (slot_t(1) << (l2_pos % d));
        ++l2_pos;
    }
  }

  void _mark_l2_on_l1(int64_t l2_pos, int64_t l2_pos_end)
  {
    auto d = CHILD_PER_SLOT;
    ceph_assert(0 <= l2_pos_end);
    ceph_assert((int64_t)l2.size() >= (l2_pos_end / d));

    auto idx = l2_pos * slotset_width;
    auto idx_end = l2_pos_end * slotset_width;
    bool all_allocated = true;
    while (idx < idx_end) {
      if (!l1._is_slot_fully_allocated(idx)) {
        all_allocated = false;
        idx = p2roundup(int64_t(++idx), int64_t(slotset_width));
      }
      else {
        ++idx;
      }
      if ((idx % slotset_width) == 0) {
        if (all_allocated) {
          l2[l2_pos / d] &= ~(slot_t(1) << (l2_pos % d));
        }
        else {
          l2[l2_pos / d] |= (slot_t(1) << (l2_pos % d));
        }
        all_allocated = true;
        ++l2_pos;
      }
    }
  }

  void _allocate_l2(uint64_t length,
    uint64_t min_length,
    uint64_t max_length,
    uint64_t hint,
    
    uint64_t* allocated,
    interval_vector_t* res)
  {
    uint64_t prev_allocated = *allocated;
    uint64_t d = CHILD_PER_SLOT;
    ceph_assert(isp2(min_length));
    ceph_assert(min_length <= l2_granularity);
    ceph_assert(max_length == 0 || max_length >= min_length);
    ceph_assert(max_length == 0 || (max_length % min_length) == 0);
    ceph_assert(length >= min_length);
    ceph_assert((length % min_length) == 0);

    uint64_t cap = 1ull << 31;
    if (max_length == 0 || max_length >= cap) {
      max_length = cap;
    }

    uint64_t l1_w = slotset_width * l1._children_per_slot();

    std::lock_guard l(lock);

    if (available < min_length) {
      return;
    }
    if (hint != 0) {
      last_pos = (hint / d) < l2.size() ? p2align(hint, d) : 0;
    }
    auto l2_pos = last_pos;
    auto last_pos0 = last_pos;
    auto pos = last_pos / d;
    auto pos_end = l2.size();
    // outer loop below is intended to optimize the performance by
    // avoiding 'modulo' operations inside the internal loop.
    // Looks like they have negative impact on the performance
    for (auto i = 0; i < 2; ++i) {
      for(; length > *allocated && pos < pos_end; ++pos) {
	slot_t& slot_val = l2[pos];
	size_t free_pos = 0;
	bool all_set = false;
	if (slot_val == all_slot_clear) {
	  l2_pos += d;
	  last_pos = l2_pos;
	  continue;
	} else if (slot_val == all_slot_set) {
	  free_pos = 0;
	  all_set = true;
	} else {
	  free_pos = find_next_set_bit(slot_val, 0);
	  ceph_assert(free_pos < bits_per_slot);
	}
	do {
	  ceph_assert(length > *allocated);
	  bool empty = l1._allocate_l1(length,
	    min_length,
	    max_length,
	    (l2_pos + free_pos) * l1_w,
	    (l2_pos + free_pos + 1) * l1_w,
	    allocated,
	    res);
	  if (empty) {
	    slot_val &= ~(slot_t(1) << free_pos);
	  }
	  if (length <= *allocated || slot_val == all_slot_clear) {
	    break;
	  }
	  ++free_pos;
	  if (!all_set) {
	    free_pos = find_next_set_bit(slot_val, free_pos);
	  }
	} while (free_pos < bits_per_slot);
	last_pos = l2_pos;
	l2_pos += d;
      }
      l2_pos = 0;
      pos = 0;
      pos_end = last_pos0 / d;
    }

    ++l2_allocs;
    auto allocated_here = *allocated - prev_allocated;
    ceph_assert(available >= allocated_here);
    available -= allocated_here;
  }

#ifndef NON_CEPH_BUILD
  // to provide compatibility with BlueStore's allocator interface
  void _free_l2(const interval_set<uint64_t> & rr)
  {
    uint64_t released = 0;
    std::lock_guard l(lock);
    for (auto r : rr) {
      released += l1._free_l1(r.first, r.second);
      uint64_t l2_pos = r.first / l2_granularity;
      uint64_t l2_pos_end = p2roundup(int64_t(r.first + r.second), int64_t(l2_granularity)) / l2_granularity;

      _mark_l2_free(l2_pos, l2_pos_end);
    }
    available += released;
  }
#endif

  template <typename T>
  void _free_l2(const T& rr)
  {
    uint64_t released = 0;
    std::lock_guard l(lock);
    for (auto r : rr) {
      released += l1._free_l1(r.offset, r.length);
      uint64_t l2_pos = r.offset / l2_granularity;
      uint64_t l2_pos_end = p2roundup(int64_t(r.offset + r.length), int64_t(l2_granularity)) / l2_granularity;

      _mark_l2_free(l2_pos, l2_pos_end);
    }
    available += released;
  }

  void _mark_allocated(uint64_t o, uint64_t len)
  {
    uint64_t l2_pos = o / l2_granularity;
    uint64_t l2_pos_end = p2roundup(int64_t(o + len), int64_t(l2_granularity)) / l2_granularity;

    std::lock_guard l(lock);
    auto allocated = l1._mark_alloc_l1(o, len);
    ceph_assert(available >= allocated);
    available -= allocated;
    _mark_l2_on_l1(l2_pos, l2_pos_end);
  }

  void _mark_free(uint64_t o, uint64_t len)
  {
    uint64_t l2_pos = o / l2_granularity;
    uint64_t l2_pos_end = p2roundup(int64_t(o + len), int64_t(l2_granularity)) / l2_granularity;

    std::lock_guard l(lock);
    available += l1._free_l1(o, len);
    _mark_l2_free(l2_pos, l2_pos_end);
  }
  void _shutdown()
  {
    last_pos = 0;
  }
  double _get_fragmentation() {
    std::lock_guard l(lock);
    return l1.get_fragmentation();
  }
};

#endif
