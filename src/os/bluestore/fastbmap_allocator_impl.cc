// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Bitmap based in-memory allocator implementation.
 * Author: Igor Fedotov, ifedotov@suse.com
 *
 */

#include "fastbmap_allocator_impl.h"

uint64_t AllocatorLevel::l0_dives = 0;
uint64_t AllocatorLevel::l0_iterations = 0;
uint64_t AllocatorLevel::l0_inner_iterations = 0;
uint64_t AllocatorLevel::alloc_fragments = 0;
uint64_t AllocatorLevel::alloc_fragments_fast = 0;
uint64_t AllocatorLevel::l2_allocs = 0;

void AllocatorLevel01Loose::_analyze_partials(uint64_t pos_start,
  uint64_t pos_end, uint64_t length, uint64_t min_length, int mode,
  search_ctx_t* ctx)
{
  auto d = CHILD_PER_SLOT;
  assert((pos_start % d) == 0);
  assert((pos_end % d) == 0);

  uint64_t l0_w = slotset_width * CHILD_PER_SLOT_L0;

  uint64_t l1_pos = pos_start;
  bool prev_pos_partial = false;
  uint64_t next_free_l1_pos = 0;
  for (auto pos = pos_start / d; pos < pos_end / d; ++pos) {
    slot_t slot_val = l1[pos];
    // FIXME minor: code below can be optimized to check slot_val against
    // all_slot_set(_clear) value

    for (auto c = 0; c < d; c++) {
      switch (slot_val & L1_ENTRY_MASK) {
      case L1_ENTRY_FREE:
        prev_pos_partial = false;
        if (!ctx->free_count) {
          ctx->free_l1_pos = l1_pos;
        } else if (l1_pos != next_free_l1_pos){
          // check if already found extent fits min_length
	  if (ctx->free_count * l1_granularity >= min_length) {
	    break;
	  }
	  // if not - proceed with the next one
          ctx->free_l1_pos = l1_pos;
          ctx->free_count = 0;
	}
	next_free_l1_pos = l1_pos + 1;
        ++ctx->free_count;
        if (mode == STOP_ON_EMPTY) {
          return;
        }
        break;
      case L1_ENTRY_FULL:
        prev_pos_partial = false;
        break;
      case L1_ENTRY_PARTIAL:
        uint64_t l;
        uint64_t p0 = 0;
        ++ctx->partial_count;

        if (!prev_pos_partial) {
          l = _get_longest_from_l0(l1_pos * l0_w, (l1_pos + 1) * l0_w, &p0);
          prev_pos_partial = true;
        } else {
          l = _get_longest_from_l0((l1_pos - 1) * l0_w, (l1_pos + 1) * l0_w, &p0);
        }
        if (l >= length) {
          if ((ctx->affordable_len == 0) ||
              ((ctx->affordable_len != 0) &&
                (l < ctx->affordable_len))) {
            ctx->affordable_len = l;
            ctx->affordable_l0_pos_start = p0;
          }
        }
        if (l >= min_length &&
	    (ctx->min_affordable_len == 0 ||
	      (l < ctx->min_affordable_len))) {

          ctx->min_affordable_len = p2align(l, min_length);
          ctx->min_affordable_l0_pos_start = p0;
        }
        if (mode == STOP_ON_PARTIAL) {
          return;
        }
        break;
      }
      slot_val >>= L1_ENTRY_WIDTH;
      ++l1_pos;
    }
  }
  ctx->fully_processed = true;
}

void AllocatorLevel01Loose::_mark_l1_on_l0(int64_t l0_pos, int64_t l0_pos_end)
{
  if (l0_pos == l0_pos_end) {
    return;
  }
  auto d0 = bits_per_slotset;
  uint64_t l1_w = CHILD_PER_SLOT;
  // this should be aligned with slotset boundaries
  assert(0 == (l0_pos % d0));
  assert(0 == (l0_pos_end % d0));

  int64_t idx = l0_pos / bits_per_slot;
  int64_t idx_end = l0_pos_end / bits_per_slot;
  bool was_all_free = true;
  bool was_all_allocated = true;

  auto l1_pos = l0_pos / d0;

  while (idx < idx_end) {
    if (l0[idx] == all_slot_clear) {
      was_all_free = false;

      // if not all prev slots are allocated then no need to check the
      // current slot set, it's partial
      ++idx;
      idx =
        was_all_allocated ? idx : p2roundup(idx, int64_t(slotset_width));
    } else if (l0[idx] == all_slot_set) {
      // all free
      was_all_allocated = false;
      // if not all prev slots are free then no need to check the
      // current slot set, it's partial
      ++idx;
      idx = was_all_free ? idx : p2roundup(idx, int64_t(slotset_width));
    } else {
      // no need to check the current slot set, it's partial
      was_all_free = false;
      was_all_allocated = false;
      ++idx;
      idx = p2roundup(idx, int64_t(slotset_width));
    }
    if ((idx % slotset_width) == 0) {

      uint64_t shift = (l1_pos % l1_w) * L1_ENTRY_WIDTH;
      slot_t& slot_val = l1[l1_pos / l1_w];
        slot_val &= ~(uint64_t(L1_ENTRY_MASK) << shift);

      if (was_all_allocated) {
        assert(!was_all_free);
        slot_val |= uint64_t(L1_ENTRY_FULL) << shift;
      } else if (was_all_free) {
        assert(!was_all_allocated);
        slot_val |= uint64_t(L1_ENTRY_FREE) << shift;
      } else {
        slot_val |= uint64_t(L1_ENTRY_PARTIAL) << shift;
      }
      was_all_free = true;
      was_all_allocated = true;
      ++l1_pos;
    }
  }
}

void AllocatorLevel01Loose::_mark_alloc_l0(int64_t l0_pos_start,
  int64_t l0_pos_end)
{
  auto d0 = CHILD_PER_SLOT_L0;

  int64_t pos = l0_pos_start;
  slot_t bits = (slot_t)1 << (l0_pos_start % d0);

  while (pos < std::min(l0_pos_end, (int64_t)p2roundup(l0_pos_start, d0))) {
    l0[pos / d0] &= ~bits;
    bits <<= 1;
    pos++;
  }

  while (pos < std::min(l0_pos_end, (int64_t)p2align(l0_pos_end, d0))) {
    l0[pos / d0] = all_slot_clear;
    pos += d0;
  }
  bits = 1;
  while (pos < l0_pos_end) {
    l0[pos / d0] &= ~bits;
    bits <<= 1;
    pos++;
  }
}

interval_t AllocatorLevel01Loose::_allocate_l1_contiguous(uint64_t length,
  uint64_t min_length, uint64_t max_length,
  uint64_t pos_start, uint64_t pos_end)
{
  interval_t res = { 0, 0 };
  uint64_t l0_w = slotset_width * CHILD_PER_SLOT_L0;

  if (unlikely(length <= l0_granularity)) {
    search_ctx_t ctx;
    _analyze_partials(pos_start, pos_end, l0_granularity, l0_granularity,
      STOP_ON_PARTIAL, &ctx);

    // check partially free slot sets first (including neighboring),
    // full length match required.
    if (ctx.affordable_len) {
      // allocate as specified
      assert(ctx.affordable_len >= length);
      auto pos_end = ctx.affordable_l0_pos_start + 1;
      _mark_alloc_l1_l0(ctx.affordable_l0_pos_start, pos_end);
      res = interval_t(ctx.affordable_l0_pos_start * l0_granularity, length);
      return res;
    }

    // allocate from free slot sets
    if (ctx.free_count) {
      auto l = std::min(length, ctx.free_count * l1_granularity);
      assert((l % l0_granularity) == 0);
      auto pos_end = ctx.free_l1_pos * l0_w + l / l0_granularity;

      _mark_alloc_l1_l0(ctx.free_l1_pos * l0_w, pos_end);
      res = interval_t(ctx.free_l1_pos * l1_granularity, l);
      return res;
    }
  } else if (unlikely(length == l1_granularity)) {
    search_ctx_t ctx;
    _analyze_partials(pos_start, pos_end, length, min_length, STOP_ON_EMPTY, &ctx);

    // allocate using contiguous extent found at l1 if any
    if (ctx.free_count) {

      auto l = std::min(length, ctx.free_count * l1_granularity);
      assert((l % l0_granularity) == 0);
      auto pos_end = ctx.free_l1_pos * l0_w + l / l0_granularity;

      _mark_alloc_l1_l0(ctx.free_l1_pos * l0_w, pos_end);
      res = interval_t(ctx.free_l1_pos * l1_granularity, l);

      return res;
    }

    // we can terminate earlier on free entry only
    assert(ctx.fully_processed);

    // check partially free slot sets first (including neighboring),
    // full length match required.
    if (ctx.affordable_len) {
      assert(ctx.affordable_len >= length);
      assert((length % l0_granularity) == 0);
      auto pos_end = ctx.affordable_l0_pos_start + length / l0_granularity;
      _mark_alloc_l1_l0(ctx.affordable_l0_pos_start, pos_end);
      res = interval_t(ctx.affordable_l0_pos_start * l0_granularity, length);
      return res;
    }
    if (ctx.min_affordable_len) {
      auto pos0 = ctx.min_affordable_l0_pos_start;
      _mark_alloc_l1_l0(pos0, pos0 + ctx.min_affordable_len / l0_granularity);
      return interval_t(pos0 * l0_granularity, ctx.min_affordable_len);
    }
  } else {
    search_ctx_t ctx;
    _analyze_partials(pos_start, pos_end, length, min_length, NO_STOP, &ctx);
    assert(ctx.fully_processed);
    // check partially free slot sets first (including neighboring),
    // full length match required.
    if (ctx.affordable_len) {
      assert(ctx.affordable_len >= length);
      assert((length % l0_granularity) == 0);
      auto pos_end = ctx.affordable_l0_pos_start + length / l0_granularity;
      _mark_alloc_l1_l0(ctx.affordable_l0_pos_start, pos_end);
      res = interval_t(ctx.affordable_l0_pos_start * l0_granularity, length);
      return res;
    }
    // allocate using contiguous extent found at l1 if affordable
    if (ctx.free_count && ctx.free_count * l1_granularity >= min_length) {

      auto l = p2align(std::min(length, ctx.free_count * l1_granularity),
	min_length);
      assert((l % l0_granularity) == 0);
      auto pos_end = ctx.free_l1_pos * l0_w + l / l0_granularity;

      _mark_alloc_l1_l0(ctx.free_l1_pos * l0_w, pos_end);
      res = interval_t(ctx.free_l1_pos * l1_granularity, l);
      return res;
    }
    if (ctx.min_affordable_len) {
      auto pos0 = ctx.min_affordable_l0_pos_start;
      _mark_alloc_l1_l0(pos0, pos0 + ctx.min_affordable_len / l0_granularity);
      return interval_t(pos0 * l0_granularity, ctx.min_affordable_len);
    }
  }
  return res;
}

bool AllocatorLevel01Loose::_allocate_l1(uint64_t length,
  uint64_t min_length, uint64_t max_length,
  uint64_t l1_pos_start, uint64_t l1_pos_end,
  uint64_t* allocated,
  interval_vector_t* res)
{
  uint64_t d0 = CHILD_PER_SLOT_L0;
  uint64_t d1 = CHILD_PER_SLOT;

  assert(0 == (l1_pos_start % (slotset_width * d1)));
  assert(0 == (l1_pos_end % (slotset_width * d1)));
  if (min_length != l0_granularity) {
    // probably not the most effecient way but
    // don't care much about that at the moment
    bool has_space = true;
    while (length > *allocated && has_space) {
      interval_t i =
        _allocate_l1_contiguous(length - *allocated, min_length, max_length,
	  l1_pos_start, l1_pos_end);
      if (i.length == 0) {
        has_space = false;
      } else {
	_fragment_and_emplace(max_length, i.offset, i.length, res);
        *allocated += i.length;
      }
    }
  } else {
    uint64_t l0_w = slotset_width * d0;

    for (auto idx = l1_pos_start / d1;
      idx < l1_pos_end / d1 && length > *allocated;
      ++idx) {
      slot_t& slot_val = l1[idx];
      if (slot_val == all_slot_clear) {
        continue;
      } else if (slot_val == all_slot_set) {
        uint64_t to_alloc = std::min(length - *allocated,
          l1_granularity * d1);
        *allocated += to_alloc;
        ++alloc_fragments_fast;
	_fragment_and_emplace(max_length, idx * d1 * l1_granularity, to_alloc,
	  res);
        _mark_alloc_l1_l0(idx * d1 * bits_per_slotset,
	  idx * d1 * bits_per_slotset + to_alloc / l0_granularity);
        continue;
      }
      auto free_pos = find_next_set_bit(slot_val, 0);
      assert(free_pos < bits_per_slot);
      do {
        assert(length > *allocated);

        bool empty;
        empty = _allocate_l0(length, max_length,
	  (idx * d1 + free_pos / L1_ENTRY_WIDTH) * l0_w,
          (idx * d1 + free_pos / L1_ENTRY_WIDTH + 1) * l0_w,
          allocated,
          res);
        slot_val &= (~slot_t(L1_ENTRY_MASK)) << free_pos;
        if (empty) {
          // the next line is no op with the current L1_ENTRY_FULL but left
          // as-is for the sake of uniformity and to avoid potential errors
          // in future
          slot_val |= slot_t(L1_ENTRY_FULL) << free_pos;
        } else {
          slot_val |= slot_t(L1_ENTRY_PARTIAL) << free_pos;
        }
        if (length <= *allocated || slot_val == all_slot_clear) {
          break;
        }
	free_pos = find_next_set_bit(slot_val, free_pos + L1_ENTRY_WIDTH);
      } while (free_pos < bits_per_slot);
    }
  }
  return _is_empty_l1(l1_pos_start, l1_pos_end);
}
