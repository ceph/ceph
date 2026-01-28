// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <bit>
#include <memory>

#include "include/interval_set.h"
#include "include/intarith.h"

/**
 * hybrid_interval_set
 *
 * Optimized interval_set that uses inline storage for 0-1 intervals,
 * falling back to interval_set for 2+ intervals.
 *
 * This eliminates heap allocations for the common case of single-interval operations.
 *
 * Performance characteristics:
 * - 0-1 intervals: Zero allocations (inline storage)
 * - 2+ intervals: Falls back to interval_set (one allocation for the set)
 *
 * One-way transition policy:
 * Once upgraded to multi-mode (2+ intervals), the data structure stays in multi-mode
 * even if reduced back to 0-1 intervals. This prevents allocation/deallocation
 * oscillation in workloads that alternate between single and multiple intervals.
 * Multi-mode is just as efficient as single-mode for 0-1 intervals (same algorithmic
 * complexity), so the only cost is the retained heap allocation. This trade-off
 * favors performance stability over memory efficiency.
 *
 * The behavior matches interval_set::clear(), which also retains allocated memory.
 *
 * Template parameters match interval_set:
 * - T: The type of the interval boundaries (e.g., uint64_t)
 * - C: The container type (default: std::map)
 * - strict: Whether to use strict interval semantics (default: true)
 */
template <
    typename T,
    template <typename, typename, typename...> class C = std::map,
    bool strict = true>
class hybrid_interval_set {
private:
  // Single interval stored inline (no allocation)
  struct single_interval {
    T offset = 0;
    T length = 0;

    bool empty() const
    {
      return length == 0;
    }

    void clear()
    {
      offset = 0;
      length = 0;
    }
  };

  single_interval single;
  std::unique_ptr<interval_set<T, C, strict>> multi;

public:
  // Check if in single mode (for testing/debugging)
  bool is_single() const
  {
    return !multi;
  }

private:

  void upgrade_to_multi()
  {
    //ceph_abort_msg("Unexpected multi");
    multi = std::make_unique<interval_set<T, C, strict>>();
    if (!single.empty()) {
      multi->insert(single.offset, single.length);
      single.clear();
    }
  }

public:
  hybrid_interval_set() = default;

  // Copy constructor
  hybrid_interval_set(const hybrid_interval_set& other) :
    single(other.single),
    multi(
        other.multi ? std::make_unique<interval_set<T, C, strict>>(*other.multi)
                    : nullptr)
  {}

  // Move constructor uses compiler default
  hybrid_interval_set(hybrid_interval_set&&) noexcept = default;

  // Copy assignment needs custom implementation for unique_ptr deep copy
  hybrid_interval_set& operator=(const hybrid_interval_set& other)
  {
    if (this != &other) {
      single = other.single;
      multi = other.multi
                  ? std::make_unique<interval_set<T, C, strict>>(*other.multi)
                  : nullptr;
    }
    return *this;
  }

  // Move assignment uses compiler default
  hybrid_interval_set& operator=(hybrid_interval_set&&) noexcept = default;

  void clear()
  {
    // Preserve multi-mode allocation to avoid oscillation between modes.
    // Once upgraded, we stay upgraded even when empty.
    if (multi) {
      multi->clear();
    } else {
      single.clear();
    }
  }

  bool empty() const
  {
    return is_single() ? single.empty() : multi->empty();
  }

  size_t num_intervals() const
  {
    if (is_single()) {
      return single.empty() ? 0 : 1;
    }
    return multi->num_intervals();
  }

  void insert(T offset, T length)
  {
    if (length == 0) {
      return;
    }

    if (is_single()) {
      if (single.empty()) {
        // First insert - use single interval
        single.offset = offset;
        single.length = length;
        return;
      }

      // Check if intervals overlap or touch - can merge and stay single
      T single_end = single.offset + single.length;
      T insert_end = offset + length;
      
      // Intervals overlap/touch if they're not completely disjoint
      // This covers: adjacent (append/prepend), overlapping, and contained cases
      if (!(insert_end < single.offset || offset > single_end)) {
        // Merge them into a single interval
        T new_start = std::min(single.offset, offset);
        T new_end = std::max(single_end, insert_end);
        single.offset = new_start;
        single.length = new_end - new_start;
        return;
      }

      // Can't merge - need to upgrade to multi
      upgrade_to_multi();
    }

    // Use multi path
    multi->insert(offset, length);
  }

  // Alias for insert (for compatibility)
  void union_insert(T offset, T length)
  {
    insert(offset, length);
  }

  void erase(T offset, T length)
  {
    if (length == 0) {
      return;
    }

    if (is_single()) {
      if (single.empty()) {
        return;
      }

      T end = offset + length;
      T single_end = single.offset + single.length;

      // Check for complete erasure
      if (offset <= single.offset && end >= single_end) {
        single.clear();
        return;
      }

      // Check for no overlap
      if (end <= single.offset || offset >= single_end) {
        return;
      }

      // Partial overlap - check if we can handle in single mode
      // Case 1: Erase from start (trim left)
      if (offset <= single.offset && end < single_end) {
        single.length = single_end - end;
        single.offset = end;
        return;
      }

      // Case 2: Erase from end (trim right)
      if (offset > single.offset && end >= single_end) {
        single.length = offset - single.offset;
        return;
      }

      // Case 3: Erase from middle - creates two intervals, need multi mode
      upgrade_to_multi();
    }

    multi->erase(offset, length);
  }

  // Erase everything after offset
  void erase_after(T offset)
  {
    if (is_single()) {
      if (single.empty()) {
        return;
      }
      if (offset <= single.offset) {
        single.clear();
      } else if (offset < single.offset + single.length) {
        single.length = offset - single.offset;
      }
    } else {
      multi->erase_after(offset);
    }
  }

  bool contains(T offset, T length) const
  {
    if (is_single()) {
      if (single.empty()) {
        return false;
      }
      return offset >= single.offset &&
             (offset + length) <= (single.offset + single.length);
    }
    return multi->contains(offset, length);
  }

  // Check if this set contains all intervals in another set
  bool contains(const hybrid_interval_set& other) const
  {
    if (other.is_single()) {
      if (other.single.empty()) {
        return true;
      }
      return contains(other.single.offset, other.single.length);
    }
    // Other is multi - need to check all its intervals
    if (is_single()) {
      // Check if our single interval contains all of other's intervals
      if (single.empty()) {
        return false;
      }
      for (const auto& [offset, length] : *other.multi) {
        if (!contains(offset, length)) {
          return false;
        }
      }
      return true;
    }
    // Both are multi - use interval_set's contains
    return multi->contains(*other.multi);
  }

  // Convert to interval_set (for compatibility with interval_map::to_interval_set)
  template <template <typename, typename, typename...> class C2, bool strict2>
  void to_interval_set(interval_set<T, C2, strict2>& out) const
  {
    if (is_single()) {
      if (!single.empty()) {
        out.insert(single.offset, single.length);
      }
    } else {
      for (auto it = multi->begin(); it != multi->end(); ++it) {
        out.insert(it.get_start(), it.get_len());
      }
    }
  }

  bool intersects(T offset, T length) const
  {
    if (is_single()) {
      if (single.empty()) {
        return false;
      }
      T end = offset + length;
      T single_end = single.offset + single.length;
      return !(end <= single.offset || offset >= single_end);
    }
    return multi->intersects(offset, length);
  }

  // Get the total span (start to end) - requires non-empty set
  T range_start() const
  {
    ceph_assert(!empty());
    if (is_single()) {
      return single.offset;
    }
    return multi->range_start();
  }

  T range_end() const
  {
    ceph_assert(!empty());
    if (is_single()) {
      return single.offset + single.length;
    }
    return multi->range_end();
  }

  // Get total size (sum of all interval lengths)
  T size() const
  {
    if (is_single()) {
      return single.length;
    }
    return multi->size();
  }

  // Union with another interval_set
  void union_of(const hybrid_interval_set& other)
  {
    if (other.empty()) {
      return;
    }

    if (other.is_single()) {
      insert(other.single.offset, other.single.length);
    } else if (!is_single()) {
      // Both are multi - use interval_set insert
      multi->insert(*other.multi);
    } else {
      // This is single, other is multi - iterate and insert
      for (auto it = other.multi->begin(); it != other.multi->end(); ++it) {
        insert(it.get_start(), it.get_len());
      }
    }
  }

  // Union of two sets (sets this to union of a and b)
  void union_of(const hybrid_interval_set& a, const hybrid_interval_set& b)
  {
    clear();
    insert(a);
    insert(b);
  }

  // Check if this is a subset of another interval_set
  bool subset_of(const hybrid_interval_set& other) const
  {
    if (empty()) {
      return true;
    }
    if (other.empty()) {
      return false;
    }

    // Check each interval in this set is contained in other
    if (is_single()) {
      return other.contains(single.offset, single.length);
    }

    for (auto it = multi->begin(); it != multi->end(); ++it) {
      if (!other.contains(it.get_start(), it.get_len())) {
        return false;
      }
    }
    return true;
  }

  // Span of two sets (sets this to span from min to max of a and b)
  void span_of(const hybrid_interval_set& a, const hybrid_interval_set& b)
  {
    clear();

    if (a.empty() && b.empty()) {
      return;
    }

    // Find min and max across both sets
    T min_start = std::numeric_limits<T>::max();
    T max_end = 0;

    if (!a.empty()) {
      if (a.is_single()) {
        min_start = std::min(min_start, a.single.offset);
        max_end = std::max(max_end, a.single.offset + a.single.length);
      } else {
        for (auto it = a.multi->begin(); it != a.multi->end(); ++it) {
          min_start = std::min(min_start, it.get_start());
          max_end = std::max(max_end, it.get_start() + it.get_len());
        }
      }
    }

    if (!b.empty()) {
      if (b.is_single()) {
        min_start = std::min(min_start, b.single.offset);
        max_end = std::max(max_end, b.single.offset + b.single.length);
      } else {
        for (auto it = b.multi->begin(); it != b.multi->end(); ++it) {
          min_start = std::min(min_start, it.get_start());
          max_end = std::max(max_end, it.get_start() + it.get_len());
        }
      }
    }

    insert(min_start, max_end - min_start);
  }

  // Insert from another interval_set
  void insert(const hybrid_interval_set& other)
  {
    union_of(other);
  }

  // Subtract another interval_set from this one
  void subtract(const hybrid_interval_set& other)
  {
    if (empty() || other.empty()) {
      return;
    }

    if (other.is_single()) {
      erase(other.single.offset, other.single.length);
    } else if (!is_single()) {
      // Both are multi - use optimized interval_set subtract
      multi->subtract(*other.multi);
    } else {
      // This is single, other is multi - iterate and erase
      for (auto it = other.multi->begin(); it != other.multi->end(); ++it) {
        erase(it.get_start(), it.get_len());
      }
    }
  }

  // Intersection with another interval_set (modifies this)
  void intersection_of(const hybrid_interval_set& other)
  {
    if (empty() || other.empty()) {
      clear();
      return;
    }

    // Optimize single-single case: result is always 0 or 1 intervals
    if (is_single() && other.is_single()) {
      T start = std::max(single.offset, other.single.offset);
      T end = std::min(single.offset + single.length,
                       other.single.offset + other.single.length);
      if (start < end) {
        single.offset = start;
        single.length = end - start;
      } else {
        clear();
      }
      return;
    }

    // For other cases, use multi mode
    if (is_single()) {
      upgrade_to_multi();
    }

    if (other.is_single()) {
      // Intersect with single interval
      hybrid_interval_set tmp;
      tmp.insert(other.single.offset, other.single.length);
      tmp.upgrade_to_multi();
      multi->intersection_of(*tmp.multi);
    } else {
      multi->intersection_of(*other.multi);
    }
  }

  // Intersection of two sets (sets this to intersection of a and b)
  void intersection_of(const hybrid_interval_set& a, const hybrid_interval_set& b)
  {
    clear();

    if (a.empty() || b.empty()) {
      return;
    }

    // Simple case: both single
    if (a.is_single() && b.is_single()) {
      T start = std::max(a.single.offset, b.single.offset);
      T a_end = a.single.offset + a.single.length;
      T b_end = b.single.offset + b.single.length;
      T end = std::min(a_end, b_end);

      if (start < end) {
        insert(start, end - start);
      }
      return;
    }

    // Need multi mode for complex intersection
    upgrade_to_multi();

    if (a.is_single()) {
      hybrid_interval_set tmp_a;
      tmp_a.insert(a.single.offset, a.single.length);
      tmp_a.upgrade_to_multi();

      if (b.is_single()) {
        hybrid_interval_set tmp_b;
        tmp_b.insert(b.single.offset, b.single.length);
        tmp_b.upgrade_to_multi();
        multi->intersection_of(*tmp_a.multi, *tmp_b.multi);
      } else {
        multi->intersection_of(*tmp_a.multi, *b.multi);
      }
    } else if (b.is_single()) {
      hybrid_interval_set tmp_b;
      tmp_b.insert(b.single.offset, b.single.length);
      tmp_b.upgrade_to_multi();
      multi->intersection_of(*a.multi, *tmp_b.multi);
    } else {
      multi->intersection_of(*a.multi, *b.multi);
    }
  }

  // Align intervals to alignment boundary
  void align(T alignment)
  {
    if (alignment == 0) {
      return;
    }

    if (is_single()) {
      if (!single.empty()) {
        // Use efficient bit manipulation for power-of-2 alignments
        if (std::has_single_bit(alignment)) {
          T aligned_start = p2align(single.offset, alignment);
          T last_byte = single.offset + single.length - 1;
          T aligned_end = p2roundup(last_byte + 1, alignment);
          single.offset = aligned_start;
          single.length = aligned_end - aligned_start;
        } else {
          // Fall back to division for non-power-of-2 alignments
          T aligned_start = (single.offset / alignment) * alignment;
          T last_byte = single.offset + single.length - 1;
          T aligned_end = ((last_byte / alignment) + 1) * alignment;
          single.offset = aligned_start;
          single.length = aligned_end - aligned_start;
        }
      }
    } else {
      multi->align(alignment);
    }
  }

  // Equality operators
  bool operator==(const hybrid_interval_set& other) const
  {
    if (is_single() && other.is_single()) {
      if (single.empty() && other.single.empty()) {
        return true;
      }
      return single.offset == other.single.offset &&
             single.length == other.single.length;
    }

    if (is_single() != other.is_single()) {
      // One is single, other is multi - need to compare contents
      if (is_single()) {
        if (single.empty()) {
          return other.multi->empty();
        }
        if (other.multi->num_intervals() != 1) {
          return false;
        }
        auto it = other.multi->begin();
        return single.offset == it.get_start() && single.length == it.get_len();
      } else {
        // Reverse comparison: if other is single and this is multi,
        // just swap the operands
        return other == *this;
      }
    }

    // Both are multi
    return *multi == *other.multi;
  }

  bool operator!=(const hybrid_interval_set& other) const
  {
    return !(*this == other);
  }

  // Iterator support - for compatibility with interval_set
  class const_iterator {
    const hybrid_interval_set* parent;
    bool at_single;
    bool at_end;
    std::optional<typename interval_set<T, C, strict>::const_iterator> multi_iter;

  public:
    using difference_type = ssize_t;
    using value_type = std::pair<T, T>;
    using pointer = const value_type*;
    using reference = const value_type&;
    using iterator_category = std::forward_iterator_tag;

    const_iterator(const hybrid_interval_set* p, bool end_iter) :
      parent(p),
      at_single(!end_iter && p->is_single() && !p->single.empty()),
      at_end(end_iter || (p->is_single() && p->single.empty()))
    {
      if (!p->is_single()) {
        multi_iter = end_iter ? p->multi->end() : p->multi->begin();
        // If we're in multi mode and not explicitly at end, check if multi is empty
        if (!end_iter && p->multi->empty()) {
          at_end = true;
        } else if (!end_iter) {
          at_end = (*multi_iter == p->multi->end());
        }
      }
    }

    const_iterator(
        const hybrid_interval_set* p,
        typename interval_set<T, C, strict>::const_iterator it) :
      parent(p), at_single(false), at_end(false), multi_iter(it)
    {
      const auto& m = *p->multi;
      at_end = (it == m.end());
    }

    bool operator==(const const_iterator& other) const
    {
      if (parent != other.parent) {
        return false;
      }
      if (at_end && other.at_end) {
        return true;
      }
      if (at_end != other.at_end) {
        return false;
      }
      if (at_single != other.at_single) {
        return false;
      }
      if (multi_iter.has_value() && other.multi_iter.has_value()) {
        return *multi_iter == *other.multi_iter;
      }
      return !multi_iter.has_value() && !other.multi_iter.has_value();
    }

    bool operator!=(const const_iterator& other) const
    {
      return !(*this == other);
    }

    // Return value_type for structured bindings
    value_type operator*() const
    {
      if (at_single) {
        return {parent->single.offset, parent->single.length};
      }
      return {multi_iter->get_start(), multi_iter->get_len()};
    }

    const_iterator& operator++()
    {
      if (at_single) {
        at_single = false;
        at_end = true;
      } else if (multi_iter.has_value() && !at_end) {
        ++(*multi_iter);
        const auto& m = *parent->multi;
        at_end = (*multi_iter == m.end());
      }
      return *this;
    }

    const_iterator operator++(int)
    {
      const_iterator tmp = *this;
      ++(*this);
      return tmp;
    }

    T get_start() const
    {
      return at_single ? parent->single.offset : multi_iter->get_start();
    }

    T get_len() const
    {
      return at_single ? parent->single.length : multi_iter->get_len();
    }

    T get_end() const
    {
      return get_start() + get_len();
    }
  };

  const_iterator begin() const
  {
    return const_iterator(this, false);
  }

  const_iterator end() const
  {
    if (is_single()) {
      return const_iterator(this, true);
    }
    return const_iterator(this, multi->end());
  }

  // Debugging
  friend std::ostream& operator<<(std::ostream& os, const hybrid_interval_set& s)
  {
    if (s.is_single()) {
      if (s.single.empty()) {
        os << "{}";
      } else {
        os << "{[" << s.single.offset << "," << s.single.length << "]}";
      }
    } else {
      os << *s.multi;
    }
    return os;
  }
};
