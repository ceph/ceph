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

#include <memory>

#include "common/interval_map.h"
#include "include/hybrid_interval_set.h"

/**
 * hybrid_interval_map
 *
 * Optimized interval_map that uses inline storage for 0-1 intervals,
 * falling back to interval_map for 2+ intervals.
 *
 * This eliminates heap allocations for the common case of single-interval operations.
 *
 * Performance characteristics:
 * - 0-1 intervals: Zero allocations (inline storage)
 * - 2+ intervals: Falls back to interval_map (one allocation for the map)
 *
 * One-way transition policy:
 * Once upgraded to multi-mode (2+ intervals), the data structure stays in multi-mode
 * even if reduced back to 0-1 intervals. This prevents allocation/deallocation
 * oscillation in workloads that alternate between single and multiple intervals.
 * Multi-mode is just as efficient as single-mode for 0-1 intervals (same algorithmic
 * complexity), so the only cost is the retained heap allocation. This trade-off
 * favors performance stability over memory efficiency.
 *
 * The behavior matches interval_map::clear(), which also retains allocated memory.
 *
 * Template parameters match interval_map:
 * - K: The type of the interval boundaries (e.g., uint64_t)
 * - V: The value type stored in each interval
 * - S: The split/merge policy for values
 * - C: The container type (default: std::map)
 * - nonconst_iterator: Whether to provide non-const iterators (default: false)
 */
template <
    typename K,
    typename V,
    typename S,
    template <typename, typename, typename...> class C = std::map,
    bool nonconst_iterator = false>
class hybrid_interval_map {
private:
  struct single_interval {
    K offset = 0;
    K length = 0;
    V value;

    bool empty() const
    {
      return length == 0;
    }

    void clear()
    {
      offset = 0;
      length = 0;
      value = V();
    }
  };

  S splitter;
  single_interval single;
  std::unique_ptr<interval_map<K, V, S, C, nonconst_iterator>> multi;

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
    multi = std::make_unique<interval_map<K, V, S, C, nonconst_iterator>>();
    if (!single.empty()) {
      multi->insert(single.offset, single.length, std::move(single.value));
      single.clear();
    }
  }

public:
  hybrid_interval_map() = default;

  // Copy constructor
  hybrid_interval_map(const hybrid_interval_map& other) :
    splitter(other.splitter),
    single(other.single),
    multi(
        other.multi
            ? std::make_unique<interval_map<K, V, S, C, nonconst_iterator>>(
                  *other.multi)
            : nullptr)
  {}

  // Move constructor uses compiler default
  hybrid_interval_map(hybrid_interval_map&&) noexcept = default;

  // Copy assignment needs custom implementation for unique_ptr deep copy
  hybrid_interval_map& operator=(const hybrid_interval_map& other)
  {
    if (this != &other) {
      splitter = other.splitter;
      single = other.single;
      multi =
          other.multi
              ? std::make_unique<interval_map<K, V, S, C, nonconst_iterator>>(
                    *other.multi)
              : nullptr;
    }
    return *this;
  }

  // Move assignment uses compiler default
  hybrid_interval_map& operator=(hybrid_interval_map&&) noexcept = default;

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

  void insert(K offset, K length, V&& value)
  {
    if (length == 0) {
      return;
    }
   
    if (is_single()) {
      // Erase first - then we can always do prepend or append.
      erase(offset, length);
    }

    // Erase might have split.
    if (is_single()) {
      if (single.empty()) {
        // After erase, single is empty - just insert
        single.offset = offset;
        single.length = length;
        single.value = std::move(value);
        return;
      }
      
      // After erase, we still have a single interval
      // Try to merge with it if adjacent
      K single_end = single.offset + single.length;
      K insert_end = offset + length;
      
      if (offset == single_end && splitter.can_merge(single.value, value)) {
        // Append and merge
        single.value = splitter.merge(std::move(single.value), std::move(value));
        single.length += length;
        return;
      }
      if (insert_end == single.offset && splitter.can_merge(value, single.value)) {
        // Prepend and merge
        single.value = splitter.merge(std::move(value), std::move(single.value));
        single.offset = offset;
        single.length += length;
        return;
      }
      
      // Not adjacent - need to upgrade to multi
      upgrade_to_multi();
    }

    // Use multi path (erase may have upgraded us, or we just upgraded)
    multi->insert(offset, length, std::move(value));
  }

  void insert(K offset, K length, const V& value)
  {
    V tmp = value;
    insert(offset, length, std::move(tmp));
  }

  // Insert another interval_map
  void insert(const hybrid_interval_map& other)
  {
    if (other.is_single()) {
      if (!other.single.empty()) {
        insert(other.single.offset, other.single.length, other.single.value);
      }
    } else {
      for (auto it = other.begin(); it != other.end(); ++it) {
        insert(it.get_off(), it.get_len(), it.get_val());
      }
    }
  }

  void erase(K offset, K length)
  {
    if (length == 0) {
      return;
    }

    if (is_single()) {
      if (single.empty()) {
        return;
      }

      K end = offset + length;
      K single_end = single.offset + single.length;

      // Check for complete erasure
      if (offset <= single.offset && end >= single_end) {
        single.clear();
        return;
      }

      // Check for no overlap
      if (end <= single.offset || offset >= single_end) {
        return;
      }

      // Partial overlap - check if we can handle without upgrading

      // Case 1: Erase from beginning (trim left)
      if (offset <= single.offset && end < single_end) {
        K trim_amount = end - single.offset;
        single.value = splitter.split(
            trim_amount, single.length - trim_amount, single.value);
        single.offset = end;
        single.length -= trim_amount;
        return;
      }

      // Case 2: Erase from end (trim right)
      if (offset > single.offset && end >= single_end) {
        K new_length = offset - single.offset;
        single.value = splitter.split(0, new_length, single.value);
        single.length = new_length;
        return;
      }

      // Case 3: Erase from middle - this splits the interval, must upgrade
      upgrade_to_multi();
    }

    multi->erase(offset, length);
  }

  // Check if map contains a specific range
  bool contains(K offset, K length) const
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

  // Get intersection with a range
  hybrid_interval_map intersect(K offset, K length) const
  {
    hybrid_interval_map result;
    if (is_single()) {
      if (single.empty()) {
        return result;
      }

      K end = offset + length;
      K single_end = single.offset + single.length;

      // Check for overlap
      if (single_end <= offset || single.offset >= end) {
        return result; // No overlap
      }

      // Calculate intersection
      K inter_start = std::max(single.offset, offset);
      K inter_end = std::min(single_end, end);
      K inter_len = inter_end - inter_start;

      // Extract the intersecting portion of the value
      V inter_val = splitter.split(
          inter_start - single.offset, inter_len, const_cast<V&>(single.value));
      result.insert(inter_start, inter_len, std::move(inter_val));
    } else {
      // Already multi - create result directly as multi with intersect result
      result.multi =
          std::make_unique<interval_map<K, V, S, C, nonconst_iterator>>(
              multi->intersect(offset, length));
    }
    return result;
  }

  // Get start/end offsets (requires non-empty map)
  K get_start_off() const
  {
    if (is_single()) {
      ceph_assert(!single.empty());
      return single.offset;
    }
    return multi->get_start_off();
  }

  K get_end_off() const
  {
    if (is_single()) {
      ceph_assert(!single.empty());
      return single.offset + single.length;
    }
    auto it = multi->end();
    --it;
    return it.get_off() + it.get_len();
  }

  // Convert to interval_set (returns new set)
  template <
      template <typename, typename, typename...> class C2 = std::map,
      bool strict = true>
  hybrid_interval_set<K, C2, strict> to_interval_set() const
  {
    hybrid_interval_set<K, C2, strict> result;
    to_interval_set(result);
    return result;
  }

  template <typename T>
  void to_interval_set(T& out) const
  {
    if (is_single()) {
      if (!single.empty()) {
        out.insert(single.offset, single.length);
      }
    } else {
      for (auto it = multi->begin(); it != multi->end(); ++it) {
        out.insert(it.get_off(), it.get_len());
      }
    }
  }

  // Check if this map contains all intervals in the given interval_set
  template <template <typename, typename, typename...> class C2, bool strict>
  bool contains(const hybrid_interval_set<K, C2, strict>& other) const
  {
    hybrid_interval_set<K, C2, strict> this_set;
    to_interval_set(this_set);
    return this_set.contains(other);
  }

  // Equality operators
  bool operator==(const hybrid_interval_map& other) const
  {
    if (is_single() && other.is_single()) {
      if (single.empty() && other.single.empty()) {
        return true;
      }
      if (single.empty() || other.single.empty()) {
        return false;
      }
      return single.offset == other.single.offset &&
             single.length == other.single.length &&
             single.value == other.single.value;
    }

    if (is_single() != other.is_single()) {
      // One is single, other is multi - need to compare contents
      if (is_single()) {
        if (single.empty()) {
          return other.multi->empty();
        }
        // Check if multi has exactly one element matching single
        auto it = other.multi->begin();
        if (it == other.multi->end()) {
          return false;
        }
        auto next = it;
        ++next;
        if (next != other.multi->end()) {
          return false; // More than one element
        }
        return single.offset == it.get_off() && single.length == it.get_len() &&
               single.value == it.get_val();
      }
      // this is multi, other is single - call in reverse
      return other == *this;
    }

    // Both are multi
    return *multi == *other.multi;
  }

  bool operator!=(const hybrid_interval_map& other) const
  {
    return !(*this == other);
  }

  // Iterator support - for compatibility with interval_map
  class const_iterator {
    const hybrid_interval_map* parent;
    bool at_single;
    bool at_end;
    std::optional<
        typename interval_map<K, V, S, C, nonconst_iterator>::const_iterator>
        multi_iter;

  public:
    using difference_type = ssize_t;
    using value_type = std::pair<K, std::pair<K, V>>;
    using pointer = const value_type*;
    using reference = const value_type&;
    using iterator_category = std::forward_iterator_tag;

    const_iterator(const hybrid_interval_map* p, bool end_iter) :
      parent(p),
      at_single(!end_iter && p->is_single() && !p->single.empty()),
      at_end(end_iter || (p->is_single() && p->single.empty()))
    {
      if (!p->is_single()) {
        const auto& m = *p->multi;
        multi_iter = end_iter ? m.end() : m.begin();
        // If we're in multi mode and not explicitly at end, check if multi is empty
        if (!end_iter && m.empty()) {
          at_end = true;
        } else if (!end_iter) {
          at_end = (*multi_iter == m.end());
        }
      }
    }

    const_iterator(
        const hybrid_interval_map* p,
        typename interval_map<K, V, S, C, nonconst_iterator>::const_iterator it) :
      parent(p), at_single(false), multi_iter(it)
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

    // Dereference operator for range-based for loops
    // Returns non-const reference like interval_map does
    const_iterator& operator*()
    {
      return *this;
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

    K get_off() const
    {
      return at_single ? parent->single.offset : multi_iter->get_off();
    }

    K get_len() const
    {
      return at_single ? parent->single.length : multi_iter->get_len();
    }

    const V& get_val() const
    {
      return at_single ? parent->single.value : multi_iter->get_val();
    }

    // Non-const version for compatibility with legacy code
    // Returns non-const reference like interval_map does
    V& get_val()
    {
      return const_cast<V&>(
          at_single ? parent->single.value : multi_iter->get_val());
    }

    // Check if this iterator's interval contains the given range
    bool contains(K offset, K length) const
    {
      K off = get_off();
      K len = get_len();
      return offset >= off && (offset + length) <= (off + len);
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
    const auto& m = *multi;
    return const_iterator(this, m.end());
  }

  // Add iterator typedef for compatibility (after const_iterator is defined)
  using iterator = const_iterator; // Read-only for now

  // Get range of iterators covering [off, off+len)
  std::pair<const_iterator, const_iterator> get_containing_range(
      K off,
      K len) const
  {
    if (is_single()) {
      if (single.empty()) {
        return {end(), end()};
      }
      // Check if single interval overlaps with requested range
      K single_end = single.offset + single.length;
      K req_end = off + len;
      if (single_end <= off || single.offset >= req_end) {
        // No overlap
        return {end(), end()};
      }
      // Single interval overlaps
      return {begin(), end()};
    }

    // Use multi's get_containing_range
    auto range = multi->get_containing_range(off, len);
    return {
        const_iterator(this, range.first), const_iterator(this, range.second)};
  }

  // Get lower range iterator
  const_iterator get_lower_range(K offset, K length) const
  {
    if (is_single()) {
      if (single.empty() || offset >= single.offset + single.length) {
        return end();
      }
      return begin();
    }
    return const_iterator(this, multi->get_lower_range(offset, length));
  }

  // Debugging
  friend std::ostream& operator<<(std::ostream& os, const hybrid_interval_map& m)
  {
    if (m.is_single()) {
      if (m.single.empty()) {
        os << "{}";
      } else {
        os << "{[" << m.single.offset << "," << m.single.length << "]=...}";
      }
    } else {
      os << "{multi:" << m.multi->empty() << " intervals}";
    }
    return os;
  }
};
