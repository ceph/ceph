// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc
 *
 * Author: Casey Bodley <cbodley@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef BOUNDED_KEY_COUNTER_H
#define BOUNDED_KEY_COUNTER_H

#include <algorithm>
#include <map>
#include <tuple>
#include <vector>

#include "include/assert.h"

/**
 * BoundedKeyCounter
 *
 * A data structure that counts the number of times a given key is inserted,
 * and can return the keys with the highest counters. The number of unique keys
 * is bounded by the given constructor argument, meaning that new keys will be
 * rejected if they would exceed this bound.
 *
 * It is optimized for use where insertion is frequent, but sorted listings are
 * both infrequent and tend to request a small subset of the available keys.
 */
template <typename Key, typename Count>
class BoundedKeyCounter {
  /// map type to associate keys with their counter values
  using map_type = std::map<Key, Count>;
  using value_type = typename map_type::value_type;

  /// view type used for sorting key-value pairs by their counter value
  using view_type = std::vector<const value_type*>;

  /// maximum number of counters to store at once
  const size_t bound;

  /// map of counters, with a maximum size given by 'bound'
  map_type counters;

  /// storage for sorted key-value pairs
  view_type sorted;

  /// remembers how much of the range is actually sorted
  typename view_type::iterator sorted_position;

  /// invalidate view of sorted entries
  void invalidate_sorted()
  {
    sorted_position = sorted.begin();
    sorted.clear();
  }

  /// value_type comparison function for sorting in descending order
  static bool value_greater(const value_type *lhs, const value_type *rhs)
  {
    return lhs->second > rhs->second;
  }

  /// map iterator that adapts value_type to value_type*
  struct const_pointer_iterator : public map_type::const_iterator {
    const_pointer_iterator(typename map_type::const_iterator i)
      : map_type::const_iterator(i) {}

    using value_type = typename map_type::const_iterator::value_type*;
    using reference = const typename map_type::const_iterator::value_type*;

    reference operator*() const {
      return &map_type::const_iterator::operator*();
    }
  };

 protected:
  /// return the number of sorted entries. marked protected for unit testing
  size_t get_num_sorted() const
  {
    using const_iterator = typename view_type::const_iterator;
    return std::distance<const_iterator>(sorted.begin(), sorted_position);
  }

 public:
  BoundedKeyCounter(size_t bound)
    : bound(bound)
  {
    sorted.reserve(bound);
    sorted_position = sorted.begin();
  }

  /// return the number of keys stored
  size_t size() const noexcept { return counters.size(); }

  /// return the maximum number of keys
  size_t capacity() const noexcept { return bound; }

  /// increment a counter for the given key and return its value. if the key was
  /// not present, insert it. if the map is full, return 0
  Count insert(const Key& key, Count n = 1)
  {
    typename map_type::iterator i;

    if (counters.size() < bound) {
      // insert new entries at count=0
      bool inserted;
      std::tie(i, inserted) = counters.emplace(key, 0);
      if (inserted) {
        sorted.push_back(&*i);
      }
    } else {
      // when full, refuse to insert new entries
      i = counters.find(key);
      if (i == counters.end()) {
        return 0;
      }
    }

    i->second += n; // add to the counter

    // update sorted position if necessary. use a binary search for the last
    // element in the sorted range that's greater than this counter
    sorted_position = std::lower_bound(sorted.begin(), sorted_position,
                                       &*i, &value_greater);

    return i->second;
  }

  /// remove the given key from the map of counters
  void erase(const Key& key)
  {
    auto i = counters.find(key);
    if (i == counters.end()) {
      return;
    }
    // removing the sorted entry would require linear search; invalidate instead
    invalidate_sorted();

    counters.erase(i);
  }

  /// query the highest N key-value pairs sorted by counter value, passing each
  /// in order to the given callback with arguments (Key, Count)
  template <typename Callback>
  void get_highest(size_t count, Callback&& cb)
  {
    if (sorted.empty()) {
      // initialize the vector with pointers to all key-value pairs
      sorted.assign(const_pointer_iterator{counters.cbegin()},
                    const_pointer_iterator{counters.cend()});
      // entire range is unsorted
      assert(sorted_position == sorted.begin());
    }

    const size_t sorted_count = get_num_sorted();
    if (sorted_count < count) {
      // move sorted_position to cover the requested number of entries
      sorted_position = sorted.begin() + std::min(count, sorted.size());

      // sort all entries in descending order up to the given position
      std::partial_sort(sorted.begin(), sorted_position, sorted.end(),
                        &value_greater);
    }

    // return the requested range via callback
    for (const auto& pair : sorted) {
      if (count-- == 0) {
        return;
      }
      cb(pair->first, pair->second);
    }
  }

  /// remove all keys and counters and invalidate the sorted range
  void clear()
  {
    invalidate_sorted();
    counters.clear();
  }
};

#endif // BOUNDED_KEY_COUNTER_H
