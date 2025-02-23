// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 IBM
 *
 * Author: Alex Ainscow
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

/* The standard bitset library does not behave like a "std::set", making it
 * hard to use as a drop-in replacement. This templated class is intended to
 * behave like a std::set() with the restriction that it can only store values
 * less than N. The contents is stored as a bitset for efficiency and there are
 * some extensions (such as insert_range) that exploit the implementation,
 * however the main intent is that this is a drop-in replacement for std::set.
 *
 * The Key must cast to/from int8_t unambiguously and support the pre-increment
 * operator.
*/

#pragma once
#include <cstdint>

#include "include/buffer.h"

template<size_t N, typename Key>
class bitset_set {
    enum {
    BITS_PER_UINT64 = 64,
    WORDS = N / 64,
    MAX = N
  };

  // N must be a multiple of 64
  static_assert(N % BITS_PER_UINT64 == 0);

  // Not possible to create a non-const iterator!
public:
  class const_iterator {
    const bitset_set *set;
    Key pos;

  public:
    using difference_type = std::int64_t;

    const_iterator() : set(nullptr), pos(0) {}
    const_iterator(const bitset_set *_set, size_t _pos) : set(_set), pos(_pos) {}
    const_iterator(const bitset_set *_set, Key _pos) : set(_set), pos(_pos) {}

    const_iterator(const bitset_set *_set) : set(_set), pos(MAX) {
      for (size_t i = 0; i < WORDS; ++i) {
        size_t p = std::countr_zero(set->words[i]);
        if (p != BITS_PER_UINT64) {
          pos = (i*BITS_PER_UINT64) + p;
          break;
        }
      }
    }

    const_iterator& operator++() {
      uint64_t v;
      size_t i = (int(pos) + 1) / BITS_PER_UINT64;
      int bit = (int(pos) + 1) % BITS_PER_UINT64;
      while (i < WORDS) {
        if (bit == BITS_PER_UINT64) {
          v = set->words[i];
        } else {
          v = set->words[i] & -1UL << bit;
        }
        bit = std::countr_zero(v);
        if (bit != BITS_PER_UINT64) {
          pos = (i * BITS_PER_UINT64) + bit;
          return *this;
        }
        ++i;
      }
      pos = MAX;
      return *this;
    }

    const_iterator operator++(int)
    {
      const_iterator tmp(*this);
      ++(*this);
      return tmp;
    }

    bool operator==(const const_iterator& rhs) const
    {
      return int(pos) == int(rhs.pos);
    }
    bool operator!=(const const_iterator& rhs) const {return !operator==(rhs);}

    const Key &operator*() const { return pos; }
  };
  static_assert(std::input_or_output_iterator<const_iterator>);

private:
  uint64_t words[WORDS];
  const_iterator _end;

public:

  /** default constructor */
  bitset_set() : _end(this, MAX) { clear(); }
  /** Copy constructor */
  bitset_set(const bitset_set& other) : _end(this, MAX)
  {
    copy(other);
  }
  /** Move constructor (copies) */
  bitset_set(bitset_set&& other) noexcept : bitset_set(other)
  {}

  /** Construct from compatible iterators */
  template< class InputIt >
  bitset_set( const InputIt first, const InputIt last ) : bitset_set()
  {
    for (InputIt it = first; it != last; ++it) {
      emplace(*it);
    }
  }
  /** Constructor for initializer lists (mini_flat_map{1,2}) */
  bitset_set(const std::initializer_list<Key> init) : bitset_set(init.begin(), init.end())
  {}
  /** Convenience utility for converting from a std::set */
  bitset_set(const std::set<Key> &std_set) : bitset_set(std_set.begin(), std_set.end())
  {}
  /** Convenience utility for converting from a std::set<int> */
  bitset_set(const std::set<int> &std_set) : bitset_set(std_set.begin(), std_set.end())
  {}

  /** insert k into set.  */
  void insert(const Key k) {
    ceph_assert(int(k) < MAX);
    words[int(k) / BITS_PER_UINT64] |= 1UL << (int(k) % BITS_PER_UINT64);
  }

  /** insert k into set.  */
  void insert(const bitset_set other) {
    for (int i = 0; i < WORDS; ++i) {
      words[i] |= other.words[i];
    }
  }

  /* Emplace key. Unusually this is LESS efficient than insert, since the key
   * must be constructed, so the int value can be inserted.  The key is
   * immediately discarded.
   *
   * Unlike std::set, this does not return a const_iterator, as this was not
   * needed by any clients.
   *
   * This is useful as a drop-in-replacement or where this template is used
   * in another template.
   *
   * Where possible, insert should be used rather than emplace.
   */
  template< class... Args >
  std::pair<const_iterator, bool> emplace(Args&&... args)
  {
    const Key k(args...);
    bool add = !contains(k);
    if (add) insert(k);
    return std::make_pair(const_iterator(this, k), add);
  }

  /** erase key from set. Unlike std::set does not return anything */
  void erase(const Key k) {
    ceph_assert(int(k) < MAX);
    words[int(k) / BITS_PER_UINT64] &= ~(1UL << (int(k) % BITS_PER_UINT64));
  }

  /** Efficiently insert a range of values. When N is small, this is
   * essentially an O(1) algorithm, although technically it is O(N)
   */
  void insert_range(const Key start, int length)
  {
    int start_word = int(start) / BITS_PER_UINT64;
    int end_word = (int(start) + length) / BITS_PER_UINT64;
    ceph_assert(0 <= end_word && end_word < MAX);

    if (start_word == end_word) {
      words[start_word] |= ((1UL << length) - 1) << (int(start) % BITS_PER_UINT64);
    } else {
      words[start_word] |= -1UL << (int(start) % BITS_PER_UINT64);
      while (++start_word < end_word) {
        words[start_word] = -1UL;
      }
      words[end_word] |= (1UL << ((int(start) + length) % BITS_PER_UINT64)) - 1;
    }
  }

  /** Efficiently erase a range of values. When N is small, this is
   * essentially an O(1) algorithm, although technically it is O(N)
   */
  void erase_range(const Key start, int length)
  {
    int start_word = int(start) / BITS_PER_UINT64;
    int end_word = (int(start) + length) / BITS_PER_UINT64;
    ceph_assert(0 <= end_word && end_word < MAX);

    if (start_word == end_word) {
      words[start_word] &= ~(((1UL << length) - 1) << (int(start) % BITS_PER_UINT64));
    } else {
      words[start_word] &= ~(-1UL << (int(start) % BITS_PER_UINT64));
      while (++start_word < end_word) {
        words[start_word] = 0;
      }
      words[end_word] &= ~((1UL << ((int(start) + length) % BITS_PER_UINT64)) - 1);
    }
  }

  /** Clear all entries from list */
  void clear() {
    for (size_t i = 0; i < WORDS; ++i) {
      words[i] = 0;
    }
  }

  /** @return true if there are no keys in the container */
  bool empty() const {
    bool empty = true;
    for (size_t i = 0; i < WORDS; ++i) {
      if (words[i] != 0) {
        empty = false;
        break;
      }
    }
    return empty;
  }

  /** @return true if the container contains Key k. */
  bool contains(Key k) const
  {
    ceph_assert(int(k) < MAX);
    return (words[int(k) / BITS_PER_UINT64] & 1UL << (int(k) % BITS_PER_UINT64));
  }

  /** @return the count of matching keys in the container. Either returns 0 or 1
   */
  int count(Key k) const {
    return contains(k) ? 1 : 0;
  }

  /** @return a const_iterator to the specified key, or end if it does not
   * exist.  O(1) complexity.
   */
  const_iterator find( const Key& k ) const
  {
    if (contains(k)) return const_iterator(this, k);
    return end();
  }

  /** @return number of keys in the container. O(1) complexity on most
   * modern CPUs.
   */
  size_t size() const {
    size_t count = 0;
    for (size_t i = 0; i < WORDS; ++i) {
      count += std::popcount(words[i]);
    }
    return count;
  }

  /** @return maximum size of set  */
  size_t max_size() const { return MAX; }

  /** Utility for encode/decode to allow serialisations */
  void bound_encode(size_t& p) const {
    for (size_t i = 0 ; i < WORDS; ++i) {
      denc_varint(words[i], p);
    }
  }
  /** Utility for encode/decode to allow serialisations */
  void encode(ceph::buffer::list::contiguous_appender &bl) const {
    for (size_t i = 0 ; i < WORDS; ++i) {
      denc_varint(words[i], bl);
    }
  }
  /** Utility for encode/decode to allow serialisations */
  void decode(ceph::buffer::ptr::const_iterator &bp) {
    for (size_t i = 0 ; i < WORDS; ++i) {
      denc_varint(words[i], bp);
    }
  }

  /** @return begin const_iterator. There is no non-const iterator */
  const_iterator begin() const {
    return const_iterator(this);
  }
  /** @return begin const_iterator. There is no non-const iterator */
  const_iterator cbegin() const { return begin(); }
  /** @return begin const_iterator. There is no non-const iterator */
  const_iterator end() const { return _end; }
  /** @return begin const_iterator. There is no non-const iterator */
  const_iterator cend() const { return _end; }

  /** Utility for implemting copy operators.  Not intented to be used
   * externally, although it is safe to do so
   */
  void copy(const bitset_set &other)
  {
    for (size_t i = 0; i < WORDS; ++i) {
      words[i] = other.words[i];
    }
  }

  /** Assign-with-copy operator */
  bitset_set &operator=(const bitset_set& other)
  {
    copy(other);
    return *this;
  }

  /** Assign with move operator */
  bitset_set &operator=(bitset_set&& other) noexcept
  {
    copy(other);
    return *this;
  }

  /** Swap contents with other. */
  void swap(bitset_set &other) noexcept
  {
    bitset_set tmp(other);
    for (size_t i = 0 ; i < WORDS; ++i) {
      other.words[i] = words[i];
      words[i] = tmp.words[i];
    }
  }

  /** Returns true if other contains a subset of the keys in this container.
   *
   * Useful to replace std::includes if comparing complete sets
   */
  bool includes(const bitset_set &other) const
  {
    for (size_t i = 0; i < WORDS; ++i) {
      if ((words[i] & other.words[i]) != other.words[i]) return false;
    }
    return true;
  }

  /** Standard copy operator */
  friend bool operator==( const bitset_set& lhs, const bitset_set& rhs )
  {
    for (size_t i = 0 ; i < WORDS; ++i) {
      if (lhs.words[i] != rhs.words[i]) return false;
    }
    return true;
  }

  /** Standard ostream operator */
  friend std::ostream& operator<<(std::ostream& lhs, const bitset_set& rhs)
  {
    unsigned int c = 0;
    lhs << "{";
    for (auto &&k : rhs) {
      lhs << k;
      c++;
      if (c < rhs.size()) lhs << ",";
    }
    lhs << "}";
    return lhs;
  }

  /** returns a bitset_set with the elements from lhs which are not found in rhs
   *
   * Useful to replace calls to std::difference which looked at the complete
   * sets.
   */
  static bitset_set difference(const bitset_set& lhs, const bitset_set& rhs)
  {
    bitset_set res;
    for (size_t i = 0 ; i < WORDS; ++i) {
      res.words[i] = lhs.words[i] & ~rhs.words[i];
    }
    return res;
  }

  /** returns a bitset_set with the elements from lhs which are found in rhs
   *
   * Useful to replace calls to std::intersection which looked at the complete
   * sets.
   */
  static bitset_set intersection(const bitset_set& lhs, const bitset_set& rhs)
  {
    bitset_set res;
    for (size_t i = 0 ; i < WORDS; ++i) {
      res.words[i] = lhs.words[i] & rhs.words[i];
    }
    return res;
  }

  /** Spaceship operator! Woosh... */
  friend std::strong_ordering operator<=>(const bitset_set &lhs, const bitset_set &rhs)
  {
    for (size_t i = 0 ; i < WORDS; ++i) {
      if (lhs.words[i] != rhs.words[i]) return lhs.words[i] <=> rhs.words[i];
    }

    return std::strong_ordering::equal;
  }
};
