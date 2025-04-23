// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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
#include <fmt/ranges.h>

#include "common/fmt_common.h"
#include "include/buffer.h"

template<typename KeyT, typename IntT>
concept ExplicitlyCastableToOrFrom = requires(KeyT key, IntT v) { IntT(key); KeyT(v); };

template<size_t NumBitsV, typename KeyT>
requires(ExplicitlyCastableToOrFrom<KeyT, int> && NumBitsV % (sizeof(uint64_t) * CHAR_BIT) == 0 && NumBitsV > 0)
class bitset_set {
  static constexpr const inline size_t bits_per_uint64_t = sizeof(uint64_t) * CHAR_BIT;
  static constexpr const inline size_t word_count = NumBitsV / bits_per_uint64_t;
  static constexpr const inline size_t max_bits = NumBitsV;

  // end_pos is permitted to wrap (e.g  for int8_t: NumBitsV=128 -> end_pos=-128 )
  static constexpr const inline KeyT end_pos = static_cast<KeyT>(max_bits);

  // Not possible to create a non-const iterator!
 public:
  class const_iterator {
    const bitset_set *set;
    KeyT pos;

   public:
    using difference_type = std::int64_t;

    const_iterator() : set(nullptr), pos(0) {
    }

    const_iterator(const bitset_set *_set, size_t _pos) : set(_set), pos(_pos) {
    }

    const_iterator(const bitset_set *_set, KeyT _pos) : set(_set), pos(_pos) {
    }

    const_iterator(const bitset_set *_set) : set(_set), pos(end_pos) {
      for (size_t i = 0; i < word_count; ++i) {
        size_t p = std::countr_zero(set->words[i]);
        if (p != bits_per_uint64_t) {
          pos = (i * bits_per_uint64_t) + p;
          break;
        }
      }
    }

    const_iterator &operator++() {
      uint64_t v;
      size_t i = (int(pos) + 1) / bits_per_uint64_t;
      int bit = (int(pos) + 1) % bits_per_uint64_t;
      while (i < word_count) {
        if (bit == bits_per_uint64_t) {
          v = set->words[i];
        } else {
          v = set->words[i] & -1ULL << bit;
        }
        bit = std::countr_zero(v);
        if (bit != bits_per_uint64_t) {
          pos = (i * bits_per_uint64_t) + bit;
          return *this;
        }
        ++i;
      }
      pos = end_pos;
      return *this;
    }

    const_iterator operator++(int) {
      const_iterator tmp(*this);
      ++(*this);
      return tmp;
    }

    bool operator==(const const_iterator &rhs) const {
      return int(pos) == int(rhs.pos);
    }

    bool operator!=(const const_iterator &rhs) const {
      return !operator==(rhs);
    }

    const KeyT &operator*() const {
      return pos;
    }
  };

  static_assert(std::input_or_output_iterator<const_iterator>);

 private:
  std::array<uint64_t, word_count> words;
  const_iterator _end;

 public:

  static unsigned int unsigned_cast(KeyT const k) {
    int8_t i = static_cast<int8_t>(k);
    return static_cast<unsigned int>(i);
  }

  /** default constructor */
  bitset_set() : _end(this, end_pos) {
    clear();
  }

  /** Copy constructor */
  bitset_set(const bitset_set &other) : _end(this, end_pos) {
    copy(other);
  }

  /** Move constructor (copies) */
  bitset_set(bitset_set &&other) noexcept : bitset_set(other) {
  }

  /** Construct from compatible iterators */
  template<class InputIt>
  bitset_set(const InputIt first, const InputIt last) : bitset_set() {
    for (InputIt it = first; it != last; ++it) {
      emplace(*it);
    }
  }

  /** Constructor for initializer lists (mini_flat_map{1,2}) */
  bitset_set(const std::initializer_list<KeyT> init)
    : bitset_set(init.begin(), init.end()) {
  }

  /** Convenience utility for converting from a std::set */
  bitset_set(const std::set<KeyT> &std_set)
    : bitset_set(std_set.begin(), std_set.end()) {
  }

  /** Convenience utility for converting from a std::set<int> */
  bitset_set(const std::set<int> &std_set)
    : bitset_set(std_set.begin(), std_set.end()) {
  }

  /** insert k into set.  */
  void insert(const KeyT k) {
    ceph_assert( unsigned_cast(k) < max_bits);
    ceph_assert(int(k) >= 0);
    words[int(k) / bits_per_uint64_t] |= 1ULL << (int(k) % bits_per_uint64_t);
  }

  /** insert k into set.  */
  void insert(const bitset_set &other) {
    for (unsigned i = 0; i < word_count; ++i) {
      words[i] |= other.words[i];
    }
  }

  /* Emplace key. Unusually this is LESS efficient than insert, since the key
   * must be constructed, so the int value can be inserted.  The key is
   * immediately discarded.
   *
   * It is provided for compatibility with std::set,
   *
   * Where possible, insert should be used rather than emplace.
   */
  template<class... Args>
  std::pair<const_iterator, bool> emplace(Args &&... args) {
    const KeyT k(args...);
    bool add = !contains(k);
    if (add) {
      insert(k);
    }
    return std::make_pair(const_iterator(this, k), add);
  }

  /** erase key from set. Unlike std::set does not return anything */
  void erase(const KeyT k) {
    ceph_assert(unsigned_cast(k) < max_bits);
    ceph_assert(int(k) >= 0);
    words[int(k) / bits_per_uint64_t] &= ~(1ULL << (int(k) % bits_per_uint64_t));
  }

  /** Efficiently insert a range of values. When N is small, this is
   * essentially an O(1) algorithm, although technically it is O(N)
   */
  void insert_range(const KeyT start, int length) {
    unsigned start_word = int(start) / bits_per_uint64_t;
    // This is not an off-by-one error. Conventionally this would have length
    // - 1, but the logic below is simpler with it as follows.
    unsigned end_word = (int(start) + length) / bits_per_uint64_t;
    ceph_assert(end_word < word_count + 1);

    if (start_word == end_word) {
      words[start_word] |=
        ((1ULL << length) - 1) << (int(start) % bits_per_uint64_t);
    } else {
      words[start_word] |= -1ULL << (int(start) % bits_per_uint64_t);
      while (++start_word < end_word) {
        words[start_word] = -1ULL;
      }
      if (end_word < word_count) {
        words[end_word] |=
          (1ULL << ((int(start) + length) % bits_per_uint64_t)) - 1;
      }
    }
  }

  /** Efficiently erase a range of values. When N is small, this is
   * essentially an O(1) algorithm, although technically it is O(N)
   */
  void erase_range(const KeyT start, int length) {
    unsigned start_word = int(start) / bits_per_uint64_t;
    // This is not an off-by-one error. Conventionally this would have length
    // - 1, but the logic below is simpler with it as follows.
    unsigned end_word = (int(start) + length) / bits_per_uint64_t;
    ceph_assert(end_word < word_count + 1);

    if (start_word == end_word) {
      words[start_word] &=
        ~(((1ULL << length) - 1) << (int(start) % bits_per_uint64_t));
    } else {
      words[start_word] &= ~(-1ULL << (int(start) % bits_per_uint64_t));
      while (++start_word < end_word) {
        words[start_word] = 0;
      }
      if (end_word < word_count) {
      words[end_word] &=
          ~((1ULL << ((int(start) + length) % bits_per_uint64_t)) - 1);
      }
    }
  }

  /** Clear all entries from list */
  void clear() {
    for (size_t i = 0; i < word_count; ++i) {
      words[i] = 0;
    }
  }

  /** @return true if there are no keys in the container */
  bool empty() const {
    bool empty = true;
    for (size_t i = 0; i < word_count; ++i) {
      if (words[i] != 0) {
        empty = false;
        break;
      }
    }
    return empty;
  }

  /** @return true if the container contains Key k. */
  bool contains(KeyT k) const {
    if (unsigned_cast(k) >= max_bits) {
      return false;
    }
    return (words[int(k) / bits_per_uint64_t]
      & 1ULL << (int(k) % bits_per_uint64_t));
  }

  /** @return the count of matching keys in the container. Either returns 0 or 1
   */
  int count(KeyT k) const {
    return contains(k) ? 1 : 0;
  }

  /** @return a const_iterator to the specified key, or end if it does not
   * exist.  O(1) complexity.
   */
  const_iterator find(const KeyT &k) const {
    if (contains(k)) {
      return const_iterator(this, k);
    }
    return end();
  }

  /** @return number of keys in the container. O(1) complexity on most
   * modern CPUs.
   */
  size_t size() const {
    size_t count = 0;
    for (size_t i = 0; i < word_count; ++i) {
      count += std::popcount(words[i]);
    }
    return count;
  }

  /** @return maximum size of set  */
  constexpr size_t max_size() const {
    return max_bits;
  }

  /** Utility for encode/decode to allow serialisations */
  void bound_encode(size_t &p) const {
    for (size_t i = 0; i < word_count; ++i) {
      denc_varint(words[i], p);
    }
  }

  /** Utility for encode/decode to allow serialisations */
  void encode(ceph::buffer::list::contiguous_appender &bl) const {
    for (size_t i = 0; i < word_count; ++i) {
      denc_varint(words[i], bl);
    }
  }

  /** Utility for encode/decode to allow serialisations */
  void decode(ceph::buffer::ptr::const_iterator &bp) {
    for (size_t i = 0; i < word_count; ++i) {
      denc_varint(words[i], bp);
    }
  }

  /** @return begin const_iterator. There is no non-const iterator */
  const_iterator begin() const {
    return const_iterator(this);
  }

  /** @return begin const_iterator. There is no non-const iterator */
  const_iterator cbegin() const {
    return begin();
  }

  /** @return begin const_iterator. There is no non-const iterator */
  const_iterator end() const {
    return _end;
  }

  /** @return begin const_iterator. There is no non-const iterator */
  const_iterator cend() const {
    return _end;
  }

  /** Utility for implemting copy operators.  Not intented to be used
   * externally, although it is safe to do so
   */
  void copy(const bitset_set &other) {
    for (size_t i = 0; i < word_count; ++i) {
      words[i] = other.words[i];
    }
  }

  /** Assign-with-copy operator */
  bitset_set &operator=(const bitset_set &other) {
    if (&other != this) {
      copy(other);
    }
    return *this;
  }

  /** Assign with move operator */
  bitset_set &operator=(bitset_set &&other) noexcept {
    if (&other != this) {
      copy(other);
    }
    return *this;
  }

  /** Swap contents with other. */
  void swap(bitset_set &other) noexcept {
    for (size_t i = 0; i < word_count; ++i) {
      uint64_t tmp = other.words[i];
      other.words[i] = words[i];
      words[i] = tmp;
    }
  }

  /** Returns true if other contains a subset of the keys in this container.
   *
   * Useful to replace std::includes if comparing complete sets
   */
  bool includes(const bitset_set &other) const {
    for (size_t i = 0; i < word_count; ++i) {
      if ((words[i] & other.words[i]) != other.words[i]) {
        return false;
      }
    }
    return true;
  }

  /** Standard copy operator */
  friend bool operator==(const bitset_set &lhs, const bitset_set &rhs) {
    for (size_t i = 0; i < word_count; ++i) {
      if (lhs.words[i] != rhs.words[i]) {
        return false;
      }
    }
    return true;
  }

  /** Standard ostream operator */
  friend std::ostream &operator<<(std::ostream &lhs, const bitset_set &rhs) {
    unsigned int c = 0;
    lhs << "{";
    for (auto &&k : rhs) {
      lhs << k;
      c++;
      if (c < rhs.size()) {
        lhs << ",";
      }
    }
    lhs << "}";
    return lhs;
  }

  std::string fmt_print() const
  requires fmt::formattable<KeyT> {
    std::string s = "{";
    int c = (int)size();
    for (auto k : *this) {
      s += fmt::format("{}", k);
      if (--c > 0) {
	s += ",";
      }
    }
    s += "}";
    return s;
  }

  /** returns a bitset_set with the elements from lhs which are not found in rhs
   *
   * Useful to replace calls to std::difference which looked at the complete
   * sets.
   */
  static bitset_set difference(const bitset_set &lhs, const bitset_set &rhs) {
    bitset_set res;
    for (size_t i = 0; i < word_count; ++i) {
      res.words[i] = lhs.words[i] & ~rhs.words[i];
    }
    return res;
  }

  /** returns a bitset_set with the elements from lhs which are found in rhs
   *
   * Useful to replace calls to std::intersection which looked at the complete
   * sets.
   */
  static bitset_set intersection(const bitset_set &lhs, const bitset_set &rhs) {
    bitset_set res;
    for (size_t i = 0; i < word_count; ++i) {
      res.words[i] = lhs.words[i] & rhs.words[i];
    }
    return res;
  }

  /** Spaceship operator! Woosh... */
  friend std::strong_ordering operator<=>(const bitset_set &lhs,
                                          const bitset_set &rhs) {
    for (size_t i = 0; i < word_count; ++i) {
      if (lhs.words[i] != rhs.words[i]) {
        return lhs.words[i] <=> rhs.words[i];
      }
    }

    return std::strong_ordering::equal;
  }
};

// make sure fmt::range would not try (and fail) to treat bitset_set as a range
template<size_t NumBitsV, typename KeyT>
struct fmt::is_range<bitset_set<NumBitsV, KeyT>, char> : std::false_type {};

