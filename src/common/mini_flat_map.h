// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstddef>
#include <fmt/ranges.h>
#include <memory>
#include <vector>

#include "common/bitset_set.h"
#include "common/fmt_common.h"
#include "include/ceph_assert.h"


/* This class struct provides an API similar to std::map, but with the
 * restriction that "Key" must cast to/from IntT without ambiguity. For
 * example, the key could be a simple wrapper for an int8_t, used to provide
 * some type safety.  Additionally, the constructor must be passed the max
 * value of the key, referred to as max_size.
 *
 * Signing:  This library allows for IntT to be signed OR unsigned. If a signed
 * type is used, then only POSITIVE values of KeyT are permitted in the map.
 *
 * The structure is a vector of optionals, indexed by the key. This therefore
 * provides O(1) lookup, with an extremely low constant overhead. The size
 * reflects the number of populated optionals, which is tracked independently.
 *
 * This was written generically, but with a single purpose in mind (in Erasure
 * Coding), so the interface is not as complete as it could be.
 */
template<typename KeyT, typename ValueT, typename IntT = int_fast8_t>
requires(ExplicitlyCastableToOrFrom<KeyT, IntT>)
class mini_flat_map {
  using vector_type = std::optional<ValueT>;
  using value_type = std::pair<const KeyT &, ValueT &>;

  static unsigned int unsigned_cast(KeyT const k) {
    IntT i = static_cast<IntT>(k);
    return static_cast<unsigned int>(i);
  }

  void range_check(KeyT const k) const {
    IntT i_s = static_cast<IntT>(k);
    unsigned int i_u = static_cast<unsigned int>(i_s);
    ceph_assert(0 <= i_s && i_u < max_size());
  }

 public:
  template<bool is_const>
  class _iterator {
    friend class mini_flat_map;
    using mini_flat_map_p = std::conditional_t<is_const,
                                               const mini_flat_map *,
                                               mini_flat_map *>;
    using value_type = std::conditional_t<is_const,
                                          const std::pair<const KeyT &,
                                                          const ValueT &>,
                                          std::pair<const KeyT &, ValueT &>>;

    mini_flat_map_p map;
    std::optional<value_type> value;
    KeyT key;

    void progress() {
      while (unsigned_cast(key) < map->data.size() && !map->_at(key)) {
        key = KeyT(static_cast<IntT>(key) + 1);
      }

      if (unsigned_cast(key) < map->data.size()) {
        value.emplace(key, *(map->_at(key)));
      }
    }

   public:
    using difference_type = std::ptrdiff_t;

    _iterator(mini_flat_map_p map) : map(map), key(0) {
      progress();
    }

    _iterator(mini_flat_map_p map, KeyT key) : map(map), key(key) {
      if (unsigned_cast(key) < map->data.size()) {
        value.emplace(key, *map->_at(key));
      } else {
        ceph_assert(unsigned_cast(key) == map->data.size());  // end
      }
    }

    // Only for end constructor.
    _iterator(mini_flat_map_p map, size_t map_size) : map(map), key(map_size) {
      ceph_assert(map_size == map->data.size());
    }

    _iterator &operator++() {
      key = KeyT(static_cast<IntT>(key) + 1);
      progress();
      return *this;
    }

    _iterator operator++(int) {
      _iterator tmp(*this);
      this->operator++();
      return tmp;
    }

    bool operator==(const _iterator &other) const {
      return key == other.key && map == other.map;
    }

    value_type &operator*() {
      return *value;
    }

    value_type *operator->() {
      return value.operator->();
    }

    _iterator &operator=(const _iterator &other) {
      if (this != &other) {
        key = other.key;
        progress();  // populate value
      }
      return *this;
    }
  };

  using iterator = _iterator<false>;
  using const_iterator = _iterator<true>;

  static_assert(std::input_or_output_iterator<iterator>);
  static_assert(std::input_or_output_iterator<const_iterator>);

 private:
  std::vector<vector_type> data;
  const iterator _end;
  const const_iterator _const_end;
  size_t _size;

  std::optional<ValueT> &_at(const KeyT &k) {
    range_check(k);
    return data[static_cast<IntT>(k)];
  }

  const std::optional<ValueT> &_at(const KeyT &k) const {
    range_check(k);
    return data[static_cast<IntT>(k)];
  }

 public:
  /** Basic constructor. The mini_flat_map cannot be re-sized, so there is no
   * default constructor.
   */
  mini_flat_map(size_t max_size)
    : data(max_size),
      _end(this, max_size),
      _const_end(this, max_size),
      _size(0) {
  }

  /** Move constructor, forwards the move to the vector
   * This has O(N) complexity.
   */
  mini_flat_map(mini_flat_map &&other) noexcept
    : data(std::move(other.data)),
      _end(this, data.size()),
      _const_end(this, data.size()),
      _size(other.size()) {}

  /** Generic initializer iterator constructor, similar to std::map constructor
   * of the same name.
   */
  template<class InputIt>
  mini_flat_map(size_t max_size, const InputIt first, const InputIt last)
    : mini_flat_map(max_size) {
    for (InputIt it = first; it != last; ++it) {
      const KeyT k(it->first);
      auto &args = it->second;
      emplace(k, args);
    }
  }

  /** Copy constructor. Forwards the copy onto the vector */
  mini_flat_map(const mini_flat_map &other) noexcept
    : mini_flat_map(other.data.size(), other.begin(), other.end()) {
    ceph_assert(_size == other._size);
  }

  /** Map compatibility. Some legacy code required conversion from std::map.
   * This is similar to the move constructor
   */
  mini_flat_map(size_t max_size, const std::map<KeyT, ValueT> &&other) : data(
    max_size), _end(this, max_size), _const_end(this, max_size), _size(0) {
    for (auto &&[k, t] : other) {
      emplace(k, std::move(t));
    }
    ceph_assert(_size == other.size());
  }

  /** Map compatibility. Some legacy code required conversion from std::map.
   * This is similar to the copy constructor
   */
  mini_flat_map(size_t max_size, const std::map<int, ValueT> &other)
    : mini_flat_map(max_size, other.begin(), other.end()) {
    ceph_assert(_size == other.size());
  }

  /** Checks if there is an element with key equivalent to key in the container.
   * @param key that may be contained
   */
  bool contains(const KeyT &key) const {
    return unsigned_cast(key) < data.size() && data.at(unsigned_cast(key));
  }

  /** Checks if the container has no elements. */
  [[nodiscard]] bool empty() const noexcept {
    return _size == 0;
  }

  /** Exchanges the contents of the container with those of other. Does not
   * invoke any move, copy, or swap operations on individual elements.
   *
   * @param other - map to be modified
   */
  void swap(mini_flat_map &other) noexcept {
    data.swap(other.data);
    std::swap(_size, other._size);
  }

  /** Erases all elements from the container. */
  void clear() {
    if (!_size) {
      return;
    }
    for (auto &&d : data) {
      d.reset();
    }
    _size = 0;
  }

  /** Assignment with move operator */
  mini_flat_map &operator=(mini_flat_map &&other) noexcept {
    data = std::move(other.data);
    _size = other._size;
    return *this;
  }

  /** Assignment with copy operator */
  mini_flat_map &operator=(const mini_flat_map &other) {
    ceph_assert(data.size() == other.data.size());
    clear();

    for (auto &&[k, v] : other) {
      emplace(k, ValueT(v));
    }

    ceph_assert(_size == other._size);

    return *this;
  }

  /** Removes specified element from the container.
   * @param i - iterator to remove
   * @return iterator - pointing at next element (or end)
   */
  iterator erase(iterator &i) {
    erase(i->first);
    i.progress();
    return i;
  }

  /** Removes specified element from the container.
   * NOTE: returns iterator, rather than const_iterator as per std::map::erase
   * @param i - const_iterator to remove
   * @return iterator - pointing at next element (or end)
   */
  iterator erase(const_iterator &i) {
    erase(i->first);
    i.progress();
    return iterator(this, i.key);
  }

  /** Removes specified element from the container.
   * @param k - key to remove
   * @return size_t - 1 if element removed, 0 otherwise.
   */
  size_t erase(const KeyT &k) {
    if (!contains(k)) {
      return 0;
    }
    _size--;
    data.at(IntT(k)).reset();
    return 1;
  }

  /** @return begin const_iterator */
  const_iterator begin() const {
    return cbegin();
  }

  /** @return end const_iterator */
  const_iterator end() const {
    return cend();
  }

  /** @return begin const_iterator */
  const_iterator cbegin() const {
    return const_iterator(this);
  }

  /** @return end const_iterator */
  const_iterator cend() const {
    return _const_end;
  }

  /** @return begin iterator */
  iterator begin() {
    return iterator(this);
  }

  /** @return end iterator */
  iterator end() {
    return _end;
  }

  /** return number of elements in map, This is the number of optionals
   * which are not null in the map.
   * @return size_t size
   */
  size_t size() const {
    return _size;
  }

  /** return maximum number of elements that container can hold.
   * @return size_t
   */
  auto max_size() const {
    return data.size();
  }

  /** Returns a reference to the mapped value of the element with specified key.
   * If no such element exists, an exception of type std::out_of_range is
   * thrown.
   *
   * @param k - key
   * @return reference to value.
   */
  ValueT &at(const KeyT &k) {
    if (!contains(k)) {
      throw std::out_of_range("Key not found");
    }
    return *data.at(IntT(k));
  }

  /** Returns a reference to the mapped value of the element with specified key.
   * If no such element exists, an exception of type std::out_of_range is
   * thrown.
   *
   * @param k - const key
   * @return const reference to value.
   */
  const ValueT &at(const KeyT &k) const {
    if (!contains(k)) {
      throw std::out_of_range("Key not found");
    }
    return *data.at(IntT(k));
  }

  /** Equality operator */
  bool operator==(mini_flat_map const &other) const {
    if (_size != other._size) {
      return false;
    }

    for (auto &&[k, v] : *this) {
      if (!other.contains(k)) {
        return false;
      }
      if (other.at(k) != v) {
        return false;
      }
    }

    return true;
  }

  /** Inserts a new element into the container constructed in-place with the
   * given args, if there is no element with the key in the container.
   *
   * The constructor of the new element is called with exactly the same
   * arguments as supplied to emplace, forwarded via
   * std::forward<Args>(args).... The element may be constructed even if there
   * already is an element with the key in the container, in which case the
   * newly constructed element will be destroyed immediately (see try_emplace()
   * if this behavior is undesirable).
   *
   * This is different to the std::map interface, in that key must be
   * provided explicitly, rather than constructed. THis does provide some
   * performance gains and should have the same behaviour.
   *
   * Careful use of emplace allows the new element to be constructed while
   * avoiding unnecessary copy or move operations.
   *
   * This also differs to std::map in that no iterators are returned
   *
   * @param k - key to add
   * @param args to construct value.
   *
   * @return true if inserted.
   */
  template<class... Args>
  bool emplace(const KeyT &k, Args &&... args) {
    if (!contains(k)) {
      _size++;
      _at(k).emplace(std::forward<Args>(args)...);
      return true;
    }
    return false;
  }

  /** Inserts an element into the container using the copy operator */
  bool insert(const KeyT &k, const ValueT &value) {
    return emplace(k, value);
  }

  /** Returns a reference to the value that is mapped to a key equivalent to
   * key, performing an insertion if such key does not already exist.
   *
   * Since the key is not stored explicitly, there is no "move" variant as
   * there is in std::map.
   */
  ValueT &operator[](const KeyT &s) {
    if (!contains(s)) {
      ceph_assert(emplace(s));
    }
    return at(s);
  }

  /** Returns the number of elements with key that compares equivalent to the
   * specified argument. Each key can only exist once, so cannot return more
   * than 1.
   */
  size_t count(const KeyT &key) const {
    return contains(key) ? 1 : 0;
  }

  /** Returns an iterator to the specified key or end if it does not exist.
   * O(1) search with low overahead.
   * @param key
   * * @return iterator.
   */
  iterator find(const KeyT &key) {
    if (!contains(key)) {
      return _end;
    }
    return iterator(this, key);
  }

  /** Returns a const_iterator to the specified key or end if it does not exist.
   * O(1) search with low overahead.
   * @param key
   * @return const_iterator.
   */
  const_iterator find(const KeyT &key) const {
    if (!contains(key)) {
      return _const_end;
    }
    return const_iterator(this, key);
  }

  template<size_t N>
  void populate_bitset_set(bitset_set<N, KeyT> &set) const {
    for (IntT ki = 0; static_cast<unsigned>(ki) < data.size(); ++ki) {
      KeyT k(ki);
      if (_at(k)) {
        set.insert(k);
      }
    }
  }

  /** Standard ostream operator */
  friend std::ostream &operator<<(std::ostream &lhs,
                                  const mini_flat_map<KeyT, ValueT> &rhs) {
    unsigned int c = 0;
    lhs << "{";
    for (auto &&[k, v] : rhs) {
      lhs << k << ":" << v;
      c++;
      if (c < rhs._size) {
        lhs << ",";
      }
    }
    lhs << "}";
    return lhs;
  }

  std::string fmt_print() const
  requires fmt::formattable<KeyT> && fmt::formattable<ValueT> {
    int c = (int)_size;
    std::string s = "{";
    for (auto&& [k, v] : *this) {
      s += fmt::format("{}:{}", k, v);
      if (--c > 0) {
	s += ",";
      }
    }
    s += "}";
    return s;
  }
};

// make sure fmt::range does not apply to mini_flat_map
template<typename KeyT, typename ValueT>
struct fmt::is_range<mini_flat_map<KeyT, ValueT>, char> : std::false_type {};

