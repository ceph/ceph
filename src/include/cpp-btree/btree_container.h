// Copyright 2018 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <algorithm>
#include <initializer_list>
#include <iterator>
#include <type_traits>
#include <utility>

#include "btree.h"

namespace btree::internal {

// A common base class for btree_set, btree_map, btree_multiset, and
// btree_multimap.
template <typename Tree>
class btree_container {
  using params_type = typename Tree::params_type;

 protected:
  // Alias used for heterogeneous lookup functions.
  // `key_arg<K>` evaluates to `K` when the functors are transparent and to
  // `key_type` otherwise. It permits template argument deduction on `K` for the
  // transparent case.
  template <class Compare>
  using is_transparent_t = typename Compare::is_transparent;
  template <class K>
  using key_arg =
    std::conditional_t<
      std::experimental::is_detected_v<is_transparent_t, typename Tree::key_compare>,
      K,
      typename Tree::key_type>;

 public:
  using key_type = typename Tree::key_type;
  using value_type = typename Tree::value_type;
  using size_type = typename Tree::size_type;
  using difference_type = typename Tree::difference_type;
  using key_compare = typename Tree::key_compare;
  using value_compare = typename Tree::value_compare;
  using allocator_type = typename Tree::allocator_type;
  using reference = typename Tree::reference;
  using const_reference = typename Tree::const_reference;
  using pointer = typename Tree::pointer;
  using const_pointer = typename Tree::const_pointer;
  using iterator = typename Tree::iterator;
  using const_iterator = typename Tree::const_iterator;
  using reverse_iterator = typename Tree::reverse_iterator;
  using const_reverse_iterator = typename Tree::const_reverse_iterator;

  // Constructors/assignments.
  btree_container() : tree_(key_compare(), allocator_type()) {}
  explicit btree_container(const key_compare &comp,
                           const allocator_type &alloc = allocator_type())
      : tree_(comp, alloc) {}
  btree_container(const btree_container &x) = default;
  btree_container(btree_container &&x) noexcept = default;
  btree_container &operator=(const btree_container &x) = default;
  btree_container &operator=(btree_container &&x) noexcept(
      std::is_nothrow_move_assignable<Tree>::value) = default;

  // Iterator routines.
  iterator begin() { return tree_.begin(); }
  const_iterator begin() const { return tree_.begin(); }
  const_iterator cbegin() const { return tree_.begin(); }
  iterator end() { return tree_.end(); }
  const_iterator end() const { return tree_.end(); }
  const_iterator cend() const { return tree_.end(); }
  reverse_iterator rbegin() { return tree_.rbegin(); }
  const_reverse_iterator rbegin() const { return tree_.rbegin(); }
  const_reverse_iterator crbegin() const { return tree_.rbegin(); }
  reverse_iterator rend() { return tree_.rend(); }
  const_reverse_iterator rend() const { return tree_.rend(); }
  const_reverse_iterator crend() const { return tree_.rend(); }

  // Lookup routines.
  template <typename K = key_type>
  iterator find(const key_arg<K> &key) {
    return tree_.find(key);
  }
  template <typename K = key_type>
  const_iterator find(const key_arg<K> &key) const {
    return tree_.find(key);
  }
  template <typename K = key_type>
  bool contains(const key_arg<K> &key) const {
    return find(key) != end();
  }
  template <typename K = key_type>
  iterator lower_bound(const key_arg<K> &key) {
    return tree_.lower_bound(key);
  }
  template <typename K = key_type>
  const_iterator lower_bound(const key_arg<K> &key) const {
    return tree_.lower_bound(key);
  }
  template <typename K = key_type>
  iterator upper_bound(const key_arg<K> &key) {
    return tree_.upper_bound(key);
  }
  template <typename K = key_type>
  const_iterator upper_bound(const key_arg<K> &key) const {
    return tree_.upper_bound(key);
  }
  template <typename K = key_type>
  std::pair<iterator, iterator> equal_range(const key_arg<K> &key) {
    return tree_.equal_range(key);
  }
  template <typename K = key_type>
  std::pair<const_iterator, const_iterator> equal_range(
      const key_arg<K> &key) const {
    return tree_.equal_range(key);
  }

  // Deletion routines. Note that there is also a deletion routine that is
  // specific to btree_set_container/btree_multiset_container.

  // Erase the specified iterator from the btree. The iterator must be valid
  // (i.e. not equal to end()).  Return an iterator pointing to the node after
  // the one that was erased (or end() if none exists).
  iterator erase(const_iterator iter) { return tree_.erase(iterator(iter)); }
  iterator erase(iterator iter) { return tree_.erase(iter); }
  iterator erase(const_iterator first, const_iterator last) {
    return tree_.erase(iterator(first), iterator(last)).second;
  }

 public:
  // Utility routines.
  void clear() { tree_.clear(); }
  void swap(btree_container &x) { tree_.swap(x.tree_); }
  void verify() const { tree_.verify(); }

  // Size routines.
  size_type size() const { return tree_.size(); }
  size_type max_size() const { return tree_.max_size(); }
  bool empty() const { return tree_.empty(); }

  friend bool operator==(const btree_container &x, const btree_container &y) {
    if (x.size() != y.size()) return false;
    return std::equal(x.begin(), x.end(), y.begin());
  }

  friend bool operator!=(const btree_container &x, const btree_container &y) {
    return !(x == y);
  }

  friend bool operator<(const btree_container &x, const btree_container &y) {
    return std::lexicographical_compare(x.begin(), x.end(), y.begin(), y.end());
  }

  friend bool operator>(const btree_container &x, const btree_container &y) {
    return y < x;
  }

  friend bool operator<=(const btree_container &x, const btree_container &y) {
    return !(y < x);
  }

  friend bool operator>=(const btree_container &x, const btree_container &y) {
    return !(x < y);
  }

  // The allocator used by the btree.
  allocator_type get_allocator() const { return tree_.get_allocator(); }

  // The key comparator used by the btree.
  key_compare key_comp() const { return tree_.key_comp(); }
  value_compare value_comp() const { return tree_.value_comp(); }

 protected:
  Tree tree_;
};

// A common base class for btree_set and btree_map.
template <typename Tree>
class btree_set_container : public btree_container<Tree> {
  using super_type = btree_container<Tree>;
  using params_type = typename Tree::params_type;
  using init_type = typename params_type::init_type;
  using is_key_compare_to = typename params_type::is_key_compare_to;
  friend class BtreeNodePeer;

 protected:
  template <class K>
  using key_arg = typename super_type::template key_arg<K>;

 public:
  using key_type = typename Tree::key_type;
  using value_type = typename Tree::value_type;
  using size_type = typename Tree::size_type;
  using key_compare = typename Tree::key_compare;
  using allocator_type = typename Tree::allocator_type;
  using iterator = typename Tree::iterator;
  using const_iterator = typename Tree::const_iterator;

  // Inherit constructors.
  using super_type::super_type;
  btree_set_container() {}

  // Range constructor.
  template <class InputIterator>
  btree_set_container(InputIterator b, InputIterator e,
                      const key_compare &comp = key_compare(),
                      const allocator_type &alloc = allocator_type())
      : super_type(comp, alloc) {
    insert(b, e);
  }

  // Initializer list constructor.
  btree_set_container(std::initializer_list<init_type> init,
                      const key_compare &comp = key_compare(),
                      const allocator_type &alloc = allocator_type())
      : btree_set_container(init.begin(), init.end(), comp, alloc) {}

  // Lookup routines.
  template <typename K = key_type>
  size_type count(const key_arg<K> &key) const {
    return this->tree_.count_unique(key);
  }

  // Insertion routines.
  std::pair<iterator, bool> insert(const value_type &x) {
    return this->tree_.insert_unique(params_type::key(x), x);
  }
  std::pair<iterator, bool> insert(value_type &&x) {
    return this->tree_.insert_unique(params_type::key(x), std::move(x));
  }
  template <typename... Args>
  std::pair<iterator, bool> emplace(Args &&... args) {
    init_type v(std::forward<Args>(args)...);
    return this->tree_.insert_unique(params_type::key(v), std::move(v));
  }
  iterator insert(const_iterator position, const value_type &x) {
    return this->tree_
        .insert_hint_unique(iterator(position), params_type::key(x), x)
        .first;
  }
  iterator insert(const_iterator position, value_type &&x) {
    return this->tree_
        .insert_hint_unique(iterator(position), params_type::key(x),
                            std::move(x))
        .first;
  }
  template <typename... Args>
  iterator emplace_hint(const_iterator position, Args &&... args) {
    init_type v(std::forward<Args>(args)...);
    return this->tree_
        .insert_hint_unique(iterator(position), params_type::key(v),
                            std::move(v))
        .first;
  }
  template <typename InputIterator>
  void insert(InputIterator b, InputIterator e) {
    this->tree_.insert_iterator_unique(b, e);
  }
  void insert(std::initializer_list<init_type> init) {
    this->tree_.insert_iterator_unique(init.begin(), init.end());
  }
  // Deletion routines.
  template <typename K = key_type>
  size_type erase(const key_arg<K> &key) {
    return this->tree_.erase_unique(key);
  }
  using super_type::erase;

  // Merge routines.
  // Moves elements from `src` into `this`. If the element already exists in
  // `this`, it is left unmodified in `src`.
  template <
    typename T,
    typename std::enable_if_t<
      std::conjunction_v<
        std::is_same<value_type, typename T::value_type>,
        std::is_same<allocator_type, typename T::allocator_type>,
        std::is_same<typename params_type::is_map_container,
                     typename T::params_type::is_map_container>>,
      int> = 0>
  void merge(btree_container<T> &src) {  // NOLINT
    for (auto src_it = src.begin(); src_it != src.end();) {
      if (insert(std::move(*src_it)).second) {
        src_it = src.erase(src_it);
      } else {
        ++src_it;
      }
    }
  }

  template <
    typename T,
    typename std::enable_if_t<
      std::conjunction_v<
        std::is_same<value_type, typename T::value_type>,
        std::is_same<allocator_type, typename T::allocator_type>,
        std::is_same<typename params_type::is_map_container,
                     typename T::params_type::is_map_container>>,
      int> = 0>
  void merge(btree_container<T> &&src) {
    merge(src);
  }
};

// A common base class for btree_map and safe_btree_map.
// Base class for btree_map.
template <typename Tree>
class btree_map_container : public btree_set_container<Tree> {
  using super_type = btree_set_container<Tree>;
  using params_type = typename Tree::params_type;

 protected:
  template <class K>
  using key_arg = typename super_type::template key_arg<K>;

 public:
  using key_type = typename Tree::key_type;
  using mapped_type = typename params_type::mapped_type;
  using value_type = typename Tree::value_type;
  using key_compare = typename Tree::key_compare;
  using allocator_type = typename Tree::allocator_type;
  using iterator = typename Tree::iterator;
  using const_iterator = typename Tree::const_iterator;

  // Inherit constructors.
  using super_type::super_type;
  btree_map_container() {}

  // Insertion routines.
  template <typename... Args>
  std::pair<iterator, bool> try_emplace(const key_type &k, Args &&... args) {
    return this->tree_.insert_unique(
        k, std::piecewise_construct, std::forward_as_tuple(k),
        std::forward_as_tuple(std::forward<Args>(args)...));
  }
  template <typename... Args>
  std::pair<iterator, bool> try_emplace(key_type &&k, Args &&... args) {
    // Note: `key_ref` exists to avoid a ClangTidy warning about moving from `k`
    // and then using `k` unsequenced. This is safe because the move is into a
    // forwarding reference and insert_unique guarantees that `key` is never
    // referenced after consuming `args`.
    const key_type& key_ref = k;
    return this->tree_.insert_unique(
        key_ref, std::piecewise_construct, std::forward_as_tuple(std::move(k)),
        std::forward_as_tuple(std::forward<Args>(args)...));
  }
  template <typename... Args>
  iterator try_emplace(const_iterator hint, const key_type &k,
                       Args &&... args) {
    return this->tree_
        .insert_hint_unique(iterator(hint), k, std::piecewise_construct,
                            std::forward_as_tuple(k),
                            std::forward_as_tuple(std::forward<Args>(args)...))
        .first;
  }
  template <typename... Args>
  iterator try_emplace(const_iterator hint, key_type &&k, Args &&... args) {
    // Note: `key_ref` exists to avoid a ClangTidy warning about moving from `k`
    // and then using `k` unsequenced. This is safe because the move is into a
    // forwarding reference and insert_hint_unique guarantees that `key` is
    // never referenced after consuming `args`.
    const key_type& key_ref = k;
    return this->tree_
        .insert_hint_unique(iterator(hint), key_ref, std::piecewise_construct,
                            std::forward_as_tuple(std::move(k)),
                            std::forward_as_tuple(std::forward<Args>(args)...))
        .first;
  }
  mapped_type &operator[](const key_type &k) {
    return try_emplace(k).first->second;
  }
  mapped_type &operator[](key_type &&k) {
    return try_emplace(std::move(k)).first->second;
  }

  template <typename K = key_type>
  mapped_type &at(const key_arg<K> &key) {
    auto it = this->find(key);
    if (it == this->end())
      throw std::out_of_range("btree_map::at");
    return it->second;
  }
  template <typename K = key_type>
  const mapped_type &at(const key_arg<K> &key) const {
    auto it = this->find(key);
    if (it == this->end())
      throw std::out_of_range("btree_map::at");
    return it->second;
  }
};

// A common base class for btree_multiset and btree_multimap.
template <typename Tree>
class btree_multiset_container : public btree_container<Tree> {
  using super_type = btree_container<Tree>;
  using params_type = typename Tree::params_type;
  using init_type = typename params_type::init_type;
  using is_key_compare_to = typename params_type::is_key_compare_to;

  template <class K>
  using key_arg = typename super_type::template key_arg<K>;

 public:
  using key_type = typename Tree::key_type;
  using value_type = typename Tree::value_type;
  using size_type = typename Tree::size_type;
  using key_compare = typename Tree::key_compare;
  using allocator_type = typename Tree::allocator_type;
  using iterator = typename Tree::iterator;
  using const_iterator = typename Tree::const_iterator;

  // Inherit constructors.
  using super_type::super_type;
  btree_multiset_container() {}

  // Range constructor.
  template <class InputIterator>
  btree_multiset_container(InputIterator b, InputIterator e,
                           const key_compare &comp = key_compare(),
                           const allocator_type &alloc = allocator_type())
      : super_type(comp, alloc) {
    insert(b, e);
  }

  // Initializer list constructor.
  btree_multiset_container(std::initializer_list<init_type> init,
                           const key_compare &comp = key_compare(),
                           const allocator_type &alloc = allocator_type())
      : btree_multiset_container(init.begin(), init.end(), comp, alloc) {}

  // Lookup routines.
  template <typename K = key_type>
  size_type count(const key_arg<K> &key) const {
    return this->tree_.count_multi(key);
  }

  // Insertion routines.
  iterator insert(const value_type &x) { return this->tree_.insert_multi(x); }
  iterator insert(value_type &&x) {
    return this->tree_.insert_multi(std::move(x));
  }
  iterator insert(const_iterator position, const value_type &x) {
    return this->tree_.insert_hint_multi(iterator(position), x);
  }
  iterator insert(const_iterator position, value_type &&x) {
    return this->tree_.insert_hint_multi(iterator(position), std::move(x));
  }
  template <typename InputIterator>
  void insert(InputIterator b, InputIterator e) {
    this->tree_.insert_iterator_multi(b, e);
  }
  void insert(std::initializer_list<init_type> init) {
    this->tree_.insert_iterator_multi(init.begin(), init.end());
  }
  template <typename... Args>
  iterator emplace(Args &&... args) {
    return this->tree_.insert_multi(init_type(std::forward<Args>(args)...));
  }
  template <typename... Args>
  iterator emplace_hint(const_iterator position, Args &&... args) {
    return this->tree_.insert_hint_multi(
        iterator(position), init_type(std::forward<Args>(args)...));
  }

  // Deletion routines.
  template <typename K = key_type>
  size_type erase(const key_arg<K> &key) {
    return this->tree_.erase_multi(key);
  }
  using super_type::erase;

  // Merge routines.
  // Moves all elements from `src` into `this`.
  template <
      typename T,
      typename std::enable_if_t<
          std::conjunction_v<
              std::is_same<value_type, typename T::value_type>,
              std::is_same<allocator_type, typename T::allocator_type>,
              std::is_same<typename params_type::is_map_container,
                           typename T::params_type::is_map_container>>,
          int> = 0>
  void merge(btree_container<T> &src) {  // NOLINT
    insert(std::make_move_iterator(src.begin()),
           std::make_move_iterator(src.end()));
    src.clear();
  }

  template <
      typename T,
      typename std::enable_if_t<
          std::conjunction_v<
              std::is_same<value_type, typename T::value_type>,
              std::is_same<allocator_type, typename T::allocator_type>,
              std::is_same<typename params_type::is_map_container,
                           typename T::params_type::is_map_container>>,
          int> = 0>
  void merge(btree_container<T> &&src) {
    merge(src);
  }
};

// A base class for btree_multimap.
template <typename Tree>
class btree_multimap_container : public btree_multiset_container<Tree> {
  using super_type = btree_multiset_container<Tree>;
  using params_type = typename Tree::params_type;

 public:
  using mapped_type = typename params_type::mapped_type;

  // Inherit constructors.
  using super_type::super_type;
  btree_multimap_container() {}
};
} // namespace btree::internal
