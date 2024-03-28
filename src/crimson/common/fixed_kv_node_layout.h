// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <algorithm>
#include <iostream>

#include <boost/iterator/counting_iterator.hpp>

#include "include/byteorder.h"
#include "include/crc32c.h"

#include "crimson/common/layout.h"

namespace crimson::common {

template <typename T, bool is_const>
struct maybe_const_t {
};
template<typename T>
struct maybe_const_t<T, true> {
  using type = const T*;
};
template<typename T>
struct maybe_const_t<T, false> {
  using type = T*;
};


/**
 * FixedKVNodeLayout
 *
 * Reusable implementation of a fixed size block mapping
 * K -> V with internal representations KINT and VINT.
 *
 * Uses absl::container_internal::Layout for the actual memory layout.
 *
 * The primary interface exposed is centered on the iterator
 * and related methods.
 *
 * Also included are helpers for doing splits and merges as for a btree.
 */
template <
  size_t CAPACITY,
  typename Meta,
  typename MetaInt,
  typename K,
  typename KINT,
  typename V,
  typename VINT,
  bool VALIDATE_INVARIANTS=true>
class FixedKVNodeLayout {
  char *buf = nullptr;

  using L = absl::container_internal::Layout<
    ceph_le32, ceph_le32, MetaInt, KINT, VINT>;
  static constexpr L layout{1, 1, 1, CAPACITY, CAPACITY};

public:
  template <bool is_const>
  struct iter_t {
    friend class FixedKVNodeLayout;
    using parent_t = typename maybe_const_t<FixedKVNodeLayout, is_const>::type;

    parent_t node;
    uint16_t offset = 0;

    iter_t() = default;
    iter_t(
      parent_t parent,
      uint16_t offset) : node(parent), offset(offset) {}

    iter_t(const iter_t &) noexcept = default;
    iter_t(iter_t &&) noexcept = default;
    template<bool is_const_ = is_const>
    iter_t(const iter_t<false>& it, std::enable_if_t<is_const_, int> = 0)
      : iter_t{it.node, it.offset}
    {}
    iter_t &operator=(const iter_t &) = default;
    iter_t &operator=(iter_t &&) = default;

    // Work nicely with for loops without requiring a nested type.
    using reference = iter_t&;
    iter_t &operator*() { return *this; }
    iter_t *operator->() { return this; }

    iter_t operator++(int) {
      auto ret = *this;
      ++offset;
      return ret;
    }

    iter_t &operator++() {
      ++offset;
      return *this;
    }

    iter_t operator--(int) {
      assert(offset > 0);
      auto ret = *this;
      --offset;
      return ret;
    }

    iter_t &operator--() {
      assert(offset > 0);
      --offset;
      return *this;
    }

    uint16_t operator-(const iter_t &rhs) const {
      assert(rhs.node == node);
      return offset - rhs.offset;
    }

    iter_t operator+(uint16_t off) const {
      return iter_t(
	node,
	offset + off);
    }
    iter_t operator-(uint16_t off) const {
      return iter_t(
	node,
	offset - off);
    }

    friend bool operator==(const iter_t &lhs, const iter_t &rhs) {
      assert(lhs.node == rhs.node);
      return lhs.offset == rhs.offset;
    }

    friend bool operator!=(const iter_t &lhs, const iter_t &rhs) {
      return !(lhs == rhs);
    }

    friend bool operator==(const iter_t<is_const> &lhs, const iter_t<!is_const> &rhs) {
      assert(lhs.node == rhs.node);
      return lhs.offset == rhs.offset;
    }
    friend bool operator!=(const iter_t<is_const> &lhs, const iter_t<!is_const> &rhs) {
      return !(lhs == rhs);
    }
    K get_key() const {
      return K(node->get_key_ptr()[offset]);
    }

    K get_next_key_or_max() const {
      auto next = *this + 1;
      if (next == node->end())
	return std::numeric_limits<K>::max();
      else
	return next->get_key();
    }

    void set_val(V val) const {
      static_assert(!is_const);
      node->get_val_ptr()[offset] = VINT(val);
    }

    V get_val() const {
      return V(node->get_val_ptr()[offset]);
    };

    bool contains(K addr) const {
      return (get_key() <= addr) && (get_next_key_or_max() > addr);
    }

    uint16_t get_offset() const {
      return offset;
    }

  private:
    void set_key(K _lb) const {
      static_assert(!is_const);
      KINT lb;
      lb = _lb;
      node->get_key_ptr()[offset] = lb;
    }

    typename maybe_const_t<char, is_const>::type get_key_ptr() const {
      return reinterpret_cast<
	typename maybe_const_t<char, is_const>::type>(
	  node->get_key_ptr() + offset);
    }

    typename maybe_const_t<char, is_const>::type get_val_ptr() const {
      return reinterpret_cast<
	typename maybe_const_t<char, is_const>::type>(
	  node->get_val_ptr() + offset);
    }
  };
  using const_iterator = iter_t<true>;
  using iterator = iter_t<false>;

  struct delta_t {
    enum class op_t : uint8_t {
      INSERT,
      REMOVE,
      UPDATE,
    } op;
    KINT key;
    VINT val;

    void replay(FixedKVNodeLayout &l) {
      switch (op) {
      case op_t::INSERT: {
	l.insert(l.lower_bound(key), key, val);
	break;
      }
      case op_t::REMOVE: {
	auto iter = l.find(key);
	assert(iter != l.end());
	l.remove(iter);
	break;
      }
      case op_t::UPDATE: {
	auto iter = l.find(key);
	assert(iter != l.end());
	l.update(iter, val);
	break;
      }
      default:
	assert(0 == "Impossible");
      }
    }

    bool operator==(const delta_t &rhs) const {
      return op == rhs.op &&
	key == rhs.key &&
	val == rhs.val;
    }
  };

public:
  class delta_buffer_t {
    std::vector<delta_t> buffer;
  public:
    bool empty() const {
      return buffer.empty();
    }
    void insert(
      const K &key,
      const V &val) {
      KINT k;
      k = key;
      buffer.push_back(
	delta_t{
	  delta_t::op_t::INSERT,
	  k,
	  VINT(val)
	});
    }
    void update(
      const K &key,
      const V &val) {
      KINT k;
      k = key;
      buffer.push_back(
	delta_t{
	  delta_t::op_t::UPDATE,
	  k,
	  VINT(val)
	});
    }
    void remove(const K &key) {
      KINT k;
      k = key;
      buffer.push_back(
	delta_t{
	  delta_t::op_t::REMOVE,
	  k,
	  VINT()
	});
    }
    void replay(FixedKVNodeLayout &node) {
      for (auto &i: buffer) {
	i.replay(node);
      }
    }
    size_t get_bytes() const {
      return buffer.size() * sizeof(delta_t);
    }
    void copy_out(char *out, size_t len) {
      assert(len == get_bytes());
      ::memcpy(out, reinterpret_cast<const void *>(buffer.data()), get_bytes());
      buffer.clear();
    }
    void copy_in(const char *out, size_t len) {
      assert(empty());
      assert(len % sizeof(delta_t) == 0);
      buffer = std::vector(
	reinterpret_cast<const delta_t*>(out),
	reinterpret_cast<const delta_t*>(out + len));
    }
    bool operator==(const delta_buffer_t &rhs) const {
      return buffer == rhs.buffer;
    }
  };

  void journal_insert(
    const_iterator _iter,
    const K &key,
    const V &val,
    delta_buffer_t *recorder) {
    auto iter = iterator(this, _iter.offset);
    if (recorder) {
      recorder->insert(
	key,
	val);
    }
    insert(iter, key, val);
  }

  void journal_update(
    const_iterator _iter,
    const V &val,
    delta_buffer_t *recorder) {
    auto iter = iterator(this, _iter.offset);
    if (recorder) {
      recorder->update(iter->get_key(), val);
    }
    update(iter, val);
  }

  void journal_replace(
    const_iterator _iter,
    const K &key,
    const V &val,
    delta_buffer_t *recorder) {
    auto iter = iterator(this, _iter.offset);
    if (recorder) {
      recorder->remove(iter->get_key());
      recorder->insert(key, val);
    }
    replace(iter, key, val);
  }


  void journal_remove(
    const_iterator _iter,
    delta_buffer_t *recorder) {
    auto iter = iterator(this, _iter.offset);
    if (recorder) {
      recorder->remove(iter->get_key());
    }
    remove(iter);
  }


  FixedKVNodeLayout(char *buf) :
    buf(buf) {}

  virtual ~FixedKVNodeLayout() = default;

  const_iterator begin() const {
    return const_iterator(
      this,
      0);
  }

  const_iterator end() const {
    return const_iterator(
      this,
      get_size());
  }

  iterator begin() {
    return iterator(
      this,
      0);
  }

  iterator end() {
    return iterator(
      this,
      get_size());
  }

  const_iterator iter_idx(uint16_t off) const {
    return const_iterator(
      this,
      off);
  }

  const_iterator find(K l) const {
    auto ret = begin();
    for (; ret != end(); ++ret) {
      if (ret->get_key() == l)
	break;
    }
    return ret;
  }
  iterator find(K l) {
    const auto &tref = *this;
    return iterator(this, tref.find(l).offset);
  }

  const_iterator lower_bound(K l) const {
    auto it = std::lower_bound(boost::make_counting_iterator<uint16_t>(0),
	                       boost::make_counting_iterator<uint16_t>(get_size()),
		               l,
		               [this](uint16_t i, K key) {
			         const_iterator iter(this, i);
			         return iter->get_key() < key;
			       });
    return const_iterator(this, *it);
  }

  iterator lower_bound(K l) {
    const auto &tref = *this;
    return iterator(this, tref.lower_bound(l).offset);
  }

  const_iterator upper_bound(K l) const {
    auto it = std::upper_bound(boost::make_counting_iterator<uint16_t>(0),
	                       boost::make_counting_iterator<uint16_t>(get_size()),
		               l,
		               [this](K key, uint16_t i) {
			         const_iterator iter(this, i);
			         return key < iter->get_key();
			       });
    return const_iterator(this, *it);
  }

  iterator upper_bound(K l) {
    const auto &tref = *this;
    return iterator(this, tref.upper_bound(l).offset);
  }

  const_iterator get_split_pivot() const {
    return iter_idx(get_size() / 2);
  }

  uint16_t get_size() const {
    return *layout.template Pointer<1>(buf);
  }

  /**
   * set_size
   *
   * Set size representation to match size
   */
  void set_size(uint16_t size) {
    *layout.template Pointer<1>(buf) = size;
  }

  uint32_t get_phy_checksum() const {
    return *layout.template Pointer<0>(buf);
  }

  void set_phy_checksum(uint32_t checksum) {
    *layout.template Pointer<0>(buf) = checksum;
  }

  uint32_t calc_phy_checksum() const {
    return calc_phy_checksum_iteratively<4>(1);
  }

  /**
   * get_meta/set_meta
   *
   * Enables stashing a templated type within the layout.
   * Cannot be modified after initial write as it is not represented
   * in delta_t
   */
  Meta get_meta() const {
    MetaInt &metaint = *layout.template Pointer<2>(buf);
    return Meta(metaint);
  }
  void set_meta(const Meta &meta) {
    *layout.template Pointer<2>(buf) = MetaInt(meta);
  }

  constexpr static size_t get_capacity() {
    return CAPACITY;
  }

  bool operator==(const FixedKVNodeLayout &rhs) const {
    if (get_size() != rhs.get_size()) {
      return false;
    }

    auto iter = begin();
    auto iter2 = rhs.begin();
    while (iter != end()) {
      if (iter->get_key() != iter2->get_key() ||
	  iter->get_val() != iter2->get_val()) {
	return false;
      }
      iter++;
      iter2++;
    }
    return true;
  }

  /**
   * split_into
   *
   * Takes *this and splits its contents into left and right.
   */
  K split_into(
    FixedKVNodeLayout &left,
    FixedKVNodeLayout &right) const {
    auto piviter = get_split_pivot();

    left.copy_from_foreign(left.begin(), begin(), piviter);
    left.set_size(piviter - begin());

    right.copy_from_foreign(right.begin(), piviter, end());
    right.set_size(end() - piviter);

    auto [lmeta, rmeta] = get_meta().split_into(piviter->get_key());
    left.set_meta(lmeta);
    right.set_meta(rmeta);

    return piviter->get_key();
  }

  /**
   * merge_from
   *
   * Takes two nodes and copies their contents into *this.
   *
   * precondition: left.size() + right.size() < CAPACITY
   */
  void merge_from(
    const FixedKVNodeLayout &left,
    const FixedKVNodeLayout &right)
  {
    copy_from_foreign(
      end(),
      left.begin(),
      left.end());
    set_size(left.get_size());
    copy_from_foreign(
      end(),
      right.begin(),
      right.end());
    set_size(left.get_size() + right.get_size());
    set_meta(Meta::merge_from(left.get_meta(), right.get_meta()));
  }

  /**
   * balance_into_new_nodes
   *
   * Takes the contents of left and right and copies them into
   * replacement_left and replacement_right such that in the
   * event that the number of elements is odd the extra goes to
   * the left side iff prefer_left.
   */
  static K balance_into_new_nodes(
    const FixedKVNodeLayout &left,
    const FixedKVNodeLayout &right,
    bool prefer_left,
    FixedKVNodeLayout &replacement_left,
    FixedKVNodeLayout &replacement_right)
  {
    auto total = left.get_size() + right.get_size();
    auto pivot_idx = (left.get_size() + right.get_size()) / 2;
    if (total % 2 && prefer_left) {
      pivot_idx++;
    }
    auto replacement_pivot = pivot_idx >= left.get_size() ?
      right.iter_idx(pivot_idx - left.get_size())->get_key() :
      left.iter_idx(pivot_idx)->get_key();

    if (pivot_idx < left.get_size()) {
      replacement_left.copy_from_foreign(
	replacement_left.end(),
	left.begin(),
	left.iter_idx(pivot_idx));
      replacement_left.set_size(pivot_idx);

      replacement_right.copy_from_foreign(
	replacement_right.end(),
	left.iter_idx(pivot_idx),
	left.end());

      replacement_right.set_size(left.get_size() - pivot_idx);
      replacement_right.copy_from_foreign(
	replacement_right.end(),
	right.begin(),
	right.end());
      replacement_right.set_size(total - pivot_idx);
    } else {
      replacement_left.copy_from_foreign(
	replacement_left.end(),
	left.begin(),
	left.end());
      replacement_left.set_size(left.get_size());

      replacement_left.copy_from_foreign(
	replacement_left.end(),
	right.begin(),
	right.iter_idx(pivot_idx - left.get_size()));
      replacement_left.set_size(pivot_idx);

      replacement_right.copy_from_foreign(
	replacement_right.end(),
	right.iter_idx(pivot_idx - left.get_size()),
	right.end());
      replacement_right.set_size(total - pivot_idx);
    }

    auto [lmeta, rmeta] = Meta::rebalance(
      left.get_meta(), right.get_meta(), replacement_pivot);
    replacement_left.set_meta(lmeta);
    replacement_right.set_meta(rmeta);
    return replacement_pivot;
  }

private:
  void insert(
    iterator iter,
    const K &key,
    const V &val) {
    if (VALIDATE_INVARIANTS) {
      if (iter != begin()) {
	assert((iter - 1)->get_key() < key);
      }
      if (iter != end()) {
	assert(iter->get_key() > key);
      }
      assert(get_size() < CAPACITY);
    }
    copy_from_local(iter + 1, iter, end());
    iter->set_key(key);
    iter->set_val(val);
    set_size(get_size() + 1);
  }

  void update(
    iterator iter,
    V val) {
    assert(iter != end());
    iter->set_val(val);
  }

  void replace(
    iterator iter,
    const K &key,
    const V &val) {
    assert(iter != end());
    if (VALIDATE_INVARIANTS) {
      if (iter != begin()) {
	assert((iter - 1)->get_key() < key);
      }
      if ((iter + 1) != end()) {
	assert((iter + 1)->get_key() > key);
      }
    }
    iter->set_key(key);
    iter->set_val(val);
  }

  void remove(iterator iter) {
    assert(iter != end());
    copy_from_local(iter, iter + 1, end());
    set_size(get_size() - 1);
  }

  /**
   * get_key_ptr
   *
   * Get pointer to start of key array
   */
  KINT *get_key_ptr() {
    return layout.template Pointer<3>(buf);
  }
  const KINT *get_key_ptr() const {
    return layout.template Pointer<3>(buf);
  }

  template <size_t N>
  uint32_t calc_phy_checksum_iteratively(uint32_t crc) const {
    uint32_t r = ceph_crc32c(
      crc,
      (unsigned char const *)layout.template Pointer<N>(buf),
      layout.template Size<N>());
    return calc_phy_checksum_iteratively<N-1>(r);
  }

  template <>
  uint32_t calc_phy_checksum_iteratively<0>(uint32_t crc) const {
    return crc;
  }

  /**
   * get_val_ptr
   *
   * Get pointer to start of val array
   */
  VINT *get_val_ptr() {
    return layout.template Pointer<4>(buf);
  }
  const VINT *get_val_ptr() const {
    return layout.template Pointer<4>(buf);
  }

  /**
   * node_resolve/unresolve_vals
   *
   * If the representation for values depends in some way on the
   * node in which they are located, users may implement
   * resolve/unresolve to enable copy_from_foreign to handle that
   * transition.
   */
  virtual void node_resolve_vals(iterator from, iterator to) const {}
  virtual void node_unresolve_vals(iterator from, iterator to) const {}

  /**
   * copy_from_foreign
   *
   * Copies entries from [from_src, to_src) to tgt.
   *
   * tgt and from_src must be from different nodes.
   * from_src and to_src must be from the same node.
   */
  static void copy_from_foreign(
    iterator tgt,
    const_iterator from_src,
    const_iterator to_src) {
    assert(tgt->node != from_src->node);
    assert(to_src->node == from_src->node);
    memcpy(
      tgt->get_val_ptr(), from_src->get_val_ptr(),
      to_src->get_val_ptr() - from_src->get_val_ptr());
    memcpy(
      tgt->get_key_ptr(), from_src->get_key_ptr(),
      to_src->get_key_ptr() - from_src->get_key_ptr());
    from_src->node->node_resolve_vals(tgt, tgt + (to_src - from_src));
    tgt->node->node_unresolve_vals(tgt, tgt + (to_src - from_src));
  }

  /**
   * copy_from_local
   *
   * Copies entries from [from_src, to_src) to tgt.
   *
   * tgt, from_src, and to_src must be from the same node.
   */
  static void copy_from_local(
    iterator tgt,
    iterator from_src,
    iterator to_src) {
    assert(tgt->node == from_src->node);
    assert(to_src->node == from_src->node);
    memmove(
      tgt->get_val_ptr(), from_src->get_val_ptr(),
      to_src->get_val_ptr() - from_src->get_val_ptr());
    memmove(
      tgt->get_key_ptr(), from_src->get_key_ptr(),
      to_src->get_key_ptr() - from_src->get_key_ptr());
  }
};

}
