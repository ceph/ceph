// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include "include/byteorder.h"

#include "crimson/common/layout.h"

namespace crimson::common {

/**
 * FixedKVNodeLayout
 *
 * Reusable implementation of a fixed size block mapping
 * K -> V with internal representations KINT and VINT.
 *
 * Uses absl::container_internal::Layout for the actual memory layout.
 *
 * The primary interface exposed is centered on the fixed_node_iter_t
 * and related methods.
 *
 * Also included are helpers for doing splits and merges as for a btree.
 */
template <
  size_t CAPACITY,
  typename K,
  typename KINT,
  typename V,
  typename VINT>
class FixedKVNodeLayout {
  char *buf = nullptr;

  using L = absl::container_internal::Layout<ceph_le32, KINT, VINT>;
  static constexpr L layout{1, CAPACITY, CAPACITY};

public:
  struct fixed_node_iter_t {
    friend class FixedKVNodeLayout;
    FixedKVNodeLayout *node;
    uint16_t offset;

    fixed_node_iter_t(
      FixedKVNodeLayout *parent,
      uint16_t offset) : node(parent), offset(offset) {}

    fixed_node_iter_t(const fixed_node_iter_t &) = default;
    fixed_node_iter_t(fixed_node_iter_t &&) = default;
    fixed_node_iter_t &operator=(const fixed_node_iter_t &) = default;
    fixed_node_iter_t &operator=(fixed_node_iter_t &&) = default;

    // Work nicely with for loops without requiring a nested type.
    fixed_node_iter_t &operator*() { return *this; }
    fixed_node_iter_t *operator->() { return this; }

    fixed_node_iter_t operator++(int) {
      auto ret = *this;
      ++offset;
      return ret;
    }

    fixed_node_iter_t &operator++() {
      ++offset;
      return *this;
    }

    uint16_t operator-(const fixed_node_iter_t &rhs) const {
      assert(rhs.node == node);
      return offset - rhs.offset;
    }

    fixed_node_iter_t operator+(uint16_t off) const {
      return fixed_node_iter_t(
	node,
	offset + off);
    }
    fixed_node_iter_t operator-(uint16_t off) const {
      return fixed_node_iter_t(
	node,
	offset - off);
    }

    bool operator==(const fixed_node_iter_t &rhs) const {
      assert(node == rhs.node);
      return rhs.offset == offset;
    }

    bool operator!=(const fixed_node_iter_t &rhs) const {
      return !(*this == rhs);
    }

    K get_key() const {
      return K(node->get_key_ptr()[offset]);
    }

    void set_key(K _lb) {
      KINT lb;
      lb = _lb;
      node->get_key_ptr()[offset] = lb;
    }

    K get_next_key_or_max() const {
      auto next = *this + 1;
      if (next == node->end())
	return std::numeric_limits<K>::max();
      else
	return next->get_key();
    }

    V get_val() const {
      return V(node->get_val_ptr()[offset]);
    };

    void set_val(V val) {
      node->get_val_ptr()[offset] = VINT(val);
    }

    bool contains(K addr) {
      return (get_key() <= addr) && (get_next_key_or_max() > addr);
    }

    uint16_t get_offset() const {
      return offset;
    }

  private:
    char *get_key_ptr() {
      return reinterpret_cast<char *>(node->get_key_ptr() + offset);
    }

    char *get_val_ptr() {
      return reinterpret_cast<char *>(node->get_val_ptr() + offset);
    }
  };

public:
  FixedKVNodeLayout(char *buf) :
    buf(buf) {}

  fixed_node_iter_t begin() {
    return fixed_node_iter_t(
      this,
      0);
  }

  fixed_node_iter_t end() {
    return fixed_node_iter_t(
      this,
      get_size());
  }

  fixed_node_iter_t iter_idx(uint16_t off) {
    return fixed_node_iter_t(
      this,
      off);
  }

  fixed_node_iter_t find(K l) {
    auto ret = begin();
    for (; ret != end(); ++ret) {
      if (ret->get_key() == l)
	break;
    }
    return ret;
  }

  fixed_node_iter_t get_split_pivot() {
    return iter_idx(get_size() / 2);
  }

private:
  KINT *get_key_ptr() {
    return layout.template Pointer<1>(buf);
  }

  VINT *get_val_ptr() {
    return layout.template Pointer<2>(buf);
  }

public:
  uint16_t get_size() const {
    return *layout.template Pointer<0>(buf);
  }

  void set_size(uint16_t size) {
    *layout.template Pointer<0>(buf) = size;
  }

  constexpr static size_t get_capacity() {
    return CAPACITY;
  }

  /**
   * copy_from_foreign
   *
   * Copies entries from [from_src, to_src) to tgt.
   *
   * tgt and from_src must be from different nodes.
   * from_src and to_src must be from the same node.
   */
  static void copy_from_foreign(
    fixed_node_iter_t tgt,
    fixed_node_iter_t from_src,
    fixed_node_iter_t to_src) {
    assert(tgt->node != from_src->node);
    assert(to_src->node == from_src->node);
    memcpy(
      tgt->get_val_ptr(), from_src->get_val_ptr(),
      to_src->get_val_ptr() - from_src->get_val_ptr());
    memcpy(
      tgt->get_key_ptr(), from_src->get_key_ptr(),
      to_src->get_key_ptr() - from_src->get_key_ptr());
  }

  /**
   * copy_from_local
   *
   * Copies entries from [from_src, to_src) to tgt.
   *
   * tgt, from_src, and to_src must be from the same node.
   */
  static void copy_from_local(
    fixed_node_iter_t tgt,
    fixed_node_iter_t from_src,
    fixed_node_iter_t to_src) {
    assert(tgt->node == from_src->node);
    assert(to_src->node == from_src->node);
    memmove(
      tgt->get_val_ptr(), from_src->get_val_ptr(),
      to_src->get_val_ptr() - from_src->get_val_ptr());
    memmove(
      tgt->get_key_ptr(), from_src->get_key_ptr(),
      to_src->get_key_ptr() - from_src->get_key_ptr());
  }

  /**
   * split_into
   *
   * Takes *this and splits its contents into left and right.
   */
  K split_into(
    FixedKVNodeLayout &left,
    FixedKVNodeLayout &right) {
    auto piviter = get_split_pivot();

    left.copy_from_foreign(left.begin(), begin(), piviter);
    left.set_size(piviter - begin());

    right.copy_from_foreign(right.begin(), piviter, end());
    right.set_size(end() - piviter);

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
    FixedKVNodeLayout &left,
    FixedKVNodeLayout &right)
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
    FixedKVNodeLayout &left,
    FixedKVNodeLayout &right,
    bool prefer_left,
    FixedKVNodeLayout &replacement_left,
    FixedKVNodeLayout &replacement_right)
  {
    auto total = left.get_size() + right.get_size();
    auto pivot_idx = (left.get_size() + right.get_size()) / 2;
    if (total % 2 && prefer_left) {
      pivot_idx++;
    }
    auto replacement_pivot = pivot_idx > left.get_size() ?
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

    return replacement_pivot;
  }
};

}
