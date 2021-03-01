// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
#include "include/buffer.h"

#include "crimson/common/fixed_kv_node_layout.h"
#include "crimson/common/errorator.h"
#include "crimson/os/seastore/extentmap_manager.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/extentmap_manager/btree/extentmap_btree_node.h"

namespace crimson::os::seastore::extentmap_manager {

struct extmap_node_meta_le_t {
  depth_le_t depth = init_depth_le(0);

  extmap_node_meta_le_t() = default;
  extmap_node_meta_le_t(const extmap_node_meta_le_t &) = default;
  explicit extmap_node_meta_le_t(const extmap_node_meta_t &val)
    : depth(init_depth_le(val.depth)) {}

  operator extmap_node_meta_t() const {
    return extmap_node_meta_t{ depth };
  }
};

/**
 * ExtMapInnerNode
 *
 * Abstracts operations on and layout of internal nodes for the
 * Extentmap Tree.
 *
 * Layout (4k):
 *   num_entries: uint32_t           4b
 *   meta       : depth              4b
 *   (padding)  :                    8b
 *   keys       : objaddr_t[340]     (340*4)b
 *   values     : laddr_t[340]       (340*8)b
 *                                    = 4096
 */
constexpr size_t INNER_NODE_CAPACITY =
                 (EXTMAP_BLOCK_SIZE - sizeof(uint32_t) - sizeof(extmap_node_meta_t))
                 / (sizeof (objaddr_t) + sizeof(laddr_t));

struct ExtMapInnerNode
  : ExtMapNode,
    common::FixedKVNodeLayout<
      INNER_NODE_CAPACITY,
      extmap_node_meta_t, extmap_node_meta_le_t,
      objaddr_t, ceph_le32,
      laddr_t, laddr_le_t> {
  using internal_iterator_t = const_iterator;
  template <typename... T>
  ExtMapInnerNode(T&&... t) :
    ExtMapNode(std::forward<T>(t)...),
    FixedKVNodeLayout(get_bptr().c_str()) {}

  static constexpr extent_types_t type = extent_types_t::EXTMAP_INNER;

  extmap_node_meta_t get_node_meta() const final {return get_meta();}

  CachedExtentRef duplicate_for_write() final {
    assert(delta_buffer.empty());
    return CachedExtentRef(new ExtMapInnerNode(*this));
  };

  delta_buffer_t delta_buffer;
  delta_buffer_t *maybe_get_delta_buffer() {
    return is_mutation_pending() ? &delta_buffer : nullptr;
  }

  find_lextent_ret find_lextent(ext_context_t ec, objaddr_t lo, extent_len_t len) final;

  insert_ret insert(ext_context_t ec, objaddr_t lo, lext_map_val_t val) final;

  rm_lextent_ret rm_lextent(ext_context_t ec, objaddr_t lo, lext_map_val_t val) final;

  split_children_ret make_split_children(ext_context_t ec) final;

  full_merge_ret make_full_merge(ext_context_t ec, ExtMapNodeRef right) final;

  make_balanced_ret make_balanced(ext_context_t ec, ExtMapNodeRef _right, bool prefer_left) final;

  std::ostream &print_detail_l(std::ostream &out) const final;

  extent_types_t get_type() const final {
    return type;
  }

  ceph::bufferlist get_delta() final {
    assert(!delta_buffer.empty());
    ceph::buffer::ptr bptr(delta_buffer.get_bytes());
    delta_buffer.copy_out(bptr.c_str(), bptr.length());
    ceph::bufferlist bl;
    bl.push_back(bptr);
    return bl;
  }

  void apply_delta(const ceph::bufferlist &_bl) final {
    assert(_bl.length());
    ceph::bufferlist bl = _bl;
    bl.rebuild();
    delta_buffer_t buffer;
    buffer.copy_in(bl.front().c_str(), bl.front().length());
    buffer.replay(*this);
  }

  bool at_max_capacity() const final {
    return get_size() == get_capacity();
  }

  bool at_min_capacity() const {
    return get_size() == get_capacity() / 2;
  }

  unsigned get_node_size() const {
    return get_size();
  }

  /* get the iterator containing [l, r]
   */
  std::pair<internal_iterator_t, internal_iterator_t> bound(
    objaddr_t l, objaddr_t r) {
    auto retl = begin();
    for (; retl != end(); ++retl) {
      if (retl->get_next_key_or_max() > l)
        break;
    }
    auto retr = retl;
    for (; retr != end(); ++retr) {
      if (retr->get_key() >= r)
        break;
    }
    return {retl, retr};
  }

  using split_entry_ertr = TransactionManager::read_extent_ertr;
  using split_entry_ret = split_entry_ertr::future<ExtMapNodeRef>;
  split_entry_ret split_entry(ext_context_t ec, objaddr_t lo,
                              internal_iterator_t, ExtMapNodeRef entry);
  using merge_entry_ertr = TransactionManager::read_extent_ertr;
  using merge_entry_ret = merge_entry_ertr::future<ExtMapNodeRef>;
  merge_entry_ret merge_entry(ext_context_t ec, objaddr_t lo,
                              internal_iterator_t iter, ExtMapNodeRef entry);
  internal_iterator_t get_containing_child(objaddr_t lo);

};

/**
 * ExtMapLeafNode
 *
 * Abstracts operations on and layout of leaf nodes for the
 * ExtentMap Tree.
 *
 * Layout (4k):
 *   num_entries: uint32_t               4b
 *   meta       : depth                  4b
 *   (padding)  :                        8b
 *   keys       : objaddr_t[204]         (204*4)b
 *   values     : lext_map_val_t[204]    (204*16)b
 *                                       = 4096
 */
constexpr size_t LEAF_NODE_CAPACITY = 
                 (EXTMAP_BLOCK_SIZE - sizeof(uint32_t) - sizeof(extmap_node_meta_t))
                 / (sizeof(objaddr_t) + sizeof(lext_map_val_t));

struct lext_map_val_le_t {
  laddr_le_t laddr;
  extent_len_le_t  length = init_extent_len_le(0);

  lext_map_val_le_t() = default;
  lext_map_val_le_t(const lext_map_val_le_t &) = default;
  explicit lext_map_val_le_t(const lext_map_val_t &val)
    : laddr(laddr_le_t(val.laddr)),
      length(init_extent_len_le(val.length)) {}

  operator lext_map_val_t() const {
    return lext_map_val_t{laddr, length};
  }
};

struct ExtMapLeafNode
  : ExtMapNode,
    common::FixedKVNodeLayout<
      LEAF_NODE_CAPACITY,
      extmap_node_meta_t, extmap_node_meta_le_t,
      objaddr_t, ceph_le32,
      lext_map_val_t, lext_map_val_le_t> {
  using internal_iterator_t = const_iterator;
  template <typename... T>
  ExtMapLeafNode(T&&... t) :
    ExtMapNode(std::forward<T>(t)...),
    FixedKVNodeLayout(get_bptr().c_str()) {}

  static constexpr extent_types_t type = extent_types_t::EXTMAP_LEAF;

  extmap_node_meta_t get_node_meta() const final { return get_meta(); }

  CachedExtentRef duplicate_for_write() final {
    assert(delta_buffer.empty());
    return CachedExtentRef(new ExtMapLeafNode(*this));
  };

  delta_buffer_t delta_buffer;
  delta_buffer_t *maybe_get_delta_buffer() {
    return is_mutation_pending() ? &delta_buffer : nullptr;
  }

  find_lextent_ret find_lextent(ext_context_t ec, objaddr_t lo, extent_len_t len) final;

  insert_ret insert(ext_context_t ec, objaddr_t lo, lext_map_val_t val) final;

  rm_lextent_ret rm_lextent(ext_context_t ec, objaddr_t lo, lext_map_val_t val) final;

  split_children_ret make_split_children(ext_context_t ec) final;

  full_merge_ret make_full_merge(ext_context_t ec, ExtMapNodeRef right) final;

  make_balanced_ret make_balanced(ext_context_t ec, ExtMapNodeRef _right, bool prefer_left) final;

  extent_types_t get_type() const final {
    return type;
  }

  ceph::bufferlist get_delta() final {
    assert(!delta_buffer.empty());
    ceph::buffer::ptr bptr(delta_buffer.get_bytes());
    delta_buffer.copy_out(bptr.c_str(), bptr.length());
    ceph::bufferlist bl;
    bl.push_back(bptr);
    return bl;
  }

  void apply_delta(const ceph::bufferlist &_bl) final {
    assert(_bl.length());
    ceph::bufferlist bl = _bl;
    bl.rebuild();
    delta_buffer_t buffer;
    buffer.copy_in(bl.front().c_str(), bl.front().length());
    buffer.replay(*this);
  }

  std::ostream &print_detail_l(std::ostream &out) const final;

  bool at_max_capacity() const final {
    return get_size() == get_capacity();
  }

  bool at_min_capacity() const final {
    return get_size() == get_capacity() / 2;
  }

  unsigned get_node_size() const {
    return get_size();
  }

  /* get the iterator containing [l, r]
   */
  std::pair<internal_iterator_t, internal_iterator_t> bound(
    objaddr_t l, objaddr_t r) {
    auto retl = begin();
    for (; retl != end(); ++retl) {
      if (retl->get_key() >= l || (retl->get_key() + retl->get_val().length) > l)
        break;
    }
    auto retr = retl;
    for (; retr != end(); ++retr) {
      if (retr->get_key() >= r)
        break;
    }
    return {retl, retr};
  }

  std::pair<internal_iterator_t, internal_iterator_t>
  get_leaf_entries(objaddr_t lo, extent_len_t len);

};
using ExtentMapLeafNodeRef = TCachedExtentRef<ExtMapLeafNode>;

}
