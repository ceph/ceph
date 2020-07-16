// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <sys/mman.h>
#include <string.h>

#include <memory>
#include <string.h>

#include "include/buffer.h"

#include "crimson/common/fixed_kv_node_layout.h"
#include "crimson/common/errorator.h"
#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/lba_manager/btree/btree_range_pin.h"

namespace crimson::os::seastore::lba_manager::btree {

constexpr size_t LBA_BLOCK_SIZE = 4096;

/**
 * lba_node_meta_le_t
 *
 * On disk layout for lba_node_meta_t
 */
struct lba_node_meta_le_t {
  laddr_le_t begin = init_le64(0);
  laddr_le_t end = init_le64(0);
  depth_le_t depth = init_les32(0);

  lba_node_meta_le_t() = default;
  lba_node_meta_le_t(const lba_node_meta_le_t &) = default;
  explicit lba_node_meta_le_t(const lba_node_meta_t &val)
    : begin(init_le64(val.begin)),
      end(init_le64(val.end)),
      depth(init_les32(val.depth)) {}

  operator lba_node_meta_t() const {
    return lba_node_meta_t{ begin, end, depth };
  }
};


/**
 * LBAInternalNode
 *
 * Abstracts operations on and layout of internal nodes for the
 * LBA Tree.
 *
 * Layout (4k):
 *   size       : uint32_t[1]                4b
 *   (padding)  :                            4b
 *   meta       : lba_node_meta_le_t[3]      (1*24)b
 *   keys       : laddr_t[255]               (254*8)b
 *   values     : paddr_t[255]               (254*8)b
 *                                           = 4096

 * TODO: make the above capacity calculation part of FixedKVNodeLayout
 * TODO: the above alignment probably isn't portable without further work
 */
constexpr size_t INTERNAL_NODE_CAPACITY = 254;
struct LBAInternalNode
  : LBANode,
    common::FixedKVNodeLayout<
      INTERNAL_NODE_CAPACITY,
      lba_node_meta_t, lba_node_meta_le_t,
      laddr_t, laddr_le_t,
      paddr_t, paddr_le_t> {
  using internal_iterator_t = const_iterator;
  template <typename... T>
  LBAInternalNode(T&&... t) :
    LBANode(std::forward<T>(t)...),
    FixedKVNodeLayout(get_bptr().c_str()) {}

  static constexpr extent_types_t type = extent_types_t::LADDR_INTERNAL;

  lba_node_meta_t get_node_meta() const final { return get_meta(); }

  CachedExtentRef duplicate_for_write() final {
    assert(delta_buffer.empty());
    return CachedExtentRef(new LBAInternalNode(*this));
  };

  delta_buffer_t delta_buffer;
  delta_buffer_t *maybe_get_delta_buffer() {
    return is_mutation_pending() ? &delta_buffer : nullptr;
  }

  lookup_range_ret lookup_range(
    op_context_t c,
    laddr_t addr,
    extent_len_t len) final;

  insert_ret insert(
    op_context_t c,
    laddr_t laddr,
    lba_map_val_t val) final;

  mutate_mapping_ret mutate_mapping(
    op_context_t c,
    laddr_t laddr,
    mutate_func_t &&f) final;

  find_hole_ret find_hole(
    op_context_t c,
    laddr_t min,
    laddr_t max,
    extent_len_t len) final;

  std::tuple<LBANodeRef, LBANodeRef, laddr_t>
  make_split_children(op_context_t c) final {
    auto left = c.cache.alloc_new_extent<LBAInternalNode>(
      c.trans, LBA_BLOCK_SIZE);
    auto right = c.cache.alloc_new_extent<LBAInternalNode>(
      c.trans, LBA_BLOCK_SIZE);
    auto pivot = split_into(*left, *right);
    left->pin.set_range(left->get_meta());
    right->pin.set_range(right->get_meta());
    return std::make_tuple(
      left,
      right,
      pivot);
  }

  LBANodeRef make_full_merge(
    op_context_t c,
    LBANodeRef &right) final {
    auto replacement = c.cache.alloc_new_extent<LBAInternalNode>(
      c.trans, LBA_BLOCK_SIZE);
    replacement->merge_from(*this, *right->cast<LBAInternalNode>());
    replacement->pin.set_range(replacement->get_meta());
    return replacement;
  }

  std::tuple<LBANodeRef, LBANodeRef, laddr_t>
  make_balanced(
    op_context_t c,
    LBANodeRef &_right,
    bool prefer_left) final {
    ceph_assert(_right->get_type() == type);
    auto &right = *_right->cast<LBAInternalNode>();
    auto replacement_left = c.cache.alloc_new_extent<LBAInternalNode>(
      c.trans, LBA_BLOCK_SIZE);
    auto replacement_right = c.cache.alloc_new_extent<LBAInternalNode>(
      c.trans, LBA_BLOCK_SIZE);

    auto pivot = balance_into_new_nodes(
      *this,
      right,
      prefer_left,
      *replacement_left,
      *replacement_right);

    replacement_left->pin.set_range(replacement_left->get_meta());
    replacement_right->pin.set_range(replacement_right->get_meta());
    return std::make_tuple(
      replacement_left,
      replacement_right,
      pivot);
  }

  /**
   * resolve_relative_addrs
   *
   * Internal relative addresses on read or in memory prior to commit
   * are either record or block relative depending on whether this
   * physical node is is_initial_pending() or just is_pending().
   *
   * User passes appropriate base depending on lifecycle and
   * resolve_relative_addrs fixes up relative internal references
   * based on base.
   */
  void resolve_relative_addrs(paddr_t base);

  void on_delta_write(paddr_t record_block_offset) final {
    // All in-memory relative addrs are necessarily record-relative
    resolve_relative_addrs(record_block_offset);
  }

  void on_initial_write() final {
    // All in-memory relative addrs are necessarily block-relative
    resolve_relative_addrs(get_paddr());
  }

  void on_clean_read() final {
    // From initial write of block, relative addrs are necessarily block-relative
    resolve_relative_addrs(get_paddr());
  }

  extent_types_t get_type() const final {
    return type;
  }

  std::ostream &print_detail(std::ostream &out) const final;

  ceph::bufferlist get_delta() final {
    assert(!delta_buffer.empty());
    ceph::buffer::ptr bptr(delta_buffer.get_bytes());
    delta_buffer.copy_out(bptr.c_str(), bptr.length());
    ceph::bufferlist bl;
    bl.push_back(bptr);
    return bl;
  }

  void apply_delta_and_adjust_crc(
    paddr_t base, const ceph::bufferlist &_bl) final {
    assert(_bl.length());
    ceph::bufferlist bl = _bl;
    bl.rebuild();
    delta_buffer_t buffer;
    buffer.copy_in(bl.front().c_str(), bl.front().length());
    buffer.replay(*this);
    set_last_committed_crc(get_crc32c());
    resolve_relative_addrs(base);
  }

  bool at_max_capacity() const final {
    return get_size() == get_capacity();
  }

  bool at_min_capacity() const {
    return get_size() == get_capacity() / 2;
  }

  /// returns iterators containing [l, r)
  std::pair<internal_iterator_t, internal_iterator_t> bound(
    laddr_t l, laddr_t r) {
    // TODO: inefficient
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
    return std::make_pair(retl, retr);
  }

  using split_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  using split_ret = split_ertr::future<LBANodeRef>;
  split_ret split_entry(
    op_context_t c,
    laddr_t addr,
    internal_iterator_t,
    LBANodeRef entry);

  using merge_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  using merge_ret = merge_ertr::future<LBANodeRef>;
  merge_ret merge_entry(
    op_context_t c,
    laddr_t addr,
    internal_iterator_t,
    LBANodeRef entry);

  /// returns iterator for subtree containing laddr
  internal_iterator_t get_containing_child(laddr_t laddr);
};

/**
 * LBALeafNode
 *
 * Abstracts operations on and layout of leaf nodes for the
 * LBA Tree.
 *
 * Layout (4k):
 *   size       : uint32_t[1]                4b
 *   (padding)  :                            4b
 *   meta       : lba_node_meta_le_t[3]      (1*24)b
 *   keys       : laddr_t[170]               (145*8)b
 *   values     : lba_map_val_t[170]         (145*20)b
 *                                           = 4092
 *
 * TODO: update FixedKVNodeLayout to handle the above calculation
 * TODO: the above alignment probably isn't portable without further work
 */
constexpr size_t LEAF_NODE_CAPACITY = 145;

/**
 * lba_map_val_le_t
 *
 * On disk layout for lba_map_val_t.
 */
struct lba_map_val_le_t {
  extent_len_le_t len = init_extent_len_le_t(0);
  paddr_le_t paddr;
  ceph_le32 refcount = init_le32(0);
  ceph_le32 checksum = init_le32(0);

  lba_map_val_le_t() = default;
  lba_map_val_le_t(const lba_map_val_le_t &) = default;
  explicit lba_map_val_le_t(const lba_map_val_t &val)
    : len(init_extent_len_le_t(val.len)),
      paddr(paddr_le_t(val.paddr)),
      refcount(init_le32(val.refcount)),
      checksum(init_le32(val.checksum)) {}

  operator lba_map_val_t() const {
    return lba_map_val_t{ len, paddr, refcount, checksum };
  }
};

struct LBALeafNode
  : LBANode,
    common::FixedKVNodeLayout<
      LEAF_NODE_CAPACITY,
      lba_node_meta_t, lba_node_meta_le_t,
      laddr_t, laddr_le_t,
      lba_map_val_t, lba_map_val_le_t> {
  using internal_iterator_t = const_iterator;
  template <typename... T>
  LBALeafNode(T&&... t) :
    LBANode(std::forward<T>(t)...),
    FixedKVNodeLayout(get_bptr().c_str()) {}

  static constexpr extent_types_t type = extent_types_t::LADDR_LEAF;

  lba_node_meta_t get_node_meta() const final { return get_meta(); }

  CachedExtentRef duplicate_for_write() final {
    assert(delta_buffer.empty());
    return CachedExtentRef(new LBALeafNode(*this));
  };

  delta_buffer_t delta_buffer;
  delta_buffer_t *maybe_get_delta_buffer() {
    return is_mutation_pending() ? &delta_buffer : nullptr;
  }

  lookup_range_ret lookup_range(
    op_context_t c,
    laddr_t addr,
    extent_len_t len) final;

  insert_ret insert(
    op_context_t c,
    laddr_t laddr,
    lba_map_val_t val) final;

  mutate_mapping_ret mutate_mapping(
    op_context_t c,
    laddr_t laddr,
    mutate_func_t &&f) final;

  find_hole_ret find_hole(
    op_context_t c,
    laddr_t min,
    laddr_t max,
    extent_len_t len) final;

  std::tuple<LBANodeRef, LBANodeRef, laddr_t>
  make_split_children(op_context_t c) final {
    auto left = c.cache.alloc_new_extent<LBALeafNode>(
      c.trans, LBA_BLOCK_SIZE);
    auto right = c.cache.alloc_new_extent<LBALeafNode>(
      c.trans, LBA_BLOCK_SIZE);
    auto pivot = split_into(*left, *right);
    left->pin.set_range(left->get_meta());
    right->pin.set_range(right->get_meta());
    return std::make_tuple(
      left,
      right,
      pivot);
  }

  LBANodeRef make_full_merge(
    op_context_t c,
    LBANodeRef &right) final {
    auto replacement = c.cache.alloc_new_extent<LBALeafNode>(
      c.trans, LBA_BLOCK_SIZE);
    replacement->merge_from(*this, *right->cast<LBALeafNode>());
    replacement->pin.set_range(replacement->get_meta());
    return replacement;
  }

  std::tuple<LBANodeRef, LBANodeRef, laddr_t>
  make_balanced(
    op_context_t c,
    LBANodeRef &_right,
    bool prefer_left) final {
    ceph_assert(_right->get_type() == type);
    auto &right = *_right->cast<LBALeafNode>();
    auto replacement_left = c.cache.alloc_new_extent<LBALeafNode>(
      c.trans, LBA_BLOCK_SIZE);
    auto replacement_right = c.cache.alloc_new_extent<LBALeafNode>(
      c.trans, LBA_BLOCK_SIZE);

    auto pivot = balance_into_new_nodes(
      *this,
      right,
      prefer_left,
      *replacement_left,
      *replacement_right);

    replacement_left->pin.set_range(replacement_left->get_meta());
    replacement_right->pin.set_range(replacement_right->get_meta());
    return std::make_tuple(
      replacement_left,
      replacement_right,
      pivot);
  }

  // See LBAInternalNode, same concept
  void resolve_relative_addrs(paddr_t base);

  void on_delta_write(paddr_t record_block_offset) final {
    resolve_relative_addrs(record_block_offset);
  }

  void on_initial_write() final {
    resolve_relative_addrs(get_paddr());
  }

  void on_clean_read() final {
    resolve_relative_addrs(get_paddr());
  }

  ceph::bufferlist get_delta() final {
    assert(!delta_buffer.empty());
    ceph::buffer::ptr bptr(delta_buffer.get_bytes());
    delta_buffer.copy_out(bptr.c_str(), bptr.length());
    ceph::bufferlist bl;
    bl.push_back(bptr);
    return bl;
  }

  void apply_delta_and_adjust_crc(
    paddr_t base, const ceph::bufferlist &_bl) final {
    assert(_bl.length());
    ceph::bufferlist bl = _bl;
    bl.rebuild();
    delta_buffer_t buffer;
    buffer.copy_in(bl.front().c_str(), bl.front().length());
    buffer.replay(*this);
    set_last_committed_crc(get_crc32c());
    resolve_relative_addrs(base);
  }

  extent_types_t get_type() const final {
    return type;
  }

  std::ostream &print_detail(std::ostream &out) const final;

  bool at_max_capacity() const final {
    return get_size() == get_capacity();
  }

  bool at_min_capacity() const final {
    return get_size() == get_capacity();
  }

  /// returns iterators <lb, ub> containing addresses [l, r)
  std::pair<internal_iterator_t, internal_iterator_t> bound(
    laddr_t l, laddr_t r) {
    // TODO: inefficient
    auto retl = begin();
    for (; retl != end(); ++retl) {
      if (retl->get_key() >= l || (retl->get_key() + retl->get_val().len) > l)
	break;
    }
    auto retr = retl;
    for (; retr != end(); ++retr) {
      if (retr->get_key() >= r)
	break;
    }
    return std::make_pair(retl, retr);
  }

  std::pair<internal_iterator_t, internal_iterator_t>
  get_leaf_entries(laddr_t addr, extent_len_t len);
};
using LBALeafNodeRef = TCachedExtentRef<LBALeafNode>;

}
