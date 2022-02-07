// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <sys/mman.h>
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

using base_iertr = LBAManager::base_iertr;

struct op_context_t {
  Cache &cache;
  Transaction &trans;
  btree_pin_set_t *pins = nullptr;
};

/**
 * lba_map_val_t
 *
 * struct representing a single lba mapping
 */
struct lba_map_val_t {
  extent_len_t len = 0;  ///< length of mapping
  paddr_t paddr;         ///< physical addr of mapping
  uint32_t refcount = 0; ///< refcount
  uint32_t checksum = 0; ///< checksum of original block written at paddr (TODO)

  lba_map_val_t() = default;
  lba_map_val_t(
    extent_len_t len,
    paddr_t paddr,
    uint32_t refcount,
    uint32_t checksum)
    : len(len), paddr(paddr), refcount(refcount), checksum(checksum) {}
};
WRITE_EQ_OPERATORS_4(
  lba_map_val_t,
  len,
  paddr,
  refcount,
  checksum);

std::ostream& operator<<(std::ostream& out, const lba_map_val_t&);

class BtreeLBAPin;
using BtreeLBAPinRef = std::unique_ptr<BtreeLBAPin>;

constexpr size_t LBA_BLOCK_SIZE = 4096;

/**
 * lba_node_meta_le_t
 *
 * On disk layout for lba_node_meta_t
 */
struct lba_node_meta_le_t {
  laddr_le_t begin = laddr_le_t(0);
  laddr_le_t end = laddr_le_t(0);
  depth_le_t depth = init_depth_le(0);

  lba_node_meta_le_t() = default;
  lba_node_meta_le_t(const lba_node_meta_le_t &) = default;
  explicit lba_node_meta_le_t(const lba_node_meta_t &val)
    : begin(ceph_le64(val.begin)),
      end(ceph_le64(val.end)),
      depth(init_depth_le(val.depth)) {}

  operator lba_node_meta_t() const {
    return lba_node_meta_t{ begin, end, depth };
  }
};

/**
 * LBANode
 *
 * Base class enabling recursive lookup between internal and leaf nodes.
 */
struct LBANode : CachedExtent {
  using LBANodeRef = TCachedExtentRef<LBANode>;

  btree_range_pin_t pin;

  LBANode(ceph::bufferptr &&ptr) : CachedExtent(std::move(ptr)), pin(this) {}
  LBANode(const LBANode &rhs)
    : CachedExtent(rhs), pin(rhs.pin, this) {}

  virtual lba_node_meta_t get_node_meta() const = 0;

  virtual ~LBANode() = default;

  void on_delta_write(paddr_t record_block_offset) final {
    // All in-memory relative addrs are necessarily record-relative
    assert(get_prior_instance());
    pin.take_pin(get_prior_instance()->cast<LBANode>()->pin);
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

  virtual void resolve_relative_addrs(paddr_t base) = 0;
};
using LBANodeRef = LBANode::LBANodeRef;

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
  using Ref = TCachedExtentRef<LBAInternalNode>;
  using internal_iterator_t = const_iterator;
  template <typename... T>
  LBAInternalNode(T&&... t) :
    LBANode(std::forward<T>(t)...),
    FixedKVNodeLayout(get_bptr().c_str()) {}

  static constexpr extent_types_t TYPE = extent_types_t::LADDR_INTERNAL;

  lba_node_meta_t get_node_meta() const { return get_meta(); }

  CachedExtentRef duplicate_for_write() final {
    assert(delta_buffer.empty());
    return CachedExtentRef(new LBAInternalNode(*this));
  };

  delta_buffer_t delta_buffer;
  delta_buffer_t *maybe_get_delta_buffer() {
    return is_mutation_pending() ? &delta_buffer : nullptr;
  }

  void update(
    const_iterator iter,
    paddr_t addr) {
    return journal_update(
      iter,
      maybe_generate_relative(addr),
      maybe_get_delta_buffer());
  }

  void insert(
    const_iterator iter,
    laddr_t pivot,
    paddr_t addr) {
    return journal_insert(
      iter,
      pivot,
      maybe_generate_relative(addr),
      maybe_get_delta_buffer());
  }

  void remove(const_iterator iter) {
    return journal_remove(
      iter,
      maybe_get_delta_buffer());
  }

  void replace(
    const_iterator iter,
    laddr_t pivot,
    paddr_t addr) {
    return journal_replace(
      iter,
      pivot,
      maybe_generate_relative(addr),
      maybe_get_delta_buffer());
  }

  std::tuple<Ref, Ref, laddr_t>
  make_split_children(op_context_t c) {
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

  Ref make_full_merge(
    op_context_t c,
    Ref &right) {
    auto replacement = c.cache.alloc_new_extent<LBAInternalNode>(
      c.trans, LBA_BLOCK_SIZE);
    replacement->merge_from(*this, *right->cast<LBAInternalNode>());
    replacement->pin.set_range(replacement->get_meta());
    return replacement;
  }

  std::tuple<Ref, Ref, laddr_t>
  make_balanced(
    op_context_t c,
    Ref &_right,
    bool prefer_left) {
    ceph_assert(_right->get_type() == get_type());
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
   * Internal relative addresses on read or in memory prior to commit
   * are either record or block relative depending on whether this
   * physical node is is_initial_pending() or just is_pending().
   *
   * User passes appropriate base depending on lifecycle and
   * resolve_relative_addrs fixes up relative internal references
   * based on base.
   */
  void resolve_relative_addrs(paddr_t base);
  void node_resolve_vals(iterator from, iterator to) const final {
    if (is_initial_pending()) {
      for (auto i = from; i != to; ++i) {
	if (i->get_val().is_relative()) {
	  assert(i->get_val().is_block_relative());
	  i->set_val(get_paddr().add_relative(i->get_val()));
	}
      }
    }
  }
  void node_unresolve_vals(iterator from, iterator to) const final {
    if (is_initial_pending()) {
      for (auto i = from; i != to; ++i) {
	if (i->get_val().is_relative()) {
	  assert(i->get_val().is_record_relative());
	  i->set_val(i->get_val() - get_paddr());
	}
      }
    }
  }

  extent_types_t get_type() const final {
    return TYPE;
  }

  std::ostream &print_detail(std::ostream &out) const final;

  ceph::bufferlist get_delta() final {
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

  constexpr static size_t get_min_capacity() {
    return (get_capacity() - 1) / 2;
  }

  bool at_max_capacity() const {
    assert(get_size() <= get_capacity());
    return get_size() == get_capacity();
  }

  bool at_min_capacity() const {
    assert(get_size() >= (get_min_capacity() - 1));
    return get_size() <= get_min_capacity();
  }

  bool below_min_capacity() const {
    assert(get_size() >= (get_min_capacity() - 1));
    return get_size() < get_min_capacity();
  }
};
using LBAInternalNodeRef = LBAInternalNode::Ref;

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
  extent_len_le_t len = init_extent_len_le(0);
  paddr_le_t paddr;
  ceph_le32 refcount{0};
  ceph_le32 checksum{0};

  lba_map_val_le_t() = default;
  lba_map_val_le_t(const lba_map_val_le_t &) = default;
  explicit lba_map_val_le_t(const lba_map_val_t &val)
    : len(init_extent_len_le(val.len)),
      paddr(paddr_le_t(val.paddr)),
      refcount(val.refcount),
      checksum(val.checksum) {}

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
  using Ref = TCachedExtentRef<LBALeafNode>;
  using internal_iterator_t = const_iterator;
  template <typename... T>
  LBALeafNode(T&&... t) :
    LBANode(std::forward<T>(t)...),
    FixedKVNodeLayout(get_bptr().c_str()) {}

  static constexpr extent_types_t TYPE = extent_types_t::LADDR_LEAF;

  lba_node_meta_t get_node_meta() const { return get_meta(); }

  CachedExtentRef duplicate_for_write() final {
    assert(delta_buffer.empty());
    return CachedExtentRef(new LBALeafNode(*this));
  };

  delta_buffer_t delta_buffer;
  delta_buffer_t *maybe_get_delta_buffer() {
    return is_mutation_pending() ? &delta_buffer : nullptr;
  }

  void update(
    const_iterator iter,
    lba_map_val_t val) {
    val.paddr = maybe_generate_relative(val.paddr);
    return journal_update(
      iter,
      val,
      maybe_get_delta_buffer());
  }

  auto insert(
    const_iterator iter,
    laddr_t addr,
    lba_map_val_t val) {
    val.paddr = maybe_generate_relative(val.paddr);
    journal_insert(
      iter,
      addr,
      val,
      maybe_get_delta_buffer());
    return iter;
  }

  void remove(const_iterator iter) {
    return journal_remove(
      iter,
      maybe_get_delta_buffer());
  }


  std::tuple<Ref, Ref, laddr_t>
  make_split_children(op_context_t c) {
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

  Ref make_full_merge(
    op_context_t c,
    Ref &right) {
    auto replacement = c.cache.alloc_new_extent<LBALeafNode>(
      c.trans, LBA_BLOCK_SIZE);
    replacement->merge_from(*this, *right->cast<LBALeafNode>());
    replacement->pin.set_range(replacement->get_meta());
    return replacement;
  }

  std::tuple<Ref, Ref, laddr_t>
  make_balanced(
    op_context_t c,
    Ref &_right,
    bool prefer_left) {
    ceph_assert(_right->get_type() == get_type());
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
  void node_resolve_vals(iterator from, iterator to) const final {
    if (is_initial_pending()) {
      for (auto i = from; i != to; ++i) {
	auto val = i->get_val();
	if (val.paddr.is_relative()) {
	  assert(val.paddr.is_block_relative());
	  val.paddr = get_paddr().add_relative(val.paddr);
	  i->set_val(val);
	}
      }
    }
  }
  void node_unresolve_vals(iterator from, iterator to) const final {
    if (is_initial_pending()) {
      for (auto i = from; i != to; ++i) {
	auto val = i->get_val();
	if (val.paddr.is_relative()) {
	  auto val = i->get_val();
	  assert(val.paddr.is_record_relative());
	  val.paddr = val.paddr - get_paddr();
	  i->set_val(val);
	}
      }
    }
  }

  ceph::bufferlist get_delta() final {
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
    return TYPE;
  }

  std::ostream &print_detail(std::ostream &out) const final;

  constexpr static size_t get_min_capacity() {
    return (get_capacity() - 1) / 2;
  }

  bool at_max_capacity() const {
    assert(get_size() <= get_capacity());
    return get_size() == get_capacity();
  }

  bool at_min_capacity() const {
    assert(get_size() >= (get_min_capacity() - 1));
    return get_size() <= get_min_capacity();
  }

  bool below_min_capacity() const {
    assert(get_size() >= (get_min_capacity() - 1));
    return get_size() < get_min_capacity();
  }
};
using LBALeafNodeRef = TCachedExtentRef<LBALeafNode>;

}
