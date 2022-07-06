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

#include "crimson/os/seastore/btree/btree_range_pin.h"
#include "crimson/os/seastore/btree/fixed_kv_btree.h"
#include "crimson/os/seastore/btree/fixed_kv_node.h"

namespace crimson::os::seastore::lba_manager::btree {

using base_iertr = LBAManager::base_iertr;
using LBANode = FixedKVNode<laddr_t>;

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

constexpr size_t LBA_BLOCK_SIZE = 4096;

using lba_node_meta_t = fixed_kv_node_meta_t<laddr_t>;

using lba_node_meta_le_t = fixed_kv_node_meta_le_t<laddr_le_t>;

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
  : FixedKVInternalNode<
      INTERNAL_NODE_CAPACITY,
      laddr_t, laddr_le_t,
      LBA_BLOCK_SIZE,
      LBAInternalNode> {
  using Ref = TCachedExtentRef<LBAInternalNode>;
  using internal_iterator_t = const_iterator;
  template <typename... T>
  LBAInternalNode(T&&... t) :
    FixedKVInternalNode(std::forward<T>(t)...) {}

  static constexpr extent_types_t TYPE = extent_types_t::LADDR_INTERNAL;

  extent_types_t get_type() const final {
    return TYPE;
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
  : FixedKVLeafNode<
      LEAF_NODE_CAPACITY,
      laddr_t, laddr_le_t,
      lba_map_val_t, lba_map_val_le_t,
      LBA_BLOCK_SIZE,
      LBALeafNode> {
  using Ref = TCachedExtentRef<LBALeafNode>;
  using internal_iterator_t = const_iterator;
  template <typename... T>
  LBALeafNode(T&&... t) :
    FixedKVLeafNode(std::forward<T>(t)...) {}

  static constexpr extent_types_t TYPE = extent_types_t::LADDR_LEAF;

  void update(
    const_iterator iter,
    lba_map_val_t val) final {
    val.paddr = maybe_generate_relative(val.paddr);
    return journal_update(
      iter,
      val,
      maybe_get_delta_buffer());
  }

  const_iterator insert(
    const_iterator iter,
    laddr_t addr,
    lba_map_val_t val) final {
    val.paddr = maybe_generate_relative(val.paddr);
    journal_insert(
      iter,
      addr,
      val,
      maybe_get_delta_buffer());
    return iter;
  }

  void remove(const_iterator iter) final {
    return journal_remove(
      iter,
      maybe_get_delta_buffer());
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

  extent_types_t get_type() const final {
    return TYPE;
  }

  std::ostream &print_detail(std::ostream &out) const final;
};
using LBALeafNodeRef = TCachedExtentRef<LBALeafNode>;

}
