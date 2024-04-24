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
  pladdr_t pladdr;         ///< physical addr of mapping or
			   //	laddr of a physical lba mapping(see btree_lba_manager.h)
  extent_ref_count_t refcount = 0; ///< refcount
  uint32_t checksum = 0; ///< checksum of original block written at paddr (TODO)

  lba_map_val_t() = default;
  lba_map_val_t(
    extent_len_t len,
    pladdr_t pladdr,
    extent_ref_count_t refcount,
    uint32_t checksum)
    : len(len), pladdr(pladdr), refcount(refcount), checksum(checksum) {}
  bool operator==(const lba_map_val_t&) const = default;
};

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
 *   keys       : laddr_t[170]               (140*8)b
 *   values     : lba_map_val_t[170]         (140*21)b
 *                                           = 4092
 *
 * TODO: update FixedKVNodeLayout to handle the above calculation
 * TODO: the above alignment probably isn't portable without further work
 */
constexpr size_t LEAF_NODE_CAPACITY = 140;

/**
 * lba_map_val_le_t
 *
 * On disk layout for lba_map_val_t.
 */
struct lba_map_val_le_t {
  extent_len_le_t len = init_extent_len_le(0);
  pladdr_le_t pladdr;
  extent_ref_count_le_t refcount{0};
  ceph_le32 checksum{0};

  lba_map_val_le_t() = default;
  lba_map_val_le_t(const lba_map_val_le_t &) = default;
  explicit lba_map_val_le_t(const lba_map_val_t &val)
    : len(init_extent_len_le(val.len)),
      pladdr(pladdr_le_t(val.pladdr)),
      refcount(val.refcount),
      checksum(val.checksum) {}

  operator lba_map_val_t() const {
    return lba_map_val_t{ len, pladdr, refcount, checksum };
  }
};

struct LBALeafNode
  : FixedKVLeafNode<
      LEAF_NODE_CAPACITY,
      laddr_t, laddr_le_t,
      lba_map_val_t, lba_map_val_le_t,
      LBA_BLOCK_SIZE,
      LBALeafNode,
      true> {
  using Ref = TCachedExtentRef<LBALeafNode>;
  using parent_type_t = FixedKVLeafNode<
			  LEAF_NODE_CAPACITY,
			  laddr_t, laddr_le_t,
			  lba_map_val_t, lba_map_val_le_t,
			  LBA_BLOCK_SIZE,
			  LBALeafNode,
			  true>;
  using internal_const_iterator_t =
    typename parent_type_t::node_layout_t::const_iterator;
  using internal_iterator_t =
    typename parent_type_t::node_layout_t::iterator;
  template <typename... T>
  LBALeafNode(T&&... t) :
    parent_type_t(std::forward<T>(t)...) {}

  static constexpr extent_types_t TYPE = extent_types_t::LADDR_LEAF;

  bool validate_stable_children() final {
    LOG_PREFIX(LBALeafNode::validate_stable_children);
    if (this->children.empty()) {
      return false;
    }

    for (auto i : *this) {
      auto child = (LogicalCachedExtent*)this->children[i.get_offset()];
      if (is_valid_child_ptr(child) && child->get_laddr() != i.get_key()) {
	SUBERROR(seastore_fixedkv_tree,
	  "stable child not valid: child {}, key {}",
	  *child,
	  i.get_key());
	ceph_abort();
	return false;
      }
    }
    return true;
  }

  void update(
    internal_const_iterator_t iter,
    lba_map_val_t val,
    LogicalCachedExtent* nextent) final {
    LOG_PREFIX(LBALeafNode::update);
    if (nextent) {
      SUBTRACE(seastore_fixedkv_tree, "trans.{}, pos {}, {}",
	this->pending_for_transaction,
	iter.get_offset(),
	*nextent);
      // child-ptr may already be correct, see LBAManager::update_mappings()
      this->update_child_ptr(iter, nextent);
    }
    if (val.pladdr.is_paddr()) {
      val.pladdr = maybe_generate_relative(val.pladdr.get_paddr());
    }
    return this->journal_update(
      iter,
      val,
      this->maybe_get_delta_buffer());
  }

  internal_const_iterator_t insert(
    internal_const_iterator_t iter,
    laddr_t addr,
    lba_map_val_t val,
    LogicalCachedExtent* nextent) final {
    LOG_PREFIX(LBALeafNode::insert);
    SUBTRACE(seastore_fixedkv_tree, "trans.{}, pos {}, key {}, extent {}",
      this->pending_for_transaction,
      iter.get_offset(),
      addr,
      (void*)nextent);
    this->insert_child_ptr(iter, nextent);
    if (val.pladdr.is_paddr()) {
      val.pladdr = maybe_generate_relative(val.pladdr.get_paddr());
    }
    this->journal_insert(
      iter,
      addr,
      val,
      this->maybe_get_delta_buffer());
    return iter;
  }

  void remove(internal_const_iterator_t iter) final {
    LOG_PREFIX(LBALeafNode::remove);
    SUBTRACE(seastore_fixedkv_tree, "trans.{}, pos {}, key {}",
      this->pending_for_transaction,
      iter.get_offset(),
      iter.get_key());
    assert(iter != this->end());
    this->remove_child_ptr(iter);
    return this->journal_remove(
      iter,
      this->maybe_get_delta_buffer());
  }

  // See LBAInternalNode, same concept
  void resolve_relative_addrs(paddr_t base);
  void node_resolve_vals(
    internal_iterator_t from,
    internal_iterator_t to) const final
  {
    if (this->is_initial_pending()) {
      for (auto i = from; i != to; ++i) {
	auto val = i->get_val();
	if (val.pladdr.is_paddr()
	    && val.pladdr.get_paddr().is_relative()) {
	  assert(val.pladdr.get_paddr().is_block_relative());
	  val.pladdr = this->get_paddr().add_relative(val.pladdr.get_paddr());
	  i->set_val(val);
	}
      }
    }
  }
  void node_unresolve_vals(
    internal_iterator_t from,
    internal_iterator_t to) const final
  {
    if (this->is_initial_pending()) {
      for (auto i = from; i != to; ++i) {
	auto val = i->get_val();
	if (val.pladdr.is_paddr()
	    && val.pladdr.get_paddr().is_relative()) {
	  assert(val.pladdr.get_paddr().is_record_relative());
	  val.pladdr = val.pladdr.get_paddr().block_relative_to(this->get_paddr());
	  i->set_val(val);
	}
      }
    }
  }

  extent_types_t get_type() const final {
    return TYPE;
  }

  std::ostream &_print_detail(std::ostream &out) const final;
};
using LBALeafNodeRef = TCachedExtentRef<LBALeafNode>;

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::lba_manager::btree::lba_node_meta_t> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::lba_manager::btree::lba_map_val_t> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::lba_manager::btree::LBAInternalNode> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::lba_manager::btree::LBALeafNode> : fmt::ostream_formatter {};
#endif
