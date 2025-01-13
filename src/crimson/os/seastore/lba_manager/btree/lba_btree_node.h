// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <sys/mman.h>
#include <memory>
#include <string.h>


#include "include/buffer.h"

#include "crimson/common/fixed_kv_node_layout.h"
#include "crimson/common/errorator.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/cached_extent.h"

#include "crimson/os/seastore/btree/btree_range_pin.h"
#include "crimson/os/seastore/btree/fixed_kv_btree.h"
#include "crimson/os/seastore/btree/fixed_kv_node.h"

namespace crimson::os::seastore {
class LogicalChildNode;
}

namespace crimson::os::seastore::lba_manager::btree {

using base_iertr = Cache::base_iertr;
using LBANode = FixedKVNode<laddr_t>;

class BtreeLBAMapping;

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
 * Layout (4KiB):
 *   checksum   : ceph_le32[1]               4B
 *   size       : ceph_le32[1]               4B
 *   meta       : lba_node_meta_le_t[1]      20B
 *   keys       : laddr_le_t[CAPACITY]       (254*8)B
 *   values     : paddr_le_t[CAPACITY]       (254*8)B
 *                                           = 4092B

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
  static_assert(
    check_capacity(LBA_BLOCK_SIZE),
    "INTERNAL_NODE_CAPACITY doesn't fit in LBA_BLOCK_SIZE");
  using Ref = TCachedExtentRef<LBAInternalNode>;
  using internal_iterator_t = const_iterator;
  using key_type = laddr_t;
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
 * Layout (4KiB):
 *   checksum   : ceph_le32[1]                4B
 *   size       : ceph_le32[1]                4B
 *   meta       : lba_node_meta_le_t[1]       20B
 *   keys       : laddr_le_t[CAPACITY]        (140*8)B
 *   values     : lba_map_val_le_t[CAPACITY]  (140*21)B
 *                                            = 4088B
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
struct __attribute__((packed)) lba_map_val_le_t {
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
      LBAInternalNode,
      LBALeafNode>,
    ParentNode<LBALeafNode, laddr_t> {
  static_assert(
    check_capacity(LBA_BLOCK_SIZE),
    "LEAF_NODE_CAPACITY doesn't fit in LBA_BLOCK_SIZE");
  using Ref = TCachedExtentRef<LBALeafNode>;
  using parent_type_t = FixedKVLeafNode<
			  LEAF_NODE_CAPACITY,
			  laddr_t, laddr_le_t,
			  lba_map_val_t, lba_map_val_le_t,
			  LBA_BLOCK_SIZE,
			  LBAInternalNode,
			  LBALeafNode>;
  using internal_const_iterator_t =
    typename parent_type_t::node_layout_t::const_iterator;
  using internal_iterator_t =
    typename parent_type_t::node_layout_t::iterator;
  using key_type = laddr_t;
  using parent_node_t = ParentNode<LBALeafNode, laddr_t>;
  using child_t = LogicalChildNode;
  LBALeafNode(ceph::bufferptr &&ptr)
    : parent_type_t(std::move(ptr)),
      parent_node_t(LEAF_NODE_CAPACITY) {}
  explicit LBALeafNode(extent_len_t length)
    : parent_type_t(length),
      parent_node_t(LEAF_NODE_CAPACITY) {}
  LBALeafNode(const LBALeafNode &rhs)
    : parent_type_t(rhs),
      parent_node_t(rhs) {}

  static constexpr extent_types_t TYPE = extent_types_t::LADDR_LEAF;

  void update(
    internal_const_iterator_t iter,
    lba_map_val_t val) final;

  internal_const_iterator_t insert(
    internal_const_iterator_t iter,
    laddr_t addr,
    lba_map_val_t val) final;

  void remove(internal_const_iterator_t iter) final {
    LOG_PREFIX(LBALeafNode::remove);
    SUBTRACE(seastore_fixedkv_tree, "trans.{}, pos {}, key {}",
      this->pending_for_transaction,
      iter.get_offset(),
      iter.get_key());
    assert(iter != this->end());
    this->on_modify();
    this->remove_child_ptr(iter.get_offset());
    return this->journal_remove(
      iter,
      this->maybe_get_delta_buffer());
  }

  // See LBAInternalNode, same concept
  void resolve_relative_addrs(paddr_t base) final;
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

  void do_on_rewrite(Transaction &t, CachedExtent &extent) final {
    this->parent_node_t::on_rewrite(t, static_cast<LBALeafNode&>(extent));
  }

  void do_on_replace_prior() final {
    this->parent_node_t::on_replace_prior();
  }

  void do_prepare_commit() final {
    this->parent_node_t::prepare_commit();
  }

  bool is_child_stable(
    op_context_t<laddr_t> c,
    uint16_t pos,
    laddr_t key) const {
    return parent_node_t::_is_child_stable(c.trans, c.cache, pos, key);
  }
  bool is_child_data_stable(
    op_context_t<laddr_t> c,
    uint16_t pos,
    laddr_t key) const {
    return parent_node_t::_is_child_stable(c.trans, c.cache, pos, key, true);
  }

  void on_split(
    Transaction &t,
    LBALeafNode &left,
    LBALeafNode &right) final {
    this->split_child_ptrs(t, left, right);
  }
  void adjust_copy_src_dest_on_split(
    Transaction &t,
    LBALeafNode &left,
    LBALeafNode &right) final {
    this->parent_node_t::adjust_copy_src_dest_on_split(t, left, right);
  }

  void on_merge(
    Transaction &t,
    LBALeafNode &left,
    LBALeafNode &right) final {
    this->merge_child_ptrs(t, left, right);
  }
  void adjust_copy_src_dest_on_merge(
    Transaction &t,
    LBALeafNode &left,
    LBALeafNode &right) final {
    this->parent_node_t::adjust_copy_src_dest_on_merge(t, left, right);
  }

  void on_balance(
    Transaction &t,
    LBALeafNode &left,
    LBALeafNode &right,
    bool prefer_left,
    LBALeafNode &replacement_left,
    LBALeafNode &replacement_right) final {
    this->balance_child_ptrs(
      t, left, right, prefer_left, replacement_left, replacement_right);
  }
  void adjust_copy_src_dest_on_balance(
    Transaction &t,
    LBALeafNode &left,
    LBALeafNode &right,
    bool prefer_left,
    LBALeafNode &replacement_left,
    LBALeafNode &replacement_right) final {
    this->parent_node_t::adjust_copy_src_dest_on_balance(
      t, left, right, prefer_left, replacement_left, replacement_right);
  }

  CachedExtentRef duplicate_for_write(Transaction&) final {
    return CachedExtentRef(new LBALeafNode(*this));
  }

  std::ostream &print_detail(std::ostream &out) const final;

  void maybe_fix_mapping_pos(BtreeLBAMapping &mapping);
  std::unique_ptr<BtreeLBAMapping> get_mapping(op_context_t<laddr_t> c, laddr_t laddr);
};
using LBALeafNodeRef = TCachedExtentRef<LBALeafNode>;

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::lba_manager::btree::lba_node_meta_t> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::lba_manager::btree::lba_map_val_t> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::lba_manager::btree::LBAInternalNode> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::lba_manager::btree::LBALeafNode> : fmt::ostream_formatter {};
#endif
