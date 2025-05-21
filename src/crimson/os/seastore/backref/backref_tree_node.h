// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/btree/fixed_kv_node.h"

namespace crimson::os::seastore {
class LogicalChildNode;
}

namespace crimson::os::seastore::backref {

using backref_node_meta_t = fixed_kv_node_meta_t<paddr_t>;
using backref_node_meta_le_t = fixed_kv_node_meta_le_t<paddr_le_t>;

/**
 * Layout (4KiB):
 *   checksum   : ceph_le32[1]               4B
 *   size       : ceph_le32[1]               4B
 *   meta       : backref_node_meta_le_t[1]  20B
 *   keys       : paddr_le_t[CAPACITY]       (254*8)B
 *   values     : paddr_le_t[CAPACITY]       (254*8)B
 *                                           = 4092B
 *
 * TODO: make the above capacity calculation part of FixedKVNodeLayout
 * TODO: the above alignment probably isn't portable without further work
 */
constexpr size_t INTERNAL_NODE_CAPACITY = 254;

/**
 * Layout (4KiB):
 *   checksum   : ceph_le32[1]                    4B
 *   size       : ceph_le32[1]                    4B
 *   meta       : backref_node_meta_le_t[1]       20B
 *   keys       : paddr_le_t[CAPACITY]            (193*8)B
 *   values     : backref_map_val_le_t[CAPACITY]  (193*13)B
 *                                                = 4081B
 *
 * TODO: update FixedKVNodeLayout to handle the above calculation
 * TODO: the above alignment probably isn't portable without further work
 */
constexpr size_t LEAF_NODE_CAPACITY = 193;

using BackrefNode = FixedKVNode<paddr_t>;

class BackrefInternalNode
  : public FixedKVInternalNode<
      INTERNAL_NODE_CAPACITY,
      paddr_t, paddr_le_t,
      BACKREF_NODE_SIZE,
      BackrefInternalNode> {
  static_assert(
    check_capacity(BACKREF_NODE_SIZE),
    "INTERNAL_NODE_CAPACITY doesn't fit in BACKREF_NODE_SIZE");
public:
  using key_type = paddr_t;
  static constexpr uint32_t CHILD_VEC_UNIT = 0;
  template <typename... T>
  BackrefInternalNode(T&&... t) :
    FixedKVInternalNode(std::forward<T>(t)...) {}

  static constexpr extent_types_t TYPE = extent_types_t::BACKREF_INTERNAL;

  extent_types_t get_type() const final {
    return TYPE;
  }
};
using BackrefInternalNodeRef = BackrefInternalNode::Ref;

class BackrefLeafNode
  : public FixedKVLeafNode<
      LEAF_NODE_CAPACITY,
      paddr_t, paddr_le_t,
      backref_map_val_t, backref_map_val_le_t,
      BACKREF_NODE_SIZE,
      BackrefInternalNode,
      BackrefLeafNode> {
  static_assert(
    check_capacity(BACKREF_NODE_SIZE),
    "LEAF_NODE_CAPACITY doesn't fit in BACKREF_NODE_SIZE");
public:
  using key_type = paddr_t;
  template <typename... T>
  BackrefLeafNode(T&&... t) :
    FixedKVLeafNode(std::forward<T>(t)...) {}

  static constexpr extent_types_t TYPE = extent_types_t::BACKREF_LEAF;

  extent_types_t get_type() const final  {
    return TYPE;
  }

  const_iterator insert(
    const_iterator iter,
    paddr_t key,
    backref_map_val_t val) final {
    journal_insert(
      iter,
      key,
      val,
      maybe_get_delta_buffer());
    return iter;
  }

  void update(
    const_iterator iter,
    backref_map_val_t val) final {
    return journal_update(
      iter,
      val,
      maybe_get_delta_buffer());
  }

  void remove(const_iterator iter) final {
    return journal_remove(
      iter,
      maybe_get_delta_buffer());
  }

  void do_on_rewrite(Transaction &t, CachedExtent &extent) final {}
  void do_on_replace_prior() final {}
  void do_prepare_commit() final {}


  void on_split(
    Transaction &t,
    BackrefLeafNode &left,
    BackrefLeafNode &right) final {}

  void on_merge(
    Transaction &t,
    BackrefLeafNode &left,
    BackrefLeafNode &right) final {}

  void on_balance(
    Transaction &t,
    BackrefLeafNode &left,
    BackrefLeafNode &right,
    uint32_t pivot_idx,
    BackrefLeafNode &replacement_left,
    BackrefLeafNode &replacement_right) final {}

  void adjust_copy_src_dest_on_split(
    Transaction &t,
    BackrefLeafNode &left,
    BackrefLeafNode &right) final {}

  void adjust_copy_src_dest_on_merge(
    Transaction &t,
    BackrefLeafNode &left,
    BackrefLeafNode &right) final {}

  void adjust_copy_src_dest_on_balance(
    Transaction &t,
    BackrefLeafNode &left,
    BackrefLeafNode &right,
    uint32_t pivot_idx,
    BackrefLeafNode &replacement_left,
    BackrefLeafNode &replacement_right) final {}
  // backref leaf nodes don't have to resolve relative addresses
  void resolve_relative_addrs(paddr_t base) final {}

  void node_resolve_vals(iterator from, iterator to) const final {}

  void node_unresolve_vals(iterator from, iterator to) const final {}
};
using BackrefLeafNodeRef = BackrefLeafNode::Ref;

} // namespace crimson::os::seastore::backref

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::backref::backref_map_val_t> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::backref::BackrefInternalNode> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::backref::BackrefLeafNode> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::backref::backref_node_meta_t> : fmt::ostream_formatter {};
#endif
