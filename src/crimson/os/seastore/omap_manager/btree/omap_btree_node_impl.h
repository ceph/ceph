// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string.h>

#include "include/buffer.h"

#include "crimson/common/errorator.h"
#include "crimson/os/seastore/omap_manager.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/omap_manager/btree/string_kv_node_layout.h"
#include "crimson/os/seastore/omap_manager/btree/omap_types.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_node.h"

namespace crimson::os::seastore::omap_manager {

/**
 * OMapInnerNode
 *
 * Abstracts operations on and layout of internal nodes for the
 * omap Tree.
 *
 * Layout (4k):
 *   num_entries:   meta    :    keys    :  values  :
 */

struct OMapInnerNode
  : OMapNode,
    StringKVInnerNodeLayout {
  using OMapInnerNodeRef = TCachedExtentRef<OMapInnerNode>;
  using internal_iterator_t = const_iterator;
  template <typename... T>
  OMapInnerNode(T&&... t) :
    OMapNode(std::forward<T>(t)...),
    StringKVInnerNodeLayout(get_bptr().c_str()) {}

  omap_node_meta_t get_node_meta() const final { return get_meta(); }
  bool extent_will_overflow(size_t ksize, std::optional<size_t> vsize) const {
    return is_overflow(ksize);
  }
  bool can_merge(OMapNodeRef right) const {
    return !is_overflow(*right->cast<OMapInnerNode>());
  }
  bool extent_is_below_min() const { return below_min(); }
  uint32_t get_node_size() { return get_size(); }

  CachedExtentRef duplicate_for_write(Transaction&) final {
    assert(delta_buffer.empty());
    return CachedExtentRef(new OMapInnerNode(*this));
  }

  delta_inner_buffer_t delta_buffer;
  delta_inner_buffer_t *maybe_get_delta_buffer() {
    return is_mutation_pending() ? &delta_buffer : nullptr;
  }

  get_value_ret get_value(omap_context_t oc, const std::string &key) final;

  insert_ret insert(
    omap_context_t oc,
    const std::string &key,
    const ceph::bufferlist &value) final;

  rm_key_ret rm_key(
    omap_context_t oc,
    const std::string &key) final;

  list_ret list(
    omap_context_t oc,
    const std::optional<std::string> &first,
    const std::optional<std::string> &last,
    omap_list_config_t config) final;

  clear_ret clear(omap_context_t oc) final;

  using split_children_iertr = base_iertr;
  using split_children_ret = split_children_iertr::future
          <std::tuple<OMapInnerNodeRef, OMapInnerNodeRef, std::string>>;
  split_children_ret make_split_children(omap_context_t oc);

  full_merge_ret make_full_merge(
    omap_context_t oc, OMapNodeRef right) final;

  make_balanced_ret make_balanced(
    omap_context_t oc, OMapNodeRef right) final;

  using make_split_insert_iertr = base_iertr; 
  using make_split_insert_ret = make_split_insert_iertr::future<mutation_result_t>;
  make_split_insert_ret make_split_insert(
    omap_context_t oc, internal_iterator_t iter,
    std::string key, laddr_t laddr);

  using merge_entry_iertr = base_iertr;
  using merge_entry_ret = merge_entry_iertr::future<mutation_result_t>;
  merge_entry_ret merge_entry(
    omap_context_t oc,
    internal_iterator_t iter, OMapNodeRef entry);

  using handle_split_iertr = base_iertr;
  using handle_split_ret = handle_split_iertr::future<mutation_result_t>;
  handle_split_ret handle_split(
    omap_context_t oc, internal_iterator_t iter,
    mutation_result_t mresult);

  std::ostream &print_detail_l(std::ostream &out) const final;

  static constexpr extent_types_t TYPE = extent_types_t::OMAP_INNER;
  extent_types_t get_type() const final {
    return TYPE;
  }

  ceph::bufferlist get_delta() final {
    ceph::bufferlist bl;
    if (!delta_buffer.empty()) {
      encode(delta_buffer, bl);
      delta_buffer.clear();
    }
    return bl;
  }

  void apply_delta(const ceph::bufferlist &bl) final {
    assert(bl.length());
    delta_inner_buffer_t buffer;
    auto bptr = bl.cbegin();
    decode(buffer, bptr);
    buffer.replay(*this);
  }

  internal_iterator_t get_containing_child(const std::string &key);
};
using OMapInnerNodeRef = OMapInnerNode::OMapInnerNodeRef;

/**
 * OMapLeafNode
 *
 * Abstracts operations on and layout of leaf nodes for the
 * OMap Tree.
 *
 * Layout (4k):
 *   num_entries:   meta   :   keys   :  values  :
 */

struct OMapLeafNode
  : OMapNode,
    StringKVLeafNodeLayout {

  using OMapLeafNodeRef = TCachedExtentRef<OMapLeafNode>;
  using internal_iterator_t = const_iterator;
  template <typename... T>
  OMapLeafNode(T&&... t) :
    OMapNode(std::forward<T>(t)...),
    StringKVLeafNodeLayout(get_bptr().c_str()) {}

  omap_node_meta_t get_node_meta() const final { return get_meta(); }
  bool extent_will_overflow(
    size_t ksize, std::optional<size_t> vsize) const {
    return is_overflow(ksize, *vsize);
  }
  bool can_merge(OMapNodeRef right) const {
    return !is_overflow(*right->cast<OMapLeafNode>());
  }
  bool extent_is_below_min() const { return below_min(); }
  uint32_t get_node_size() { return get_size(); }

  CachedExtentRef duplicate_for_write(Transaction&) final {
    assert(delta_buffer.empty());
    return CachedExtentRef(new OMapLeafNode(*this));
  }

  delta_leaf_buffer_t delta_buffer;
  delta_leaf_buffer_t *maybe_get_delta_buffer() {
    return is_mutation_pending() ? &delta_buffer : nullptr;
  }

  get_value_ret get_value(
    omap_context_t oc, const std::string &key) final;

  insert_ret insert(
    omap_context_t oc,
    const std::string &key,
    const ceph::bufferlist &value) final;

  rm_key_ret rm_key(
    omap_context_t oc, const std::string &key) final;

  list_ret list(
    omap_context_t oc,
    const std::optional<std::string> &first,
    const std::optional<std::string> &last,
    omap_list_config_t config) final;

  clear_ret clear(
    omap_context_t oc) final;

  using split_children_iertr = base_iertr;
  using split_children_ret = split_children_iertr::future
          <std::tuple<OMapLeafNodeRef, OMapLeafNodeRef, std::string>>;
  split_children_ret make_split_children(
    omap_context_t oc);

  full_merge_ret make_full_merge(
    omap_context_t oc,
    OMapNodeRef right) final;

  make_balanced_ret make_balanced(
    omap_context_t oc,
    OMapNodeRef _right) final;

  static constexpr extent_types_t TYPE = extent_types_t::OMAP_LEAF;
  extent_types_t get_type() const final {
    return TYPE;
  }

  ceph::bufferlist get_delta() final {
    ceph::bufferlist bl;
    if (!delta_buffer.empty()) {
      encode(delta_buffer, bl);
      delta_buffer.clear();
    }
    return bl;
  }

  void apply_delta(const ceph::bufferlist &_bl) final {
    assert(_bl.length());
    ceph::bufferlist bl = _bl;
    bl.rebuild();
    delta_leaf_buffer_t buffer;
    auto bptr = bl.cbegin();
    decode(buffer, bptr);
    buffer.replay(*this);
  }

  std::ostream &print_detail_l(std::ostream &out) const final;

  std::pair<internal_iterator_t, internal_iterator_t>
  get_leaf_entries(std::string &key);

};
using OMapLeafNodeRef = OMapLeafNode::OMapLeafNodeRef;

std::ostream &operator<<(std::ostream &out, const omap_inner_key_t &rhs);
std::ostream &operator<<(std::ostream &out, const omap_leaf_key_t &rhs);
}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::omap_manager::OMapInnerNode> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::omap_manager::OMapLeafNode> : fmt::ostream_formatter {};
#endif
