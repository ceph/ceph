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

extent_len_t get_leaf_size(omap_type_t type);

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
    StringKVInnerNodeLayout,
    ParentNode<OMapInnerNode, std::string>,
    ChildNode<OMapInnerNode, OMapInnerNode, std::string> {
  using OMapInnerNodeRef = TCachedExtentRef<OMapInnerNode>;
  using internal_const_iterator_t = const_iterator;
  using internal_iterator_t = iterator;
  using parent_node_t = ParentNode<OMapInnerNode, std::string>;
  using base_child_t = BaseChildNode<OMapInnerNode, std::string>;
  using child_node_t = ChildNode<OMapInnerNode, OMapInnerNode, std::string>;
  static constexpr uint32_t CHILD_VEC_UNIT = 128;

  explicit OMapInnerNode(ceph::bufferptr &&ptr)
    : OMapNode(std::move(ptr)),
      StringKVInnerNodeLayout(get_bptr().c_str()),
      parent_node_t(0) {
    this->parent_node_t::sync_children_capacity();
  }
  // Must be identical with OMapInnerNode(ptr) after on_fully_loaded()
  explicit OMapInnerNode(extent_len_t length)
    : OMapNode(length),
      StringKVInnerNodeLayout(nullptr),
      parent_node_t(0) {}
  OMapInnerNode(const OMapInnerNode &rhs)
    : OMapNode(rhs),
      StringKVInnerNodeLayout(get_bptr().c_str()),
      parent_node_t(0) {
    this->parent_node_t::sync_children_capacity();
  }

  iterator begin() {
    return iter_begin();
  }

  iterator end() {
    return iter_end();
  }

  void do_on_rewrite(Transaction &t, LogicalCachedExtent &extent) final {
    auto &ext = static_cast<OMapInnerNode&>(extent);
    this->parent_node_t::on_rewrite(t, ext);
    auto &other = static_cast<OMapInnerNode&>(extent);
    this->init_range(other.get_begin(), other.get_end());
    this->sync_children_capacity();
  }

  void prepare_commit() final {
    if (unlikely(!is_seen_by_users())) {
      ceph_assert(is_rewrite());
      auto &prior = *get_prior_instance()->template cast<OMapInnerNode>();
      if (!prior.is_seen_by_users()) {
	return;
      }
      set_seen_by_users();
    }
    this->parent_node_t::prepare_commit();
    if (is_rewrite()) {
      auto &prior = *get_prior_instance()->template cast<OMapInnerNode>();
      assert(prior.is_seen_by_users());
      // Chances are that this transaction is in parallel with another
      // user transaction that set the prior's root to true, so we need
      // to do this.
      set_root(prior.is_btree_root());
      if (!is_btree_root()) {
	assert(prior.base_child_t::has_parent_tracker());
	assert(prior.is_seen_by_users());
	// unlike fixed-kv nodes, rewriting child nodes of the omap tree
	// won't affect parent nodes, so we have to manually take prior
	// instances' parent trackers here.
	this->child_node_t::take_parent_from_prior();
      }
    }
  }

  void do_on_replace_prior() final {
    this->parent_node_t::on_replace_prior();
    if (!this->is_btree_root()) {
      auto &prior = *get_prior_instance()->template cast<OMapInnerNode>();
      assert(prior.base_child_t::has_parent_tracker());
      this->child_node_t::on_replace_prior();
    }
  }

  void on_invalidated(Transaction &t) final {
    this->child_node_t::on_invalidated();
  }

  void on_initial_write() final {
    if (this->is_btree_root()) {
      //TODO: should involve RootChildNode
      this->child_node_t::reset_parent_tracker();
    }
  }

  btreenode_pos_t get_node_split_pivot() const {
    return this->get_split_pivot().get_offset();
  }

  omap_node_meta_t get_node_meta() const final { return get_meta(); }
  bool extent_will_overflow(size_t ksize, std::optional<size_t> vsize) const {
    return is_overflow(ksize);
  }
  bool can_merge(OMapNodeRef right) const {
    return !is_overflow(*right->cast<OMapInnerNode>());
  }
  bool extent_is_below_min() const { return below_min(); }
  uint32_t get_node_size() { return get_size(); }

  void on_fully_loaded() final {
    this->set_layout_buf(this->get_bptr().c_str());
  }

  void on_clean_read() final {
    this->sync_children_capacity();
  }

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

  bool exceeds_max_kv_limit(
    const std::string &key,
    const ceph::bufferlist &value) const final {
    return (key.length() + sizeof(laddr_le_t)) > (capacity() / 4);
  }

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
    omap_context_t oc, OMapNodeRef right, uint32_t pivot_idx) final;

  using make_split_insert_iertr = base_iertr; 
  using make_split_insert_ret = make_split_insert_iertr::future<mutation_result_t>;
  make_split_insert_ret make_split_insert(
    omap_context_t oc, internal_const_iterator_t iter,
    std::string key, OMapNodeRef &node);

  using merge_entry_iertr = base_iertr;
  using merge_entry_ret = merge_entry_iertr::future<mutation_result_t>;
  merge_entry_ret merge_entry(
    omap_context_t oc,
    internal_const_iterator_t iter, OMapNodeRef entry);

  using handle_split_iertr = base_iertr::extend<
    crimson::ct_error::value_too_large>;
  using handle_split_ret = handle_split_iertr::future<mutation_result_t>;
  handle_split_ret handle_split(
    omap_context_t oc, internal_const_iterator_t iter,
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
    this->parent_node_t::sync_children_capacity();
  }

  internal_const_iterator_t get_containing_child(const std::string &key);

  internal_iterator_t lower_bound(const std::string &key) {
    return string_lower_bound(key);
  }

  internal_iterator_t upper_bound(const std::string &key) {
    return string_upper_bound(key);
  }

  bool is_in_range(const std::string &key) const {
    return get_begin() <= key && get_end() > key;
  }

  ~OMapInnerNode() {
    if (this->is_valid()
	&& !this->is_pending()
	&& !this->is_btree_root()
	// dirty omap extent may not be accessed/linked yet
	&& this->base_child_t::has_parent_tracker()) {
      this->child_node_t::destroy();
    }
  }
private:
  merge_entry_ret do_merge(
    omap_context_t oc,
    internal_const_iterator_t liter,
    internal_const_iterator_t riter,
    OMapNodeRef l,
    OMapNodeRef r);

  merge_entry_ret do_balance(
    omap_context_t oc,
    internal_const_iterator_t liter,
    internal_const_iterator_t riter,
    OMapNodeRef l,
    OMapNodeRef r);

  using get_child_node_iertr = OMapNode::base_iertr;
  using get_child_node_ret = get_child_node_iertr::future<OMapNodeRef>;
  get_child_node_ret get_child_node(
    omap_context_t oc,
    internal_const_iterator_t child_pt);

  get_child_node_ret get_child_node(omap_context_t oc, const std::string &key) {
    auto child_pt = get_containing_child(key);
    assert(child_pt != iter_cend());
    return get_child_node(oc, child_pt);
  }
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
    StringKVLeafNodeLayout,
    ChildNode<OMapInnerNode, OMapLeafNode, std::string> {
  using OMapLeafNodeRef = TCachedExtentRef<OMapLeafNode>;
  using internal_const_iterator_t = const_iterator;
  using base_child_t = BaseChildNode<OMapInnerNode, std::string>;
  using child_node_t = ChildNode<OMapInnerNode, OMapLeafNode, std::string>;

  void do_on_rewrite(Transaction &t, LogicalCachedExtent &extent) final {
    auto &other = static_cast<OMapInnerNode&>(extent);
    this->init_range(other.get_begin(), other.get_end());
  }

  void on_invalidated(Transaction &t) final {
    this->child_node_t::on_invalidated();
  }

  void prepare_commit() final {
    if (unlikely(!is_seen_by_users())) {
      ceph_assert(is_rewrite());
      auto &prior = *get_prior_instance()->template cast<OMapLeafNode>();
      if (!prior.is_seen_by_users()) {
	return;
      }
      set_seen_by_users();
    }
    if (is_rewrite()) {
      auto &prior = *get_prior_instance()->template cast<OMapLeafNode>();
      assert(prior.is_seen_by_users());
      // Chances are that this transaction is in parallel with another
      // user transaction that set the prior's root to true, so we need
      // to do this.
      set_root(prior.is_btree_root());
      if (!is_btree_root()) {
	assert(prior.base_child_t::has_parent_tracker());
	assert(prior.is_seen_by_users());
	// unlike fixed-kv nodes, rewriting child nodes of the omap tree
	// won't affect parent nodes, so we have to manually take prior
	// instances' parent trackers here.
	this->child_node_t::take_parent_from_prior();
      }
    }
  }

  void do_on_replace_prior() final {
    ceph_assert(!this->is_rewrite());
    if (!this->is_btree_root()) {
      auto &prior = *get_prior_instance()->template cast<OMapLeafNode>();
      assert(prior.base_child_t::has_parent_tracker());
      this->child_node_t::on_replace_prior();
    }
  }

  ~OMapLeafNode() {
    if (this->is_valid()
	&& !this->is_pending()
	&& !this->is_btree_root()
	// dirty omap extent may not be accessed/linked yet
	&& this->base_child_t::has_parent_tracker()) {
      this->child_node_t::destroy();
    }
  }

  explicit OMapLeafNode(ceph::bufferptr &&ptr)
    : OMapNode(std::move(ptr)) {
    this->set_layout_buf(this->get_bptr().c_str(), this->get_bptr().length());
  }
  // Must be identical with OMapLeafNode(ptr) after on_fully_loaded()
  explicit OMapLeafNode(extent_len_t length)
    : OMapNode(length) {}
  OMapLeafNode(const OMapLeafNode &rhs)
    : OMapNode(rhs) {
    this->set_layout_buf(this->get_bptr().c_str(), this->get_bptr().length());
  }

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

  void on_fully_loaded() final {
    this->set_layout_buf(this->get_bptr().c_str(), this->get_bptr().length());
  }

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

  bool exceeds_max_kv_limit(
    const std::string &key,
    const ceph::bufferlist &value) const final {
    return (key.length() + value.length()) > (capacity() / 4);
  }

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
    OMapNodeRef _right,
    uint32_t pivot_idx) final;

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

  btreenode_pos_t get_node_split_pivot() const {
    return this->get_split_pivot().get_offset();
  }

  std::ostream &print_detail_l(std::ostream &out) const final;

  std::pair<internal_const_iterator_t, internal_const_iterator_t>
  get_leaf_entries(std::string &key);
};
using OMapLeafNodeRef = OMapLeafNode::OMapLeafNodeRef;

using omap_load_extent_iertr = OMapNode::base_iertr;
template <typename T>
requires std::is_same_v<OMapInnerNode, T> || std::is_same_v<OMapLeafNode, T>
omap_load_extent_iertr::future<TCachedExtentRef<T>>
omap_load_extent(
  omap_context_t oc,
  laddr_t laddr,
  depth_t depth,
  std::string begin,
  std::string end,
  std::optional<child_pos_t<OMapInnerNode>> chp = std::nullopt)
{
  LOG_PREFIX(omap_load_extent);
  assert(end <= END_KEY);
  auto size = std::is_same_v<OMapInnerNode, T>
    ? OMAP_INNER_BLOCK_SIZE : get_leaf_size(oc.type);
  return oc.tm.read_extent<T>(
    oc.t, laddr, size,
    [begin=std::move(begin), end=std::move(end), FNAME,
    oc, chp=std::move(chp)](T &extent) mutable {
      assert(!extent.is_seen_by_users());
      extent.init_range(std::move(begin), std::move(end));
      if (extent.is_btree_root()) {
        return;
      }
      SUBDEBUGT(seastore_omap, "linking {} to {}",
        oc.t, extent, *chp->get_parent());
      assert(chp);
      assert(!extent.T::base_child_t::is_parent_valid());
      chp->link_child(&extent);
    }
  ).handle_error_interruptible(
    omap_load_extent_iertr::pass_further{},
    crimson::ct_error::assert_all{ "Invalid error in omap_load_extent" }
  ).si_then([](auto maybe_indirect_extent) {
    assert(!maybe_indirect_extent.is_indirect());
    assert(!maybe_indirect_extent.is_clone);
    return seastar::make_ready_future<TCachedExtentRef<T>>(
	std::move(maybe_indirect_extent.extent));
  });
}

std::ostream &operator<<(std::ostream &out, const omap_inner_key_t &rhs);
std::ostream &operator<<(std::ostream &out, const omap_leaf_key_t &rhs);
}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::omap_manager::OMapInnerNode> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::omap_manager::OMapLeafNode> : fmt::ostream_formatter {};
#endif
