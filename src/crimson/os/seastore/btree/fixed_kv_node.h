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
#include "crimson/os/seastore/root_block.h"

namespace crimson::os::seastore {

/**
 * FixedKVNode
 *
 * Base class enabling recursive lookup between internal and leaf nodes.
 */
template <typename node_key_t>
struct FixedKVNode : CachedExtent {
  using FixedKVNodeRef = TCachedExtentRef<FixedKVNode>;
  fixed_kv_node_meta_t<node_key_t> range;

  bool is_btree_root() const {
    return range.is_root();
  }

  FixedKVNode(ceph::bufferptr &&ptr)
    : CachedExtent(std::move(ptr)) {}
  // Must be identical with FixedKVNode(ptr) after on_fully_loaded()
  explicit FixedKVNode(extent_len_t length)
    : CachedExtent(length) {}
  FixedKVNode(const FixedKVNode &rhs)
    : CachedExtent(rhs),
      range(rhs.range) {}

  virtual fixed_kv_node_meta_t<node_key_t> get_node_meta() const = 0;
  virtual ~FixedKVNode() = default;
  virtual void do_on_rewrite(Transaction &t, CachedExtent &extent) = 0;

  void on_rewrite(Transaction &t, CachedExtent &extent, extent_len_t off) final {
    assert(get_type() == extent.get_type());
    assert(off == 0);
    range = get_node_meta();
    do_on_rewrite(t, extent);
    
    /* This is a bit underhanded.  Any relative addrs here must necessarily
     * be record relative as we are rewriting a dirty extent.  Thus, we
     * are using resolve_relative_addrs with a (likely negative) block
     * relative offset to correct them to block-relative offsets adjusted
     * for our new transaction location.
     *
     * Upon commit, these now block relative addresses will be interpretted
     * against the real final address.
     */
    if (!get_paddr().is_absolute()) {
      // backend_type_t::SEGMENTED
      assert(get_paddr().is_record_relative());
      resolve_relative_addrs(
	make_record_relative_paddr(0).block_relative_to(get_paddr()));
    } // else: backend_type_t::RANDOM_BLOCK
  }

  void on_delta_write(paddr_t record_block_offset) final {
    // All in-memory relative addrs are necessarily record-relative
    assert(get_prior_instance());
    assert(pending_for_transaction);
    resolve_relative_addrs(record_block_offset);
  }

  void on_clean_read() final {
    // From initial write of block, relative addrs are necessarily block-relative
    resolve_relative_addrs(get_paddr());
  }

  node_key_t get_begin() const {
    return this->range.begin;
  }
  node_key_t get_end() const {
    return this->range.end;
  }
  virtual void resolve_relative_addrs(paddr_t base) = 0;
  virtual bool is_linked() const = 0;
  virtual uint16_t get_node_split_pivot() const = 0;
};

/**
 * FixedKVInternalNode
 *
 * Abstracts operations on and layout of internal nodes for the
 * FixedKVBTree.
 */
template <
  size_t CAPACITY,
  typename NODE_KEY,
  typename NODE_KEY_LE,
  size_t node_size,
  typename node_type_t>
struct FixedKVInternalNode
  : FixedKVNode<NODE_KEY>,
    common::FixedKVNodeLayout<
      CAPACITY,
      fixed_kv_node_meta_t<NODE_KEY>,
      fixed_kv_node_meta_le_t<NODE_KEY_LE>,
      NODE_KEY, NODE_KEY_LE,
      paddr_t, paddr_le_t>,
    RootChildNode<RootBlock, node_type_t>,
    ParentNode<node_type_t, NODE_KEY>,
    ChildNode<node_type_t, node_type_t, NODE_KEY> {
  using Ref = TCachedExtentRef<node_type_t>;
  using base_t = FixedKVNode<NODE_KEY>;
  using base_ref = typename FixedKVNode<NODE_KEY>::FixedKVNodeRef;
  using node_layout_t =
    common::FixedKVNodeLayout<
      CAPACITY,
      fixed_kv_node_meta_t<NODE_KEY>,
      fixed_kv_node_meta_le_t<NODE_KEY_LE>,
      NODE_KEY,
      NODE_KEY_LE,
      paddr_t,
      paddr_le_t>;
  using internal_const_iterator_t = typename node_layout_t::const_iterator;
  using internal_iterator_t = typename node_layout_t::iterator;
  using this_type_t = FixedKVInternalNode<
    CAPACITY,
    NODE_KEY,
    NODE_KEY_LE,
    node_size,
    node_type_t>;
  using parent_node_t = ParentNode<node_type_t, NODE_KEY>;
  using base_child_node_t = BaseChildNode<node_type_t, NODE_KEY>;
  using child_node_t = ChildNode<node_type_t, node_type_t, NODE_KEY>;
  using root_node_t = RootChildNode<RootBlock, node_type_t>;

  bool is_linked() const final {
    return this->has_parent_tracker() ||
	   (this->is_btree_root() && this->has_root_parent());
  }

  void do_on_rewrite(Transaction &t, CachedExtent &extent) final {
    this->parent_node_t::on_rewrite(t, static_cast<node_type_t&>(extent));
  }

  explicit FixedKVInternalNode(ceph::bufferptr &&ptr)
    : FixedKVNode<NODE_KEY>(std::move(ptr)),
      ParentNode<node_type_t, NODE_KEY>(CAPACITY) {
    this->set_layout_buf(this->get_bptr().c_str());
  }
  // Must be identical with FixedKVInternalNode(ptr) after on_fully_loaded()
  explicit FixedKVInternalNode(extent_len_t length)
    : FixedKVNode<NODE_KEY>(length),
      ParentNode<node_type_t, NODE_KEY>(CAPACITY) {}
  FixedKVInternalNode(const FixedKVInternalNode &rhs)
    : FixedKVNode<NODE_KEY>(rhs),
      ParentNode<node_type_t, NODE_KEY>(rhs) {
    this->set_layout_buf(this->get_bptr().c_str());
  }

  uint16_t get_node_split_pivot() const final{
    return this->get_split_pivot().get_offset();
  }

  void prepare_commit() final {
    parent_node_t::prepare_commit();
  }

  virtual ~FixedKVInternalNode() {
    if (this->is_valid() && !this->is_pending()) {
      if (this->is_btree_root()) {
	this->root_node_t::destroy();
      } else {
	this->child_node_t::destroy();
      }
    }
  }

  void on_initial_write() final {
    // All in-memory relative addrs are necessarily block-relative
    resolve_relative_addrs(this->get_paddr());
    if (this->is_btree_root()) {
      this->root_node_t::on_initial_write();
    }
  }

  void on_invalidated(Transaction &t) final {
    this->child_node_t::on_invalidated();
  }

  fixed_kv_node_meta_t<NODE_KEY> get_node_meta() const {
    return this->get_meta();
  }

  uint32_t calc_crc32c() const final {
    return this->calc_phy_checksum();
  }

  void update_in_extent_chksum_field(uint32_t crc) final {
    this->set_phy_checksum(crc);
  }

  uint32_t get_in_extent_checksum() const {
    return this->get_phy_checksum();
  }

  typename node_layout_t::delta_buffer_t delta_buffer;
  typename node_layout_t::delta_buffer_t *maybe_get_delta_buffer() {
    return this->is_mutation_pending() 
	    ? &delta_buffer : nullptr;
  }

  CachedExtentRef duplicate_for_write(Transaction&) override {
    assert(delta_buffer.empty());
    return CachedExtentRef(new node_type_t(*this));
  };

  void on_replace_prior() final {
    this->parent_node_t::on_replace_prior();
    if (this->is_btree_root()) {
      this->root_node_t::on_replace_prior();
    } else {
      this->child_node_t::on_replace_prior();
    }
  }

  void update(
    internal_const_iterator_t iter,
    paddr_t addr,
    base_child_node_t* nextent) {
    LOG_PREFIX(FixedKVInternalNode::update);
    SUBTRACE(seastore_fixedkv_tree, "trans.{}, pos {}, {}",
      this->pending_for_transaction,
      iter.get_offset(),
      (void*)nextent);
    this->update_child_ptr(iter.get_offset(), nextent);
    return this->journal_update(
      iter,
      this->maybe_generate_relative(addr),
      maybe_get_delta_buffer());
  }

  void insert(
    internal_const_iterator_t iter,
    NODE_KEY pivot,
    paddr_t addr,
    base_child_node_t* nextent) {
    LOG_PREFIX(FixedKVInternalNode::insert);
    SUBTRACE(seastore_fixedkv_tree, "trans.{}, pos {}, key {}, {}",
      this->pending_for_transaction,
      iter.get_offset(),
      pivot,
      (void*)nextent);
    this->insert_child_ptr(iter.get_offset(), nextent);
    return this->journal_insert(
      iter,
      pivot,
      this->maybe_generate_relative(addr),
      maybe_get_delta_buffer());
  }

  void remove(internal_const_iterator_t iter) {
    LOG_PREFIX(FixedKVInternalNode::remove);
    SUBTRACE(seastore_fixedkv_tree, "trans.{}, pos {}, key {}",
      this->pending_for_transaction,
      iter.get_offset(),
      iter.get_key());
    this->remove_child_ptr(iter.get_offset());
    return this->journal_remove(
      iter,
      maybe_get_delta_buffer());
  }

  void replace(
    internal_const_iterator_t iter,
    NODE_KEY pivot,
    paddr_t addr,
    base_child_node_t* nextent) {
    LOG_PREFIX(FixedKVInternalNode::replace);
    SUBTRACE(seastore_fixedkv_tree, "trans.{}, pos {}, old key {}, key {}, {}",
      this->pending_for_transaction,
      iter.get_offset(),
      iter.get_key(),
      pivot,
      (void*)nextent);
    this->update_child_ptr(iter.get_offset(), nextent);
    return this->journal_replace(
      iter,
      pivot,
      this->maybe_generate_relative(addr),
      maybe_get_delta_buffer());
  }

  std::tuple<Ref, Ref, NODE_KEY>
  make_split_children(op_context_t<NODE_KEY> c) {
    auto left = c.cache.template alloc_new_non_data_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    auto right = c.cache.template alloc_new_non_data_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    this->split_child_ptrs(c.trans, *left, *right);
    auto pivot = this->split_into(*left, *right);
    left->range = left->get_meta();
    right->range = right->get_meta();
    this->adjust_copy_src_dest_on_split(c.trans, *left, *right);
    return std::make_tuple(
      left,
      right,
      pivot);
  }

  Ref make_full_merge(
    op_context_t<NODE_KEY> c,
    Ref &right) {
    auto replacement = c.cache.template alloc_new_non_data_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    replacement->merge_child_ptrs(
      c.trans, static_cast<node_type_t&>(*this), *right);
    replacement->merge_from(*this, *right->template cast<node_type_t>());
    replacement->range = replacement->get_meta();
    replacement->adjust_copy_src_dest_on_merge(
      c.trans, static_cast<node_type_t&>(*this), *right);
    return replacement;
  }

  std::tuple<Ref, Ref, NODE_KEY>
  make_balanced(
    op_context_t<NODE_KEY> c,
    Ref &_right,
    bool prefer_left) {
    ceph_assert(_right->get_type() == this->get_type());
    auto &right = *_right->template cast<node_type_t>();
    auto replacement_left = c.cache.template alloc_new_non_data_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    auto replacement_right = c.cache.template alloc_new_non_data_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);

    this->balance_child_ptrs(
      c.trans,
      static_cast<node_type_t&>(*this),
      right,
      prefer_left,
      *replacement_left,
      *replacement_right);
    auto pivot = this->balance_into_new_nodes(
      *this,
      right,
      prefer_left,
      *replacement_left,
      *replacement_right);
    replacement_left->range = replacement_left->get_meta();
    replacement_right->range = replacement_right->get_meta();
    this->adjust_copy_src_dest_on_balance(
      c.trans,
      static_cast<node_type_t&>(*this),
      right,
      prefer_left,
      *replacement_left,
      *replacement_right);
    return std::make_tuple(
      replacement_left,
      replacement_right,
      pivot);
  }

  void on_fully_loaded() final {
    this->set_layout_buf(this->get_bptr().c_str());
  }

  /**
   * Internal relative addresses on read or in memory prior to commit
   * are either record or block relative depending on whether this
   * physical node is is_initial_pending() or just is_mutable().
   *
   * User passes appropriate base depending on lifecycle and
   * resolve_relative_addrs fixes up relative internal references
   * based on base.
   */
  void resolve_relative_addrs(paddr_t base) final {
    LOG_PREFIX(FixedKVInternalNode::resolve_relative_addrs);
    for (auto i: *this) {
      if (i->get_val().is_relative()) {
	auto updated = base.add_relative(i->get_val());
	SUBTRACE(seastore_fixedkv_tree, "{} -> {}", i->get_val(), updated);
	i->set_val(updated);
      }
    }
  }

  void node_resolve_vals(
    internal_iterator_t from,
    internal_iterator_t to) const {
    if (this->is_initial_pending()) {
      for (auto i = from; i != to; ++i) {
	if (i->get_val().is_relative()) {
	  assert(i->get_val().is_block_relative());
	  i->set_val(this->get_paddr().add_relative(i->get_val()));
	}
      }
    }
  }
  void node_unresolve_vals(
    internal_iterator_t from,
    internal_iterator_t to) const {
    if (this->is_initial_pending()) {
      for (auto i = from; i != to; ++i) {
	if (i->get_val().is_relative()) {
	  assert(i->get_val().is_record_relative());
	  i->set_val(i->get_val().block_relative_to(this->get_paddr()));
	}
      }
    }
  }

  std::ostream &print_detail(std::ostream &out) const
  {
    out << ", size=" << this->get_size()
	<< ", meta=" << this->get_meta()
	<< ", my_tracker=" << (void*)this->my_tracker;
    if (this->my_tracker) {
      out << ", my_tracker->parent=" << (void*)this->my_tracker->get_parent().get();
    }
    return out << ", root_block=" << (void*)this->parent_of_root.get();
  }

  ceph::bufferlist get_delta() {
    ceph::buffer::ptr bptr(delta_buffer.get_bytes());
    delta_buffer.copy_out(bptr.c_str(), bptr.length());
    ceph::bufferlist bl;
    bl.push_back(bptr);
    return bl;
  }

  void apply_delta_and_adjust_crc(
    paddr_t base, const ceph::bufferlist &_bl) {
    assert(_bl.length());
    ceph::bufferlist bl = _bl;
    bl.rebuild();
    typename node_layout_t::delta_buffer_t buffer;
    buffer.copy_in(bl.front().c_str(), bl.front().length());
    buffer.replay(*this);
    auto crc = calc_crc32c();
    this->set_last_committed_crc(crc);
    this->update_in_extent_chksum_field(crc);
    resolve_relative_addrs(base);
  }

  constexpr static size_t get_min_capacity() {
    return (node_layout_t::get_capacity() - 1) / 2;
  }

  bool at_max_capacity() const {
    assert(this->get_size() <= node_layout_t::get_capacity());
    return this->get_size() == node_layout_t::get_capacity();
  }

  bool at_min_capacity() const {
    assert(this->get_size() >= (get_min_capacity() - 1));
    return this->get_size() <= get_min_capacity();
  }

  bool below_min_capacity() const {
    assert(this->get_size() >= (get_min_capacity() - 1));
    return this->get_size() < get_min_capacity();
  }
};

template <
  size_t CAPACITY,
  typename NODE_KEY,
  typename NODE_KEY_LE,
  typename VAL,
  typename VAL_LE,
  size_t node_size,
  typename internal_node_type_t,
  typename node_type_t>
struct FixedKVLeafNode
  : FixedKVNode<NODE_KEY>,
    common::FixedKVNodeLayout<
      CAPACITY,
      fixed_kv_node_meta_t<NODE_KEY>,
      fixed_kv_node_meta_le_t<NODE_KEY_LE>,
      NODE_KEY, NODE_KEY_LE,
      VAL, VAL_LE>,
    RootChildNode<RootBlock, node_type_t>,
    ChildNode<internal_node_type_t, node_type_t, NODE_KEY> {
  using Ref = TCachedExtentRef<node_type_t>;
  using node_layout_t =
    common::FixedKVNodeLayout<
      CAPACITY,
      fixed_kv_node_meta_t<NODE_KEY>,
      fixed_kv_node_meta_le_t<NODE_KEY_LE>,
      NODE_KEY,
      NODE_KEY_LE,
      VAL,
      VAL_LE>;
  using internal_const_iterator_t = typename node_layout_t::const_iterator;
  using this_type_t = FixedKVLeafNode<
    CAPACITY,
    NODE_KEY,
    NODE_KEY_LE,
    VAL,
    VAL_LE,
    node_size,
    internal_node_type_t,
    node_type_t>;
  using base_t = FixedKVNode<NODE_KEY>;
  using child_node_t = ChildNode<internal_node_type_t, node_type_t, NODE_KEY>;
  using root_node_t = RootChildNode<RootBlock, node_type_t>;
  explicit FixedKVLeafNode(ceph::bufferptr &&ptr)
    : FixedKVNode<NODE_KEY>(std::move(ptr)) {
    this->set_layout_buf(this->get_bptr().c_str());
  }
  // Must be identical with FixedKVLeafNode(ptr) after on_fully_loaded()
  explicit FixedKVLeafNode(extent_len_t length)
    : FixedKVNode<NODE_KEY>(length) {}
  FixedKVLeafNode(const FixedKVLeafNode &rhs)
    : FixedKVNode<NODE_KEY>(rhs),
      modifications(rhs.modifications) {
    this->set_layout_buf(this->get_bptr().c_str());
  }

  bool is_linked() const final {
    return this->has_parent_tracker() ||
	   (this->is_btree_root() && this->has_root_parent());
  }

  // for the stable extent, modifications is always 0;
  // it will increase for each transaction-local change, so that
  // modifications can be detected (see BtreeLBAMapping.parent_modifications)
  uint64_t modifications = 0;

  void on_invalidated(Transaction &t) final {
    this->child_node_t::on_invalidated();
  }

  void on_initial_write() final {
    // All in-memory relative addrs are necessarily block-relative
    this->resolve_relative_addrs(this->get_paddr());
    if (this->is_btree_root()) {
      this->root_node_t::on_initial_write();
    }
  }

  void on_modify() {
    modifications++;
  }

  bool modified_since(uint64_t v) const {
    ceph_assert(v <= modifications);
    return v != modifications;
  }

  uint16_t get_node_split_pivot() const final{
    return this->get_split_pivot().get_offset();
  }

  virtual ~FixedKVLeafNode() {
    if (this->is_valid() && !this->is_pending()) {
      if (this->is_btree_root()) {
	this->root_node_t::destroy();
      } else {
	this->child_node_t::destroy();
      }
    }
  }

  void on_fully_loaded() final {
    this->set_layout_buf(this->get_bptr().c_str());
  }

  virtual void do_prepare_commit() = 0;
  void prepare_commit() final {
    do_prepare_commit();
    modifications = 0;
  }

  virtual void do_on_replace_prior() = 0;
  void on_replace_prior() final {
    ceph_assert(!this->is_rewrite());
    do_on_replace_prior();
    if (this->is_btree_root()) {
      this->root_node_t::on_replace_prior();
    } else {
      this->child_node_t::on_replace_prior();
    }
    modifications = 0;
  }

  fixed_kv_node_meta_t<NODE_KEY> get_node_meta() const {
    return this->get_meta();
  }

  uint32_t calc_crc32c() const final {
    return this->calc_phy_checksum();
  }

  void update_in_extent_chksum_field(uint32_t crc) final {
    this->set_phy_checksum(crc);
  }

  uint32_t get_in_extent_checksum() const {
    return this->get_phy_checksum();
  }

  typename node_layout_t::delta_buffer_t delta_buffer;
  virtual typename node_layout_t::delta_buffer_t *maybe_get_delta_buffer() {
    return this->is_mutation_pending() ? &delta_buffer : nullptr;
  }

  CachedExtentRef duplicate_for_write(Transaction&) override {
    assert(delta_buffer.empty());
    return CachedExtentRef(new node_type_t(*static_cast<node_type_t*>(this)));
  };

  virtual void update(
    internal_const_iterator_t iter,
    VAL val) = 0;
  virtual internal_const_iterator_t insert(
    internal_const_iterator_t iter,
    NODE_KEY addr,
    VAL val) = 0;
  virtual void remove(internal_const_iterator_t iter) = 0;
  virtual void on_split(
    Transaction &t,
    node_type_t &left,
    node_type_t &right) = 0;
  virtual void adjust_copy_src_dest_on_split(
    Transaction &t,
    node_type_t &left,
    node_type_t &right) = 0;

  std::tuple<Ref, Ref, NODE_KEY>
  make_split_children(op_context_t<NODE_KEY> c) {
    auto left = c.cache.template alloc_new_non_data_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    auto right = c.cache.template alloc_new_non_data_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    this->on_split(c.trans, *left, *right);
    auto pivot = this->split_into(*left, *right);
    left->range = left->get_meta();
    right->range = right->get_meta();
    this->adjust_copy_src_dest_on_split(c.trans, *left, *right);
    return std::make_tuple(
      left,
      right,
      pivot);
  }

  virtual void on_merge(
    Transaction &t,
    node_type_t &left,
    node_type_t &right) = 0;
  virtual void adjust_copy_src_dest_on_merge(
    Transaction &t,
    node_type_t &left,
    node_type_t &right) = 0;

  Ref make_full_merge(
    op_context_t<NODE_KEY> c,
    Ref &right) {
    auto replacement = c.cache.template alloc_new_non_data_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    replacement->on_merge(c.trans, static_cast<node_type_t&>(*this), *right);
    replacement->merge_from(*this, *right->template cast<node_type_t>());
    replacement->range = replacement->get_meta();
    replacement->adjust_copy_src_dest_on_merge(
      c.trans, static_cast<node_type_t&>(*this), *right);
    return replacement;
  }

  virtual void on_balance(
    Transaction &t,
    node_type_t &left,
    node_type_t &right,
    bool prefer_left,
    node_type_t &replacement_left,
    node_type_t &replacement_right) = 0;
  virtual void adjust_copy_src_dest_on_balance(
    Transaction &t,
    node_type_t &left,
    node_type_t &right,
    bool prefer_left,
    node_type_t &replacement_left,
    node_type_t &replacement_right) = 0;

  std::tuple<Ref, Ref, NODE_KEY>
  make_balanced(
    op_context_t<NODE_KEY> c,
    Ref &_right,
    bool prefer_left) {
    ceph_assert(_right->get_type() == this->get_type());
    auto &right = *_right->template cast<node_type_t>();
    auto replacement_left = c.cache.template alloc_new_non_data_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    auto replacement_right = c.cache.template alloc_new_non_data_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);

    this->on_balance(
      c.trans,
      static_cast<node_type_t&>(*this),
      right,
      prefer_left,
      *replacement_left,
      *replacement_right);
    auto pivot = this->balance_into_new_nodes(
      *this,
      right,
      prefer_left,
      *replacement_left,
      *replacement_right);
    replacement_left->range = replacement_left->get_meta();
    replacement_right->range = replacement_right->get_meta();
    this->adjust_copy_src_dest_on_balance(
      c.trans,
      static_cast<node_type_t&>(*this),
      right,
      prefer_left,
      *replacement_left,
      *replacement_right);
    return std::make_tuple(
      replacement_left,
      replacement_right,
      pivot);
  }

  ceph::bufferlist get_delta() {
    ceph::buffer::ptr bptr(delta_buffer.get_bytes());
    delta_buffer.copy_out(bptr.c_str(), bptr.length());
    ceph::bufferlist bl;
    bl.push_back(bptr);
    return bl;
  }

  void apply_delta_and_adjust_crc(
    paddr_t base, const ceph::bufferlist &_bl) {
    assert(_bl.length());
    ceph::bufferlist bl = _bl;
    bl.rebuild();
    typename node_layout_t::delta_buffer_t buffer;
    buffer.copy_in(bl.front().c_str(), bl.front().length());
    buffer.replay(*this);
    auto crc = calc_crc32c();
    this->set_last_committed_crc(crc);
    this->update_in_extent_chksum_field(crc);
    this->resolve_relative_addrs(base);
  }

  std::ostream &print_detail(std::ostream &out) const
  {
    return out << ", size=" << this->get_size()
	       << ", meta=" << this->get_meta();
  }

  constexpr static size_t get_min_capacity() {
    return (node_layout_t::get_capacity() - 1) / 2;
  }

  bool at_max_capacity() const {
    assert(this->get_size() <= node_layout_t::get_capacity());
    return this->get_size() == node_layout_t::get_capacity();
  }

  bool at_min_capacity() const {
    assert(this->get_size() >= (get_min_capacity() - 1));
    return this->get_size() <= get_min_capacity();
  }

  bool below_min_capacity() const {
    assert(this->get_size() >= (get_min_capacity() - 1));
    return this->get_size() < get_min_capacity();
  }
};

} // namespace crimson::os::seastore

#if FMT_VERSION >= 90000
template <>
struct fmt::formatter<
  crimson::os::seastore::FixedKVNode<
    crimson::os::seastore::laddr_t>> : fmt::ostream_formatter {};
template <>
struct fmt::formatter<
  crimson::os::seastore::FixedKVNode<
    crimson::os::seastore::paddr_t>> : fmt::ostream_formatter {};
#endif
