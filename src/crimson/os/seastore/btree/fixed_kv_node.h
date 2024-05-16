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
struct FixedKVNode : ChildableCachedExtent {
  using FixedKVNodeRef = TCachedExtentRef<FixedKVNode>;
  fixed_kv_node_meta_t<node_key_t> range;

  struct copy_source_cmp_t {
    using is_transparent = node_key_t;
    bool operator()(const FixedKVNodeRef &l, const FixedKVNodeRef &r) const {
      assert(l->range.end <= r->range.begin
	|| r->range.end <= l->range.begin
	|| (l->range.begin == r->range.begin
	    && l->range.end == r->range.end));
      return l->range.begin < r->range.begin;
    }
    bool operator()(const node_key_t &l, const FixedKVNodeRef &r) const {
      return l < r->range.begin;
    }
    bool operator()(const FixedKVNodeRef &l, const node_key_t &r) const {
      return l->range.begin < r;
    }
  };

  /*
   *
   * Nodes of fixed-kv-btree connect to their child nodes by pointers following
   * invariants below:
   *
   * 1. if nodes are stable:
   * 	a. parent points at the node's stable parent
   * 	b. prior_instance is empty
   * 	c. child pointers point at stable children. Child resolution is done
   * 	   directly via this array.
   * 	d. copy_sources is empty
   * 2. if nodes are mutation_pending:
   * 	a. parent is empty and needs to be fixed upon commit
   * 	b. prior_instance points to its stable version
   * 	c. child pointers are null except for initial_pending() children of
   * 	   this transaction. Child resolution is done by first checking this
   * 	   array, and then recursively resolving via the parent. We copy child
   * 	   pointers from parent on commit.
   * 	d. copy_sources is empty
   * 3. if nodes are initial_pending
   * 	a. parent points at its pending parent on this transaction (must exist)
   * 	b. prior_instance is empty or, if it's the result of rewrite, points to
   * 	   its stable predecessor
   * 	c. child pointers are null except for initial_pending() children of
   * 	   this transaction (live due to 3a below). Child resolution is done
   * 	   by first checking this array, and then recursively resolving via
   * 	   the correct copy_sources entry. We copy child pointers from copy_sources
   * 	   on commit.
   * 	d. copy_sources contains the set of stable nodes at the same tree-level(only
   * 	   its "prior_instance" if the node is the result of a rewrite), with which
   * 	   the lba range of this node overlaps.
   * 4. EXIST_CLEAN and EXIST_MUTATION_PENDING belong to 3 above (except that they
   * 	cannot be rewritten) because their parents must be mutated upon remapping.
   */
  std::vector<ChildableCachedExtent*> children;
  std::set<FixedKVNodeRef, copy_source_cmp_t> copy_sources;
  uint16_t capacity = 0;
  parent_tracker_t* my_tracker = nullptr;
  RootBlockRef root_block;

  bool is_linked() {
    assert(!has_parent_tracker() || !(bool)root_block);
    return (bool)has_parent_tracker() || (bool)root_block;
  }

  FixedKVNode(uint16_t capacity, ceph::bufferptr &&ptr)
    : ChildableCachedExtent(std::move(ptr)),
      children(capacity, nullptr),
      capacity(capacity) {}
  FixedKVNode(const FixedKVNode &rhs)
    : ChildableCachedExtent(rhs),
      range(rhs.range),
      children(rhs.capacity, nullptr),
      capacity(rhs.capacity) {}

  virtual fixed_kv_node_meta_t<node_key_t> get_node_meta() const = 0;
  virtual uint16_t get_node_size() const = 0;

  virtual ~FixedKVNode() = default;
  virtual node_key_t get_key_from_idx(uint16_t idx) const = 0;

  template<typename iter_t>
  void update_child_ptr(iter_t iter, ChildableCachedExtent* child) {
    children[iter.get_offset()] = child;
    set_child_ptracker(child);
  }

  virtual bool is_leaf_and_has_children() const = 0;

  template<typename iter_t>
  void insert_child_ptr(iter_t iter, ChildableCachedExtent* child) {
    auto raw_children = children.data();
    auto offset = iter.get_offset();
    std::memmove(
      &raw_children[offset + 1],
      &raw_children[offset],
      (get_node_size() - offset) * sizeof(ChildableCachedExtent*));
    if (child) {
      children[offset] = child;
      set_child_ptracker(child);
    } else {
      // this can only happen when reserving lba spaces
      ceph_assert(is_leaf_and_has_children());
      // this is to avoid mistakenly copying pointers from
      // copy sources when committing this lba node, because
      // we rely on pointers' "nullness" to avoid copying
      // pointers for updated values
      children[offset] = get_reserved_ptr();
    }
  }

  template<typename iter_t>
  void remove_child_ptr(iter_t iter) {
    LOG_PREFIX(FixedKVNode::remove_child_ptr);
    auto raw_children = children.data();
    auto offset = iter.get_offset();
    SUBTRACE(seastore_fixedkv_tree, "trans.{}, pos {}, total size {}, extent {}",
      this->pending_for_transaction,
      offset,
      get_node_size(),
      (void*)raw_children[offset]);
    // parent tracker of the child being removed will be
    // reset when the child is invalidated, so no need to
    // reset it here
    std::memmove(
      &raw_children[offset],
      &raw_children[offset + 1],
      (get_node_size() - offset - 1) * sizeof(ChildableCachedExtent*));
  }

  virtual bool have_children() const = 0;

  void on_rewrite(CachedExtent &extent, extent_len_t off) final {
    assert(get_type() == extent.get_type());
    assert(off == 0);
    auto &foreign_extent = (FixedKVNode&)extent;
    range = get_node_meta();

    if (have_children()) {
      if (!foreign_extent.is_pending()) {
	copy_sources.emplace(&foreign_extent);
      } else {
	ceph_assert(foreign_extent.is_mutation_pending());
	copy_sources.emplace(
	  foreign_extent.get_prior_instance()->template cast<FixedKVNode>());
	children = std::move(foreign_extent.children);
	adjust_ptracker_for_children();
      }
    }
    
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

  FixedKVNode& get_stable_for_key(node_key_t key) const {
    ceph_assert(is_pending());
    if (is_mutation_pending()) {
      return (FixedKVNode&)*get_prior_instance();
    } else {
      ceph_assert(!copy_sources.empty());
      auto it = copy_sources.upper_bound(key);
      it--;
      auto &copy_source = *it;
      ceph_assert(copy_source->get_node_meta().is_in_range(key));
      return *copy_source;
    }
  }

  static void push_copy_sources(
    FixedKVNode &dest,
    FixedKVNode &src)
  {
    ceph_assert(dest.is_initial_pending());
    if (!src.is_pending()) {
      dest.copy_sources.emplace(&src);
    } else if (src.is_mutation_pending()) {
      dest.copy_sources.emplace(
	src.get_prior_instance()->template cast<FixedKVNode>());
    } else {
      ceph_assert(src.is_initial_pending());
      dest.copy_sources.insert(
	src.copy_sources.begin(),
	src.copy_sources.end());
    }
  }

  virtual uint16_t get_node_split_pivot() = 0;

  static void move_child_ptrs(
    FixedKVNode &dest,
    FixedKVNode &src,
    size_t dest_start,
    size_t src_start,
    size_t src_end)
  {
    std::memmove(
      dest.children.data() + dest_start,
      src.children.data() + src_start,
      (src_end - src_start) * sizeof(ChildableCachedExtent*));

    ceph_assert(src_start < src_end);
    ceph_assert(src.children.size() >= src_end);
    for (auto it = src.children.begin() + src_start;
	it != src.children.begin() + src_end;
	it++)
    {
      auto child = *it;
      if (is_valid_child_ptr(child)) {
	dest.set_child_ptracker(child);
      }
    }
  }

  void link_child(ChildableCachedExtent* child, uint16_t pos) {
    assert(pos < get_node_size());
    assert(child);
    ceph_assert(!is_pending());
    ceph_assert(child->is_valid() && !child->is_pending());
    assert(!children[pos]);
    children[pos] = child;
    set_child_ptracker(child);
  }

  virtual bool is_child_stable(op_context_t<node_key_t>, uint16_t pos) const = 0;
  virtual bool is_child_data_stable(op_context_t<node_key_t>, uint16_t pos) const = 0;

  template <typename T>
  get_child_ret_t<T> get_child(
    op_context_t<node_key_t> c,
    uint16_t pos,
    node_key_t key)
  {
    assert(children.capacity());
    auto child = children[pos];
    ceph_assert(!is_reserved_ptr(child));
    if (is_valid_child_ptr(child)) {
      return c.cache.template get_extent_viewable_by_trans<T>(c.trans, (T*)child);
    } else if (is_pending()) {
      auto &sparent = get_stable_for_key(key);
      auto spos = sparent.lower_bound_offset(key);
      auto child = sparent.children[spos];
      if (is_valid_child_ptr(child)) {
	return c.cache.template get_extent_viewable_by_trans<T>(c.trans, (T*)child);
      } else {
	return child_pos_t(&sparent, spos);
      }
    } else {
      return child_pos_t(this, pos);
    }
  }

  template <typename T, typename iter_t>
  get_child_ret_t<T> get_child(op_context_t<node_key_t> c, iter_t iter) {
    return get_child<T>(c, iter.get_offset(), iter.get_key());
  }

  void split_child_ptrs(
    FixedKVNode &left,
    FixedKVNode &right)
  {
    assert(!left.my_tracker);
    assert(!right.my_tracker);
    push_copy_sources(left, *this);
    push_copy_sources(right, *this);
    if (is_pending()) {
      uint16_t pivot = get_node_split_pivot();
      move_child_ptrs(left, *this, 0, 0, pivot);
      move_child_ptrs(right, *this, 0, pivot, get_node_size());
      my_tracker = nullptr;
    }
  }

  void merge_child_ptrs(
    FixedKVNode &left,
    FixedKVNode &right)
  {
    ceph_assert(!my_tracker);
    push_copy_sources(*this, left);
    push_copy_sources(*this, right);

    if (left.is_pending()) {
      move_child_ptrs(*this, left, 0, 0, left.get_node_size());
      left.my_tracker = nullptr;
    }

    if (right.is_pending()) {
      move_child_ptrs(*this, right, left.get_node_size(), 0, right.get_node_size());
      right.my_tracker = nullptr;
    }
  }

  static void balance_child_ptrs(
    FixedKVNode &left,
    FixedKVNode &right,
    bool prefer_left,
    FixedKVNode &replacement_left,
    FixedKVNode &replacement_right)
  {
    size_t l_size = left.get_node_size();
    size_t r_size = right.get_node_size();
    size_t total = l_size + r_size;
    size_t pivot_idx = (l_size + r_size) / 2;
    if (total % 2 && prefer_left) {
      pivot_idx++;
    }

    assert(!replacement_left.my_tracker);
    assert(!replacement_right.my_tracker);
    if (pivot_idx < l_size) {
      // deal with left
      push_copy_sources(replacement_left, left);
      push_copy_sources(replacement_right, left);
      if (left.is_pending()) {
	move_child_ptrs(replacement_left, left, 0, 0, pivot_idx);
	move_child_ptrs(replacement_right, left, 0, pivot_idx, l_size);
	left.my_tracker = nullptr;
      }

      // deal with right
      push_copy_sources(replacement_right, right);
      if (right.is_pending()) {
	move_child_ptrs(replacement_right, right, l_size - pivot_idx, 0, r_size);
	right.my_tracker= nullptr;
      }
    } else {
      // deal with left
      push_copy_sources(replacement_left, left);
      if (left.is_pending()) {
	move_child_ptrs(replacement_left, left, 0, 0, l_size);
	left.my_tracker = nullptr;
      }

      // deal with right
      push_copy_sources(replacement_left, right);
      push_copy_sources(replacement_right, right);
      if (right.is_pending()) {
	move_child_ptrs(replacement_left, right, l_size, 0, pivot_idx - l_size);
	move_child_ptrs(replacement_right, right, 0, pivot_idx - l_size, r_size);
	right.my_tracker= nullptr;
      }
    }
  }

  void set_parent_tracker_from_prior_instance() {
    assert(is_mutation_pending());
    auto &prior = (FixedKVNode&)(*get_prior_instance());
    if (range.is_root()) {
      ceph_assert(prior.root_block);
      ceph_assert(pending_for_transaction);
      root_block = prior.root_block;
      link_phy_tree_root_node(root_block, this);
      return;
    }
    ceph_assert(!root_block);
    take_prior_parent_tracker();
    assert(is_parent_valid());
    auto parent = get_parent_node<FixedKVNode>();
    //TODO: can this search be avoided?
    auto off = parent->lower_bound_offset(get_node_meta().begin);
    assert(parent->get_key_from_idx(off) == get_node_meta().begin);
    parent->children[off] = this;
  }

  bool is_children_empty() const {
    for (auto it = children.begin();
	it != children.begin() + get_node_size();
	it++) {
      if (is_valid_child_ptr(*it)
	  && (*it)->is_valid()) {
	return false;
      }
    }
    return true;
  }

  void set_children_from_prior_instance() {
    assert(get_prior_instance());
    auto &prior = (FixedKVNode&)(*get_prior_instance());
    assert(prior.my_tracker || prior.is_children_empty());

    if (prior.my_tracker) {
      prior.my_tracker->reset_parent(this);
      my_tracker = prior.my_tracker;
      // All my initial pending children is pointing to the original
      // tracker which has been dropped by the above line, so need
      // to adjust them to point to the new tracker
      adjust_ptracker_for_children();
    }
    assert(my_tracker || is_children_empty());
  }

  void adjust_ptracker_for_children() {
    auto begin = children.begin();
    auto end = begin + get_node_size();
    ceph_assert(end <= children.end());
    for (auto it = begin; it != end; it++) {
      auto child = *it;
      if (is_valid_child_ptr(child)) {
	set_child_ptracker(child);
      }
    }
  }

  void on_delta_write(paddr_t record_block_offset) final {
    // All in-memory relative addrs are necessarily record-relative
    assert(get_prior_instance());
    assert(pending_for_transaction);
    resolve_relative_addrs(record_block_offset);
  }

  virtual uint16_t lower_bound_offset(node_key_t) const = 0;
  virtual uint16_t upper_bound_offset(node_key_t) const = 0;

  virtual bool validate_stable_children() = 0;

  template<typename iter_t>
  uint16_t copy_children_from_stable_source(
    FixedKVNode &source,
    iter_t foreign_start_it,
    iter_t foreign_end_it,
    iter_t local_start_it) {
    auto foreign_it = foreign_start_it, local_it = local_start_it;
    while (foreign_it != foreign_end_it
	  && local_it.get_offset() < get_node_size())
    {
      auto &child = children[local_it.get_offset()];
      if (foreign_it.get_key() == local_it.get_key()) {
	// the foreign key is preserved
	if (!child) {
	  child = source.children[foreign_it.get_offset()];
	  // child can be either valid if present, nullptr if absent,
	  // or reserved ptr.
	}
	foreign_it++;
	local_it++;
      } else if (foreign_it.get_key() < local_it.get_key()) {
	// the foreign key has been removed, because, if it hasn't,
	// there must have been a local key before the one pointed
	// by the current "local_it" that's equal to this foreign key
	// and has pushed the foreign_it forward.
	foreign_it++;
      } else {
	// the local key must be a newly inserted one.
	local_it++;
      }
    }
    return local_it.get_offset();
  }

  template<typename Func>
  void copy_children_from_stable_sources(Func &&get_iter) {
    if (!copy_sources.empty()) {
      auto it = --copy_sources.upper_bound(get_node_meta().begin);
      auto &cs = *it;
      uint16_t start_pos = cs->lower_bound_offset(
	get_node_meta().begin);
      if (start_pos == cs->get_node_size()) {
	it++;
	start_pos = 0;
      }
      uint16_t local_next_pos = 0;
      for (; it != copy_sources.end(); it++) {
	auto& copy_source = *it;
	auto end_pos = copy_source->get_node_size();
	if (copy_source->get_node_meta().is_in_range(get_node_meta().end)) {
	  end_pos = copy_source->upper_bound_offset(get_node_meta().end);
	}
	auto local_start_iter = get_iter(*this, local_next_pos);
	auto foreign_start_iter = get_iter(*copy_source, start_pos);
	auto foreign_end_iter = get_iter(*copy_source, end_pos);
	local_next_pos = copy_children_from_stable_source(
	  *copy_source, foreign_start_iter, foreign_end_iter, local_start_iter);
	if (end_pos != copy_source->get_node_size()) {
	  break;
	}
	start_pos = 0;
      }
    }
  }

  void on_invalidated(Transaction &t) final {
    reset_parent_tracker();
  }

  void on_initial_write() final {
    // All in-memory relative addrs are necessarily block-relative
    resolve_relative_addrs(get_paddr());
    if (range.is_root()) {
      reset_parent_tracker();
    }
    assert(has_parent_tracker() ? (is_parent_valid()) : true);
  }

  void set_child_ptracker(ChildableCachedExtent *child) {
    if (!this->my_tracker) {
      this->my_tracker = new parent_tracker_t(this);
    }
    child->reset_parent_tracker(this->my_tracker);
  }

  void on_clean_read() final {
    // From initial write of block, relative addrs are necessarily block-relative
    resolve_relative_addrs(get_paddr());
  }

  virtual void resolve_relative_addrs(paddr_t base) = 0;
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
      paddr_t, paddr_le_t> {
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

  FixedKVInternalNode(ceph::bufferptr &&ptr)
    : FixedKVNode<NODE_KEY>(CAPACITY, std::move(ptr)),
      node_layout_t(this->get_bptr().c_str()) {}
  FixedKVInternalNode(const FixedKVInternalNode &rhs)
    : FixedKVNode<NODE_KEY>(rhs),
      node_layout_t(this->get_bptr().c_str()) {}

  bool have_children() const final {
    return true;
  }

  bool is_leaf_and_has_children() const final {
    return false;
  }

  uint16_t get_node_split_pivot() final {
    return this->get_split_pivot().get_offset();
  }

  void prepare_commit() final {
    if (this->is_initial_pending()) {
      if (this->is_rewrite()) {
	this->set_children_from_prior_instance();
      }
      this->copy_children_from_stable_sources(
	[this](base_t &node, uint16_t pos) {
	  ceph_assert(node.get_type() == this->get_type());
	  auto &n = static_cast<this_type_t&>(node);
	  return n.iter_idx(pos);
	}
      );
      if (this->is_rewrite()) {
	this->reset_prior_instance();
      } else {
	this->adjust_ptracker_for_children();
      }
      assert(this->validate_stable_children());
      this->copy_sources.clear();
    }
  }

  bool is_child_stable(op_context_t<NODE_KEY>, uint16_t pos) const final {
    ceph_abort("impossible");
    return false;
  }
  bool is_child_data_stable(op_context_t<NODE_KEY>, uint16_t pos) const final {
    ceph_abort("impossible");
    return false;
  }

  bool validate_stable_children() final {
    LOG_PREFIX(FixedKVInternalNode::validate_stable_children);
    if (this->children.empty()) {
      return false;
    }

    for (auto i : *this) {
      auto child = (FixedKVNode<NODE_KEY>*)this->children[i.get_offset()];
      if (child && child->range.begin != i.get_key()) {
	SUBERROR(seastore_fixedkv_tree,
	  "stable child not valid: child {}, child meta{}, key {}",
	  *child,
	  child->get_node_meta(),
	  i.get_key());
	ceph_abort();
	return false;
      }
    }
    return true;
  }

  virtual ~FixedKVInternalNode() {
    if (this->is_valid() && !this->is_pending()) {
      if (this->range.is_root()) {
	ceph_assert(this->root_block);
	unlink_phy_tree_root_node<NODE_KEY>(this->root_block);
      } else {
	ceph_assert(this->is_parent_valid());
	auto parent = this->template get_parent_node<FixedKVNode<NODE_KEY>>();
	auto off = parent->lower_bound_offset(this->get_meta().begin);
	assert(parent->get_key_from_idx(off) == this->get_meta().begin);
	assert(parent->children[off] == this);
	parent->children[off] = nullptr;
      }
    }
  }

  uint16_t lower_bound_offset(NODE_KEY key) const final {
    return this->lower_bound(key).get_offset();
  }

  uint16_t upper_bound_offset(NODE_KEY key) const final {
    return this->upper_bound(key).get_offset();
  }

  NODE_KEY get_key_from_idx(uint16_t idx) const final {
    return this->iter_idx(idx).get_key();
  }

  fixed_kv_node_meta_t<NODE_KEY> get_node_meta() const {
    return this->get_meta();
  }

  uint16_t get_node_size() const final {
    return this->get_size();
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
    ceph_assert(!this->is_rewrite());
    this->set_children_from_prior_instance();
    auto &prior = (this_type_t&)(*this->get_prior_instance());
    auto copied = this->copy_children_from_stable_source(
      prior,
      prior.begin(),
      prior.end(),
      this->begin());
    ceph_assert(copied <= get_node_size());
    assert(this->validate_stable_children());
    this->set_parent_tracker_from_prior_instance();
  }

  void update(
    internal_const_iterator_t iter,
    paddr_t addr,
    FixedKVNode<NODE_KEY>* nextent) {
    LOG_PREFIX(FixedKVInternalNode::update);
    SUBTRACE(seastore_fixedkv_tree, "trans.{}, pos {}, {}",
      this->pending_for_transaction,
      iter.get_offset(),
      *nextent);
    this->update_child_ptr(iter, nextent);
    return this->journal_update(
      iter,
      this->maybe_generate_relative(addr),
      maybe_get_delta_buffer());
  }

  void insert(
    internal_const_iterator_t iter,
    NODE_KEY pivot,
    paddr_t addr,
    FixedKVNode<NODE_KEY>* nextent) {
    LOG_PREFIX(FixedKVInternalNode::insert);
    SUBTRACE(seastore_fixedkv_tree, "trans.{}, pos {}, key {}, {}",
      this->pending_for_transaction,
      iter.get_offset(),
      pivot,
      *nextent);
    this->insert_child_ptr(iter, nextent);
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
    this->remove_child_ptr(iter);
    return this->journal_remove(
      iter,
      maybe_get_delta_buffer());
  }

  void replace(
    internal_const_iterator_t iter,
    NODE_KEY pivot,
    paddr_t addr,
    FixedKVNode<NODE_KEY>* nextent) {
    LOG_PREFIX(FixedKVInternalNode::replace);
    SUBTRACE(seastore_fixedkv_tree, "trans.{}, pos {}, old key {}, key {}, {}",
      this->pending_for_transaction,
      iter.get_offset(),
      iter.get_key(),
      pivot,
      *nextent);
    this->update_child_ptr(iter, nextent);
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
    this->split_child_ptrs(*left, *right);
    auto pivot = this->split_into(*left, *right);
    left->range = left->get_meta();
    right->range = right->get_meta();
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
    replacement->merge_child_ptrs(*this, *right);
    replacement->merge_from(*this, *right->template cast<node_type_t>());
    replacement->range = replacement->get_meta();
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

    auto pivot = this->balance_into_new_nodes(
      *this,
      right,
      prefer_left,
      *replacement_left,
      *replacement_right);
    this->balance_child_ptrs(
      *this,
      right,
      prefer_left,
      *replacement_left,
      *replacement_right);

    replacement_left->range = replacement_left->get_meta();
    replacement_right->range = replacement_right->get_meta();
    return std::make_tuple(
      replacement_left,
      replacement_right,
      pivot);
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
  void resolve_relative_addrs(paddr_t base)
  {
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

  std::ostream &_print_detail(std::ostream &out) const
  {
    out << ", size=" << this->get_size()
	<< ", meta=" << this->get_meta()
	<< ", my_tracker=" << (void*)this->my_tracker;
    if (this->my_tracker) {
      out << ", my_tracker->parent=" << (void*)this->my_tracker->get_parent().get();
    }
    return out << ", root_block=" << (void*)this->root_block.get();
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
  typename node_type_t,
  bool has_children>
struct FixedKVLeafNode
  : FixedKVNode<NODE_KEY>,
    common::FixedKVNodeLayout<
      CAPACITY,
      fixed_kv_node_meta_t<NODE_KEY>,
      fixed_kv_node_meta_le_t<NODE_KEY_LE>,
      NODE_KEY, NODE_KEY_LE,
      VAL, VAL_LE> {
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
    node_type_t,
    has_children>;
  using base_t = FixedKVNode<NODE_KEY>;
  FixedKVLeafNode(ceph::bufferptr &&ptr)
    : FixedKVNode<NODE_KEY>(has_children ? CAPACITY : 0, std::move(ptr)),
      node_layout_t(this->get_bptr().c_str()) {}
  FixedKVLeafNode(const FixedKVLeafNode &rhs)
    : FixedKVNode<NODE_KEY>(rhs),
      node_layout_t(this->get_bptr().c_str()) {}

  static constexpr bool do_has_children = has_children;

  bool have_children() const final {
    return do_has_children;
  }

  bool is_leaf_and_has_children() const final {
    return has_children;
  }

  uint16_t get_node_split_pivot() final {
    return this->get_split_pivot().get_offset();
  }

  // children are considered stable if any of the following case is true:
  // 1. The child extent is absent in cache
  // 2. The child extent is stable
  //
  // For reserved mappings, the return values are undefined.
  bool is_child_stable(op_context_t<NODE_KEY> c, uint16_t pos) const final {
    return _is_child_stable(c, pos);
  }
  bool is_child_data_stable(op_context_t<NODE_KEY> c, uint16_t pos) const final {
    return _is_child_stable(c, pos, true);
  }

  bool _is_child_stable(op_context_t<NODE_KEY> c, uint16_t pos, bool data_only = false) const {
    auto child = this->children[pos];
    if (is_reserved_ptr(child)) {
      return true;
    } else if (is_valid_child_ptr(child)) {
      ceph_assert(child->is_logical());
      ceph_assert(
	child->is_pending_in_trans(c.trans.get_trans_id())
	|| this->is_stable_written());
      if (data_only) {
	return c.cache.is_viewable_extent_data_stable(c.trans, child);
      } else {
	return c.cache.is_viewable_extent_stable(c.trans, child);
      }
    } else if (this->is_pending()) {
      auto key = this->iter_idx(pos).get_key();
      auto &sparent = this->get_stable_for_key(key);
      auto spos = sparent.lower_bound_offset(key);
      auto child = sparent.children[spos];
      if (is_valid_child_ptr(child)) {
	ceph_assert(child->is_logical());
	if (data_only) {
	  return c.cache.is_viewable_extent_data_stable(c.trans, child);
	} else {
	  return c.cache.is_viewable_extent_stable(c.trans, child);
	}
      } else {
	return true;
      }
    } else {
      return true;
    }
  }

  bool validate_stable_children() override {
    return true;
  }

  virtual ~FixedKVLeafNode() {
    if (this->is_valid() && !this->is_pending()) {
      if (this->range.is_root()) {
	ceph_assert(this->root_block);
	unlink_phy_tree_root_node<NODE_KEY>(this->root_block);
      } else {
	ceph_assert(this->is_parent_valid());
	auto parent = this->template get_parent_node<FixedKVNode<NODE_KEY>>();
	auto off = parent->lower_bound_offset(this->get_meta().begin);
	assert(parent->get_key_from_idx(off) == this->get_meta().begin);
	assert(parent->children[off] == this);
	parent->children[off] = nullptr;
      }
    }
  }

  void prepare_commit() final {
    if constexpr (has_children) {
      if (this->is_initial_pending()) {
	if (this->is_rewrite()) {
	  this->set_children_from_prior_instance();
	}
	this->copy_children_from_stable_sources(
	  [this](base_t &node, uint16_t pos) {
	    ceph_assert(node.get_type() == this->get_type());
	    auto &n = static_cast<this_type_t&>(node);
	    return n.iter_idx(pos);
	  }
	);
	if (this->is_rewrite()) {
	  this->reset_prior_instance();
	} else {
	  this->adjust_ptracker_for_children();
	}
	assert(this->validate_stable_children());
	this->copy_sources.clear();
      }
    }
    assert(this->is_initial_pending()
      ? this->copy_sources.empty():
      true);
  }

  void on_replace_prior() final {
    ceph_assert(!this->is_rewrite());
    if constexpr (has_children) {
      this->set_children_from_prior_instance();
      auto &prior = (this_type_t&)(*this->get_prior_instance());
      auto copied = this->copy_children_from_stable_source(
	prior,
	prior.begin(),
	prior.end(),
	this->begin());
      ceph_assert(copied <= get_node_size());
      assert(this->validate_stable_children());
      this->set_parent_tracker_from_prior_instance();
    } else {
      this->set_parent_tracker_from_prior_instance();
    }
  }

  uint16_t lower_bound_offset(NODE_KEY key) const final {
    return this->lower_bound(key).get_offset();
  }

  uint16_t upper_bound_offset(NODE_KEY key) const final {
    return this->upper_bound(key).get_offset();
  }

  NODE_KEY get_key_from_idx(uint16_t idx) const final {
    return this->iter_idx(idx).get_key();
  }

  fixed_kv_node_meta_t<NODE_KEY> get_node_meta() const {
    return this->get_meta();
  }

  uint16_t get_node_size() const final {
    return this->get_size();
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
    return CachedExtentRef(new node_type_t(*this));
  };

  virtual void update(
    internal_const_iterator_t iter,
    VAL val,
    LogicalCachedExtent* nextent) = 0;
  virtual internal_const_iterator_t insert(
    internal_const_iterator_t iter,
    NODE_KEY addr,
    VAL val,
    LogicalCachedExtent* nextent) = 0;
  virtual void remove(internal_const_iterator_t iter) = 0;

  std::tuple<Ref, Ref, NODE_KEY>
  make_split_children(op_context_t<NODE_KEY> c) {
    auto left = c.cache.template alloc_new_non_data_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    auto right = c.cache.template alloc_new_non_data_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    if constexpr (has_children) {
      this->split_child_ptrs(*left, *right);
    }
    auto pivot = this->split_into(*left, *right);
    left->range = left->get_meta();
    right->range = right->get_meta();
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
    if constexpr (has_children) {
      replacement->merge_child_ptrs(*this, *right);
    }
    replacement->merge_from(*this, *right->template cast<node_type_t>());
    replacement->range = replacement->get_meta();
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

    auto pivot = this->balance_into_new_nodes(
      *this,
      right,
      prefer_left,
      *replacement_left,
      *replacement_right);
    if constexpr (has_children) {
      this->balance_child_ptrs(
	*this,
	right,
	prefer_left,
	*replacement_left,
	*replacement_right);
    }

    replacement_left->range = replacement_left->get_meta();
    replacement_right->range = replacement_right->get_meta();
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

  std::ostream &_print_detail(std::ostream &out) const
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
