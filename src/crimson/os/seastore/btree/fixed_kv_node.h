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

  enum op_t : uint8_t {
    NONE = 0,
    INSERT,
    REMOVE,
    UPDATE,
    NUM_OPS
  };

  // this structure is for efficient merge of child poiner arrays
  struct pending_child_tracker_t
    : public boost::intrusive::set_base_hook<
	      boost::intrusive::link_mode<
		boost::intrusive::auto_unlink>>
  {
    ChildableCachedExtent* child = nullptr;
    uint16_t pos = 0;
    node_key_t key;
    op_t op = NONE;

    pending_child_tracker_t(
      ChildableCachedExtent* child,
      uint16_t pos,
      node_key_t key,
      op_t op)
      : child(child),
	pos(pos),
	key(key),
	op(op)
    {}

    pending_child_tracker_t(const pending_child_tracker_t &other)
      : child(other.child),
	pos(other.pos),
	key(other.key),
	op(other.op)
    {}

    friend bool operator<(
      const pending_child_tracker_t &left,
      const pending_child_tracker_t &right) {
      return left.key < right.key;
    }
    friend bool operator>(
      const pending_child_tracker_t &left,
      const pending_child_tracker_t &right) {
      return left.key > right.key;
    }
    friend bool operator==(
      const pending_child_tracker_t &left,
      const pending_child_tracker_t &right) {
      return left.key == right.key;
    }

    struct cmp_t {
      bool operator()(
	const pending_child_tracker_t &left,
	const node_key_t &right) const {
	return left.key < right;
      }
      bool operator()(
	const node_key_t &left,
	const pending_child_tracker_t &right) const {
	return left < right.key;
      }
    };
  };

  using FixedKVNodeRef = TCachedExtentRef<FixedKVNode>;
  using pending_children_set_t =
    boost::intrusive::set<
      pending_child_tracker_t,
      boost::intrusive::constant_time_size<false>>;

  fixed_kv_node_meta_t<node_key_t> range;

  /*
   *
   * mutate_state_t
   *
   * used to cache all new pending children which will be merged
   * into "stable_children" when transactions commit. All the pending
   * children are sorted in the pending_children_set by their keys.
   *
   * The methods "pending_update", "pending_insert", "pending_remove"
   * and "pending_replace" are used to update/insert/remove/replace
   * pending children. When mutating the mutate_state, these methods
   * will emulate position shifts that happen on the children at the
   * back of the set.
   *
   * For example:
   * 1. pending children A is position at pos 10;
   * 2. pending children B is inserted at pos 5;
   * 3. pending children A's pos is shifted to 11
   *
   * With this approach, all pending children would be at the correct
   * position when merging them into stable_children.
   *
   */
  struct mutate_state_t {
    std::list<pending_child_tracker_t> mutates;
    pending_children_set_t pending_children; // pending new children

    mutate_state_t() = default;
    mutate_state_t(mutate_state_t &&other)
      : mutates(std::move(other.mutates)),
	pending_children(std::move(other.pending_children))
    {}

    mutate_state_t& operator=(mutate_state_t &&other) {
      mutates = std::move(other.mutates);
      pending_children = std::move(other.pending_children);
      return *this;
    }

    bool empty() {
      ceph_assert(mutates.empty() == pending_children.empty());
      return mutates.empty();
    }

    void clear() {
      mutates.clear();
      pending_children.clear();
    }

    template <typename iter_t>
    void pending_update(
      iter_t iter,
      ChildableCachedExtent* nextent)
    {
      auto &ntracker = this->mutates.emplace_back(
	nextent,
	iter.get_offset(),
	iter.get_key(),
	op_t::UPDATE);
      auto [it, inserted] = this->pending_children.insert(ntracker);
      if (!inserted) {
	assert(it->op == op_t::INSERT || it->op == op_t::UPDATE);
	it->child = nextent;
      }
    }

    template <typename iter_t>
    void pending_insert(
      iter_t iter,
      node_key_t key,
      ChildableCachedExtent* nextent)
    {
      auto &ntracker = this->mutates.emplace_back(
	nextent,
	iter.get_offset(),
	key,
	op_t::INSERT);
      auto [it, inserted] = this->pending_children.insert(ntracker);
      if (!inserted) {
	assert(it->op == op_t::REMOVE);
	ntracker.op = op_t::UPDATE;
	this->pending_children.replace_node(it, ntracker);
	it = pending_children_set_t::s_iterator_to(ntracker);
      }

      for (it++; it != this->pending_children.end(); it++) {
	it->pos++;
      }
    }

    template <typename iter_t>
    void pending_remove(iter_t iter) {
      auto &ntracker = this->mutates.emplace_back(
	nullptr,
	iter.get_offset(),
	iter.get_key(),
	op_t::REMOVE);
      auto [it, inserted] = this->pending_children.insert(ntracker);

      if (!inserted) {
	assert(it->op != op_t::REMOVE);
	if (it->op == op_t::UPDATE) {
	  this->pending_children.replace_node(it, ntracker);
	  it = pending_children_set_t::s_iterator_to(ntracker);
	}
      }

      auto it2 = it;
      for (it2++; it2 != this->pending_children.end(); it2++) {
	it2->pos--;
      }

      if (it->op == op_t::INSERT) {
	assert(!inserted);
	this->pending_children.erase(it);
      }
    }

    template <typename iter_t>
    void pending_replace(
      iter_t iter,
      node_key_t key,
      ChildableCachedExtent* nextent)
    {
      pending_remove(iter);
      pending_insert(iter, key, nextent);
    }
  } mutate_state;

  void mutate_push_back(
    typename pending_children_set_t::iterator begin,
    typename pending_children_set_t::iterator end,
    int16_t pos_shift) {
    for (auto it = begin; it != end; it++) {
      it->pos -= pos_shift;
      auto &tracker = mutate_state.mutates.emplace_back(*it);
      auto [it2, inserted] = mutate_state.pending_children.insert(tracker);
      ceph_assert(inserted);
      if (tracker.op != op_t::REMOVE) {
	if (tracker.child) {
	  set_child_ptracker(tracker.child);
	}
      }
    }
  }

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

  std::vector<ChildableCachedExtent*> stable_children;
  std::set<FixedKVNodeRef, copy_source_cmp_t> copy_sources;
  uint16_t capacity = 0;
  parent_tracker_t* my_tracker = nullptr;
  RootBlockRef root_block;

  bool is_linked() {
    assert(!(bool)parent_tracker || !(bool)root_block);
    return (bool)parent_tracker || (bool)root_block;
  }

  FixedKVNode(uint16_t capacity, ceph::bufferptr &&ptr)
    : ChildableCachedExtent(std::move(ptr)),
      stable_children(capacity, nullptr),
      capacity(capacity) {}
  FixedKVNode(const FixedKVNode &rhs)
    : ChildableCachedExtent(rhs),
      range(rhs.range),
      stable_children(0),
      capacity(rhs.capacity) {}

  virtual fixed_kv_node_meta_t<node_key_t> get_node_meta() const = 0;
  virtual uint16_t get_node_size() const = 0;

  virtual ~FixedKVNode() = default;
  virtual node_key_t get_key_from_idx(uint16_t idx) const = 0;

  FixedKVNode& get_stable_for_key(node_key_t key) {
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
    ceph_assert(dest.is_pending());
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


  void link_child(ChildableCachedExtent* child, uint16_t pos) {
    assert(pos < get_node_size());
    assert(child);
    ceph_assert(!is_pending());
    ceph_assert(child->is_valid() && !child->is_pending());
    assert(!stable_children[pos]);
    stable_children[pos] = child;
    set_child_ptracker(child);
  }

  virtual child_pos_t get_logical_child(
    Transaction &t,
    uint16_t pos) = 0;

  template <typename iter_t>
  child_pos_t get_child(Transaction &t, iter_t iter) {
    if (!is_pending()) {
      auto pos = iter.get_offset();
      auto child = stable_children[pos];
      if (child) {
	return child_pos_t(child->get_transactional_view(t));
      } else {
	return child_pos_t(this, pos);
      }
    } else {
      auto key = iter.get_key();
      auto it = mutate_state.pending_children.find(
	key,
	typename pending_child_tracker_t::cmp_t());
      if (it != mutate_state.pending_children.end()) {
	return child_pos_t(it->child);
      } else {
	auto &sparent = get_stable_for_key(key);
	auto spos = sparent.child_pos_for_key(key);
	auto child = sparent.stable_children[spos];
	if (child) {
	  return child_pos_t(child->get_transactional_view(t));
	} else {
	  return child_pos_t(&sparent, spos);
	}
      }
    }
  }

  void split_mutate_state(
    FixedKVNode &left,
    FixedKVNode &right)
  {
    assert(!left.my_tracker);
    assert(!right.my_tracker);
    ceph_assert(left.mutate_state.empty() && right.mutate_state.empty());
    push_copy_sources(left, *this);
    push_copy_sources(right, *this);
    if (is_pending()) {
      uint16_t pivot = get_node_size() / 2;
      auto key = get_key_from_idx(pivot);
      auto mid_it = mutate_state.pending_children.lower_bound(
	key, typename pending_child_tracker_t::cmp_t());
      my_tracker = nullptr;
      left.mutate_push_back(
	mutate_state.pending_children.begin(),
	mid_it,
	0);

      right.mutate_push_back(
	mid_it,
	mutate_state.pending_children.end(),
	pivot);
    }
  }

  void merge_mutate_state(
    FixedKVNode &left,
    FixedKVNode &right)
  {
    ceph_assert(!my_tracker);
    push_copy_sources(*this, left);
    push_copy_sources(*this, right);

    if (left.is_pending()) {
      left.my_tracker = nullptr;
      mutate_push_back(
	left.mutate_state.pending_children.begin(),
	left.mutate_state.pending_children.end(),
	0);
    }

    if (right.is_pending()) {
      right.my_tracker = nullptr;
      mutate_push_back(
	right.mutate_state.pending_children.begin(),
	right.mutate_state.pending_children.end(),
	-left.get_node_size());
    }
  }

  static void balance_mutate_state(
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
	left.my_tracker = nullptr;
	auto key = left.get_key_from_idx(pivot_idx);
	auto mid_it = left.mutate_state.pending_children.lower_bound(
	  key, typename pending_child_tracker_t::cmp_t());
	replacement_left.mutate_push_back(
	  left.mutate_state.pending_children.begin(),
	  mid_it,
	  0);

	replacement_right.mutate_push_back(
	  mid_it,
	  left.mutate_state.pending_children.end(),
	  pivot_idx);
      }

      // deal with right
      push_copy_sources(replacement_right, right);
      if (right.is_pending()) {
	right.my_tracker= nullptr;
	replacement_right.mutate_push_back(
	  right.mutate_state.pending_children.begin(),
	  right.mutate_state.pending_children.end(),
	  -(l_size - pivot_idx));
      }
    } else {
      // deal with left
      push_copy_sources(replacement_left, left);
      if (left.is_pending()) {
	left.my_tracker = nullptr;
	replacement_left.mutate_push_back(
	  left.mutate_state.pending_children.begin(),
	  left.mutate_state.pending_children.end(),
	  0);
      }

      // deal with right
      push_copy_sources(replacement_left, right);
      push_copy_sources(replacement_right, right);
      if (right.is_pending()) {
	right.my_tracker= nullptr;
	auto key = right.get_key_from_idx(pivot_idx - l_size);
	auto mid_it = right.mutate_state.pending_children.lower_bound(
	  key, typename pending_child_tracker_t::cmp_t());
	replacement_left.mutate_push_back(
	  right.mutate_state.pending_children.begin(),
	  mid_it,
	  -l_size);

	replacement_right.mutate_push_back(
	  mid_it,
	  right.mutate_state.pending_children.end(),
	  pivot_idx - l_size);
      }
    }
  }

  int16_t copy_from_src_and_apply_mutates(
    typename pending_children_set_t::iterator begin,
    typename pending_children_set_t::iterator end,
    FixedKVNode &prior,
    uint16_t prior_base_pos,
    uint16_t base_pos)
  {
    LOG_PREFIX(FixedKVNode::copy_from_src_and_apply_mutates);
    auto src = prior.stable_children.data() + prior_base_pos;
    auto next_pos = base_pos;
    auto dest = stable_children.data() + base_pos;
    uint16_t copied = 0;
    uint16_t old_copied = 0;
#ifndef NDEBUG
    uint16_t last_pos = std::numeric_limits<uint16_t>::max();
#endif
    for (auto it = begin; it != end; it++) {
      auto &child_tracker = *it;
      int16_t count = child_tracker.pos - next_pos;
      assert(count >= 0);
      assert(last_pos == std::numeric_limits<uint16_t>::max()
	|| last_pos <= child_tracker.pos);
#ifndef NDEBUG
      last_pos = child_tracker.pos;
#endif
      std::memmove(dest, src, count * sizeof(CachedExtent*));
      copied += count;
      old_copied += count;
      switch (child_tracker.op) {
      case op_t::INSERT:
	{
	  SUBTRACE(seastore_fixedkv_tree,
	    "trans.{}, insert, pos {}, key {}, child {}",
	    this->pending_for_transaction,
	    child_tracker.pos,
	    child_tracker.key,
	    (void*)child_tracker.child);
	  stable_children[child_tracker.pos] = child_tracker.child;
	  src += count;
	  next_pos += count + 1;
	  dest += count + 1;
	  copied++;
	}
	break;
      case op_t::UPDATE:
	{
	  SUBTRACE(seastore_fixedkv_tree,
	    "trans.{}, update, pos {}, key{}, child {}",
	    this->pending_for_transaction,
	    child_tracker.pos,
	    child_tracker.key,
	    (void*)child_tracker.child);
	  stable_children[child_tracker.pos] = child_tracker.child;
	  src += count + 1;
	  next_pos += count + 1;
	  dest += count + 1;
	  copied++;
	  old_copied++;
	}
	break;
      case op_t::REMOVE:
	SUBTRACE(seastore_fixedkv_tree, "trans.{}, remove, pos {}, key {}",
	  this->pending_for_transaction,
	  child_tracker.pos,
	  child_tracker.key);
	src += count + 1;
	next_pos += count;
	dest += count;
	old_copied++;
	break;
      default:
	ceph_abort("impossible");
      }
    }
    assert(old_copied <= prior.get_node_size());
    auto size = get_node_size();
    assert(size - base_pos >= copied);
    int16_t count = std::min(
      prior.get_node_size() - prior_base_pos - old_copied,
      size - base_pos - copied);
    std::memmove(dest, src, count * sizeof(CachedExtent*));
    copied += count;
    return copied;
  }

  void set_parent_tracker() {
    assert(is_mutation_pending());
    auto &prior = (FixedKVNode&)(*get_prior_instance());
    if (range.is_root()) {
      ceph_assert(prior.root_block);
      ceph_assert(pending_for_transaction);
      root_block = (RootBlock*)prior.root_block->get_transactional_view(
	pending_for_transaction);
      set_phy_tree_root_node<true, node_key_t>(root_block, this);
      return;
    }
    ceph_assert(!root_block);
    parent_tracker = prior.parent_tracker;
    assert(parent_tracker->get_parent<FixedKVNode>());
    auto parent = parent_tracker->get_parent<FixedKVNode>();
    assert(parent->is_valid());
    //TODO: can this search be avoided?
    auto off = parent->lower_bound_offset(get_node_meta().begin);
    assert(parent->get_key_from_idx(off) == get_node_meta().begin);
    parent->stable_children[off] = this;
  }

  bool empty_stable_children() {
    for (auto it = stable_children.begin();
	it != stable_children.begin() + get_node_size();
	it++) {
      if (*it != nullptr) {
	return false;
      }
    }
    return true;
  }

  void take_prior_tracker() {
    assert(get_prior_instance());
    auto &prior = (FixedKVNode&)(*get_prior_instance());

    if (prior.my_tracker) {
      prior.my_tracker->reset_parent(this);
      my_tracker = prior.my_tracker;
      adjust_ptracker_for_pending_children();
    }
    assert(my_tracker || prior.empty_stable_children());
  }

  void adjust_ptracker_for_pending_children() {
    for (auto &tracker : mutate_state.pending_children) {
      if (tracker.op == op_t::REMOVE) {
	continue;
      }

      auto c = (FixedKVNode*)tracker.child;
      assert(c || get_node_meta().depth == 1);
      if (c) {
	set_child_ptracker(c);
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
  virtual uint16_t child_pos_for_key(node_key_t) const = 0;

  virtual bool validate_stable_children() = 0;

  void copy_from_srcs_and_apply_mutates() {
    int16_t copied = 0;

    // copy children pointers from "copy_sources"
    auto begin = mutate_state.pending_children.begin();
    auto end = begin;
    if (!copy_sources.empty()) {
      auto it = --copy_sources.upper_bound(get_node_meta().begin);
      auto &cs = *it;
      uint16_t prior_base_pos = cs->lower_bound_offset(
	get_node_meta().begin);
      if (prior_base_pos == cs->get_node_size()) {
	it++;
	prior_base_pos = 0;
      }
      for (; it != copy_sources.end(); it++) {
	auto &copy_source = *it;
	end = mutate_state.pending_children.lower_bound(
	  copy_source->get_node_meta().end,
	  typename pending_child_tracker_t::cmp_t());
	copied += copy_from_src_and_apply_mutates(
	  begin,
	  end,
	  *copy_source,
	  prior_base_pos,
	  copied);
	prior_base_pos = 0;
	begin = end;
	ceph_assert(copied <= get_node_size());
	if (copied == get_node_size()) {
	  break;
	}
      }
    }
    if (begin != mutate_state.pending_children.end()) {
      uint16_t next_pos = begin->pos;
      for (; begin != mutate_state.pending_children.end(); begin++) {
	auto &tracker = *begin;
	if (tracker.op != op_t::REMOVE) {
	  assert(next_pos++ == tracker.pos);
	  stable_children[tracker.pos] = tracker.child;
	  copied++;
	}
      }
    }
    assert(copied == get_node_size());
  }

  void on_invalidated(Transaction &t) final {
    if (child_trans_view_hook.is_linked()) {
      child_trans_view_hook.unlink();
    }
    parent_tracker.reset();
  }

  virtual void adjust_ptracker_for_stable_children() = 0;

  bool is_rewrite_from_mutation_pending() {
    return is_initial_pending() && get_prior_instance();
  }

  virtual void on_fixed_kv_node_initial_write() = 0;
  void on_initial_write() final {
    // All in-memory relative addrs are necessarily block-relative
    resolve_relative_addrs(get_paddr());
    if (range.is_root()) {
      parent_tracker.reset();
    }
    assert(parent_tracker
	? (parent_tracker->get_parent()
	  && parent_tracker->get_parent()->is_valid())
	: true);
    on_fixed_kv_node_initial_write();
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
  FixedKVInternalNode(ceph::bufferptr &&ptr)
    : FixedKVNode<NODE_KEY>(CAPACITY, std::move(ptr)),
      node_layout_t(this->get_bptr().c_str()) {}
  FixedKVInternalNode(const FixedKVInternalNode &rhs)
    : FixedKVNode<NODE_KEY>(rhs),
      node_layout_t(this->get_bptr().c_str()) {}

  void on_fixed_kv_node_initial_write() final {
    this->copy_from_srcs_and_apply_mutates();
    if (this->is_rewrite_from_mutation_pending()) {
      // this must be a rewritten extent
      this->take_prior_tracker();
      this->reset_prior_instance();
    } else {
      this->adjust_ptracker_for_stable_children();
    }
    assert(this->validate_stable_children());
    this->mutate_state.clear();
    this->copy_sources.clear();
  }

  child_pos_t get_logical_child(Transaction &, uint16_t) final
  {
    ceph_abort("impossible");
    return child_pos_t(nullptr);
  }

  void adjust_ptracker_for_stable_children() final {
    for (auto it = this->stable_children.begin();
	it != this->stable_children.begin() + this->get_size() &&
	  it != this->stable_children.end();
	it++) {
      auto child = *it;
      if (!child) {
	continue;
      }
      this->set_child_ptracker(child);
    }
  }

  bool validate_stable_children() final {
    LOG_PREFIX(FixedKVInternalNode::validate_stable_children);
    if (this->stable_children.empty()) {
      return false;
    }

    for (auto i : *this) {
      auto child = (FixedKVNode<NODE_KEY>*)this->stable_children[i.get_offset()];
      if (child && !child->is_clean_pending()
	  && child->get_node_meta().begin != i.get_key()) {
	SUBERROR(seastore_fixedkv_tree,
	  "stable child not valid: child {}, child meta{}, key {}",
	  *child,
	  child->get_node_meta(),
	  i.get_key());
	ceph_abort();
	return false;
      }
    }

    for (auto &tracker : this->mutate_state.pending_children) {
      if (tracker.op == base_t::op_t::REMOVE)
	continue;

      assert(tracker.child);
      assert(tracker.child == this->stable_children[tracker.pos]);
    }
    return true;
  }

  virtual ~FixedKVInternalNode() {
    if (this->is_valid() && !this->is_pending()) {
      if (this->range.is_root()) {
	ceph_assert(this->root_block);
	set_phy_tree_root_node<false, NODE_KEY>(
	  this->root_block, (FixedKVNode<NODE_KEY>*)nullptr);
      } else {
	ceph_assert(this->parent_tracker);
	assert(this->parent_tracker->get_parent());
	auto parent = this->parent_tracker->template get_parent<
	  FixedKVNode<NODE_KEY>>();
	auto off = parent->lower_bound_offset(this->get_meta().begin);
	assert(parent->get_key_from_idx(off) == this->get_meta().begin);
	assert(parent->stable_children[off] == this);
	parent->stable_children[off] = nullptr;
      }
    }
  }

  uint16_t lower_bound_offset(NODE_KEY key) const final {
    return this->lower_bound(key).get_offset();
  }

  uint16_t child_pos_for_key(NODE_KEY key) const final {
    auto it = this->upper_bound(key);
    assert(it != this->begin());
    --it;
    return it.get_offset();
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

  typename node_layout_t::delta_buffer_t delta_buffer;
  typename node_layout_t::delta_buffer_t *maybe_get_delta_buffer() {
    return this->is_mutation_pending() 
	    ? &delta_buffer : nullptr;
  }

  CachedExtentRef duplicate_for_write(Transaction&) override {
    assert(delta_buffer.empty());
    return CachedExtentRef(new node_type_t(*this));
  };

  void on_replace_prior(Transaction &t) final {
    auto &prior = (FixedKVNode<NODE_KEY>&)(*this->get_prior_instance());
    this->stable_children.resize(this->capacity, nullptr);
    auto copied = this->copy_from_src_and_apply_mutates(
      this->mutate_state.pending_children.begin(),
      this->mutate_state.pending_children.end(),
      prior,
      0,
      0);
    ceph_assert(copied == get_node_size());
    assert(this->validate_stable_children());
    this->set_parent_tracker();
    this->take_prior_tracker();
    this->mutate_state.clear();
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
    this->mutate_state.pending_update(iter, nextent);
    this->set_child_ptracker(nextent);
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
    this->mutate_state.pending_insert(iter, pivot, nextent);
    this->set_child_ptracker(nextent);
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
    this->mutate_state.pending_remove(iter);
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
    this->mutate_state.pending_replace(iter, pivot, nextent);
    this->set_child_ptracker(nextent);
    return this->journal_replace(
      iter,
      pivot,
      this->maybe_generate_relative(addr),
      maybe_get_delta_buffer());
  }

  std::tuple<Ref, Ref, NODE_KEY>
  make_split_children(op_context_t<NODE_KEY> c) {
    auto left = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    auto right = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    this->split_mutate_state(*left, *right);
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
    auto replacement = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    replacement->merge_mutate_state(*this, *right);
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
    auto replacement_left = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    auto replacement_right = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);

    auto pivot = this->balance_into_new_nodes(
      *this,
      right,
      prefer_left,
      *replacement_left,
      *replacement_right);
    this->balance_mutate_state(
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
   * physical node is is_initial_pending() or just is_pending().
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

  std::ostream &print_detail(std::ostream &out) const
  {
    out << ", size=" << this->get_size()
	<< ", meta=" << this->get_meta()
	<< ", parent_tracker=" << (void*)this->parent_tracker.get();
    if (this->parent_tracker) {
      out << ", parent=" << (void*)this->parent_tracker->get_parent().get();
    }
    out << ", my_tracker=" << (void*)this->my_tracker;
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
    this->set_last_committed_crc(this->get_crc32c());
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
  FixedKVLeafNode(ceph::bufferptr &&ptr)
    : FixedKVNode<NODE_KEY>(has_children ? CAPACITY : 0, std::move(ptr)),
      node_layout_t(this->get_bptr().c_str()) {}
  FixedKVLeafNode(const FixedKVLeafNode &rhs)
    : FixedKVNode<NODE_KEY>(rhs),
      node_layout_t(this->get_bptr().c_str()) {}

  static constexpr bool children = has_children;

  child_pos_t get_logical_child(Transaction &t, uint16_t pos) final {
    if (!this->is_pending()) {
      auto child = this->stable_children[pos];
      if (child) {
	return child_pos_t(child->get_transactional_view(t));
      } else {
	return child_pos_t(this, pos);
      }
    } else {
      auto key = this->get_key_from_idx(pos);
      auto it = this->mutate_state.pending_children.find(
	key,
	typename FixedKVNode<NODE_KEY>::pending_child_tracker_t::cmp_t());
      if (it != this->mutate_state.pending_children.end()) {
	ceph_assert(it->pos == pos);
	return child_pos_t(it->child);
      } else {
	auto &sparent = this->get_stable_for_key(key);
	auto spos = sparent.child_pos_for_key(key);
	auto child = sparent.stable_children[spos];
	if (child) {
	  return child_pos_t(child->get_transactional_view(t));
	} else {
	  return child_pos_t(&sparent, spos);
	}
      }
    }
  }

  void adjust_ptracker_for_stable_children() final {
    for (auto it = this->stable_children.begin();
	it != this->stable_children.begin() + this->get_size() &&
	  it != this->stable_children.end();
	it++) {
      auto child = *it;
      if (!child) {
	continue;
      }
      this->set_child_ptracker(child);
    }
  }

  bool validate_stable_children() override {
    return true;
  }

  virtual ~FixedKVLeafNode() {
    if (this->is_valid() && !this->is_pending()) {
      if (this->range.is_root()) {
	ceph_assert(this->root_block);
	set_phy_tree_root_node<false, NODE_KEY>(
	  this->root_block, (FixedKVNode<NODE_KEY>*)nullptr);
      } else {
	ceph_assert(this->parent_tracker);
	assert(this->parent_tracker->get_parent());
	auto parent = this->parent_tracker->template get_parent<
	  FixedKVNode<NODE_KEY>>();
	auto off = parent->lower_bound_offset(this->get_meta().begin);
	assert(parent->get_key_from_idx(off) == this->get_meta().begin);
	assert(parent->stable_children[off] == this);
	parent->stable_children[off] = nullptr;
      }
    }
  }

  void on_fixed_kv_node_initial_write() final {
    if constexpr (has_children) {
      this->copy_from_srcs_and_apply_mutates();
      if (this->is_rewrite_from_mutation_pending()) {
	// this must be a rewritten extent
	this->take_prior_tracker();
	this->reset_prior_instance();
      } else {
	this->adjust_ptracker_for_stable_children();
      }
      assert(this->validate_stable_children());
      this->mutate_state.clear();
      this->copy_sources.clear();
    }
    assert(this->mutate_state.empty());
    assert(this->copy_sources.empty());
  }

  void on_replace_prior(Transaction &t) final {
    if constexpr (has_children) {
      auto &prior = (FixedKVNode<NODE_KEY>&)(*this->get_prior_instance());
      this->stable_children.resize(this->capacity, nullptr);
      auto copied = this->copy_from_src_and_apply_mutates(
	this->mutate_state.pending_children.begin(),
	this->mutate_state.pending_children.end(),
	prior,
	0,
	0);
      ceph_assert(copied == get_node_size());
      assert(this->validate_stable_children());
      this->set_parent_tracker();
      this->take_prior_tracker();
      this->mutate_state.clear();
    } else {
      this->set_parent_tracker();
      assert(this->mutate_state.empty());
    }
  }

  uint16_t lower_bound_offset(NODE_KEY key) const final {
    return this->lower_bound(key).get_offset();
  }

  uint16_t child_pos_for_key(NODE_KEY key) const final {
    return lower_bound_offset(key);
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
    auto left = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    auto right = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    if constexpr (has_children) {
      this->split_mutate_state(*left, *right);
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
    auto replacement = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    if constexpr (has_children) {
      replacement->merge_mutate_state(*this, *right);
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
    auto replacement_left = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    auto replacement_right = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);

    auto pivot = this->balance_into_new_nodes(
      *this,
      right,
      prefer_left,
      *replacement_left,
      *replacement_right);
    if constexpr (has_children) {
      this->balance_mutate_state(
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
    this->set_last_committed_crc(this->get_crc32c());
    this->resolve_relative_addrs(base);
  }

  std::ostream &print_detail(std::ostream &out) const
  {
    out << ", size=" << this->get_size()
	<< ", meta=" << this->get_meta()
	<< ", parent_tracker=" << (void*)this->parent_tracker.get();
    if (this->parent_tracker) {
      out << ", parent=" << (void*)this->parent_tracker->get_parent().get();
    }
    return out;
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
