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
#include "crimson/os/seastore/btree/btree_child_tracker.h"

#include "crimson/os/seastore/btree/btree_range_pin.h"
#include "crimson/os/seastore/btree/fixed_kv_btree.h"

namespace crimson::os::seastore {

/**
 * FixedKVNode
 *
 * Base class enabling recursive lookup between internal and leaf nodes.
 */
template <typename node_key_t>
struct FixedKVNode : CachedExtent {
  using FixedKVNodeRef = TCachedExtentRef<FixedKVNode>;

  btree_range_pin_t<node_key_t> pin;

  // child_tracker_t is referenced by raw pointers in the parent node. Raw pointers
  // being used here is for the sake of the performance of btree modifications. According
  // to the results of our tests, copying(std::memmove) raw pointers is five times
  // faster than copying intrusive pointers(or other types of smart pointers).
  //
  // TODO: probably need to introduce something like a child_tracker_t pool to avoid
  //       the cpu overhead involved by (de)allocating individual child_tracker_t
  //       instances
  //
  // These raw pointers are initialized when:
  // 	1. on_clean_read
  // 	2. new mapping is added
  // and are destroyed when:
  // 	1. the node is valid and evicted out of Cache
  // 	2. the parent and child of the pointer are both
  //	   pending, and the parent got invalidated by
  // 	   transaction reset
  // 	3. if the mapping is removed from the node or replaced by a new one,
  // 	   trackers are destroyed when the transaction is committed
  //
  // NOTE THAT: invalidating a clean/dirty node doesn't necessarily mean these
  // 		raw pointers should be destroyed, as there must be at least one
  // 		pending node that's referencing the corresponding child trackers
  // 		as long as the mapping still exists
  std::vector<child_tracker_t*> child_trackers;
  child_trans_views_t child_trans_views;
  parent_tracker_ref parent_tracker;
  size_t capacity = 0;

  FixedKVNode(size_t capacity, ceph::bufferptr &&ptr)
    : CachedExtent(std::move(ptr)),
      pin(this),
      child_trackers(capacity, nullptr),
      child_trans_views(capacity),
      capacity(capacity)
  {}
  FixedKVNode(const FixedKVNode &rhs)
    : CachedExtent(rhs),
      pin(rhs.pin, this),
      child_trackers(rhs.child_trackers),
      capacity(rhs.capacity)
  {}

  virtual fixed_kv_node_meta_t<node_key_t> get_node_meta() const = 0;

  virtual ~FixedKVNode() = default;

  void on_delta_write(paddr_t record_block_offset) final {
    // All in-memory relative addrs are necessarily record-relative
    assert(get_prior_instance());
    ceph_assert(touched_by);
    pin.take_pin(get_prior_instance()->template cast<FixedKVNode>()->pin);
    resolve_relative_addrs(record_block_offset);
  }

  void on_replace_prior(Transaction &t) final {
    ceph_assert(touched_by == t.get_trans_id());
    if (child_trans_view_hook.is_linked()) {
      // change my parent to point to me
      ceph_assert(parent_tracker);
      auto parent = parent_tracker->parent;
      ceph_assert(parent);
      auto tracker = ((FixedKVNode*)parent)->child_trackers[parent_tracker->pos];
      child_trans_view_hook.unlink();
      tracker->child = weak_from_this();

      auto &parent_child_tvs = ((FixedKVNode*)parent)->child_trans_views;
      auto &tv_map = parent_child_tvs.views_by_transaction[parent_tracker->pos];
#ifndef NDEBUG
      auto it = tv_map->find(touched_by);
      ceph_assert(it != tv_map->end());
      ceph_assert(it->second = this);
      tv_map.reset();
#else
      tv_map.reset();
#endif

      parent_tracker.reset();
    }
    this->child_trans_views.views_by_transaction.resize(capacity, std::nullopt);
  }

  void on_invalidated(Transaction &t, bool transaction_reset = false) final {
    if (child_trans_view_hook.is_linked()) {
      ceph_assert(parent_tracker);
      child_trans_view_hook.unlink();

      auto parent = parent_tracker->parent;
      ceph_assert(parent);
      auto &parent_child_tvs = ((FixedKVNode*)parent)->child_trans_views;
      auto &tv_map = parent_child_tvs.views_by_transaction[parent_tracker->pos];
      // if the invalidation is caused by this extent's prior instance being
      // replace, tv_map would be empty
      if (tv_map) {
	auto it = tv_map->find(touched_by);
	if (it != tv_map->end()) {
	  ceph_assert(it->second = this);
	  tv_map->erase(it);
	  if (tv_map->empty()) {
	    tv_map.reset();
	  }
	}
      }
      parent_tracker.reset();
    }
  }

  void on_initial_write() final {
    // All in-memory relative addrs are necessarily block-relative
    resolve_relative_addrs(get_paddr());
  }

  void on_clean_read() final {
    // From initial write of block, relative addrs are necessarily block-relative
    resolve_relative_addrs(get_paddr());
    init_child_trackers();
  }

  virtual void init_child_trackers() = 0;
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
  using base_ref = typename base_t::FixedKVNodeRef;
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

  void init_child_trackers() {
    LOG_PREFIX(FixedKVInternalNode::init_child_trackers);
    for (auto it = this->child_trackers.begin();
	 it != this->child_trackers.begin() + this->get_size();
	 it++) {
      *it = new child_tracker_t();
      SUBTRACE(seastore_fixedkv_tree,
	"init tracker: {}, this: {}", (void*)(*it), *this);
    }
  }

  void link_child(CachedExtent &child, uint64_t pos) {
    auto tracker = this->child_trackers[pos];
    ceph_assert(tracker != nullptr);
    ceph_assert(!tracker->child);
    //ceph_assert((!this->is_pending() && !child.is_pending())
    //  || (child.is_pending() && this->is_pending()));
    tracker->child = child.weak_from_this();
  }

  void new_child(Transaction &t, CachedExtent &child, uint64_t pos) {
    LOG_PREFIX(FixedKVInternalNode::new_child);
    auto &tracker = this->child_trackers[pos];
    assert(tracker != nullptr);
    ceph_assert(child.is_mutation_pending() && this->is_pending());
    t.trackers_to_rm.push_back(tracker);
    tracker = new child_tracker_t(&child);
    t.new_pending_trackers.push_back(tracker);
    SUBTRACET(seastore_fixedkv_tree,
      "new tracker: {}, this: {}, child: {}",
      t, (void*)tracker, *this, child);
  }

  void add_child_trans_view(FixedKVNode<NODE_KEY> &child, uint64_t pos) {
    ceph_assert(pos < this->get_size());
    this->child_trans_views.new_trans_view(child, pos);

    ceph_assert(!this->is_pending() && child.is_mutation_pending());
    ceph_assert(!child.parent_tracker);
    ceph_assert(this->child_trackers[pos]);
    child.parent_tracker = std::make_unique<parent_tracker_t>(this, pos);
  }

  // this method should only be invoked to rewrite extents
  void copy_child_trackers_out(
    Transaction &t,
    FixedKVInternalNode &new_node) {
    LOG_PREFIX(FixedKVInternalNode::copy_child_trackers_out);
    SUBTRACET(seastore_fixedkv_tree,
      "coping {} trackers from {} to {}",
      t, this->get_size(), *this, new_node);

    ceph_assert(this->get_type() == new_node.get_type());
    auto data = this->child_trackers.data();
    auto n_data = new_node.child_trackers.data();
#ifndef NDEBUG
    for (int i = 0; i < this->get_size(); i++) {
      assert(this->child_trackers[i]);
    }
#endif
    std::memmove(n_data, data, this->get_size() * sizeof(child_tracker_t*));
    if (!this->is_pending()) {
      auto children = this->child_trans_views
	.template remove_trans_view<base_t>(t);
      for (auto [child, pos] : children) {
	auto &tracker = new_node.child_trackers[pos];
	tracker = new child_tracker_t(child);
	((base_t*)child)->parent_tracker.reset();
	t.new_pending_trackers.push_back(tracker);
	t.trackers_to_rm.push_back(this->child_trackers[pos]);
	SUBTRACET(seastore_fixedkv_tree,
	  "new tracker: {}, this: {}, child: {}",
	  t, (void*)new_node.child_trackers[pos], new_node, *child);
      }
    }
  }

  void split_child_trackers(
    Transaction &t,
    node_type_t &left,
    node_type_t &right)
  {
    ceph_assert(left.get_size() > 0);
    ceph_assert(right.get_size() > 0);
    LOG_PREFIX(FixedKVNode::split_child_trackers);
    size_t pivot = this->get_size() / 2;
    child_tracker_t** l_data = left.child_trackers.data();
    child_tracker_t** r_data = right.child_trackers.data();
    child_tracker_t** data = this->child_trackers.data();
    size_t l_size = pivot;
    size_t r_size = this->get_size() - pivot;

    std::memmove(l_data, data, sizeof(child_tracker_t*) * l_size);
    std::memmove(r_data, data + pivot, sizeof(child_tracker_t*) * r_size);

    if (!this->is_pending()) {
      auto children = this->child_trans_views
	.template remove_trans_view<base_t>(t);
      for (auto [child, pos] : children) {
	if (pos < pivot) {
	  auto &tracker = left.child_trackers[pos];
	  tracker = new child_tracker_t(child);
	  t.new_pending_trackers.push_back(tracker);
	  SUBTRACET(seastore_fixedkv_tree,
	    "new tracker: {}, this: {}, child: {}",
	    t, (void*)tracker, left, *child);
	} else {
	  auto &tracker = right.child_trackers[pos - pivot];
	  tracker = new child_tracker_t(child);
	  t.new_pending_trackers.push_back(tracker);
	  SUBTRACET(seastore_fixedkv_tree,
	    "new tracker: {}, this: {}, child: {}",
	    t, (void*)tracker, right, *child);
	}
	t.trackers_to_rm.push_back(this->child_trackers[pos]);
	((base_t*)child)->parent_tracker.reset();
      }
    }

    SUBTRACET(seastore_fixedkv_tree,
      "l_size: {}, {}; r_size: {}, {}",
      t, l_size, left, r_size, right);
  }

  template <typename T1, typename T2>
  void merge_child_trackers(
    Transaction &t,
    T1 &left,
    T2 &right)
  {
    LOG_PREFIX(FixedKVInternalNode::merge_child_trackers);
    static_assert(std::is_base_of_v<FixedKVNode<NODE_KEY>, T1>);
    static_assert(std::is_base_of_v<FixedKVNode<NODE_KEY>, T2>);
    auto l_data = left.child_trackers.data();
    auto r_data = right.child_trackers.data();
    auto data = this->child_trackers.data();
    auto l_size = left.get_size();
    auto r_size = right.get_size();
    ceph_assert(l_size + r_size <= CAPACITY);

    std::memmove(data, l_data, l_size * sizeof(child_tracker_t*));
    std::memmove(data + l_size, r_data, r_size * sizeof(child_tracker_t*));

    if (!left.is_pending()) {
      auto children = left.child_trans_views
	.template remove_trans_view<base_t>(t);
      for (auto [child, pos] : children) {
	auto &tracker = this->child_trackers[pos];
	tracker = new child_tracker_t(child);
	((base_t*)child)->parent_tracker.reset();
	t.new_pending_trackers.push_back(tracker);
	t.trackers_to_rm.push_back(left.child_trackers[pos]);
	SUBTRACET(seastore_fixedkv_tree,
	  "new tracker: {}, this: {}, child: {}",
	  t, (void*)this->child_trackers[pos], *this, *child);
      }
    }

    if (!right.is_pending()) {
      auto children = right.child_trans_views
	.template remove_trans_view<base_t>(t);
      for (auto [child, pos] : children) {
	auto &tracker = this->child_trackers[l_size + pos];
	tracker = new child_tracker_t(child);
	((base_t*)child)->parent_tracker.reset();
	t.new_pending_trackers.push_back(tracker);
	t.trackers_to_rm.push_back(right.child_trackers[pos]);
	SUBTRACET(seastore_fixedkv_tree,
	  "new tracker: {}, this: {}, child: {}",
	  t, (void*)this->child_trackers[l_size + pos], *this, *child);
      }
    }
  }

  template <typename T1, typename T2>
  static void balance_child_trackers(
    Transaction &t,
    T1 &left,
    T2 &right,
    bool prefer_left,
    T2 &replacement_left,
    T2 &replacement_right)
  {
    ceph_assert(replacement_left.get_size() > 0);
    ceph_assert(replacement_right.get_size() > 0);
    static_assert(std::is_base_of_v<FixedKVNode<NODE_KEY>, T1>);
    static_assert(std::is_base_of_v<FixedKVNode<NODE_KEY>, T2>);
    size_t l_size = left.get_size();
    size_t r_size = right.get_size();
    size_t total = l_size + r_size;
    size_t pivot_idx = (l_size + r_size) / 2;
    if (total % 2 && prefer_left) {
      pivot_idx++;
    }
    LOG_PREFIX(FixedKVNode::balance_child_trackers);
    SUBTRACE(seastore_fixedkv_tree,
      "l_size: {}, r_size: {}, pivot_idx: {}",
      l_size,
      r_size,
      pivot_idx);

    auto l_data = left.child_trackers.data();
    auto r_data = right.child_trackers.data();
    auto rep_l_data = replacement_left.child_trackers.data();
    auto rep_r_data = replacement_right.child_trackers.data();

    if (pivot_idx < l_size) {
      std::memmove(rep_l_data, l_data, pivot_idx * sizeof(child_tracker_t*));
      std::memmove(rep_r_data, l_data + pivot_idx,
	(l_size - pivot_idx) * sizeof(child_tracker_t*));
      std::memmove(
	rep_r_data + (l_size - pivot_idx),
	r_data,
	r_size * sizeof(child_tracker_t*));

      if (!left.is_pending()) {
	auto children = left.child_trans_views
	  .template remove_trans_view<base_t>(t);
	for (auto [child, pos] : children) {
	  if (pos < pivot_idx){
	    auto &tracker = replacement_left.child_trackers[pos];
	    tracker = new child_tracker_t(child);
	    t.new_pending_trackers.push_back(tracker);
	    SUBTRACET(seastore_fixedkv_tree,
	      "new tracker: {}, this: {}, child: {}",
	      t, (void*)replacement_left.child_trackers[pos],
	      replacement_left, *child);
	  } else {
	    auto &tracker = replacement_right.child_trackers[pos - pivot_idx];
	    tracker = new child_tracker_t(child);
	    t.new_pending_trackers.push_back(tracker);
	    SUBTRACET(seastore_fixedkv_tree,
	      "new tracker: {}, this: {}, child: {}",
	      t, (void*)replacement_right.child_trackers[pos - pivot_idx],
	      replacement_right, *child);
	  }
	  t.trackers_to_rm.push_back(left.child_trackers[pos]);
	  ((base_t*)child)->parent_tracker.reset();
	}
      }

      if (!right.is_pending()) {
	auto children = right.child_trans_views
	  .template remove_trans_view<base_t>(t);
	for (auto [child, pos] : children) {
	  auto &tracker = replacement_right.child_trackers[pos + l_size - pivot_idx];
	  tracker = new child_tracker_t(child);
	  t.new_pending_trackers.push_back(tracker);
	  t.trackers_to_rm.push_back(right.child_trackers[pos]);
	  ((base_t*)child)->parent_tracker.reset();
	  SUBTRACET(seastore_fixedkv_tree,
	    "new tracker: {}, this: {}, child: {}",
	    t, (void*)replacement_right.child_trackers[pos + l_size - pivot_idx],
	    replacement_right, *child);
	}
      }
    } else {
      std::memmove(rep_l_data, l_data, l_size * sizeof(child_tracker_t*));
      std::memmove(rep_l_data + l_size, r_data,
	(pivot_idx - l_size) * sizeof(child_tracker_t*));
      std::memmove(rep_r_data, r_data + pivot_idx - l_size,
	(r_size + l_size - pivot_idx) * sizeof(child_tracker_t*));

      if (!left.is_pending()) {
	auto children = left.child_trans_views
	  .template remove_trans_view<base_t>(t);
	for (auto [child, pos] : children) {
	  auto &tracker = replacement_left.child_trackers[pos];
	  tracker = new child_tracker_t(child);
	  t.new_pending_trackers.push_back(tracker);
	  t.trackers_to_rm.push_back(left.child_trackers[pos]);
	  ((base_t*)child)->parent_tracker.reset();
	  SUBTRACET(seastore_fixedkv_tree,
	    "new tracker: {}, this: {}, child: {}",
	    t, (void*)replacement_left.child_trackers[pos],
	    replacement_left, *child);
	}
      }

      if (!right.is_pending()) {
	auto children = right.child_trans_views
	  .template remove_trans_view<base_t>(t);
	for (auto [child, pos] : children) {
	  if (pos < pivot_idx - l_size) {
	    auto &tracker = replacement_left.child_trackers[pos + l_size];
	    tracker = new child_tracker_t(child);
	    t.new_pending_trackers.push_back(tracker);
	    SUBTRACET(seastore_fixedkv_tree,
	      "new tracker: {}, this: {}, child: {}",
	      t, (void*)replacement_left.child_trackers[pos + l_size],
	      replacement_left, *child);
	  } else {
	    auto &tracker = replacement_right.child_trackers[pos + l_size - pivot_idx];
	    tracker = new child_tracker_t(child);
	    t.new_pending_trackers.push_back(tracker);
	    SUBTRACET(seastore_fixedkv_tree,
	      "new tracker: {}, this: {}, child: {}",
	      t, (void*)replacement_right.child_trackers[pos + l_size - pivot_idx],
	      replacement_right, *child);
	  }
	  t.trackers_to_rm.push_back(right.child_trackers[pos]);
	  ((base_t*)child)->parent_tracker.reset();
	}
      }
    }
  }

  virtual ~FixedKVInternalNode() {
    LOG_PREFIX(FixedKVInternalNode::~FixedKVInternalNode);
    if (this->is_valid()) {
      for (auto it = this->child_trackers.begin();
	   it != this->child_trackers.begin() + this->get_size();
	   it++) {
	SUBTRACE(seastore_fixedkv_tree,
	  "delete tracker: {}, this: {}", (void*)*it, (void*)this);
	delete *it;
      }
    }
  }

  fixed_kv_node_meta_t<NODE_KEY> get_node_meta() const {
    return this->get_meta();
  }

  typename node_layout_t::delta_buffer_t delta_buffer;
  typename node_layout_t::delta_buffer_t *maybe_get_delta_buffer() {
    return this->is_mutation_pending() 
	    ? &delta_buffer : nullptr;
  }

  CachedExtentRef duplicate_for_write(Transaction& t) override {
    LOG_PREFIX(FixedKVInternalNode::duplicate_for_write);
    assert(delta_buffer.empty());
    auto ext = new node_type_t(*this);
    auto children = this->child_trans_views.template remove_trans_view<base_t>(t);
    for (auto [child, pos] : children) {
      auto &tracker = ext->child_trackers[pos];
      tracker = new child_tracker_t(child);
      t.new_pending_trackers.push_back(tracker);
      t.trackers_to_rm.push_back(this->child_trackers[pos]);
      ((base_t*)child)->parent_tracker.reset();
      SUBTRACET(seastore_fixedkv_tree,
	"new tracker: {}, this: {}, child: {}",
	t, (void*)ext->child_trackers[pos],
	*ext, *child);
    }
    return ext;
  }

  template <typename T>
  TCachedExtentRef<T> get_child(Transaction &t, uint16_t pos) {
    static_assert(std::is_base_of_v<FixedKVNode<NODE_KEY>, T>);
    ceph_assert(pos < this->get_size());
    if (!this->is_pending()) {
      auto child_trans_view =
	this->child_trans_views.get_child_trans_view(t, pos);
      if (child_trans_view) {
	ceph_assert(child_trans_view->get_type() == T::TYPE);
	return (T*)child_trans_view;
      }
    }

    auto tracker = this->child_trackers[pos];
    assert(tracker);
    return (T*)tracker->child.get();
  }

  void update(
    Transaction &t,
    internal_const_iterator_t iter,
    paddr_t addr,
    CachedExtentRef new_node) {
    ceph_assert(this->child_trans_views.empty());
    ceph_assert(this->is_pending());
    ceph_assert(is_fixed_kv_node(new_node->get_type()));
    assert(this->child_trackers[iter.get_offset()]);
    assert(this->child_trackers[iter.get_offset()]->child);
    LOG_PREFIX(FixedKVInternalNode::update);

    auto &tracker = this->child_trackers[iter.get_offset()];
    auto old_tracker = tracker;
    ceph_assert(tracker);
    t.trackers_to_rm.push_back(tracker);
    tracker = new child_tracker_t(new_node);
    t.new_pending_trackers.push_back(tracker);
    SUBTRACE(seastore_fixedkv_tree,
      "old tracker: {}, new tracker: {}, new extent: {}, this: {}",
      (void*)old_tracker,
      (void*)tracker,
      *new_node,
      *this);

    return this->journal_update(
      iter,
      this->maybe_generate_relative(addr),
      maybe_get_delta_buffer());
  }

  void new_root(Transaction &t, CachedExtentRef &old_root) {
    LOG_PREFIX(FixedKVInternalNode::new_root);
    ceph_assert(this->get_meta().is_root());
    auto &tracker = this->child_trackers[0];
    ceph_assert(tracker == nullptr);
    tracker = new child_tracker_t(old_root);
    t.new_pending_trackers.push_back(tracker);
    SUBTRACE(seastore_fixedkv_tree,
      "new tracker: {}, this: {}, child: {}",
      (void*)tracker, *this, old_root);
  }

  void insert(
    Transaction &t,
    internal_const_iterator_t iter,
    NODE_KEY pivot,
    paddr_t addr,
    CachedExtentRef new_node) {
    LOG_PREFIX(FixedKVInternalNode::insert);
    ceph_assert(this->child_trans_views.empty());
    ceph_assert(this->is_pending());
    ceph_assert(is_fixed_kv_node(new_node->get_type()));

    // move child trackers
    size_t count = sizeof(child_tracker_t*) * (
      this->get_size() - iter.get_offset());
    void* src = this->child_trackers.data() + iter.get_offset();
    void* dest = this->child_trackers.data() + iter.get_offset() + 1;
    std::memmove(dest, src, count);

    auto &tracker = this->child_trackers[iter.get_offset()];
    tracker = new child_tracker_t(new_node);
    t.new_pending_trackers.push_back(tracker);
    SUBTRACE(seastore_fixedkv_tree,
      "new tracker: {}, this: {} new extent: {}",
      (void*)this->child_trackers[iter.get_offset()],
      *this,
      *new_node);

    return this->journal_insert(
      iter,
      pivot,
      this->maybe_generate_relative(addr),
      maybe_get_delta_buffer());
  }

  void remove(Transaction &t, internal_const_iterator_t iter) {
    ceph_assert(this->child_trans_views.empty());
    ceph_assert(this->is_pending());
    assert(this->child_trackers[iter.get_offset()]);
    assert(this->child_trackers[iter.get_offset()]->child);
    LOG_PREFIX(FixedKVInternalNode::remove);

    auto &tracker = this->child_trackers[iter.get_offset()];
    ceph_assert(tracker);
    t.trackers_to_rm.push_back(tracker);
    SUBTRACE(seastore_fixedkv_tree,
      "old tracker: {}, this: {}",
      (void*)t.trackers_to_rm.back(),
      *this);

    // move child trackers
    size_t count = sizeof(child_tracker_t*) * (
      this->get_size() - iter.get_offset() - 1);
    void* src = this->child_trackers.data() + iter.get_offset() + 1;
    void* dest = this->child_trackers.data() + iter.get_offset();
    std::memmove(dest, src, count);
    // remove the entry outside this->get_size()
    this->child_trackers[this->get_size() - 1] = nullptr;

    return this->journal_remove(
      iter,
      maybe_get_delta_buffer());
  }

  void replace(
    Transaction &t,
    internal_const_iterator_t iter,
    NODE_KEY pivot,
    paddr_t addr,
    CachedExtentRef new_node) {
    ceph_assert(this->child_trans_views.empty());
    ceph_assert(this->is_pending());
    assert(this->child_trackers[iter.get_offset()]);
    assert(this->child_trackers[iter.get_offset()]->child);
    LOG_PREFIX(FixedKVInternalNode::remove);

    auto &tracker = this->child_trackers[iter.get_offset()];
    auto old_tracker = tracker;
    ceph_assert(tracker);
    t.trackers_to_rm.push_back(tracker);
    tracker = new child_tracker_t(new_node);
    t.new_pending_trackers.push_back(tracker);
    SUBTRACE(seastore_fixedkv_tree,
      "old tracker: {}, new tracker: {}, this: {} new extent: {}",
      (void*)old_tracker,
      (void*)tracker,
      *this,
      *new_node);

    return this->journal_replace(
      iter,
      pivot,
      this->maybe_generate_relative(addr),
      maybe_get_delta_buffer());
  }

  std::tuple<Ref, Ref, NODE_KEY>
  make_split_children(op_context_t<NODE_KEY> c) {
    auto left = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, 0);
    auto right = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, 0);
    auto pivot = this->split_into(*left, *right);
    this->split_child_trackers(c.trans, *left, *right);
    left->pin.set_range(left->get_meta());
    right->pin.set_range(right->get_meta());
    return std::make_tuple(
      left,
      right,
      pivot);
  }

  Ref make_full_merge(
    op_context_t<NODE_KEY> c,
    Ref &right) {
    auto replacement = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, 0);
    replacement->merge_child_trackers(c.trans, *this, *right);
    replacement->merge_from(*this, *right->template cast<node_type_t>());
    replacement->pin.set_range(replacement->get_meta());
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
      c.trans, node_size, placement_hint_t::HOT, 0);
    auto replacement_right = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, 0);

    auto pivot = this->balance_into_new_nodes(
      *this,
      right,
      prefer_left,
      *replacement_left,
      *replacement_right);
    this->balance_child_trackers(
      c.trans,
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
    return out << ", size=" << this->get_size()
	       << ", meta=" << this->get_meta();
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
    buffer.replay(
      *this,
      [this](auto &iter) {
	LOG_PREFIX(FixedKVInternalNode::apply_delta_and_adjust_crc);
	size_t count = sizeof(child_tracker_t*) * (
	  this->get_size() - iter.get_offset());
	void* src = this->child_trackers.data() + iter.get_offset();
	void* dest = this->child_trackers.data() + iter.get_offset() + 1;
	std::memmove(dest, src, count);
	this->child_trackers[iter.get_offset()] = new child_tracker_t();
	SUBTRACE(seastore_fixedkv_tree, "insert pos {}, tracker: {}",
	  iter.get_offset(), (void*)this->child_trackers[iter.get_offset()]);
      },
      [this](auto &iter) {
	LOG_PREFIX(FixedKVInternalNode::apply_delta_and_adjust_crc);
	SUBTRACE(seastore_fixedkv_tree, "remove pos {}, tracker: {}",
	  iter.get_offset(), (void*)this->child_trackers[iter.get_offset()]);
	size_t count = sizeof(child_tracker_t*) * (
	  this->get_size() - iter.get_offset() - 1);
	void* src = this->child_trackers.data() + iter.get_offset() + 1;
	void* dest = this->child_trackers.data() + iter.get_offset();
	delete this->child_trackers[iter.get_offset()];
	std::memmove(dest, src, count);
      },
      [this](auto &iter) {
	LOG_PREFIX(FixedKVInternalNode::apply_delta_and_adjust_crc);
	auto tracker = this->child_trackers[iter.get_offset()];
	delete tracker;
	this->child_trackers[iter.get_offset()] = new child_tracker_t();
	SUBTRACE(seastore_fixedkv_tree, "update pos {}, old tracker: {}, new: {}",
	  iter.get_offset(), (void*)tracker,
	  (void*)this->child_trackers[iter.get_offset()]);
      });
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
  typename node_type_t>
struct FixedKVLeafNode
  : FixedKVNode<NODE_KEY>,
    common::FixedKVNodeLayout<
      CAPACITY,
      fixed_kv_node_meta_t<NODE_KEY>,
      fixed_kv_node_meta_le_t<NODE_KEY_LE>,
      NODE_KEY, NODE_KEY_LE,
      VAL, VAL_LE> {
  using base_t = FixedKVNode<NODE_KEY>;
  using base_ref = typename base_t::FixedKVNodeRef;
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
    : FixedKVNode<NODE_KEY>(0, std::move(ptr)),
      node_layout_t(this->get_bptr().c_str()) {}
  FixedKVLeafNode(const FixedKVLeafNode &rhs)
    : FixedKVNode<NODE_KEY>(rhs),
      node_layout_t(this->get_bptr().c_str()) {}

  void init_child_trackers() {
    //TODO: noop for now, as we are only dealing with intra-fixed-kv-btree
    //	    node trackers now.
    //	    In the future, when linking logical extents with their parents,
    //	    this method should be implemented.
  }

  template <typename... T>
  FixedKVLeafNode(T&&... t) :
    FixedKVNode<NODE_KEY>(std::forward<T>(t)...),
    node_layout_t(this->get_bptr().c_str()) {}

  virtual ~FixedKVLeafNode() {}

  fixed_kv_node_meta_t<NODE_KEY> get_node_meta() const {
    return this->get_meta();
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
    VAL val) = 0;
  virtual internal_const_iterator_t insert(
    internal_const_iterator_t iter,
    NODE_KEY addr,
    VAL val) = 0;
  virtual void remove(internal_const_iterator_t iter) = 0;

  std::tuple<Ref, Ref, NODE_KEY>
  make_split_children(op_context_t<NODE_KEY> c) {
    auto left = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, 0);
    auto right = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, 0);
    auto pivot = this->split_into(*left, *right);
    left->pin.set_range(left->get_meta());
    right->pin.set_range(right->get_meta());
    return std::make_tuple(
      left,
      right,
      pivot);
  }

  Ref make_full_merge(
    op_context_t<NODE_KEY> c,
    Ref &right) {
    auto replacement = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, 0);
    replacement->merge_from(*this, *right->template cast<node_type_t>());
    replacement->pin.set_range(replacement->get_meta());
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
      c.trans, node_size, placement_hint_t::HOT, 0);
    auto replacement_right = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, 0);

    auto pivot = this->balance_into_new_nodes(
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
    buffer.replay(*this, [](auto&) {}, [](auto&) {}, [](auto&) {});
    this->set_last_committed_crc(this->get_crc32c());
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
