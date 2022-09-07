// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "transaction.h"
#include "crimson/common/interruptible_future.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/backref/backref_tree_node.h"

namespace crimson::interruptible {
template
thread_local interrupt_cond_t<::crimson::os::seastore::TransactionConflictCondition>
interrupt_cond<::crimson::os::seastore::TransactionConflictCondition>;
}

namespace crimson::os::seastore {

void Transaction::invalidate_clear_write_set() {
  for (auto &&i: write_set) {
    i.state = CachedExtent::extent_state_t::INVALID;
    i.on_invalidated(*this, true);
  }
  write_set.clear();
  pending_lba_nodes.clear();
  pending_backref_nodes.clear();
}

template <typename node_key_t, typename T>
TCachedExtentRef<T> Transaction::may_get_fixedkv_leaf_node(
  extent_types_t type,
  node_key_t key) {
  static_assert(std::is_same_v<node_key_t, laddr_t>
    || std::is_same_v<node_key_t, paddr_t>);
  if constexpr (std::is_same_v<node_key_t, laddr_t>) {
    auto &layer = pending_lba_nodes.pin_layers[1];
    if (layer.empty()) {
      return nullptr;
    }
    auto iter = layer.lower_bound(
      key,
      typename btree_range_pin_t<laddr_t>::node_bound_cmp_t());
    if (iter == layer.begin()) {
      return (iter->range.is_within_range(key))
	? (T*)&(iter->get_extent())
	: nullptr;
    }
    if (iter == layer.end()) {
      iter--;
      return (iter->range.is_within_range(key))
	? (T*)&(iter->get_extent())
	: nullptr;
    }
    if (iter->range.is_within_range(key)
	|| (--iter)->range.is_within_range(key)) {
      return (T*)&(iter->get_extent());
    } else {
      return nullptr;
    }
  } else {
    auto &layer = pending_backref_nodes.pin_layers[1];
    if (layer.empty()) {
      return nullptr;
    }
    auto iter = layer.lower_bound(
      key,
      typename btree_range_pin_t<paddr_t>::node_bound_cmp_t());
    if (iter == layer.begin()) {
      return (iter->range.is_within_range(key))
	? (T*)&(iter->get_extent())
	: nullptr;
    }
    if (iter == layer.end()) {
      iter--;
      return (iter->range.is_within_range(key))
	? (T*)&(iter->get_extent())
	: nullptr;
    }
    if (iter->range.is_within_range(key)
	|| (--iter)->range.is_within_range(key)) {
      return (T*)&(iter->get_extent());
    } else {
      return nullptr;
    }
  }
}

template <typename node_key_t, typename T>
std::list<TCachedExtentRef<T>> Transaction::get_fixedkv_leaves_in_range(
  node_key_t key, extent_len_t length)
{
  static_assert(std::is_same_v<node_key_t, laddr_t>
    || std::is_same_v<node_key_t, paddr_t>);

  if constexpr (std::is_same_v<node_key_t, laddr_t>) {
    std::list<TCachedExtentRef<T>> leaves;

    auto &layer = pending_lba_nodes.pin_layers[1];
    if (layer.empty()) {
      return leaves;
    }
    auto iter = layer.lower_bound(
      key,
      typename btree_range_pin_t<node_key_t>::node_bound_cmp_t());

    if (iter != layer.begin() &&
	(iter == layer.end() || iter->range.begin != key)) {
      --iter;
    }

    for (; iter != layer.end()
	    && iter->range.begin < key + length
	    && iter->range.end > key;
	iter++) {
      leaves.push_back((T*)&(iter->get_extent()));
    }

    return leaves;
  } else {
    std::list<TCachedExtentRef<T>> leaves;

    auto &layer = pending_backref_nodes.pin_layers[1];
    if (layer.empty()) {
      return leaves;
    }
    auto iter = layer.lower_bound(
      key,
      typename btree_range_pin_t<node_key_t>::node_bound_cmp_t());

    if (iter != layer.begin() &&
	(iter == layer.end() || iter->range.begin != key)) {
      --iter;
    }

    for (; iter != layer.end()
	    && iter->range.begin < key + length
	    && iter->range.end > key;
	iter++) {
      leaves.push_back((T*)&(iter->get_extent()));
    }

    return leaves;
  }
}

void Transaction::may_link_fixedkv_pin(CachedExtentRef &ref) {
  if (is_lba_node(ref->get_type())) {
    auto &lba_node = (lba_manager::btree::LBANode&)*ref;
    if (lba_node.is_leaf()) {
      assert(!lba_node.pin.is_linked());
      auto &layer = pending_lba_nodes.pin_layers[1];
      layer.insert(lba_node.pin);
    }
  } else if (is_backref_node(ref->get_type())) {
    auto &backref_node = (backref::BackrefNode&)*ref;
    if (backref_node.is_leaf()) {
      assert(!backref_node.pin.is_linked());
      auto &layer = pending_backref_nodes.pin_layers[1];
      layer.insert(backref_node.pin);
    }
  }
}

void Transaction::may_remove_linked_fixedkv_pin(CachedExtentRef &ref) {
  if (is_lba_node(ref->get_type())) {
    auto &lba_node = (lba_manager::btree::LBANode&)*ref;
    if (lba_node.is_leaf()) {
      assert(lba_node.pin.is_linked());
      auto &layer = pending_lba_nodes.pin_layers[1];
      layer.erase(
	btree_range_pin_t<
	  laddr_t>::index_t::s_iterator_to(lba_node.pin));
    }
  } else if (is_backref_node(ref->get_type())) {
    auto &backref_node = (backref::BackrefNode&)*ref;
    if (backref_node.is_leaf()) {
      assert(backref_node.pin.is_linked());
      auto &layer = pending_backref_nodes.pin_layers[1];
      layer.erase(
	btree_range_pin_t<
	  paddr_t>::index_t::s_iterator_to(backref_node.pin));
    }
  }
}

void Transaction::link_fixedkv_leaf_node(CachedExtentRef ref) {
  assert(is_lba_node(ref->get_type())
    || is_backref_node(ref->get_type()));
#ifndef NDEBUG
  if (is_lba_node(ref->get_type())) {
    assert(ref->cast<lba_manager::btree::LBANode>()->is_leaf());
  } else {
    assert(ref->cast<backref::BackrefNode>()->is_leaf());
  }
#endif
  may_link_fixedkv_pin(ref);
}


template TCachedExtentRef<lba_manager::btree::LBALeafNode>
Transaction::may_get_fixedkv_leaf_node<laddr_t, lba_manager::btree::LBALeafNode>(
  extent_types_t type,
  laddr_t key);
template TCachedExtentRef<backref::BackrefLeafNode>
Transaction::may_get_fixedkv_leaf_node<paddr_t, backref::BackrefLeafNode>(
  extent_types_t type,
  paddr_t key);

template std::list<TCachedExtentRef<lba_manager::btree::LBALeafNode>>
Transaction::get_fixedkv_leaves_in_range(
  laddr_t key, extent_len_t length);
} // namespace crimson::os::seastore
