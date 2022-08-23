// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/os/seastore/root_block.h"
#include "crimson/os/seastore/transaction.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/backref/backref_tree_node.h"

namespace crimson::os::seastore {

template <typename T>
void RootBlock::new_fixedkv_root(CachedExtent &node, Transaction &t) {
  static_assert(std::is_same_v<T, laddr_t>
    || std::is_same_v<T, paddr_t>);
  ceph_assert(is_pending() && node.is_pending());
  ceph_assert(t.get_trans_id() == this->pending_for_transaction);
  if constexpr (std::is_same_v<T, laddr_t>) {
    t.trackers_to_rm.push_back(lba_root_node);
    lba_root_node = new child_tracker_t(&node);
    t.new_pending_trackers.push_back(lba_root_node);
  } else {
    t.trackers_to_rm.push_back(backref_root_node);
    backref_root_node = new child_tracker_t(&node);
    t.new_pending_trackers.push_back(backref_root_node);
  }
}

CachedExtentRef RootBlock::duplicate_for_write(Transaction &t) {
  auto ext = new RootBlock(*this);

  auto it = lba_root_trans_views.find(
    t.get_trans_id(),
    trans_spec_view_t::cmp_t());
  if (it != lba_root_trans_views.end()) {
    auto &root_node = (lba_manager::btree::LBANode&)*it;
    root_node.parent_tracker.reset();
    ext->lba_root_node = new child_tracker_t(&root_node);
    t.new_pending_trackers.push_back(ext->lba_root_node);
    t.trackers_to_rm.push_back(lba_root_node);
    lba_root_trans_views.erase(it);
  }

  it = backref_root_trans_views.find(
    t.get_trans_id(),
    trans_spec_view_t::cmp_t());
  if (it != backref_root_trans_views.end()) {
    auto &root_node = (backref::BackrefNode&)*it;
    root_node.parent_tracker.reset();
    ext->backref_root_node = new child_tracker_t(&root_node);
    t.new_pending_trackers.push_back(ext->backref_root_node);
    t.trackers_to_rm.push_back(backref_root_node);
    backref_root_trans_views.erase(it);
  }
  return ext;
}

template <typename T>
CachedExtentRef RootBlock::get_phy_root_node(Transaction &t) {
  static_assert(std::is_same_v<T, laddr_t>
    || std::is_same_v<T, paddr_t>);
  if constexpr (std::is_same_v<T, laddr_t>) {
    if (!is_pending()) {
      auto it = lba_root_trans_views.find(
        t.get_trans_id(),
      trans_spec_view_t::cmp_t());
      if (it != lba_root_trans_views.end()) {
        return (CachedExtent*)&(*it);
      }
    }
    return lba_root_node->child.get();
  } else {
    if (!is_pending()) {
      auto it = backref_root_trans_views.find(
        t.get_trans_id(),
        trans_spec_view_t::cmp_t());
      if (it != backref_root_trans_views.end()) {
        return (CachedExtent*)&(*it);
      }
    }
    return backref_root_node->child.get();
  }
}

template <typename T>
void RootBlock::add_fixedkv_root_trans_view(CachedExtent &node) {
  static_assert(std::is_same_v<T, laddr_t>
    || std::is_same_v<T, paddr_t>);
  ceph_assert(!is_pending() && node.is_pending());
  if constexpr (std::is_same_v<T, laddr_t>) {
    auto [iter, inserted] = lba_root_trans_views.insert(node);
    ceph_assert(inserted);
    ((lba_manager::btree::LBANode&)node).parent_tracker =
      std::make_unique<parent_tracker_t>(this, 0);
  } else {
    auto [iter, inserted] = backref_root_trans_views.insert(node);
    ceph_assert(inserted);
    ((backref::BackrefNode&)node).parent_tracker =
      std::make_unique<parent_tracker_t>(this, 0);
  }
}

template void RootBlock::new_fixedkv_root<laddr_t>(
  CachedExtent &node, Transaction &t);
template void RootBlock::new_fixedkv_root<paddr_t>(
  CachedExtent &node, Transaction &t);
template CachedExtentRef RootBlock::get_phy_root_node<laddr_t>(Transaction &t);
template CachedExtentRef RootBlock::get_phy_root_node<paddr_t>(Transaction &t);
template void RootBlock::add_fixedkv_root_trans_view<laddr_t>(CachedExtent &node);
template void RootBlock::add_fixedkv_root_trans_view<paddr_t>(CachedExtent &node);

} // namespace crimson::os::seastore
