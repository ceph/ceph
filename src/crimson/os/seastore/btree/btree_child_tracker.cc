// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/transaction.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/backref/backref_tree_node.h"

namespace crimson::os::seastore {

CachedExtent* child_trans_views_t::get_child_trans_view(
  Transaction &t, uint64_t pos)
{
  if (views_by_transaction.empty()) {
    return nullptr;
  }

  auto &tv_map = views_by_transaction[pos];
  if (!tv_map) {
    return nullptr;
  } else {
    ceph_assert(!tv_map->empty());
    auto it = tv_map->find(t.get_trans_id());
    if (it == tv_map->end()) {
      return nullptr;
    } else {
      return it->second;
    }
  }
}

template <typename T>
std::list<std::pair<CachedExtent*, uint64_t>>
child_trans_views_t::remove_trans_view(Transaction &t) {
  ceph_assert(views_by_transaction.capacity());
  std::list<std::pair<CachedExtent*, uint64_t>> res;
  auto tid = t.get_trans_id();
  auto iter = trans_views.lower_bound(
    tid,
    trans_spec_view_t::cmp_t());
  for (; iter->pending_for_transaction == tid;) {
    auto &child_trans_view = *iter;
    iter = trans_views.erase(iter);
    auto child_pos = ((T&)child_trans_view).parent_tracker->pos;
    auto &tv_map = views_by_transaction[child_pos];
    ceph_assert(tv_map);
    auto it = tv_map->find(tid);
    ceph_assert(it != tv_map->end());
    ceph_assert(it->second == &child_trans_view);
    tv_map->erase(it);
    if (tv_map->empty())
      tv_map.reset();
    res.push_back(std::make_pair((CachedExtent*)&child_trans_view, child_pos));
  }
  return res;
}

template <typename key_t>
back_tracker_t<key_t>::~back_tracker_t() {
  if constexpr (std::is_same_v<key_t, laddr_t>) {
    auto &p = (lba_manager::btree::LBANode&)*parent;
    ceph_assert(p.back_tracker_to_me && p.back_tracker_to_me == this);
    p.back_tracker_to_me = nullptr;
  } else {
    static_assert(std::is_same_v<key_t, paddr_t>);
    auto &p = (backref::BackrefNode&)*parent;
    ceph_assert(p.back_tracker_to_me && p.back_tracker_to_me == this);
    p.back_tracker_to_me = nullptr;
  }
}

template std::list<std::pair<CachedExtent*, uint64_t>>
child_trans_views_t::remove_trans_view<lba_manager::btree::LBANode>(Transaction &t);

template std::list<std::pair<CachedExtent*, uint64_t>>
child_trans_views_t::remove_trans_view<backref::BackrefNode>(Transaction &t);

template class back_tracker_t<laddr_t>;
template class back_tracker_t<paddr_t>;

} // namespace crimson::os::seastore
