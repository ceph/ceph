// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/btree/btree_range_pin.h"
#include "crimson/os/seastore/btree/fixed_kv_node.h"

namespace crimson::os::seastore {

template <typename key_t, typename val_t>
get_child_ret_t<LogicalCachedExtent>
BtreeNodeMapping<key_t, val_t>::get_logical_extent(
  Transaction &t)
{
  assert(parent);
  assert(parent->is_valid());
  assert(pos != std::numeric_limits<uint16_t>::max());
  auto &p = (FixedKVNode<key_t>&)*parent;
  auto v = p.get_logical_child(ctx, pos);
  if (!v.has_child()) {
    this->child_pos = v.get_child_pos();
  }
  return v;
}

template <typename key_t, typename val_t>
bool BtreeNodeMapping<key_t, val_t>::is_stable() const
{
  assert(parent);
  assert(parent->is_valid());
  assert(pos != std::numeric_limits<uint16_t>::max());
  auto &p = (FixedKVNode<key_t>&)*parent;
  return p.is_child_stable(pos);
}

template class BtreeNodeMapping<laddr_t, paddr_t>;
template class BtreeNodeMapping<paddr_t, laddr_t>;
} // namespace crimson::os::seastore
