// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/btree/btree_range_pin.h"
#include "crimson/os/seastore/btree/fixed_kv_node.h"

namespace crimson::os::seastore {

template <typename key_t, typename val_t>
child_pos_t BtreeNodePin<key_t, val_t>::get_logical_extent(
  Transaction &t)
{
  assert(parent);
  assert(parent->is_valid());
  assert(pos != std::numeric_limits<uint16_t>::max());
  auto &p = (FixedKVNode<key_t>&)*parent;
  return p.get_logical_child(t, pos);
}

template child_pos_t BtreeNodePin<laddr_t, paddr_t>::get_logical_extent(
  Transaction &t);
template child_pos_t BtreeNodePin<paddr_t, laddr_t>::get_logical_extent(
  Transaction &t);
} // namespace crimson::os::seastore
