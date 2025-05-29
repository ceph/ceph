// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/btree/btree_types.h"
#include "crimson/os/seastore/lba/lba_btree_node.h"
#include "crimson/os/seastore/backref/backref_tree_node.h"

namespace crimson::os::seastore {

namespace lba {

std::ostream& operator<<(std::ostream& out, const lba_map_val_t& v)
{
  return out << "lba_map_val_t("
             << v.pladdr
             << "~0x" << std::hex << v.len
             << ", checksum=0x" << v.checksum
             << ", refcount=" << std::dec << v.refcount
             << ")";
}

} // namespace lba

namespace backref {

std::ostream& operator<<(std::ostream &out, const backref_map_val_t& val) {
  return out << "backref_map_val_t("
	     << val.laddr
	     << "~0x" << std::hex << val.len << std::dec
	     << ")";
}

} // namespace backref

namespace {
template <typename key_t, typename T>
bool modified_since(T &&extent, uint64_t iter_modifications) {
  using backref::BackrefLeafNode;
  using lba::LBALeafNode;
  if constexpr (std::is_same_v<key_t, laddr_t>) {
    assert(extent->get_type() == extent_types_t::LADDR_LEAF);
    auto leaf = extent->template cast<LBALeafNode>();
    return leaf->modified_since(iter_modifications);
  } else {
    assert(extent->get_type() == extent_types_t::BACKREF_LEAF);
    auto leaf = extent->template cast<BackrefLeafNode>();
    return leaf->modified_since(iter_modifications);
  }
}
}

template <typename key_t, typename val_t>
bool BtreeCursor<key_t, val_t>::is_viewable() const {
  LOG_PREFIX(BtreeCursor::is_viewable());
  if (!parent->is_valid() ||
      modified_since<key_t>(parent, modifications)) {
    return false;
  }

  auto [viewable, state] = parent->is_viewable_by_trans(ctx.trans);
  assert(state != CachedExtent::viewable_state_t::invalid);
  SUBTRACET(seastore_cache, "{} with viewable state {}",
            ctx.trans, *parent, state);
  return viewable;
}

template struct BtreeCursor<laddr_t, lba::lba_map_val_t>;
template struct BtreeCursor<paddr_t, backref::backref_map_val_t>;

} // namespace crimson::os::seastore
