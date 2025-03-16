// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/lba_mapping.h"

namespace crimson::os::seastore {

std::ostream &operator<<(std::ostream &out, const LBAMapping &rhs)
{
  if (rhs.is_end()) {
    return out << "LBAMapping(END)";
  }
  out << "LBAMapping(" << rhs.get_key()
      << "~0x" << std::hex << rhs.get_length();
  if (rhs.is_complete()) {
    out << std::dec
	<< "->" << rhs.get_val();
  } else {
    out << std::dec << "->" << rhs.indirect_cursor->val;
  }
  if (rhs.is_complete_indirect()) {
    out << ",indirect(" << rhs.get_intermediate_base()
        << "~0x" << std::hex << rhs.get_intermediate_length()
        << "@0x" << rhs.get_intermediate_offset() << std::dec
        << ")";
  }
  out << ")";
  return out;
}

std::ostream &operator<<(std::ostream &out, const lba_mapping_list_t &rhs)
{
  bool first = true;
  out << '[';
  for (const auto &i: rhs) {
    out << (first ? "" : ",") << i;
    first = false;
  }
  return out << ']';
}

using lba_manager::btree::LBALeafNode;

get_child_ret_t<LBALeafNode, LogicalChildNode>
LBAMapping::get_logical_extent(Transaction &t)
{
  assert(!is_null());
  ceph_assert(direct_cursor->is_valid());
  auto &i = *direct_cursor;
  assert(i.pos != std::numeric_limits<uint16_t>::max());
  ceph_assert(t.get_trans_id() == i.ctx.trans.get_trans_id());
  auto p = direct_cursor->parent->cast<LBALeafNode>();
  return p->template get_child<LogicalChildNode>(
    t, i.ctx.cache, i.pos, i.key);
}

bool LBAMapping::is_stable() const {
  assert(!is_null());
  ceph_assert(direct_cursor->is_valid());
  auto leaf = direct_cursor->parent->cast<LBALeafNode>();
  return leaf->is_child_stable(
    direct_cursor->ctx,
    direct_cursor->pos,
    direct_cursor->key);
}

bool LBAMapping::is_data_stable() const {
  assert(!is_null());
  ceph_assert(direct_cursor->is_valid());
  auto leaf = direct_cursor->parent->cast<LBALeafNode>();
  return leaf->is_child_data_stable(
    direct_cursor->ctx,
    direct_cursor->pos,
    direct_cursor->key);
}

} // namespace crimson::os::seastore
