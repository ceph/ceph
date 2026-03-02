// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/os/seastore/lba_mapping.h"
#include "crimson/os/seastore/lba/btree_lba_manager.h"

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

using lba::LBALeafNode;

get_child_ret_t<LBALeafNode, LogicalChildNode>
LBAMapping::get_logical_extent(Transaction &t) const
{
  assert(is_linked_direct());
  ceph_assert(direct_cursor->is_viewable());
  ceph_assert(direct_cursor->ctx.trans.get_trans_id()
	      == t.get_trans_id());
  assert(!direct_cursor->is_end());
  auto &i = *direct_cursor;
  assert(i.pos != BTREENODE_POS_NULL);
  ceph_assert(t.get_trans_id() == i.ctx.trans.get_trans_id());
  auto p = direct_cursor->parent->cast<LBALeafNode>();
  return p->template get_child<LogicalChildNode>(
    t, i.ctx.cache, i.pos, i.key);
}

bool LBAMapping::is_stable() const {
  assert(is_linked_direct());
  ceph_assert(direct_cursor->is_viewable());
  assert(!direct_cursor->is_end());
  auto leaf = direct_cursor->parent->cast<LBALeafNode>();
  return leaf->is_child_stable(
    direct_cursor->ctx,
    direct_cursor->pos,
    direct_cursor->key);
}

bool LBAMapping::is_data_stable() const {
  assert(is_linked_direct());
  ceph_assert(direct_cursor->is_viewable());
  assert(!direct_cursor->is_end());
  auto leaf = direct_cursor->parent->cast<LBALeafNode>();
  return leaf->is_child_data_stable(
    direct_cursor->ctx,
    direct_cursor->pos,
    direct_cursor->key);
}

base_iertr::future<LBAMapping> LBAMapping::next()
{
  LOG_PREFIX(LBAMapping::next);
  auto ctx = get_effective_cursor().ctx;
  SUBDEBUGT(seastore_lba, "{}", ctx.trans, *this);
  return refresh().si_then([ctx](auto mapping) {
    return with_btree_state<lba::LBABtree, LBAMapping>(
      ctx.cache,
      ctx,
      std::move(mapping),
      [ctx](auto &btree, auto &mapping) mutable {
      auto &cursor = mapping.get_effective_cursor();
      auto iter = btree.make_partial_iter(ctx, cursor);
      return iter.next(ctx).si_then([ctx, &mapping](auto iter) {
	if (!iter.is_end() && iter.get_val().pladdr.is_laddr()) {
	  mapping = LBAMapping::create_indirect(nullptr, iter.get_cursor(ctx));
	} else {
	  mapping = LBAMapping::create_direct(iter.get_cursor(ctx));
	}
      });
    });
  });
}

base_iertr::future<LBAMapping> LBAMapping::refresh()
{
  if (is_viewable()) {
    return base_iertr::make_ready_future<LBAMapping>(*this);
  }
  return seastar::do_with(
    direct_cursor,
    indirect_cursor,
    [](auto &direct_cursor, auto &indirect_cursor) {
    return seastar::futurize_invoke([&direct_cursor] {
      if (direct_cursor) {
	return direct_cursor->refresh();
      }
      return base_iertr::now();
    }).si_then([&indirect_cursor] {
      if (indirect_cursor) {
	return indirect_cursor->refresh();
      }
      return base_iertr::now();
    }).si_then([&direct_cursor, &indirect_cursor] {
      return LBAMapping(direct_cursor, indirect_cursor);
    });
  });
}

base_iertr::future<> LBAMapping::co_refresh()
{
  if (is_viewable()) {
    co_return;
  }
  if (direct_cursor) {
    co_await direct_cursor->refresh();
  }
  if (indirect_cursor) {
    co_await indirect_cursor->refresh();
  }
}

bool LBAMapping::is_initial_pending() const {
  assert(is_linked_direct());
  ceph_assert(direct_cursor->is_viewable());
  assert(!direct_cursor->is_end());
  auto leaf = direct_cursor->parent->cast<LBALeafNode>();
  return leaf->is_child_initial_pending(
    direct_cursor->ctx,
    direct_cursor->pos,
    direct_cursor->key);
}

} // namespace crimson::os::seastore
