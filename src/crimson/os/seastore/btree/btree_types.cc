// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/os/seastore/btree/btree_types.h"
#include "crimson/os/seastore/lba/lba_btree_node.h"
#include "crimson/os/seastore/backref/backref_tree_node.h"
#include "crimson/os/seastore/lba/btree_lba_manager.h"

namespace crimson::os::seastore {

base_iertr::future<> LBACursor::refresh()
{
  LOG_PREFIX(LBACursor::refresh);
  return with_btree<lba::LBABtree>(
    ctx.cache,
    ctx,
    [this, FNAME, c=ctx](auto &btree) {
    c.trans.cursor_stats.num_refresh_parent_total++;

    if (!parent->is_valid()) {
      c.trans.cursor_stats.num_refresh_invalid_parent++;
      SUBTRACET(
	seastore_lba,
	"cursor {} parent is invalid, re-search from scratch",
	 c.trans, *this);
      return btree.lower_bound(c, this->get_laddr()
      ).si_then([this](lba::LBABtree::iterator iter) {
	auto leaf = iter.get_leaf_node();
	parent = leaf;
	modifications = leaf->modifications;
	pos = iter.get_leaf_pos();
	if (!is_end()) {
	  ceph_assert(!iter.is_end());
	  ceph_assert(iter.get_key() == get_laddr());
	  val = iter.get_val();
	  assert(is_viewable());
	}
      });
    }
    assert(parent->is_stable() ||
      parent->is_pending_in_trans(c.trans.get_trans_id()));
    auto leaf = parent->cast<lba::LBALeafNode>();
    if (leaf->is_pending_in_trans(c.trans.get_trans_id())) {
      if (leaf->modified_since(modifications)) {
	c.trans.cursor_stats.num_refresh_modified_viewable_parent++;
      } else {
	// no need to refresh
	return base_iertr::now();
      }
    } else {
      auto [viewable, l] = leaf->resolve_transaction(c.trans, key);
      SUBTRACET(
	seastore_lba,
	"cursor: {} viewable: {}",
	c.trans, *this, viewable);
      if (!viewable) {
	leaf = l;
	c.trans.cursor_stats.num_refresh_unviewable_parent++;
	parent = leaf;
      } else {
	assert(leaf.get() == l.get());
	assert(leaf->is_stable());
	return base_iertr::now();
      }
    }

    modifications = leaf->modifications;
    if (is_end()) {
      pos = leaf->get_size();
      assert(!val);
    } else {
      auto i = leaf->lower_bound(get_laddr());
      pos = i.get_offset();
      val = i.get_val();

      auto iter = lba::LBALeafNode::iterator(leaf.get(), pos);
      ceph_assert(iter.get_key() == key);
      ceph_assert(iter.get_val() == val);
      assert(is_viewable());
    }

    return base_iertr::now();
  });
}

namespace lba {

std::ostream& operator<<(std::ostream& out, const lba_map_val_t& v)
{
  return out << "lba_map_val_t("
             << v.pladdr
             << "~0x" << std::hex << v.len
             << ", type=" << (extent_types_t)v.type
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
  SUBTRACET(seastore_cache, "{} with viewable state {}",
            ctx.trans, *parent, state);
  return viewable;
}

template struct BtreeCursor<laddr_t, lba::lba_map_val_t>;
template struct BtreeCursor<paddr_t, backref::backref_map_val_t>;

} // namespace crimson::os::seastore
