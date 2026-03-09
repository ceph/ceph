// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <sys/mman.h>
#include <string.h>

#include <memory>
#include <string.h>

#include "include/buffer.h"
#include "include/byteorder.h"

#include "crimson/os/seastore/lba/btree_lba_manager.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/logical_child_node.h"

SET_SUBSYS(seastore_lba);

namespace crimson::os::seastore::lba {

std::ostream &LBALeafNode::print_detail(std::ostream &out) const
{
  out << ", size=" << this->get_size()
      << ", meta=" << this->get_meta()
      << ", modifications=" << this->modifications
      << ", my_tracker=" << (void*)this->my_tracker;
  if (this->my_tracker) {
    out << ", my_tracker->parent=" << (void*)this->my_tracker->get_parent().get();
  }
  return out << ", root_block=" << (void*)this->parent_of_root.get();
}

void LBALeafNode::resolve_relative_addrs(paddr_t base)
{
  LOG_PREFIX(LBALeafNode::resolve_relative_addrs);
  for (auto i: *this) {
    auto val = i->get_val();
    if (val.pladdr.is_paddr() &&
	val.pladdr.get_paddr().is_relative()) {
      val.pladdr = base.add_relative(val.pladdr.get_paddr());
      TRACE("{} -> {}", i->get_val().pladdr, val.pladdr);
      i->set_val(val);
    }
  }
}

void LBALeafNode::update(
  internal_const_iterator_t iter,
  lba_map_val_t val)
{
  LOG_PREFIX(LBALeafNode::update);
  SUBTRACE(seastore_fixedkv_tree, "trans.{}, pos {}",
    this->pending_for_transaction,
    iter.get_offset());
  this->on_modify();
  if (val.pladdr.is_paddr()) {
    val.pladdr = maybe_generate_relative(val.pladdr.get_paddr());
  }
  return this->journal_update(
    iter,
    val,
    this->maybe_get_delta_buffer());
}

LBALeafNode::internal_const_iterator_t LBALeafNode::insert(
  internal_const_iterator_t iter,
  laddr_t addr,
  lba_map_val_t val)
{
  LOG_PREFIX(LBALeafNode::insert);
  SUBTRACE(seastore_fixedkv_tree, "trans.{}, pos {}, key {}",
    this->pending_for_transaction,
    iter.get_offset(),
    addr);
  this->on_modify();
  if (val.pladdr.is_paddr()) {
    val.pladdr = maybe_generate_relative(val.pladdr.get_paddr());
  }
  this->journal_insert(
    iter,
    addr,
    val,
    this->maybe_get_delta_buffer());
  return iter;
}

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
      ).si_then([this](lba::LBABtree::iterator it) {
	assert(this->get_laddr() == it.get_key());
	iter = LBALeafNode::iterator(
	  it.get_leaf_node().get(),
	  it.get_leaf_pos());
	auto leaf = it.get_leaf_node();
	parent = leaf;
	modifications = leaf->modifications;
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
      auto [viewable, l] = leaf->resolve_transaction(c.trans, get_laddr());
	SUBTRACET(seastore_lba, "cursor: {} viewable: {}",
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
    iter = leaf->lower_bound(get_laddr());
    assert(is_viewable());

    return base_iertr::now();
  });
}

}
