// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include <memory>
#include <string.h>

#include "include/buffer.h"
#include "include/byteorder.h"

#include "crimson/os/seastore/lba_manager/btree/btree_lba_manager.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/logical_child_node.h"

SET_SUBSYS(seastore_lba);

namespace crimson::os::seastore::lba_manager::btree {

std::ostream& operator<<(std::ostream& out, const lba_map_val_t& v)
{
  return out << "lba_map_val_t("
             << v.pladdr
             << "~" << v.len
             << ", refcount=" << v.refcount
             << ", checksum=" << v.checksum
             << ")";
}

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

void LBALeafNode::maybe_fix_mapping_pos(BtreeLBAMapping &mapping)
{
  assert(mapping.get_parent() == this);
  auto key = mapping.is_indirect()
    ? mapping.get_intermediate_base()
    : mapping.get_key();
  if (key != iter_idx(mapping.get_pos()).get_key()) {
    auto iter = lower_bound(key);
    {
      // a mapping that no longer exist or has its value
      // modified is considered an outdated one, and
      // shouldn't be used anymore
      ceph_assert(iter != end());
      assert(iter.get_val() == mapping.get_map_val());
    }
    mapping._new_pos(iter.get_offset());
  }
}

BtreeLBAMappingRef LBALeafNode::get_mapping(
  op_context_t<laddr_t> c, laddr_t laddr)
{
  auto iter = lower_bound(laddr);
  ceph_assert(iter != end() && iter->get_key() == laddr);
  auto val = iter.get_val();
  return std::make_unique<BtreeLBAMapping>(
    c,
    this,
    iter.get_offset(),
    val,
    lba_node_meta_t{laddr, (laddr + val.len).checked_to_laddr(), 0});
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

}
