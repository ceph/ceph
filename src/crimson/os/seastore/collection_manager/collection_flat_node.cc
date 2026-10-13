// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "include/buffer.h"
#include "osd/osd_types.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/collection_manager/collection_flat_node.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore);
  }
}

namespace crimson::os::seastore::collection_manager {

void delta_t::replay(coll_map_t &l) const
{
  switch (op) {
  case op_t::INSERT: {
    l.insert(coll, value);
    break;
  }
  case op_t::UPDATE: {
    l.update(coll, value);
    break;
  }
  case op_t::REMOVE: {
    l.erase(coll);
    break;
  }
  case op_t::INVALID: {
    assert(0 == "impossible");
    break;
  }
  __builtin_unreachable();
  }
}


std::ostream &FlatCollectionNode::print_detail_l(std::ostream &out) const
{
  return out;
}

FlatCollectionNode::list_ret
FlatCollectionNode::list()
{
  logger().debug("FlatCollectionNode:{}, {}", __func__, *this);
  CollectionManager::list_ret_bare list_result;
  for (auto &[coll, value] : decoded) {
    list_result.emplace_back(coll, coll_info_t(value.bits, value.onode_root));
  }
  return list_ret(
    interruptible::ready_future_marker{},
    std::move(list_result));
}

FlatCollectionNode::create_ret
FlatCollectionNode::create(coll_context_t cc, coll_t coll, coll_value_t value)
{
  logger().debug("FlatCollectionNode:{}", __func__);
  if (!is_mutable()) {
    auto mut = cc.tm.get_mutable_extent(cc.t, this)->cast<FlatCollectionNode>();
    return mut->create(cc, coll, value);
  }
  logger().debug("FlatCollectionNode::create {} {} {}", coll, value.bits, *this);
  auto [iter, inserted] = decoded.insert(coll, value);
  assert(inserted);
  if (encoded_sizeof((base_coll_map_t&)decoded) > get_bptr().length()) {
    decoded.erase(iter);
    return create_ret(
      interruptible::ready_future_marker{},
      create_result_t::OVERFLOW);
  } else {
    if (auto buffer = maybe_get_delta_buffer(); buffer) {
      buffer->insert(coll, value);
    }
    copy_to_node();
    return create_ret(
      interruptible::ready_future_marker{},
      create_result_t::SUCCESS);
  }
}

FlatCollectionNode::update_ret
FlatCollectionNode::update(coll_context_t cc, coll_t coll, coll_value_t value)
{
  logger().debug("trans.{} FlatCollectionNode:{} {} {}",
    cc.t.get_trans_id(), __func__, coll, value.bits);
  if (!is_mutable()) {
    auto mut = cc.tm.get_mutable_extent(cc.t, this)->cast<FlatCollectionNode>();
    return mut->update(cc, coll, value);
  }
  if (auto buffer = maybe_get_delta_buffer(); buffer) {
    buffer->update(coll, value);
  }
  decoded.update(coll, value);
  copy_to_node();
  return seastar::now();
}

void FlatCollectionNode::update_value(coll_context_t cc, coll_t coll, coll_value_t value)
{
  logger().debug("trans.{} FlatCollectionNode:{} {} {}",
    cc.t.get_trans_id(), __func__, coll, value.bits);
  ceph_assert(is_mutable());
  if (auto buffer = maybe_get_delta_buffer(); buffer) {
    buffer->update(coll, value);
  }
  decoded.update(coll, value);
  copy_to_node();
}

FlatCollectionNode::remove_ret
FlatCollectionNode::remove(coll_context_t cc, coll_t coll)
{
  logger().debug("trans.{} FlatCollectionNode:{} {}",
    cc.t.get_trans_id(),__func__, coll);
  if (!is_mutable()) {
    auto mut = cc.tm.get_mutable_extent(cc.t, this)->cast<FlatCollectionNode>();
    return mut->remove(cc, coll);
  }
  if (auto buffer = maybe_get_delta_buffer(); buffer) {
    buffer->remove(coll);
  }
  // TODO: once retire_root() (see FLTreeOnodeManager::remove_tree()) is
  // implemented, assert decoded.get(coll).onode_root == L_ADDR_NULL here.
  // The onode tree root must be retired before its coll_info_t entry is
  // dropped. remove_tree() currently only erases in-memory tracking, so the
  // root extent leaks instead.
  decoded.remove(coll);
  copy_to_node();
  return seastar::now();
}

}
