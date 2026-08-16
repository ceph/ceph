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


std::ostream &CollectionNode::print_detail_l(std::ostream &out) const
{
  return out;
}

CollectionNode::list_ret
CollectionNode::list()
{
  logger().debug("CollectionNode:{}, {}", __func__, *this);
  CollectionManager::list_ret_bare list_result;
  for (auto &[coll, value] : decoded) {
    list_result.emplace_back(coll, coll_info_t(value.bits, value.onode_root));
  }
  return list_ret(
    interruptible::ready_future_marker{},
    std::move(list_result));
}

CollectionNode::create_ret
CollectionNode::create(coll_context_t cc, coll_t coll, coll_value_t value)
{
  logger().debug("CollectionNode:{}", __func__);
  if (!is_mutable()) {
    auto mut = cc.tm.get_mutable_extent(cc.t, this)->cast<CollectionNode>();
    return mut->create(cc, coll, value);
  }
  logger().debug("CollectionNode::create {} {} {}", coll, value.bits, *this);
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

CollectionNode::update_ret
CollectionNode::update(coll_context_t cc, coll_t coll, coll_value_t value)
{
  logger().debug("trans.{} CollectionNode:{} {} {}",
    cc.t.get_trans_id(), __func__, coll, value.bits);
  if (!is_mutable()) {
    auto mut = cc.tm.get_mutable_extent(cc.t, this)->cast<CollectionNode>();
    return mut->update(cc, coll, value);
  }
  if (auto buffer = maybe_get_delta_buffer(); buffer) {
    buffer->update(coll, value);
  }
  decoded.update(coll, value);
  copy_to_node();
  return seastar::now();
}

CollectionNode::remove_ret
CollectionNode::remove(coll_context_t cc, coll_t coll)
{
  logger().debug("trans.{} CollectionNode:{} {}",
    cc.t.get_trans_id(),__func__, coll);
  if (!is_mutable()) {
    auto mut = cc.tm.get_mutable_extent(cc.t, this)->cast<CollectionNode>();
    return mut->remove(cc, coll);
  }
  if (auto buffer = maybe_get_delta_buffer(); buffer) {
    buffer->remove(coll);
  }
  decoded.remove(coll);
  copy_to_node();
  return seastar::now();
}

}
