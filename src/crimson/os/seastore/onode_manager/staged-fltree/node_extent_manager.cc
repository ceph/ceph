// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "node_extent_manager.h"

#include "node_extent_manager/dummy.h"
#include "node_extent_manager/seastore.h"
#include "stages/node_stage_layout.h"

namespace crimson::os::seastore::onode {

std::pair<node_type_t, field_type_t> NodeExtent::get_types() const
{
  const auto header = reinterpret_cast<const node_header_t*>(get_read());
  auto node_type = header->get_node_type();
  auto field_type = header->get_field_type();
  if (!field_type.has_value()) {
    throw std::runtime_error("load failed: bad field type");
  }
  return {node_type, *field_type};
}

NodeExtentManagerURef NodeExtentManager::create_dummy(bool is_sync)
{
  if (is_sync) {
    return NodeExtentManagerURef(new DummyNodeExtentManager<true>());
  } else {
    return NodeExtentManagerURef(new DummyNodeExtentManager<false>());
  }
}

NodeExtentManagerURef NodeExtentManager::create_seastore(
    TransactionManager& tm, laddr_t min_laddr)
{
  return NodeExtentManagerURef(new SeastoreNodeExtentManager(tm, min_laddr));
}

}
