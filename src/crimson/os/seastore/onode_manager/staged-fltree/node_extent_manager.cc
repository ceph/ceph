// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "node_extent_manager.h"
#include "node_extent_manager/dummy.h"
#include "node_extent_manager/seastore.h"

namespace crimson::os::seastore::onode {

NodeExtentManagerURef NodeExtentManager::create_dummy(bool is_sync) {
  if (is_sync) {
    return NodeExtentManagerURef(new DummyNodeExtentManager<true>());
  } else {
    return NodeExtentManagerURef(new DummyNodeExtentManager<false>());
  }
}

NodeExtentManagerURef NodeExtentManager::create_seastore(
    TransactionManager& tm, laddr_t min_laddr) {
  return NodeExtentManagerURef(new SeastoreNodeExtentManager(tm, min_laddr));
}

}
