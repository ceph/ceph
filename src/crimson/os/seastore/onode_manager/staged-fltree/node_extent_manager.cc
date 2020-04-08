// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "node_extent_manager.h"
#include "node_extent_manager/dummy.h"
#include "node_extent_manager/seastore.h"

namespace crimson::os::seastore::onode {

NodeExtentManagerURef NodeExtentManager::create_dummy() {
  return NodeExtentManagerURef(new DummyNodeExtentManager());
}

NodeExtentManagerURef NodeExtentManager::create_seastore(
    TransactionManager& tm, laddr_t min_laddr) {
  return NodeExtentManagerURef(new SeastoreNodeExtentManager(tm, min_laddr));
}

}
