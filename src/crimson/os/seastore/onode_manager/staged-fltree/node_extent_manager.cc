// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "node_extent_manager.h"

#include "node_extent_manager/dummy.h"
#include "node_extent_manager/seastore.h"

namespace crimson::os::seastore::onode {

NodeExtentManagerURef NodeExtentManager::create_dummy(bool is_sync)
{
  if (is_sync) {
    return NodeExtentManagerURef(new DummyNodeExtentManager<true>());
  } else {
    return NodeExtentManagerURef(new DummyNodeExtentManager<false>());
  }
}

NodeExtentManagerURef NodeExtentManager::create_seastore(
    InterruptedTransactionManager tm, laddr_t min_laddr, double p_eagain)
{
  if (p_eagain == 0.0) {
    return NodeExtentManagerURef(
        new SeastoreNodeExtentManager<false>(tm, min_laddr, p_eagain));
  } else {
    return NodeExtentManagerURef(
        new SeastoreNodeExtentManager<true>(tm, min_laddr, p_eagain));
  }
}

}
