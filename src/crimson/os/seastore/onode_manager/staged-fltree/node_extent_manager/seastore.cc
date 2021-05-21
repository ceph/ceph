// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "seastore.h"

#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_accessor.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/stages/node_stage_layout.h"

namespace {
LOG_PREFIX(OTree::Seastore);
}

namespace crimson::os::seastore::onode {

static DeltaRecorderURef create_replay_recorder(
    node_type_t node_type, field_type_t field_type)
{
  if (node_type == node_type_t::LEAF) {
    if (field_type == field_type_t::N0) {
      return DeltaRecorderT<node_fields_0_t, node_type_t::LEAF>::create_for_replay();
    } else if (field_type == field_type_t::N1) {
      return DeltaRecorderT<node_fields_1_t, node_type_t::LEAF>::create_for_replay();
    } else if (field_type == field_type_t::N2) {
      return DeltaRecorderT<node_fields_2_t, node_type_t::LEAF>::create_for_replay();
    } else if (field_type == field_type_t::N3) {
      return DeltaRecorderT<leaf_fields_3_t, node_type_t::LEAF>::create_for_replay();
    } else {
      ceph_abort("impossible path");
    }
  } else if (node_type == node_type_t::INTERNAL) {
    if (field_type == field_type_t::N0) {
      return DeltaRecorderT<node_fields_0_t, node_type_t::INTERNAL>::create_for_replay();
    } else if (field_type == field_type_t::N1) {
      return DeltaRecorderT<node_fields_1_t, node_type_t::INTERNAL>::create_for_replay();
    } else if (field_type == field_type_t::N2) {
      return DeltaRecorderT<node_fields_2_t, node_type_t::INTERNAL>::create_for_replay();
    } else if (field_type == field_type_t::N3) {
      return DeltaRecorderT<internal_fields_3_t, node_type_t::INTERNAL>::create_for_replay();
    } else {
      ceph_abort("impossible path");
    }
  } else {
    ceph_abort("impossible path");
  }
}

NodeExtentRef SeastoreNodeExtent::mutate(
    context_t c, DeltaRecorderURef&& _recorder)
{
  DEBUGT("mutate {:#x} ...", c.t, get_laddr());
  auto p_handle = static_cast<TransactionManagerHandle*>(&c.nm);
  auto extent = p_handle->tm.get_mutable_extent(c.t, this);
  auto ret = extent->cast<SeastoreNodeExtent>();
  // A replayed extent may already have an empty recorder, we discard it for
  // simplicity.
  assert(!ret->recorder || ret->recorder->is_empty());
  ret->recorder = std::move(_recorder);
  return ret;
}

void SeastoreNodeExtent::apply_delta(const ceph::bufferlist& bl)
{
  DEBUG("replay {:#x} ...", get_laddr());
  if (!recorder) {
    auto [node_type, field_type] = get_types();
    recorder = create_replay_recorder(node_type, field_type);
  } else {
#ifndef NDEBUG
    auto [node_type, field_type] = get_types();
    assert(recorder->node_type() == node_type);
    assert(recorder->field_type() == field_type);
#endif
  }
  auto node = do_get_mutable();
  auto p = bl.cbegin();
  while (p != bl.end()) {
    recorder->apply_delta(p, node, get_laddr());
  }
}

}
