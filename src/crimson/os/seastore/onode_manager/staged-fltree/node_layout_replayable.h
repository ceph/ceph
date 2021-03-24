// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "node_extent_mutable.h"
#include "stages/node_stage.h"
#include "stages/stage.h"

#define STAGE_T node_to_stage_t<node_stage_t>

namespace crimson::os::seastore::onode {

/**
 * NodeLayoutReplayableT
 *
 * Contains templated logics to modify the layout of a NodeExtend which are
 * also replayable. Used by NodeExtentAccessorT at runtime and by
 * DeltaRecorderT during replay.
 */
template <typename FieldType, node_type_t NODE_TYPE>
struct NodeLayoutReplayableT {
  using node_stage_t = node_extent_t<FieldType, NODE_TYPE>;
  using position_t = typename STAGE_T::position_t;
  using StagedIterator = typename STAGE_T::StagedIterator;
  using value_input_t = value_input_type_t<NODE_TYPE>;
  using value_t = value_type_t<NODE_TYPE>;
  static constexpr auto FIELD_TYPE = FieldType::FIELD_TYPE;

  template <KeyT KT>
  static const value_t* insert(
      NodeExtentMutable& mut,
      const node_stage_t& node_stage,
      const full_key_t<KT>& key,
      const value_input_t& value,
      position_t& insert_pos,
      match_stage_t& insert_stage,
      node_offset_t& insert_size) {
    auto p_value = STAGE_T::template proceed_insert<KT, false>(
        mut, node_stage, key, value, insert_pos, insert_stage, insert_size);
    return p_value;
  }

  static void split(
      NodeExtentMutable& mut,
      const node_stage_t& node_stage,
      StagedIterator& split_at) {
    node_stage_t::update_is_level_tail(mut, node_stage, false);
    STAGE_T::trim(mut, split_at);
  }

  template <KeyT KT>
  static const value_t* split_insert(
      NodeExtentMutable& mut,
      const node_stage_t& node_stage,
      StagedIterator& split_at,
      const full_key_t<KT>& key,
      const value_input_t& value,
      position_t& insert_pos,
      match_stage_t& insert_stage,
      node_offset_t& insert_size) {
    node_stage_t::update_is_level_tail(mut, node_stage, false);
    STAGE_T::trim(mut, split_at);
    auto p_value = STAGE_T::template proceed_insert<KT, true>(
        mut, node_stage, key, value, insert_pos, insert_stage, insert_size);
    return p_value;
  }

  static void update_child_addr(
      NodeExtentMutable& mut, const laddr_t new_addr, laddr_packed_t* p_addr) {
    assert(NODE_TYPE == node_type_t::INTERNAL);
    mut.copy_in_absolute(p_addr, new_addr);
  }
};

}
