// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "node_extent_mutable.h"
#include "stages/node_stage.h"
#include "stages/stage.h"

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
  using stage_t = node_to_stage_t<node_stage_t>;
  using position_t = typename stage_t::position_t;
  using StagedIterator = typename stage_t::StagedIterator;
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
    auto p_value = stage_t::template proceed_insert<KT, false>(
        mut, node_stage, key, value, insert_pos, insert_stage, insert_size);
    return p_value;
  }

  static void split(
      NodeExtentMutable& mut,
      const node_stage_t& node_stage,
      StagedIterator& split_at) {
    node_stage_t::update_is_level_tail(mut, node_stage, false);
    stage_t::trim(mut, split_at);
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
    stage_t::trim(mut, split_at);
    auto p_value = stage_t::template proceed_insert<KT, true>(
        mut, node_stage, key, value, insert_pos, insert_stage, insert_size);
    return p_value;
  }

  static void update_child_addr(
      NodeExtentMutable& mut, const laddr_t new_addr, laddr_packed_t* p_addr) {
    assert(NODE_TYPE == node_type_t::INTERNAL);
    mut.copy_in_absolute(p_addr, new_addr);
  }

  static std::tuple<match_stage_t, position_t> erase(
      NodeExtentMutable& mut,
      const node_stage_t& node_stage,
      const position_t& _erase_pos) {
    if (_erase_pos.is_end()) {
      // must be internal node
      assert(node_stage.is_level_tail());
      // return erase_stage, last_pos
      return update_last_to_tail(mut, node_stage);
    }

    assert(node_stage.keys() != 0);
    position_t erase_pos = _erase_pos;
    auto erase_stage = stage_t::erase(mut, node_stage, erase_pos);
    // return erase_stage, next_pos
    return {erase_stage, erase_pos};
  }

  static position_t make_tail(
      NodeExtentMutable& mut,
      const node_stage_t& node_stage) {
    assert(!node_stage.is_level_tail());
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      auto [r_stage, r_last_pos] = update_last_to_tail(mut, node_stage);
      std::ignore = r_stage;
      return r_last_pos;
    } else {
      node_stage_t::update_is_level_tail(mut, node_stage, true);
      // no need to calculate the last pos
      return position_t::end();
    }
  }

 private:
  static std::tuple<match_stage_t, position_t> update_last_to_tail(
      NodeExtentMutable& mut,
      const node_stage_t& node_stage) {
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      assert(node_stage.keys() != 0);
      position_t last_pos;
      laddr_t last_value = L_ADDR_NULL;
      {
        const laddr_packed_t* p_last_value;
        stage_t::template get_largest_slot<true, false, true>(
            node_stage, &last_pos, nullptr, &p_last_value);
        last_value = p_last_value->value;
      }

      auto erase_pos = last_pos;
      auto erase_stage = stage_t::erase(mut, node_stage, erase_pos);
      assert(erase_pos.is_end());

      node_stage_t::update_is_level_tail(mut, node_stage, true);
      auto p_last_value = const_cast<laddr_packed_t*>(
          node_stage.get_end_p_laddr());
      mut.copy_in_absolute(p_last_value, last_value);
      // return erase_stage, last_pos
      return {erase_stage, last_pos};
    } else {
      ceph_abort("impossible path");
    }
  }
};

}
