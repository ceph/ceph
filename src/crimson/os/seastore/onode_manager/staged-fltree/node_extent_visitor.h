// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "node_extent_manager.h"
#include "node_layout_replayable.h"

namespace crimson::os::seastore::onode {

template <typename FieldType, node_type_t NODE_TYPE>
class NodeExtentT {
 public:
  using layout_t = NodeLayoutReplayableT<FieldType, NODE_TYPE>;
  using node_stage_t = typename layout_t::node_stage_t;
  using position_t = typename layout_t::position_t;
  using StagedIterator = typename layout_t::StagedIterator;
  using value_t = typename layout_t::value_t;
  static constexpr auto FIELD_TYPE = layout_t::FIELD_TYPE;
  enum class state_t {
    NO_RECORDING,  // extent_state_t::INITIAL_WRITE_PENDING
    RECORDING,     // extent_state_t::MUTATION_PENDING
    PENDING_MUTATE // extent_state_t::CLEAN/DIRTY
  };

  NodeExtentT(state_t state, NodeExtentRef extent)
      : state{state}, extent{extent},
        node_stage{reinterpret_cast<const FieldType*>(extent->get_read())} {
    if (state == state_t::NO_RECORDING) {
      assert(!mut.has_value());
      mut.emplace(extent->get_mutable());
      // TODO: recorder = nullptr;
    } else if (state == state_t::RECORDING) {
      assert(!mut.has_value());
      mut.emplace(extent->get_mutable());
      // TODO: get recorder from extent
    } else if (state == state_t::PENDING_MUTATE) {
      // TODO: recorder = nullptr;
    } else {
      ceph_abort("impossible path");
    }
  }
  ~NodeExtentT() = default;
  NodeExtentT(const NodeExtentT&) = delete;
  NodeExtentT(NodeExtentT&&) = delete;
  NodeExtentT& operator=(const NodeExtentT&) = delete;
  NodeExtentT& operator=(NodeExtentT&&) = delete;

  const node_stage_t& read() const { return node_stage; }
  laddr_t get_laddr() const { return extent->get_laddr(); }

  // must be called before any mutate attempes.
  // for the safety of mixed read and mutate, call before read.
  void prepare_mutate(context_t c) {
    if (state == state_t::PENDING_MUTATE) {
      assert(!extent->is_pending());
      // TODO: create and set recorder DeltaRecorderT
      extent = extent->mutate(c/* recorder */);
      assert(extent->is_mutation_pending());
      state = state_t::RECORDING;
      node_stage = node_stage_t(
          reinterpret_cast<const FieldType*>(extent->get_read()));
      mut.emplace(extent->get_mutable());
    }
  }

  // TODO: translate absolute modifications to relative
  template <KeyT KT>
  const value_t* insert_replayable(
      const full_key_t<KT>& key,
      const value_t& value,
      position_t& insert_pos,
      match_stage_t& insert_stage,
      node_offset_t& insert_size) {
    assert(state != state_t::PENDING_MUTATE);
    // TODO: encode params to recorder as delta
    return layout_t::template insert<KT>(
        *mut, read(), key, value,
        insert_pos, insert_stage, insert_size);
  }

  void split_replayable(StagedIterator& split_at) {
    assert(state != state_t::PENDING_MUTATE);
    // TODO: encode params to recorder as delta
    layout_t::split(*mut, read(), split_at);
  }

  template <KeyT KT>
  const value_t* split_insert_replayable(
      StagedIterator& split_at,
      const full_key_t<KT>& key,
      const value_t& value,
      position_t& insert_pos,
      match_stage_t& insert_stage,
      node_offset_t& insert_size) {
    assert(state != state_t::PENDING_MUTATE);
    // TODO: encode params to recorder as delta
    return layout_t::template split_insert<KT>(
        *mut, read(), split_at, key, value,
        insert_pos, insert_stage, insert_size);
  }

  void update_child_addr_replayable(
      const laddr_t new_addr, laddr_t* p_addr) {
    assert(state != state_t::PENDING_MUTATE);
    // TODO: encode params to recorder as delta
    return layout_t::update_child_addr(*mut, new_addr, p_addr);
  }

  void test_copy_to(NodeExtentMutable& to) const {
    assert(extent->get_length() == to.get_length());
    std::memcpy(to.get_write(), extent->get_read(), extent->get_length());
  }

  static state_t loaded(NodeExtentRef extent) {
    state_t state;
    if (extent->is_initial_pending()) {
      state = state_t::NO_RECORDING;
    } else if (extent->is_mutation_pending()) {
      state = state_t::RECORDING;
    } else if (!extent->is_valid()) {
      state = state_t::PENDING_MUTATE;
    } else {
      ceph_abort("invalid extent");
    }
    return state;
  }

  static std::tuple<state_t, NodeExtentMutable> allocated(
      NodeExtentRef extent, bool is_level_tail, level_t level) {
    assert(extent->is_initial_pending());
    auto mut = extent->get_mutable();
    node_stage_t::bootstrap_extent(
        mut, FIELD_TYPE, NODE_TYPE, is_level_tail, level);
    return {state_t::NO_RECORDING, mut};
  }

 private:
  state_t state;
  NodeExtentRef extent;
  node_stage_t node_stage;
  std::optional<NodeExtentMutable> mut;
  // TODO: DeltaRecorderT* recorder;
};

}
