// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "node_extent_manager.h"
#include "node_delta_recorder.h"
#include "node_layout_replayable.h"

namespace crimson::os::seastore::onode {

template <typename FieldType, node_type_t NODE_TYPE>
class DeltaRecorderT final: public DeltaRecorder {
 public:
  using layout_t = NodeLayoutReplayableT<FieldType, NODE_TYPE>;
  using position_t = typename layout_t::position_t;
  using StagedIterator = typename layout_t::StagedIterator;
  using value_t = typename layout_t::value_t;
  static constexpr auto FIELD_TYPE = layout_t::FIELD_TYPE;

  ~DeltaRecorderT() override = default;

  template <KeyT KT>
  void encode_insert(
      const full_key_t<KT>& key,
      const value_t& value,
      const position_t& insert_pos,
      const match_stage_t& insert_stage,
      const node_offset_t& insert_size) {
    // TODO encode to encoded
  }

  void encode_split(
      const StagedIterator& split_at,
      const char* p_start) {
    // TODO encode to encoded
  }

  template <KeyT KT>
  void encode_split_insert(
      const StagedIterator& split_at,
      const full_key_t<KT>& key,
      const value_t& value,
      const position_t& insert_pos,
      const match_stage_t& insert_stage,
      const node_offset_t& insert_size,
      const char* p_start) {
    // TODO encode to encoded
  }

  void encode_update_child_addr(
      const laddr_t new_addr,
      const laddr_packed_t* p_addr,
      const char* p_start) {
    // TODO encode to encoded
  }

  static DeltaRecorderURef create() {
    return std::unique_ptr<DeltaRecorder>(new DeltaRecorderT());
  }

 private:
  DeltaRecorderT() = default;
  node_type_t node_type() const override { return NODE_TYPE; }
  field_type_t field_type() const override { return FIELD_TYPE; }
  void apply_delta(ceph::bufferlist::const_iterator& delta,
                   NodeExtentMutable& node) override {
    assert(is_empty());
    // TODO decode and apply
    assert(false && "not implemented");
  }
};

template <typename FieldType, node_type_t NODE_TYPE>
class NodeExtentT {
 public:
  using layout_t = NodeLayoutReplayableT<FieldType, NODE_TYPE>;
  using node_stage_t = typename layout_t::node_stage_t;
  using position_t = typename layout_t::position_t;
  using recorder_t = DeltaRecorderT<FieldType, NODE_TYPE>;
  using StagedIterator = typename layout_t::StagedIterator;
  using value_t = typename layout_t::value_t;
  static constexpr auto FIELD_TYPE = layout_t::FIELD_TYPE;
  enum class state_t {
    NO_RECORDING,  // extent_state_t::INITIAL_WRITE_PENDING
                   //   can mutate, no recording
    RECORDING,     // extent_state_t::MUTATION_PENDING
                   //   can mutate, recording
    PENDING_MUTATE // extent_state_t::CLEAN/DIRTY
                   //   cannot mutate
  };

  NodeExtentT(state_t state, NodeExtentRef extent)
      : state{state}, extent{extent},
        node_stage{reinterpret_cast<const FieldType*>(extent->get_read())} {
    if (state == state_t::NO_RECORDING) {
      mut.emplace(extent->get_mutable());
      assert(extent->get_recorder() == nullptr);
      recorder = nullptr;
    } else if (state == state_t::RECORDING) {
      mut.emplace(extent->get_mutable());
      auto p_recorder = extent->get_recorder();
      assert(p_recorder != nullptr);
      assert(p_recorder->node_type() == NODE_TYPE);
      assert(p_recorder->field_type() == FIELD_TYPE);
      recorder = static_cast<recorder_t*>(p_recorder);
    } else if (state == state_t::PENDING_MUTATE) {
      // mut is empty
      assert(extent->get_recorder() == nullptr ||
             extent->get_recorder()->is_empty());
      recorder = nullptr;
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
      auto ref_recorder = recorder_t::create();
      recorder = static_cast<recorder_t*>(ref_recorder.get());
      extent = extent->mutate(c, std::move(ref_recorder));

      state = state_t::RECORDING;
      assert(extent->is_mutation_pending());
      node_stage = node_stage_t(
          reinterpret_cast<const FieldType*>(extent->get_read()));
      assert(recorder == static_cast<recorder_t*>(extent->get_recorder()));
      mut.emplace(extent->get_mutable());
    }
  }

  template <KeyT KT>
  const value_t* insert_replayable(
      const full_key_t<KT>& key,
      const value_t& value,
      position_t& insert_pos,
      match_stage_t& insert_stage,
      node_offset_t& insert_size) {
    assert(state != state_t::PENDING_MUTATE);
    if (state == state_t::RECORDING) {
      recorder->template encode_insert<KT>(
          key, value, insert_pos, insert_stage, insert_size);
    }
    return layout_t::template insert<KT>(
        *mut, read(), key, value,
        insert_pos, insert_stage, insert_size);
  }

  void split_replayable(StagedIterator& split_at) {
    assert(state != state_t::PENDING_MUTATE);
    if (state == state_t::RECORDING) {
      recorder->encode_split(split_at, read().p_start());
    }
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
    if (state == state_t::RECORDING) {
      recorder->template encode_split_insert<KT>(
          split_at, key, value, insert_pos, insert_stage, insert_size,
          read().p_start());
    }
    return layout_t::template split_insert<KT>(
        *mut, read(), split_at, key, value,
        insert_pos, insert_stage, insert_size);
  }

  void update_child_addr_replayable(
      const laddr_t new_addr, laddr_packed_t* p_addr) {
    assert(state != state_t::PENDING_MUTATE);
    if (state == state_t::RECORDING) {
      recorder->encode_update_child_addr(new_addr, p_addr, read().p_start());
    }
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
    } else if (extent->is_valid()) {
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
  // owned by extent
  recorder_t* recorder;
};

}
