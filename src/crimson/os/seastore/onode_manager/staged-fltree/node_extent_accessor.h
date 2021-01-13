// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/log.h"
#include "node_extent_manager.h"
#include "node_delta_recorder.h"
#include "node_layout_replayable.h"

#ifndef NDEBUG
#include "node_extent_manager/test_replay.h"
#endif

namespace crimson::os::seastore::onode {

/**
 * DeltaRecorderT
 *
 * Responsible to encode and decode delta, and apply delta for a specific node
 * layout.
 */
template <typename FieldType, node_type_t NODE_TYPE>
class DeltaRecorderT final: public DeltaRecorder {
  enum class op_t : uint8_t {
    INSERT,
    SPLIT,
    SPLIT_INSERT,
    UPDATE_CHILD_ADDR,
  };

 public:
  using layout_t = NodeLayoutReplayableT<FieldType, NODE_TYPE>;
  using node_stage_t = typename layout_t::node_stage_t;
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
    ceph::encode(op_t::INSERT, encoded);
    encode_key<KT>(key, encoded);
    encode_value(value, encoded);
    insert_pos.encode(encoded);
    ceph::encode(insert_stage, encoded);
    ceph::encode(insert_size, encoded);
  }

  void encode_split(
      const StagedIterator& split_at,
      const char* p_node_start) {
    ceph::encode(op_t::SPLIT, encoded);
    split_at.encode(p_node_start, encoded);
  }

  template <KeyT KT>
  void encode_split_insert(
      const StagedIterator& split_at,
      const full_key_t<KT>& key,
      const value_t& value,
      const position_t& insert_pos,
      const match_stage_t& insert_stage,
      const node_offset_t& insert_size,
      const char* p_node_start) {
    ceph::encode(op_t::SPLIT_INSERT, encoded);
    split_at.encode(p_node_start, encoded);
    encode_key<KT>(key, encoded);
    encode_value(value, encoded);
    insert_pos.encode(encoded);
    ceph::encode(insert_stage, encoded);
    ceph::encode(insert_size, encoded);
  }

  void encode_update_child_addr(
      const laddr_t new_addr,
      const laddr_packed_t* p_addr,
      const char* p_node_start) {
    ceph::encode(op_t::UPDATE_CHILD_ADDR, encoded);
    ceph::encode(new_addr, encoded);
    int node_offset = reinterpret_cast<const char*>(p_addr) - p_node_start;
    assert(node_offset > 0 && node_offset <= NODE_BLOCK_SIZE);
    ceph::encode(static_cast<node_offset_t>(node_offset), encoded);
  }

  static DeltaRecorderURef create() {
    return std::unique_ptr<DeltaRecorder>(new DeltaRecorderT());
  }

 protected:
  DeltaRecorderT() = default;
  node_type_t node_type() const override { return NODE_TYPE; }
  field_type_t field_type() const override { return FIELD_TYPE; }
  void apply_delta(ceph::bufferlist::const_iterator& delta,
                   NodeExtentMutable& node) override {
    assert(is_empty());
    node_stage_t stage(reinterpret_cast<const FieldType*>(node.get_read()));
    op_t op;
    try {
      ceph::decode(op, delta);
      switch (op) {
      case op_t::INSERT: {
        logger().debug("OTree::Extent::Replay: decoding INSERT ...");
        auto key = key_hobj_t::decode(delta);

        std::unique_ptr<char[]> value_storage_heap;
        value_t value_storage_stack;
        auto p_value = decode_value(delta, value_storage_heap, value_storage_stack);

        auto insert_pos = position_t::decode(delta);
        match_stage_t insert_stage;
        ceph::decode(insert_stage, delta);
        node_offset_t insert_size;
        ceph::decode(insert_size, delta);
        logger().debug("OTree::Extent::Replay: apply {}, {}, "
                       "insert_pos({}), insert_stage={}, insert_size={}B ...",
                       key, *p_value, insert_pos, insert_stage, insert_size);
        layout_t::template insert<KeyT::HOBJ>(
          node, stage, key, *p_value, insert_pos, insert_stage, insert_size);
        break;
      }
      case op_t::SPLIT: {
        logger().debug("OTree::Extent::Replay: decoding SPLIT ...");
        auto split_at = StagedIterator::decode(stage.p_start(), delta);
        logger().debug("OTree::Extent::Replay: apply split_at={} ...", split_at);
        layout_t::split(node, stage, split_at);
        break;
      }
      case op_t::SPLIT_INSERT: {
        logger().debug("OTree::Extent::Replay: decoding SPLIT_INSERT ...");
        auto split_at = StagedIterator::decode(stage.p_start(), delta);
        auto key = key_hobj_t::decode(delta);

        std::unique_ptr<char[]> value_storage_heap;
        value_t value_storage_stack;
        auto p_value = decode_value(delta, value_storage_heap, value_storage_stack);

        auto insert_pos = position_t::decode(delta);
        match_stage_t insert_stage;
        ceph::decode(insert_stage, delta);
        node_offset_t insert_size;
        ceph::decode(insert_size, delta);
        logger().debug("OTree::Extent::Replay: apply split_at={}, {}, {}, "
                       "insert_pos({}), insert_stage={}, insert_size={}B ...",
                       split_at, key, *p_value, insert_pos, insert_stage, insert_size);
        layout_t::template split_insert<KeyT::HOBJ>(
          node, stage, split_at, key, *p_value, insert_pos, insert_stage, insert_size);
        break;
      }
      case op_t::UPDATE_CHILD_ADDR: {
        logger().debug("OTree::Extent::Replay: decoding UPDATE_CHILD_ADDR ...");
        laddr_t new_addr;
        ceph::decode(new_addr, delta);
        node_offset_t update_offset;
        ceph::decode(update_offset, delta);
        auto p_addr = reinterpret_cast<laddr_packed_t*>(
            node.get_write() + update_offset);
        logger().debug("OTree::Extent::Replay: apply {:#x} to offset {:#x} ...",
                       new_addr, update_offset);
        layout_t::update_child_addr(node, new_addr, p_addr);
        break;
      }
      default:
        logger().error("OTree::Extent::Replay: got unknown op {} when replay {:#x}",
                       op, node.get_laddr());
        ceph_abort();
      }
    } catch (buffer::error& e) {
      logger().error("OTree::Extent::Replay: got decode error {} when replay {:#x}",
                     e, node.get_laddr());
      ceph_abort();
    }
  }

 private:
  static void encode_value(const value_t& value, ceph::bufferlist& encoded) {
    if constexpr (std::is_same_v<value_t, laddr_packed_t>) {
      // NODE_TYPE == node_type_t::INTERNAL
      ceph::encode(value.value, encoded);
    } else if constexpr (std::is_same_v<value_t, onode_t>) {
      // NODE_TYPE == node_type_t::LEAF
      value.encode(encoded);
    } else {
      ceph_abort("impossible path");
    }
  }

  static value_t* decode_value(ceph::bufferlist::const_iterator& delta,
                               std::unique_ptr<char[]>& value_storage_heap,
                               value_t& value_storage_stack) {
    if constexpr (std::is_same_v<value_t, laddr_packed_t>) {
      // NODE_TYPE == node_type_t::INTERNAL
      laddr_t value;
      ceph::decode(value, delta);
      value_storage_stack.value = value;
      return &value_storage_stack;
    } else if constexpr (std::is_same_v<value_t, onode_t>) {
      // NODE_TYPE == node_type_t::LEAF
      auto value_config = onode_t::decode(delta);
      value_storage_heap = onode_t::allocate(value_config);
      return reinterpret_cast<onode_t*>(value_storage_heap.get());
    } else {
      ceph_abort("impossible path");
    }
  }

  static seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
};

/**
 * NodeExtentAccessorT
 *
 * This component is responsible to reference and mutate the underlying
 * NodeExtent, record mutation parameters when needed, and apply the recorded
 * modifications for a specific node layout.
 */
template <typename FieldType, node_type_t NODE_TYPE>
class NodeExtentAccessorT {
 public:
  using layout_t = NodeLayoutReplayableT<FieldType, NODE_TYPE>;
  using node_stage_t = typename layout_t::node_stage_t;
  using position_t = typename layout_t::position_t;
  using recorder_t = DeltaRecorderT<FieldType, NODE_TYPE>;
  using StagedIterator = typename layout_t::StagedIterator;
  using value_t = typename layout_t::value_t;
  static constexpr auto FIELD_TYPE = layout_t::FIELD_TYPE;

  NodeExtentAccessorT(NodeExtentRef extent)
      : extent{extent},
        node_stage{reinterpret_cast<const FieldType*>(extent->get_read())} {
    if (no_recording()) {
      mut.emplace(extent->get_mutable());
      assert(extent->get_recorder() == nullptr);
      recorder = nullptr;
    } else if (needs_recording()) {
      mut.emplace(extent->get_mutable());
      auto p_recorder = extent->get_recorder();
      assert(p_recorder != nullptr);
      assert(p_recorder->node_type() == NODE_TYPE);
      assert(p_recorder->field_type() == FIELD_TYPE);
      recorder = static_cast<recorder_t*>(p_recorder);
    } else if (needs_mutate()) {
      // mut is empty
      assert(extent->get_recorder() == nullptr ||
             extent->get_recorder()->is_empty());
      recorder = nullptr;
    } else {
      ceph_abort("impossible path");
    }
#ifndef NDEBUG
    auto ref_recorder = recorder_t::create();
    test_recorder = static_cast<recorder_t*>(ref_recorder.get());
    test_extent = TestReplayExtent::create(
        extent->get_length(), std::move(ref_recorder));
#endif
  }
  ~NodeExtentAccessorT() = default;
  NodeExtentAccessorT(const NodeExtentAccessorT&) = delete;
  NodeExtentAccessorT(NodeExtentAccessorT&&) = delete;
  NodeExtentAccessorT& operator=(const NodeExtentAccessorT&) = delete;
  NodeExtentAccessorT& operator=(NodeExtentAccessorT&&) = delete;

  const node_stage_t& read() const { return node_stage; }
  laddr_t get_laddr() const { return extent->get_laddr(); }

  // must be called before any mutate attempes.
  // for the safety of mixed read and mutate, call before read.
  void prepare_mutate(context_t c) {
    if (needs_mutate()) {
      auto ref_recorder = recorder_t::create();
      recorder = static_cast<recorder_t*>(ref_recorder.get());
      extent = extent->mutate(c, std::move(ref_recorder));
      assert(needs_recording());
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
    assert(!needs_mutate());
    if (needs_recording()) {
      recorder->template encode_insert<KT>(
          key, value, insert_pos, insert_stage, insert_size);
    }
#ifndef NDEBUG
    test_extent->prepare_replay(extent);
    test_recorder->template encode_insert<KT>(
        key, value, insert_pos, insert_stage, insert_size);
#endif
    auto ret = layout_t::template insert<KT>(
        *mut, read(), key, value,
        insert_pos, insert_stage, insert_size);
#ifndef NDEBUG
    test_extent->replay_and_verify(extent);
#endif
    return ret;
  }

  void split_replayable(StagedIterator& split_at) {
    assert(!needs_mutate());
    if (needs_recording()) {
      recorder->encode_split(split_at, read().p_start());
    }
#ifndef NDEBUG
    test_extent->prepare_replay(extent);
    test_recorder->template encode_split(split_at, read().p_start());
#endif
    layout_t::split(*mut, read(), split_at);
#ifndef NDEBUG
    test_extent->replay_and_verify(extent);
#endif
  }

  template <KeyT KT>
  const value_t* split_insert_replayable(
      StagedIterator& split_at,
      const full_key_t<KT>& key,
      const value_t& value,
      position_t& insert_pos,
      match_stage_t& insert_stage,
      node_offset_t& insert_size) {
    assert(!needs_mutate());
    if (needs_recording()) {
      recorder->template encode_split_insert<KT>(
          split_at, key, value, insert_pos, insert_stage, insert_size,
          read().p_start());
    }
#ifndef NDEBUG
    test_extent->prepare_replay(extent);
    test_recorder->template encode_split_insert<KT>(
        split_at, key, value, insert_pos, insert_stage, insert_size,
        read().p_start());
#endif
    auto ret = layout_t::template split_insert<KT>(
        *mut, read(), split_at, key, value,
        insert_pos, insert_stage, insert_size);
#ifndef NDEBUG
    test_extent->replay_and_verify(extent);
#endif
    return ret;
  }

  void update_child_addr_replayable(
      const laddr_t new_addr, laddr_packed_t* p_addr) {
    assert(!needs_mutate());
    if (needs_recording()) {
      recorder->encode_update_child_addr(new_addr, p_addr, read().p_start());
    }
#ifndef NDEBUG
    test_extent->prepare_replay(extent);
    test_recorder->encode_update_child_addr(new_addr, p_addr, read().p_start());
#endif
    layout_t::update_child_addr(*mut, new_addr, p_addr);
#ifndef NDEBUG
    test_extent->replay_and_verify(extent);
#endif
  }

  void test_copy_to(NodeExtentMutable& to) const {
    assert(extent->get_length() == to.get_length());
    std::memcpy(to.get_write(), extent->get_read(), extent->get_length());
  }

 private:
  /**
   * Possible states with CachedExtent::extent_state_t:
   *   INITIAL_WRITE_PENDING -- can mutate, no recording
   *   MUTATION_PENDING      -- can mutate, needs recording
   *   CLEAN/DIRTY           -- pending mutate
   *   INVALID               -- impossible
   */
  bool no_recording() const {
    return extent->is_initial_pending();
  }
  bool needs_recording() const {
    return extent->is_mutation_pending();
  }
  bool needs_mutate() const {
    assert(extent->is_valid());
    return !extent->is_pending();
  }

  NodeExtentRef extent;
  node_stage_t node_stage;
  std::optional<NodeExtentMutable> mut;
  // owned by extent
  recorder_t* recorder;

#ifndef NDEBUG
  // verify record replay using a different memory block
  TestReplayExtent::Ref test_extent;
  recorder_t* test_recorder;
#endif
};

}
