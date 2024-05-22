// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/logging.h"

#include "node_extent_manager.h"
#include "node_delta_recorder.h"
#include "node_layout_replayable.h"
#include "value.h"

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
 public:
  using layout_t = NodeLayoutReplayableT<FieldType, NODE_TYPE>;
  using node_stage_t = typename layout_t::node_stage_t;
  using position_t = typename layout_t::position_t;
  using StagedIterator = typename layout_t::StagedIterator;
  using value_input_t = typename layout_t::value_input_t;
  static constexpr auto FIELD_TYPE = layout_t::FIELD_TYPE;

  ~DeltaRecorderT() override = default;

  template <KeyT KT>
  void encode_insert(
      const full_key_t<KT>& key,
      const value_input_t& value,
      const position_t& insert_pos,
      const match_stage_t& insert_stage,
      const node_offset_t& insert_size) {
    ceph::encode(node_delta_op_t::INSERT, encoded);
    encode_key(key, encoded);
    encode_value(value, encoded);
    insert_pos.encode(encoded);
    ceph::encode(insert_stage, encoded);
    ceph::encode(insert_size, encoded);
  }

  void encode_split(
      const StagedIterator& split_at,
      const char* p_node_start) {
    ceph::encode(node_delta_op_t::SPLIT, encoded);
    split_at.encode(p_node_start, encoded);
  }

  template <KeyT KT>
  void encode_split_insert(
      const StagedIterator& split_at,
      const full_key_t<KT>& key,
      const value_input_t& value,
      const position_t& insert_pos,
      const match_stage_t& insert_stage,
      const node_offset_t& insert_size,
      const char* p_node_start) {
    ceph::encode(node_delta_op_t::SPLIT_INSERT, encoded);
    split_at.encode(p_node_start, encoded);
    encode_key(key, encoded);
    encode_value(value, encoded);
    insert_pos.encode(encoded);
    ceph::encode(insert_stage, encoded);
    ceph::encode(insert_size, encoded);
  }

  void encode_update_child_addr(
      const laddr_t new_addr,
      const laddr_packed_t* p_addr,
      const char* p_node_start,
      extent_len_t node_size) {
    ceph::encode(node_delta_op_t::UPDATE_CHILD_ADDR, encoded);
    ceph::encode(new_addr, encoded);
    int node_offset = reinterpret_cast<const char*>(p_addr) - p_node_start;
    assert(node_offset > 0 && node_offset < (int)node_size);
    ceph::encode(static_cast<node_offset_t>(node_offset), encoded);
  }

  void encode_erase(
      const position_t& erase_pos) {
    ceph::encode(node_delta_op_t::ERASE, encoded);
    erase_pos.encode(encoded);
  }

  void encode_make_tail() {
    ceph::encode(node_delta_op_t::MAKE_TAIL, encoded);
  }

  static DeltaRecorderURef create_for_encode(const ValueBuilder& v_builder) {
    return std::unique_ptr<DeltaRecorder>(new DeltaRecorderT(v_builder));
  }

  static DeltaRecorderURef create_for_replay() {
    return std::unique_ptr<DeltaRecorder>(new DeltaRecorderT());
  }

 protected:
  DeltaRecorderT() : DeltaRecorder() {}
  DeltaRecorderT(const ValueBuilder& vb) : DeltaRecorder(vb) {}
  node_type_t node_type() const override { return NODE_TYPE; }
  field_type_t field_type() const override { return FIELD_TYPE; }
  void apply_delta(ceph::bufferlist::const_iterator& delta,
                   NodeExtentMutable& mut,
                   const NodeExtent& node) override {
    LOG_PREFIX(OTree::Extent::Replay);
    assert(is_empty());
    node_stage_t stage(reinterpret_cast<const FieldType*>(mut.get_read()),
                       mut.get_length());
    node_delta_op_t op;
    try {
      ceph::decode(op, delta);
      switch (op) {
      case node_delta_op_t::INSERT: {
        SUBDEBUG(seastore_onode, "decoding INSERT ...");
        auto key = key_hobj_t::decode(delta);
        auto value = decode_value(delta);
        auto insert_pos = position_t::decode(delta);
        match_stage_t insert_stage;
        ceph::decode(insert_stage, delta);
        node_offset_t insert_size;
        ceph::decode(insert_size, delta);
        SUBDEBUG(seastore_onode,
            "apply {}, {}, insert_pos({}), insert_stage={}, "
            "insert_size={}B ...",
            key, value, insert_pos, insert_stage, insert_size);
        layout_t::template insert<KeyT::HOBJ>(
          mut, stage, key, value, insert_pos, insert_stage, insert_size);
        break;
      }
      case node_delta_op_t::SPLIT: {
        SUBDEBUG(seastore_onode, "decoding SPLIT ...");
        auto split_at = StagedIterator::decode(
            mut.get_read(), mut.get_length(), delta);
        SUBDEBUG(seastore_onode, "apply split_at={} ...", split_at);
        layout_t::split(mut, stage, split_at);
        break;
      }
      case node_delta_op_t::SPLIT_INSERT: {
        SUBDEBUG(seastore_onode, "decoding SPLIT_INSERT ...");
        auto split_at = StagedIterator::decode(
            mut.get_read(), mut.get_length(), delta);
        auto key = key_hobj_t::decode(delta);
        auto value = decode_value(delta);
        auto insert_pos = position_t::decode(delta);
        match_stage_t insert_stage;
        ceph::decode(insert_stage, delta);
        node_offset_t insert_size;
        ceph::decode(insert_size, delta);
        SUBDEBUG(seastore_onode,
            "apply split_at={}, {}, {}, insert_pos({}), insert_stage={}, "
            "insert_size={}B ...",
            split_at, key, value, insert_pos, insert_stage, insert_size);
        layout_t::template split_insert<KeyT::HOBJ>(
          mut, stage, split_at, key, value, insert_pos, insert_stage, insert_size);
        break;
      }
      case node_delta_op_t::UPDATE_CHILD_ADDR: {
        SUBDEBUG(seastore_onode, "decoding UPDATE_CHILD_ADDR ...");
        laddr_t new_addr;
        ceph::decode(new_addr, delta);
        node_offset_t update_offset;
        ceph::decode(update_offset, delta);
        auto p_addr = reinterpret_cast<laddr_packed_t*>(
            mut.get_write() + update_offset);
        SUBDEBUG(seastore_onode,
            "apply {:#x} to offset {:#x} ...",
            new_addr, update_offset);
        layout_t::update_child_addr(mut, new_addr, p_addr);
        break;
      }
      case node_delta_op_t::ERASE: {
        SUBDEBUG(seastore_onode, "decoding ERASE ...");
        auto erase_pos = position_t::decode(delta);
        SUBDEBUG(seastore_onode, "apply erase_pos({}) ...", erase_pos);
        layout_t::erase(mut, stage, erase_pos);
        break;
      }
      case node_delta_op_t::MAKE_TAIL: {
        SUBDEBUG(seastore_onode, "decoded MAKE_TAIL, apply ...");
        layout_t::make_tail(mut, stage);
        break;
      }
      case node_delta_op_t::SUBOP_UPDATE_VALUE: {
        SUBDEBUG(seastore_onode, "decoding SUBOP_UPDATE_VALUE ...");
        node_offset_t value_header_offset;
        ceph::decode(value_header_offset, delta);
        auto p_header = mut.get_read() + value_header_offset;
        auto p_header_ = reinterpret_cast<const value_header_t*>(p_header);
        SUBDEBUG(seastore_onode, "update {} at {:#x} ...", *p_header_, value_header_offset);
        auto payload_mut = p_header_->get_payload_mutable(mut);
        auto value_addr = node.get_laddr() + payload_mut.get_node_offset();
        get_value_replayer(p_header_->magic)->apply_value_delta(
            delta, payload_mut, value_addr);
        break;
      }
      default:
        SUBERROR(seastore_onode,
            "got unknown op {} when replay {}",
            op, node);
        ceph_abort("fatal error");
      }
    } catch (buffer::error& e) {
      SUBERROR(seastore_onode,
          "got decode error {} when replay {}",
          e.what(), node);
      ceph_abort("fatal error");
    }
  }

 private:
  ValueDeltaRecorder* get_value_replayer(value_magic_t magic) {
    // Replay procedure is independent of Btree and happens at lower level in
    // seastore. There is no ValueBuilder so the recoder needs to build the
    // ValueDeltaRecorder by itself.
    if (value_replayer) {
      if (value_replayer->get_header_magic() != magic) {
        ceph_abort_msgf("OTree::Extent::Replay: value magic mismatch %x != %x",
                        value_replayer->get_header_magic(), magic);
      }
    } else {
      value_replayer = build_value_recorder_by_type(encoded, magic);
      if (!value_replayer) {
        ceph_abort_msgf("OTree::Extent::Replay: got unexpected value magic = %x",
                        magic);
      }
    }
    return value_replayer.get();
  }

  void encode_value(const value_input_t& value, ceph::bufferlist& encoded) const {
    if constexpr (std::is_same_v<value_input_t, laddr_t>) {
      // NODE_TYPE == node_type_t::INTERNAL
      ceph::encode(value, encoded);
    } else if constexpr (std::is_same_v<value_input_t, value_config_t>) {
      // NODE_TYPE == node_type_t::LEAF
      value.encode(encoded);
    } else {
      ceph_abort("impossible path");
    }
  }

  value_input_t decode_value(ceph::bufferlist::const_iterator& delta) const {
    if constexpr (std::is_same_v<value_input_t, laddr_t>) {
      // NODE_TYPE == node_type_t::INTERNAL
      laddr_t value;
      ceph::decode(value, delta);
      return value;
    } else if constexpr (std::is_same_v<value_input_t, value_config_t>) {
      // NODE_TYPE == node_type_t::LEAF
      return value_config_t::decode(delta);
    } else {
      ceph_abort("impossible path");
    }
  }

  std::unique_ptr<ValueDeltaRecorder> value_replayer;
};

/**
 * NodeExtentAccessorT
 *
 * This component is responsible to reference and mutate the underlying
 * NodeExtent, record mutation parameters when needed, and apply the recorded
 * modifications for a specific node layout.
 *
 * For possible internal states, see node_types.h.
 */
template <typename FieldType, node_type_t NODE_TYPE>
class NodeExtentAccessorT {
 public:
  using layout_t = NodeLayoutReplayableT<FieldType, NODE_TYPE>;
  using node_stage_t = typename layout_t::node_stage_t;
  using position_t = typename layout_t::position_t;
  using recorder_t = DeltaRecorderT<FieldType, NODE_TYPE>;
  using StagedIterator = typename layout_t::StagedIterator;
  using value_input_t = typename layout_t::value_input_t;
  using value_t = typename layout_t::value_t;
  static constexpr auto FIELD_TYPE = layout_t::FIELD_TYPE;

  NodeExtentAccessorT(NodeExtentRef extent)
      : extent{extent},
        node_stage{reinterpret_cast<const FieldType*>(extent->get_read()),
                   extent->get_length()} {
    assert(is_valid_node_size(extent->get_length()));
    if (extent->is_initial_pending()) {
      state = nextent_state_t::FRESH;
      mut.emplace(extent->get_mutable());
      assert(extent->get_recorder() == nullptr);
      recorder = nullptr;
    } else if (extent->is_mutation_pending()) {
      state = nextent_state_t::MUTATION_PENDING;
      mut.emplace(extent->get_mutable());
      auto p_recorder = extent->get_recorder();
      assert(p_recorder != nullptr);
      assert(p_recorder->node_type() == NODE_TYPE);
      assert(p_recorder->field_type() == FIELD_TYPE);
      recorder = static_cast<recorder_t*>(p_recorder);
    } else if (!extent->is_mutable() && extent->is_valid()) {
      state = nextent_state_t::READ_ONLY;
      // mut is empty
      assert(extent->get_recorder() == nullptr ||
             extent->get_recorder()->is_empty());
      recorder = nullptr;
    } else {
      // extent is invalid or retired
      ceph_abort("impossible path");
    }
#ifndef NDEBUG
    auto ref_recorder = recorder_t::create_for_replay();
    test_recorder = static_cast<recorder_t*>(ref_recorder.get());
    test_extent = TestReplayExtent::create(
        get_length(), std::move(ref_recorder));
#endif
  }
  ~NodeExtentAccessorT() = default;
  NodeExtentAccessorT(const NodeExtentAccessorT&) = delete;
  NodeExtentAccessorT(NodeExtentAccessorT&&) = delete;
  NodeExtentAccessorT& operator=(const NodeExtentAccessorT&) = delete;
  NodeExtentAccessorT& operator=(NodeExtentAccessorT&&) = delete;

  const node_stage_t& read() const { return node_stage; }
  laddr_t get_laddr() const { return extent->get_laddr(); }
  extent_len_t get_length() const {
    auto len = extent->get_length();
    assert(is_valid_node_size(len));
    return len;
  }
  nextent_state_t get_state() const {
    assert(!is_retired());
    // we cannot rely on the underlying extent state because
    // FRESH/MUTATION_PENDING can become DIRTY after transaction submission.
    return state;
  }

  bool is_retired() const {
    if (extent) {
      return false;
    } else {
      return true;
    }
  }

  // must be called before any mutate attempes.
  // for the safety of mixed read and mutate, call before read.
  void prepare_mutate(context_t c) {
    assert(!is_retired());
    if (state == nextent_state_t::READ_ONLY) {
      assert(!extent->is_mutable());
      auto ref_recorder = recorder_t::create_for_encode(c.vb);
      recorder = static_cast<recorder_t*>(ref_recorder.get());
      extent = extent->mutate(c, std::move(ref_recorder));
      state = nextent_state_t::MUTATION_PENDING;
      assert(extent->is_mutation_pending());
      node_stage = node_stage_t(reinterpret_cast<const FieldType*>(extent->get_read()),
                                get_length());
      assert(recorder == static_cast<recorder_t*>(extent->get_recorder()));
      mut.emplace(extent->get_mutable());
    }
    assert(extent->is_mutable());
  }

  template <KeyT KT>
  const value_t* insert_replayable(
      const full_key_t<KT>& key,
      const value_input_t& value,
      position_t& insert_pos,
      match_stage_t& insert_stage,
      node_offset_t& insert_size) {
    assert(extent->is_mutable());
    assert(state != nextent_state_t::READ_ONLY);
    if (state == nextent_state_t::MUTATION_PENDING) {
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
    assert(extent->is_mutable());
    assert(state != nextent_state_t::READ_ONLY);
    if (state == nextent_state_t::MUTATION_PENDING) {
      recorder->encode_split(split_at, read().p_start());
    }
#ifndef NDEBUG
    test_extent->prepare_replay(extent);
    test_recorder->encode_split(split_at, read().p_start());
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
      const value_input_t& value,
      position_t& insert_pos,
      match_stage_t& insert_stage,
      node_offset_t& insert_size) {
    assert(extent->is_mutable());
    assert(state != nextent_state_t::READ_ONLY);
    if (state == nextent_state_t::MUTATION_PENDING) {
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
    assert(extent->is_mutable());
    assert(state != nextent_state_t::READ_ONLY);
    if (state == nextent_state_t::MUTATION_PENDING) {
      recorder->encode_update_child_addr(
          new_addr, p_addr, read().p_start(), get_length());
    }
#ifndef NDEBUG
    test_extent->prepare_replay(extent);
    test_recorder->encode_update_child_addr(
        new_addr, p_addr, read().p_start(), get_length());
#endif
    layout_t::update_child_addr(*mut, new_addr, p_addr);
#ifndef NDEBUG
    test_extent->replay_and_verify(extent);
#endif
  }

  std::tuple<match_stage_t, position_t> erase_replayable(const position_t& pos) {
    assert(extent->is_mutable());
    assert(state != nextent_state_t::READ_ONLY);
    if (state == nextent_state_t::MUTATION_PENDING) {
      recorder->encode_erase(pos);
    }
#ifndef NDEBUG
    test_extent->prepare_replay(extent);
    test_recorder->encode_erase(pos);
#endif
    auto ret = layout_t::erase(*mut, read(), pos);
#ifndef NDEBUG
    test_extent->replay_and_verify(extent);
#endif
    return ret;
  }

  position_t make_tail_replayable() {
    assert(extent->is_mutable());
    assert(state != nextent_state_t::READ_ONLY);
    if (state == nextent_state_t::MUTATION_PENDING) {
      recorder->encode_make_tail();
    }
#ifndef NDEBUG
    test_extent->prepare_replay(extent);
    test_recorder->encode_make_tail();
#endif
    auto ret = layout_t::make_tail(*mut, read());
#ifndef NDEBUG
    test_extent->replay_and_verify(extent);
#endif
    return ret;
  }

  std::pair<NodeExtentMutable&, ValueDeltaRecorder*>
  prepare_mutate_value_payload(context_t c) {
    prepare_mutate(c);
    ValueDeltaRecorder* p_value_recorder = nullptr;
    if (state == nextent_state_t::MUTATION_PENDING) {
      p_value_recorder = recorder->get_value_recorder();
    }
    return {*mut, p_value_recorder};
  }

  void test_copy_to(NodeExtentMutable& to) const {
    assert(extent->get_length() == to.get_length());
    std::memcpy(to.get_write(), extent->get_read(), get_length());
  }

  eagain_ifuture<NodeExtentMutable> rebuild(context_t c, laddr_t hint) {
    LOG_PREFIX(OTree::Extent::rebuild);
    assert(!is_retired());
    if (state == nextent_state_t::FRESH) {
      assert(extent->is_initial_pending());
      // already fresh and no need to record
      return eagain_iertr::make_ready_future<NodeExtentMutable>(*mut);
    }
    assert(!extent->is_initial_pending());
    auto alloc_size = get_length();
    return c.nm.alloc_extent(c.t, hint, alloc_size
    ).handle_error_interruptible(
      eagain_iertr::pass_further{},
      crimson::ct_error::input_output_error::assert_failure(
          [FNAME, c, alloc_size, l_to_discard = extent->get_laddr()] {
        SUBERRORT(seastore_onode,
            "EIO during allocate -- node_size={}, to_discard={:x}",
            c.t, alloc_size, l_to_discard);
      })
    ).si_then([this, c, FNAME] (auto fresh_extent) {
      SUBDEBUGT(seastore_onode,
          "update addr from {:#x} to {:#x} ...",
          c.t, extent->get_laddr(), fresh_extent->get_laddr());
      assert(fresh_extent);
      assert(fresh_extent->is_initial_pending());
      assert(fresh_extent->get_recorder() == nullptr);
      assert(get_length() == fresh_extent->get_length());
      auto fresh_mut = fresh_extent->get_mutable();
      std::memcpy(fresh_mut.get_write(), extent->get_read(), get_length());
      NodeExtentRef to_discard = extent;

      extent = fresh_extent;
      node_stage = node_stage_t(reinterpret_cast<const FieldType*>(extent->get_read()),
                                get_length());
      state = nextent_state_t::FRESH;
      mut.emplace(fresh_mut);
      recorder = nullptr;

      return c.nm.retire_extent(c.t, to_discard
      ).handle_error_interruptible(
        eagain_iertr::pass_further{},
        crimson::ct_error::input_output_error::assert_failure(
            [FNAME, c, l_to_discard = to_discard->get_laddr(),
             l_fresh = fresh_extent->get_laddr()] {
          SUBERRORT(seastore_onode,
              "EIO during retire -- to_disgard={:x}, fresh={:x}",
              c.t, l_to_discard, l_fresh);
        }),
        crimson::ct_error::enoent::assert_failure(
            [FNAME, c, l_to_discard = to_discard->get_laddr(),
             l_fresh = fresh_extent->get_laddr()] {
          SUBERRORT(seastore_onode,
              "ENOENT during retire -- to_disgard={:x}, fresh={:x}",
              c.t, l_to_discard, l_fresh);
        })
      );
    }).si_then([this, c] {
      boost::ignore_unused(c);  // avoid clang warning;
      assert(!c.t.is_conflicted());
      return *mut;
    });
  }

  eagain_ifuture<> retire(context_t c) {
    LOG_PREFIX(OTree::Extent::retire);
    assert(!is_retired());
    auto addr = extent->get_laddr();
    return c.nm.retire_extent(c.t, std::move(extent)
    ).handle_error_interruptible(
      eagain_iertr::pass_further{},
      crimson::ct_error::input_output_error::assert_failure(
          [FNAME, c, addr] {
        SUBERRORT(seastore_onode, "EIO -- addr={:x}", c.t, addr);
      }),
      crimson::ct_error::enoent::assert_failure(
          [FNAME, c, addr] {
        SUBERRORT(seastore_onode, "ENOENT -- addr={:x}", c.t, addr);
      })
#ifndef NDEBUG
    ).si_then([c] {
      assert(!c.t.is_conflicted());
    }
#endif
    );
  }

 private:
  NodeExtentRef extent;
  node_stage_t node_stage;
  nextent_state_t state;
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
