// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/onode_manager/staged-fltree/node_delta_recorder.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager.h"

/** test_replay.h
 *
 * A special version of NodeExtent to help verify delta encode, decode and
 * replay in recorder_t under debug build.
 */

namespace crimson::os::seastore::onode {

class TestReplayExtent final: public NodeExtent {
 public:
  using Ref = crimson::os::seastore::TCachedExtentRef<TestReplayExtent>;

  void prepare_replay(NodeExtentRef from_extent) {
    assert(get_length() == from_extent->get_length());
    auto mut = do_get_mutable();
    std::memcpy(mut.get_write(), from_extent->get_read(), get_length());
  }

  void replay_and_verify(NodeExtentRef replayed_extent) {
    assert(get_length() == replayed_extent->get_length());
    auto mut = do_get_mutable();
    auto bl = recorder->get_delta();
    assert(bl.length());
    auto p = bl.cbegin();
    recorder->apply_delta(p, mut, *this);
    assert(p == bl.end());
    auto cmp = std::memcmp(get_read(), replayed_extent->get_read(), get_length());
    ceph_assert(cmp == 0 && "replay mismatch!");
  }

  static Ref create(extent_len_t length, DeltaRecorderURef&& recorder) {
    auto r = ceph::buffer::create_aligned(length, 4096);
    auto bp = ceph::bufferptr(std::move(r));
    return new TestReplayExtent(std::move(bp), std::move(recorder));
  }

 protected:
  NodeExtentRef mutate(context_t, DeltaRecorderURef&&) override {
    ceph_abort("impossible path"); }
  DeltaRecorder* get_recorder() const override {
    ceph_abort("impossible path"); }
  CachedExtentRef duplicate_for_write(Transaction&) override {
    ceph_abort("impossible path"); }
  extent_types_t get_type() const override {
    return extent_types_t::TEST_BLOCK; }
  ceph::bufferlist get_delta() override {
    ceph_abort("impossible path"); }
  void apply_delta(const ceph::bufferlist&) override {
    ceph_abort("impossible path"); }

 private:
  TestReplayExtent(ceph::bufferptr&& ptr, DeltaRecorderURef&& recorder)
      : NodeExtent(std::move(ptr)), recorder(std::move(recorder)) {
    state = extent_state_t::MUTATION_PENDING;
  }
  DeltaRecorderURef recorder;
};

}
