// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <fmt/format.h>
#include <seastar/core/condition-variable.hh>
#include "crimson/osd/osd_operations/client_request.h"

namespace crimson::osd {

// when PG interval changes, we are supposed to interrupt all in-flight ops.
// but in the order in which the ops are interrupted are not determined
// because they are scheduled by the seastar scheduler, if we just interrupt
// them at seeing a different interval when moving to a new continuation. but
// we are supposed to replay the ops from the same client targeting the same
// PG in the exact order that they are received.
//
// the way how we address this problem is to set up a blocker which blocks an
// op until the preceding op is unblocked if the blocked one is issued in a new
// pg interval.
//
// here, the ops from the same client are grouped by PG, and then ordered by
// their id which is monotonically increasing and unique on per PG basis, so we
// can keep an op waiting in the case explained above.
class OpSequencer {
  bool resequencing{false};

  static bool is_unfinished(const ClientRequest& this_op) {
    // TODO: kill the tombstone; reuse op status tracking.
    return !this_op.finished;
  }

  std::uint64_t get_prev_id(const ClientRequest& this_op,
                            const OSDOperationRegistry& registry) {
    // an alternative to iterating till the registy's beginning could be
    // holding a pointer to next(last_completed).
    constexpr auto type_idx = static_cast<size_t>(ClientRequest::type);
    for (auto it = OSDOperationRegistry::op_list::s_iterator_to(this_op);
         it != std::begin(registry.get_registry<type_idx>());
         --it) {
      // we're iterating over the operation registry of all ClientRequests.
      // this list aggrates every single instance in the system, and thus
      // we need to skip operations coming from different client's session
      // or targeting different PGs.
      // as this is supposed to happen on cold paths only, the overhead is
      // a thing we can live with.
      auto* maybe_prev_op = std::addressof(static_cast<const ClientRequest&>(*it));
      if (maybe_prev_op->same_session_and_pg(this_op)) {
        if (is_unfinished(*maybe_prev_op)) {
          return maybe_prev_op->get_id();
        } else {
          // an early exited one
        }
      }
    }
    // the prev op of this session targeting the same PG as this_op must has
    // been completed and hence already has been removed from the list, that's
    // the only way we got here
    return last_completed_id;
  }

public:
  template <typename HandleT,
            typename FuncT,
            typename Result = std::invoke_result_t<FuncT>>
  seastar::futurize_t<Result>
  start_op(const ClientRequest& op,
           HandleT& handle,
           const OSDOperationRegistry& registry,
           FuncT&& do_op) {
    ::crimson::get_logger(ceph_subsys_osd).debug(
      "OpSequencer::{}: op={}, last_started={}, last_unblocked={}, last_completed={}",
      __func__, op.get_id(), last_started_id, last_unblocked_id, last_completed_id);
    auto have_green_light = seastar::make_ready_future<>();
    if (last_started_id < op.get_id()) {
      // starting a new op, let's advance the last_started!
      last_started_id = op.get_id();
    }
    if (__builtin_expect(resequencing, false)) {
      // this implies that there was a reset condition and there may me some
      // older ops before me, so i have to wait until they are unblocked.
      //
      // i should leave the current pipeline stage when waiting for the blocked
      // ones, so that the following ops can be queued up here. we cannot let
      // the seastar scheduler to determine the order of performing these ops,
      // once they are unblocked after the first op of the same pg interval is
      // scheduled.
      const auto prev_id = get_prev_id(op, registry);
      assert(prev_id >= last_unblocked_id);
      handle.exit();
      ::crimson::get_logger(ceph_subsys_osd).debug(
        "OpSequencer::start_op: {} resequencing ({} >= {})",
        op, prev_id, last_unblocked_id);
      have_green_light = unblocked.wait([&op, &registry, this] {
        // wait until the previous op is unblocked
        const bool unblocking =
          get_prev_id(op, registry) == last_unblocked_id;
        if (unblocking) {
          // stop resequencing if everything is handled which means there is no
          // operation after us. the range could be minimized by snapshotting
          // `last_started` on `maybe_reset()`.
          // `<=` is to handle the situation when `last_started` has finished out-
          // of-the-order.
          resequencing = !(last_started_id <= op.get_id());
        }
        return unblocking;
      });
    }
    return have_green_light.then([&op, do_op=std::move(do_op), this]() mutable {
      auto result = seastar::futurize_invoke(std::move(do_op));
      // unblock the next one
      last_unblocked_id = op.get_id();
      unblocked.broadcast();
      return result;
    });
  }
  void finish_op_in_order(ClientRequest& op) {
    ::crimson::get_logger(ceph_subsys_osd).debug(
      "OpSequencer::{}: op={}, last_started={}, last_unblocked={}, last_completed={}",
      __func__, op.get_id(), last_started_id, last_unblocked_id, last_completed_id);
    assert(op.get_id() > last_completed_id);
    last_completed_id = op.get_id();
    op.finished = true;
  }
  void finish_op_out_of_order(ClientRequest& op,
                              const OSDOperationRegistry& registry) {
    ::crimson::get_logger(ceph_subsys_osd).debug(
      "OpSequencer::{}: op={}, last_started={}, last_unblocked={}, last_completed={}",
      __func__, op.get_id(), last_started_id, last_unblocked_id, last_completed_id);
    op.finished = true;
    // fix the `last_unblocked_id`. otherwise we wouldn't be able to leave
    // the wait loop in `start_op()` as any concrete value of `last_unblocked_id`
    // can wake at most one blocked operation (the successor of `op` if there is any)
    // and above we lowered this number to 0 (`get_prev_id()` there would never return
    // a value matching `last_unblocked_id`).
    if (last_unblocked_id == op.get_id()) {
      last_unblocked_id = get_prev_id(op, registry);
    }
  }
  void maybe_reset(const ClientRequest& op) {
    ::crimson::get_logger(ceph_subsys_osd).debug(
      "OpSequencer::{}: op={}, last_started={}, last_unblocked={}, last_completed={}",
      __func__, op.get_id(), last_started_id, last_unblocked_id, last_completed_id);
    const auto op_id = op.get_id();
    // pg interval changes, so we need to reenqueue the previously unblocked
    // ops by rewinding the "last_unblock" ID.
    if (op_id <= last_unblocked_id) {
      ::crimson::get_logger(ceph_subsys_osd).debug(
        "OpSequencer::maybe_reset:{}  {} <= {}, resetting to {}",
        op, op_id, last_unblocked_id, last_completed_id);
      last_unblocked_id = last_completed_id;
      resequencing = true;
    }
  }
  void abort() {
    ::crimson::get_logger(ceph_subsys_osd).debug(
      "OpSequencer::{}: last_started={}, last_unblocked={}, last_completed={}",
      __func__, last_started_id, last_unblocked_id, last_completed_id);
    // all blocked ops should be canceled, likely due to the osd is not primary
    // anymore.
    unblocked.broken();
  }
private:
  //          /--- unblocked (in pg pipeline)
  //         |      /--- blocked
  //         V      V
  // |////|.....|.......| <--- last_started
  //      ^     ^       ^
  //      |     |       \- prev_op
  //      |      \--- last_unblocked
  //      last_completed
  //
  // the id of last op which is issued
  std::uint64_t last_started_id = 0;
  // the id of last op which is unblocked
  std::uint64_t last_unblocked_id = 0;
  // the id of last op which is completed
  std::uint64_t last_completed_id = 0;
  seastar::condition_variable unblocked;

  friend fmt::formatter<OpSequencer>;
};


class OpSequencers {
public:
  OpSequencer& get(const spg_t& pgid) {
    return pg_ops.at(pgid);
  }
  OpSequencer& operator[](const spg_t& pgid) {
    // TODO: trim pg_ops if there are too many empty sequencers
    return pg_ops[pgid];
  }
private:
  std::map<spg_t, OpSequencer> pg_ops;
};
} // namespace crimson::osd

template <>
struct fmt::formatter<crimson::osd::OpSequencer> {
  // ignore the format string
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const crimson::osd::OpSequencer& sequencer,
              FormatContext& ctx)
  {
    return fmt::format_to(ctx.out(),
                          "(last_completed={},last_unblocked={},last_started={})",
                          sequencer.last_completed_id,
                          sequencer.last_unblocked_id,
                          sequencer.last_started_id);
  }
};
