// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>

#include "crimson/common/operation.h"
#include "osd/osd_types.h"

namespace crimson::osd {

template <typename>
struct OperationComparator;

template <typename T>
class OperationRepeatSequencer {
public:
  using OpRef = boost::intrusive_ptr<T>;
  using ops_sequence_t = std::map<OpRef, seastar::promise<>>;

  template <typename Func, typename HandleT, typename Result = std::invoke_result_t<Func>>
  seastar::futurize_t<Result> start_op(
      HandleT& handle,
      epoch_t same_interval_since,
      OpRef& op,
      const spg_t& pgid,
      Func&& func) {
    auto& ops = pg_ops[pgid];
    if (!op->pos) {
      [[maybe_unused]] auto [it, inserted] = ops.emplace(op, seastar::promise<>());
      assert(inserted);
      op->pos = it;
    }

    auto curr_op_pos = *(op->pos);
    const bool first = (curr_op_pos == ops.begin());
    auto prev_op_pos = first ? curr_op_pos : std::prev(curr_op_pos);
    auto prev_ops_drained = seastar::now();
    if (epoch_t prev_interval = prev_op_pos->first->interval_start_epoch;
        !first && same_interval_since > prev_interval) {
      // need to wait for previous operations,
      // release the current pipepline stage
      handle.exit();
      auto& [prev_op, prev_op_complete] = *prev_op_pos;
      ::crimson::get_logger(ceph_subsys_osd).debug(
          "{}, same_interval_since: {}, previous op: {}, last_interval_start: {}",
          *op, same_interval_since, prev_op, prev_interval);
      prev_ops_drained = prev_op_complete.get_future();
    } else {
      assert(same_interval_since == prev_interval || first);
    }
    return prev_ops_drained.then(
      [op, same_interval_since, func=std::forward<Func>(func)]() mutable {
      op->interval_start_epoch = same_interval_since;
      auto fut = seastar::futurize_invoke(func);
      auto curr_op_pos = *(op->pos);
      curr_op_pos->second.set_value();
      curr_op_pos->second = seastar::promise<>();
      return fut;
    });
  }

  void finish_op(OpRef& op, const spg_t& pgid, bool interrutped) {
    assert(op->pos);
    auto curr_op_pos = *(op->pos);
    if (interrutped) {
      curr_op_pos->second.set_value();
    }
    pg_ops.at(pgid).erase(curr_op_pos);
  }
private:
  std::map<spg_t, std::map<OpRef, seastar::promise<>, OperationComparator<T>>> pg_ops;
};
template <typename T>
struct OperationComparator {
  bool operator()(
    const typename OperationRepeatSequencer<T>::OpRef& left,
    const typename OperationRepeatSequencer<T>::OpRef& right) const {
    return left->get_id() < right->get_id();
  }
};

} // namespace crimson::osd
