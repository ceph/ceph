// SPDX-License-Identifier: LGPL-2.1-or-later
#pragma once

#include <deque>
#include <functional>
#include <optional>
#include <utility>
#include <variant>
#include <vector>

namespace ECWriteJournal {

// The drain request latches admission off, including after a timed-out stop.
// Waiters own their completion state so a shutdown timeout cannot leave a
// callback referencing the shutdown thread's stack.
class DrainWaiters {
  bool draining = false;
  std::vector<std::function<void(int)>> waiters;
 public:
  bool requested() const { return draining; }
  void request(std::function<void(int)> done) {
    draining = true;
    waiters.push_back(std::move(done));
  }
  void finish(int result) {
    auto done = std::move(waiters);
    waiters.clear();
    for (auto& cb : done) {
      cb(result);
    }
  }
  void maybe_finish(bool journal_empty, bool admission_empty) {
    if (journal_empty && admission_empty) {
      finish(0);
    }
  }
};

// Admission barriers must preserve the same order as writes. Once a write is
// admitted, the existing RMW pipeline/cache handles its physical ordering.
// All access is serialized by the PG lock; callbacks may enqueue more work.
template<typename Op>
class AdmissionQueue {
  std::deque<std::variant<Op, std::function<void()>>> entries;
 public:
  void push(Op op) { entries.emplace_back(std::move(op)); }
  void ordered(std::function<void()> cb) { entries.emplace_back(std::move(cb)); }
  bool empty() const { return entries.empty(); }
  size_t size() const { return entries.size(); }
  void clear() { entries.clear(); }
  void pop() { entries.pop_front(); }
  template<typename F>
  void for_each_op(F&& f) {
    for (auto& entry : entries) {
      if (auto op = std::get_if<Op>(&entry)) {
        f(*op);
      }
    }
  }

  // A blocked write remains at the front. Never forward a later log barrier
  // until that write has entered the ordinary pipeline.
  template<typename Forward>
  std::optional<Op> next(Forward&& forward) {
    while (!entries.empty()) {
      if (auto op = std::get_if<Op>(&entries.front())) {
        return *op;
      }
      auto cb = std::move(std::get<std::function<void()>>(entries.front()));
      entries.pop_front();
      forward(std::move(cb));
    }
    return std::nullopt;
  }
};

} // namespace ECWriteJournal