// SPDX-License-Identifier: LGPL-2.1-or-later
#pragma once

#include <functional>
#include <utility>
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

} // namespace ECWriteJournal