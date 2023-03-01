// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/condition-variable.hh>
// pull seastar::timer<...>::timer definitions. FIX SEASTAR or reactor.hh
// is obligatory and should be included everywhere?
#include <seastar/core/reactor.hh>

#include "common/ThrottleInterface.h"

namespace crimson::common {

class Throttle final : public ThrottleInterface {
  size_t max = 0;
  size_t count = 0;
  size_t pending = 0;
  // we cannot change the "count" of seastar::semaphore after it is created,
  // so use condition_variable instead.
  seastar::condition_variable on_free_slots;
public:
  explicit Throttle(size_t m)
    : max(m)
  {}
  int64_t take(int64_t c = 1) override;
  int64_t put(int64_t c = 1) override;
  seastar::future<> get(size_t c);
  size_t get_current() const {
    return count;
  }
  size_t get_max() const {
    return max;
  }
  size_t get_pending() const {
    return pending;
  }
  void reset_max(size_t m);
private:
  bool _should_wait(size_t c) const;
};

} // namespace crimson::common
