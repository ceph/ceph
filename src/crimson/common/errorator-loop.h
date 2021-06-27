// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include <seastar/core/future.hh>

#include "crimson/common/errorator.h"


namespace crimson {
template <class... AllowedErrors>
class parallel_for_each_state final : private seastar::continuation_base<> {
  using future_t = typename errorator<AllowedErrors...>::template future<>;
  std::vector<future_t> _incomplete;
  seastar::promise<> _result;
  std::exception_ptr _ex;
private:
  void wait_for_one() noexcept {
    while (!_incomplete.empty() && _incomplete.back().available()) {
      if (_incomplete.back().failed()) {
        _ex = _incomplete.back().get_exception();
      }
      _incomplete.pop_back();
    }
    if (!_incomplete.empty()) {
      seastar::internal::set_callback(_incomplete.back(), static_cast<continuation_base<>*>(this));
      _incomplete.pop_back();
      return;
    }
    if (__builtin_expect(bool(_ex), false)) {
      _result.set_exception(std::move(_ex));
    } else {
      _result.set_value();
    }
    delete this;
  }
  virtual void run_and_dispose() noexcept override {
    if (_state.failed()) {
      _ex = std::move(_state).get_exception();
    }
    _state = {};
    wait_for_one();
  }
  task* waiting_task() noexcept override { return _result.waiting_task(); }
public:
  parallel_for_each_state(size_t n) {
    _incomplete.reserve(n);
  }
  void add_future(future_t&& f) {
    _incomplete.push_back(std::move(f));
  }
  future_t get_future() {
    auto ret = _result.get_future();
    wait_for_one();
    return ret;
  }
};

} // namespace crimson
