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
      seastar::internal::set_callback(std::move(_incomplete.back()),
                                      static_cast<continuation_base<>*>(this));
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

template <typename Iterator, typename Func, typename... AllowedErrors>
static inline typename errorator<AllowedErrors...>::template future<>
parallel_for_each(Iterator first, Iterator last, Func&& func) noexcept {
  parallel_for_each_state<AllowedErrors...>* s = nullptr;
  // Process all elements, giving each future the following treatment:
  //   - available, not failed: do nothing
  //   - available, failed: collect exception in ex
  //   - not available: collect in s (allocating it if needed)
  for (;first != last; ++first) {
    auto f = seastar::futurize_invoke(std::forward<Func>(func), *first);
    if (!f.available() || f.failed()) {
      if (!s) {
        auto n = (seastar::internal::iterator_range_estimate_vector_capacity(
              first, last) + 1);
        s = new parallel_for_each_state<AllowedErrors...>(n);
      }
      s->add_future(std::move(f));
    }
  }
  // If any futures were not available, hand off to parallel_for_each_state::start().
  // Otherwise we can return a result immediately.
  if (s) {
    // s->get_future() takes ownership of s (and chains it to one of the futures it contains)
    // so this isn't a leak
    return s->get_future();
  }
  return seastar::make_ready_future<>();
}

} // namespace crimson
