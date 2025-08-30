// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <concepts>
#include <exception>
#include <mutex>
#include <variant>

#include "include/function2.hpp"

#include "yield_context.h"
#include "detail/call_once.h"

namespace ceph::async {

/// Stores the result of call_once(), if any.
///
/// The cached Result type must be semiregular (copyable and default-
/// constructible).
template <std::semiregular Result>
class once_result {
 public:
  /// Call f() once and cache its return value.
  ///
  /// If a call has already completed, return the cached result. If a call
  /// is already in progress, wait for its completion and return the result.
  /// If a yield context is provided in y, its coroutine will be suspended
  /// while waiting. f() may also suspend/resume the same coroutine.
  ///
  /// f() must return a Result when called with no arguments. If it throws,
  /// that exception is rethrown by all calls.
  ///
  /// This function is thread-safe.
  template <typename Callable>
  friend Result call_once(once_result& self, optional_yield y, Callable&& f)
      requires std::convertible_to<std::invoke_result_t<Callable>, Result>
  {
    auto lock = std::unique_lock{self.mutex};
    return std::visit(
        fu2::overload(
            [&] (const Init&) { return self.do_init(lock, f); },
            [&] (Wait& w) { return self.do_wait(lock, y, w); },
            [] (const Complete& c) { return do_complete(c); }),
        self.state);
  }

 private:
  std::mutex mutex;

  // state machine: [Init] -> [Wait] -> [Complete]
  using Init = std::monostate;
  using Wait = detail::call_once::WaitState;
  struct Complete {
    Result result;
    std::exception_ptr eptr;
  };
  std::variant<Init, Wait, Complete> state;

  Result do_init(std::unique_lock<std::mutex>& lock, auto&& f)
  {
    // transition to Wait state
    auto& wait = state.template emplace<Wait>();
    lock.unlock();

    Complete complete;
    try {
      complete.result = f();
    } catch (const std::exception&) {
      complete.eptr = std::current_exception();
    }

    lock.lock();
    // wake any waiters
    wait.wake_all();

    // transition to Complete state and return the result
    return do_complete(state.template emplace<Complete>(std::move(complete)));
  }

  Result do_wait(std::unique_lock<std::mutex>& lock,
                 optional_yield y, Wait& wait)
  {
    // wait for the response to an outstanding request
    wait.wait(lock, y);

    // state must be Complete after wakeup
    return do_complete(std::get<Complete>(state));
  }

  static Result do_complete(const Complete& complete)
  {
    if (complete.eptr) { // rethrow cached exception
      std::rethrow_exception(complete.eptr);
    }
    return complete.result; // return cached result
  }
};

} // namespace ceph::async
