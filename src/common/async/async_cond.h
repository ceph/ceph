// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

/// \file common/async/async_cond.h

#include <cassert>
#include <concepts>
#include <mutex>
#include <utility>
#include <vector>

#include <boost/asio/any_completion_handler.hpp>
#include <boost/asio/append.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/consign.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/executor_work_guard.hpp>

#include <boost/system/error_code.hpp>

namespace ceph::async {
/// \brief A non-blocking condition variable
///
/// This is effectively a condition variable, but rather than
/// blocking, the `wait` function takes an Asio completion token and
/// invokes the associated handler on wakeup.
template<typename Executor>
class async_cond {
  Executor executor;
  std::mutex m;
  bool done = false;
  std::vector<std::pair<
    boost::asio::any_completion_handler<
    void(boost::system::error_code)>, std::unique_lock<std::mutex>*>> handlers;

public:

  /// \brief Constructor
  ///
  /// \param executor The executor on which to post handlers.
  async_cond(Executor executor)
    : executor(executor) {}

  /// \brief Destructor
  ///
  /// Will call `reset`, dispatching all handlers with
  /// `asio::error::operation_aborted`.
  ~async_cond() {
    reset();
  }

  async_cond(const async_cond&) = delete;
  async_cond& operator =(const async_cond&) = delete;
  async_cond(async_cond&&) = delete;
  async_cond& operator =(async_cond&&) = delete;


  /// \brief Wait for notification
  ///
  /// This will dispatch the handler for the provided completion token
  /// when `notify` is called. If `notify` has already been called,
  /// dispatch immediately.
  ///
  /// \param token Boost.Asio completion token.
  ///
  /// \returns Whatever is appropriate to the completion token. See
  /// Boost.Asio documentation.
  template<boost::asio::completion_token_for<void(boost::system::error_code)>
	   CompletionToken>
  auto wait(std::unique_lock<std::mutex>& caller_lock, CompletionToken&& token) {
    namespace asio = boost::asio;
    namespace sys = boost::system;
    assert(caller_lock.owns_lock());
    auto consigned = asio::consign(
      std::forward<CompletionToken>(token), asio::make_work_guard(
	asio::get_associated_executor(token, get_executor())));
    return asio::async_initiate<decltype(consigned), void(sys::error_code)>(
      [this, &caller_lock](auto handler) {
	std::unique_lock l(m);
	if (done) {
	  caller_lock.unlock();
	  asio::post(executor,
		     [handler = std::move(handler), &caller_lock]() mutable {
		       caller_lock.lock();
		       std::move(handler)(sys::error_code{});
		     });
	} else {
	  handlers.emplace_back(std::move(handler), &caller_lock);
	  caller_lock.unlock();
	}
      }, consigned);
  }

  /// \brief Dispatch all handlers currently waiting
  ///
  /// Dispatches all handlers currently waiting. After this function
  /// is called, any new calls to `wait` will return immediately.
  void notify() {
    namespace asio = boost::asio;
    namespace sys = boost::system;
    std::unique_lock l(m);
    done = true;
    if (!handlers.empty()) {
      auto workhandlers = std::move(handlers);
      handlers.resize(0);
      l.unlock();
      for (auto&& [handler, lock] : workhandlers) {
	asio::post(executor,
		   [handler = std::move(handler), lock]() mutable {
		     lock->lock();
		     std::move(handler)(sys::error_code{});
		   });

      }
    }
  }

  /// \brief Dispatch all handlers currently waiting with an error
  ///
  /// This wakes all handlers currently waiting and dispatches them with
  /// `asio::error::operation_aborted`.
  void reset() {
    namespace asio = boost::asio;
    std::unique_lock l(m);
    done = false;
    if (!handlers.empty()) {
      auto workhandlers = std::move(handlers);
      handlers.resize(0);
      l.unlock();
      for (auto&& [handler, lock] : workhandlers) {
	asio::post(executor,
		   [handler = std::move(handler), lock]() mutable {
		     lock->lock();
		     std::move(handler)(asio::error::operation_aborted);
		   });

      }
    }
  }

  /// \brief Type of the executor we dispatch on
  using executor_type = Executor;

  /// \brief Return the executor we dispatch on
  auto get_executor() const {
    return executor;
  }
};
}
