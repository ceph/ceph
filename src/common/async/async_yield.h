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

/// \file common/async/async_yield.h
///
/// \brief Contains the `async_yield` function

#include <concepts>
#include <exception>
#include <type_traits>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/compose.hpp>
#include <boost/asio/co_spawn.hpp>

#include <spawn/spawn.hpp>

namespace ceph::async {

/// \brief Call yielding function and wait for it to complete
///
/// For calling into stackful coroutines from C++20 coroutines or similar.
///
/// \param executor The executor on which to call the function
/// \param f The function to call, must take yield_context
/// \param token Boost.Asio completion token
/// \param ioe Parameter pack of executors and I/O Objects to keep live
///
/// \return The return value of `f` in a way appropriate to the
/// completion token. See Boost.Asio documentation.
template<typename YieldContext,
	 std::invocable<YieldContext> F,
	 boost::asio::completion_token_for<
	   void(std::invoke_result_t<F, YieldContext>)> CompletionToken,
	 typename ...IOE>
auto async_yield(auto executor, F&& f, CompletionToken&& token, IOE&& ...ioe)
{
  namespace asio = boost::asio;
  return asio::async_compose<
    CompletionToken,
    void(std::invoke_result_t<F, YieldContext>)>(
      [&executor, f = std::move(f)](auto& self) mutable {
	spawn::spawn(
	  executor,
	  [f = std::move(f), self = std::move(self)]
	  (YieldContext y) mutable {
	    self.complete(f(y));
	  });
      }, token, executor, std::forward<IOE>(ioe)...);
}
}
