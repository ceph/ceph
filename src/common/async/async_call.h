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

/// \file common/async/async_call.h
///
/// \brief Wait for functions to run on arbitrary executors

#include <concepts>
#include <type_traits>

#include <boost/asio/compose.hpp>
#include <boost/asio/defer.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>

namespace ceph::async {
/// \brief Dispatch a function on another executor and wait for it to
/// finish
///
/// Useful with strands and similar
///
/// \param executor The executor on which to call the function
/// \param f The function to call
/// \param token Boost.Asio completion token
/// \param ioe Parameter pack of executors and I/O Objects to keep live
///
/// \return The return value of `f` in a way appropriate to the
/// completion token. See Boost.Asio documentation.
template<std::invocable<> F,
	 boost::asio::completion_token_for<
	   void(std::invoke_result_t<F>)> CompletionToken,
	 typename ...IOE>
auto async_dispatch(auto executor, F&& f, CompletionToken&& token, IOE&& ...ioe)
{
  namespace asio = boost::asio;
  return asio::async_compose<
    CompletionToken,
    void(std::invoke_result_t<F>)>(
      [&executor, f = std::move(f)] (auto& self) mutable {
	asio::dispatch(
	  executor,
	  [f = std::move(f), self = std::move(self)]() mutable {
	    self.complete(std::invoke(f));
	  });
      }, token, executor, std::forward<IOE>(ioe)...);
}

/// \brief Dispatch a function on another executor and wait for it to
/// finish
///
/// Useful with strands and similar
///
/// \param executor The executor on which to call the function
/// \param f The function to call
/// \param token Boost.Asio completion token
/// \param ioe Parameter pack of executors and I/O Objects to keep live
///
/// \return The return value of `f` in a way appropriate to the
/// completion token. See Boost.Asio documentation.
template<std::invocable<> F,
	 boost::asio::completion_token_for<void()> CompletionToken,
	 typename ...IOE,
	 typename = std::enable_if_t<std::is_void_v<std::invoke_result_t<F>>>>
auto async_dispatch(auto executor, F&& f, CompletionToken&& token, IOE&& ...ioe)
{
  namespace asio = boost::asio;
  return asio::async_compose<
    CompletionToken, void()>(
      [&executor, f = std::move(f)] (auto& self) mutable {
	asio::dispatch(
	  executor,
	  [f = std::move(f), self = std::move(self)]() mutable {
	    std::invoke(f);
	    self.complete();
	  });
      }, token, executor, std::forward<IOE>(ioe)...);
}

/// \brief Post a function on another executor and wait for it to
/// finish
///
/// Useful with strands and similar
///
/// \param executor The executor on which to call the function
/// \param f The function to call
/// \param token Boost.Asio completion token
/// \param ioe Parameter pack of executors and I/O Objects to keep live
///
/// \return The return value of `f` in a way appropriate to the
/// completion token. See Boost.Asio documentation.
template<std::invocable<> F,
	 boost::asio::completion_token_for<
	   void(std::invoke_result_t<F>)> CompletionToken,
	 typename ...IOE>
auto async_post(auto executor, F&& f, CompletionToken&& token, IOE&& ...ioe)
{
  namespace asio = boost::asio;
  return asio::async_compose<
    CompletionToken,
    void(std::invoke_result_t<F>)>(
      [&executor, f = std::move(f)] (auto& self) mutable {
	asio::post(
	  executor,
	  [f = std::move(f), self = std::move(self)]() mutable {
	    self.complete(std::invoke(f));
	  });
      }, token, executor, std::forward<IOE>(ioe)...);
}

/// \brief Post a function on another executor and wait for it to
/// finish
///
/// Useful with strands and similar
///
/// \param executor The executor on which to call the function
/// \param f The function to call
/// \param token Boost.Asio completion token
/// \param ioe Parameter pack of executors and I/O Objects to keep live
///
/// \return The return value of `f` in a way appropriate to the
/// completion token. See Boost.Asio documentation.
template<std::invocable<> F,
	 boost::asio::completion_token_for<void()> CompletionToken,
	 typename ...IOE,
	 typename = std::enable_if_t<std::is_void_v<std::invoke_result_t<F>>>>
auto async_post(auto executor, F&& f, CompletionToken&& token, IOE&& ...ioe)
{
  namespace asio = boost::asio;
  return asio::async_compose<
    CompletionToken, void()>(
      [&executor, f = std::move(f)] (auto& self) mutable {
	asio::post(
	  executor,
	  [f = std::move(f), self = std::move(self)]() mutable {
	    std::invoke(f);
	    self.complete();
	  });
      }, token, executor, std::forward<IOE>(ioe)...);
}

/// \brief Defer a function on another executor and wait for it to
/// finish
///
/// Useful with strands and similar
///
/// \param executor The executor on which to call the function
/// \param f The function to call
/// \param token Boost.Asio completion token
/// \param ioe Parameter pack of executors and I/O Objects to keep live
///
/// \return The return value of `f` in a way appropriate to the
/// completion token. See Boost.Asio documentation.
template<std::invocable<> F,
	 boost::asio::completion_token_for<
	   void(std::invoke_result_t<F>)> CompletionToken,
	 typename ...IOE>
auto async_defer(auto executor, F&& f, CompletionToken&& token, IOE&& ...ioe)
{
  namespace asio = boost::asio;
  return asio::async_compose<
    CompletionToken,
    void(std::invoke_result_t<F>)>(
      [&executor, f = std::move(f)] (auto& self) mutable {
	asio::defer(
	  executor,
	  [f = std::move(f), self = std::move(self)]() mutable {
	    self.complete(std::invoke(f));
	  });
      }, token, executor, std::forward<IOE>(ioe)...);
}

/// \brief Defer a function on another executor and wait for it to
/// finish
///
/// Useful with strands and similar
///
/// \param executor The executor on which to call the function
/// \param f The function to call
/// \param token Boost.Asio completion token
/// \param ioe Parameter pack of executors and I/O Objects to keep live
///
/// \return The return value of `f` in a way appropriate to the
/// completion token. See Boost.Asio documentation.
template<std::invocable<> F,
	 boost::asio::completion_token_for<void()> CompletionToken,
	 typename ...IOE,
	 typename = std::enable_if_t<std::is_void_v<std::invoke_result_t<F>>>>
auto async_defer(auto executor, F&& f, CompletionToken&& token, IOE&& ...ioe)
{
  namespace asio = boost::asio;
  return asio::async_compose<
    CompletionToken, void()>(
      [&executor, f = std::move(f)] (auto& self) mutable {
	asio::defer(
	  executor,
	  [f = std::move(f), self = std::move(self)]() mutable {
	    std::invoke(f);
	    self.complete();
	  });
      }, token, executor, std::forward<IOE>(ioe)...);
}
}
