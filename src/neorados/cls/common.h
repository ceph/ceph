// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM
 *
 * See file COPYING for license information.
 *
 */

#pragma once

#include <concepts>
#include <coroutine>
#include <cstddef>
#include <string>
#include <type_traits>

#include <boost/asio/async_result.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/asio/experimental/co_composed.hpp>

#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>

#include "include/neorados/RADOS.hpp"

#include "include/buffer.h"
#include "include/encoding.h"

/// \file neorados/cls/common.h
///
/// \brief Helpers for writing simple CLS clients
///
/// For writing functions that call out to the OSD, perform a CLS
/// call, and return its data in a user friendly way, we need to
/// coordinate asynchronous operation with decoding and returning the
/// relevant data.
///
/// Ideally, these functions will help.

namespace neorados::cls {
/// \brief Perform a CLS read operation and return the result
///
/// Asynchronously call into the OSD, decode its response and
/// pass it to a supplied function that extracts the relevant
/// information.
///
/// \tparam Rep The type of the CLS operation's result that will be
///             passed to `f`
///
/// \param r RADOS handle
/// \param oid Object name
/// \param ioc IOContext locator
/// \param cls Name of the object class
/// \param method Object class method to call
/// \param req Request (parameters to the class method). Pass nullptr if there
///            is no request structure.
/// \param f Function to extract/process call result
/// \param token Boost.Asio CompletionToken
///
/// \return The relevant data in a way appropriate to the completion
/// token. See Boost.Asio documentation. The signature is the return
/// type of `f`.
template<std::default_initializable Rep, typename Req,
	 std::invocable<Rep&&> F,
	 std::default_initializable Ret = std::invoke_result_t<F&&, Rep&&>,
	 boost::asio::completion_token_for<
	   void(boost::system::error_code, Ret)> CompletionToken>
auto exec(
  RADOS& r,
  Object oid,
  IOContext ioc,
  std::string cls,
  std::string method,
  const Req& req,
  F&& f,
  CompletionToken&& token)
{
  namespace asio = boost::asio;
  namespace buffer = ceph::buffer;
  using boost::system::error_code;
  using boost::system::system_error;

  buffer::list in;
  if (!std::is_same_v<Req, std::nullptr_t>) {
    encode(req, in);
  }
// In this case, the warning is spurious as the 'mismatched' `operator
// new` calls directly into the matching `operator new`, returning its
// result.
  return asio::async_initiate<CompletionToken,
			      void(error_code, Ret)>
    (asio::experimental::co_composed<void(error_code, Ret)>
     ([](auto state, RADOS& r, Object oid, IOContext ioc, std::string cls,
	 std::string method, buffer::list in, F&& f) -> void {
       try {
	 ReadOp op;
	 buffer::list out;
	 error_code ec;
	 op.exec(cls, method, std::move(in), &out, &ec);
	 co_await r.execute(std::move(oid), std::move(ioc), std::move(op),
			    nullptr, asio::deferred);
	 if (ec) {
	   co_return {ec, Ret{}};
	 }
	 Rep rep;
	 decode(rep, out);
	 co_return {error_code{},
	            std::invoke(std::forward<F>(f), std::move(rep))};
       } catch (const system_error& e) {
	 co_return {e.code(), Ret{}};
       }
     }, r.get_executor()),
     token, std::ref(r), std::move(oid), std::move(ioc), std::move(cls),
     std::move(method), std::move(in), std::forward<F>(f));
}
} // namespace neorados::cls
