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

/// \file common/async/coro_aiocomplete.h
///
/// \brief Contains the `coro_aiocomplete` function for briding C++20
/// coroutines to librados::AioCompletion.

#include <concepts>
#include <exception>
#include <type_traits>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>

#include <boost/system/error_code.hpp>

#include "include/rados/librados.hpp"

#include "common/dout.h"
#include "common/error_code.h"

#include "librados/AioCompletionImpl.h"

namespace ceph::async {
namespace detail {
inline void do_complete_aiocompletion(const DoutPrefixProvider* dpp,
                                      librados::AioCompletion* c,
                                      std::exception_ptr eptr, int r)
{
  namespace sys = boost::system;
  if (!eptr) {
    librados::CB_AioCompleteAndSafe{c->pc}(r);
  } else try {
      std::rethrow_exception(eptr);
    } catch (const sys::system_error& e) {
      if (dpp) {
	ldpp_dout(dpp, 1) << "coro_aicomplete: Got system_error: " << e.what()
			  << ", passing to AioCompletionHandler as "
			  << ceph::from_error_code(e.code()) << dendl;
      }
      librados::CB_AioCompleteAndSafe{c->pc}(ceph::from_error_code(e.code()));
    } catch (const std::exception& e) {
      if (dpp) {
	ldpp_dout(dpp, 1) << "coro_aicomplete: Got exception: " << e.what()
			  << ", passing to AioCompletionHandler as EFAULT"
			  << dendl;
      }
      librados::CB_AioCompleteAndSafe{c->pc}(-EFAULT);
    }
}
}

/// \brief Call coroutine, completing with RADOS AIoCompletion
///
/// For bridging between C++20 coroutines and librados::AioCompletion
///
/// \param dpp DoutPrefixProvider, may be null.
/// \param executor The executor on which to call the function
/// \param coro An awaitable, must return void
/// \param c The AioCompletion
template<typename AwaitExecutor>
void coro_aiocomplete(const DoutPrefixProvider* dpp,
		      auto executor,
		      boost::asio::awaitable<void, AwaitExecutor>&& coro,
		      librados::AioCompletion* c)
{
  namespace asio = boost::asio;
  asio::co_spawn(
    executor, std::move(coro),
    [dpp, c](std::exception_ptr eptr) {
      detail::do_complete_aiocompletion(dpp, c, eptr, 0);
    });
}

/// \brief Call coroutine, completing with RADOS AIoCompletion
///
/// For bridging between C++20 coroutines and librados::AioCompletion
///
/// \param dpp DoutPrefixProvider, may be null.
/// \param executor The executor on which to call the function
/// \param coro An awaitable, must return int
/// \param c The AioCompletion
template<typename AwaitExecutor>
void coro_aiocomplete(const DoutPrefixProvider* dpp,
		      auto executor,
		      boost::asio::awaitable<int, AwaitExecutor>&& coro,
		      librados::AioCompletion* c)
{
  namespace asio = boost::asio;
  asio::co_spawn(
    executor, std::move(coro),
    [dpp, c](std::exception_ptr eptr, int r) {
      detail::do_complete_aiocompletion(dpp, c, eptr, r);
    });
}

/// \brief Call coroutine, completing with RADOS AIoCompletion
///
/// For bridging between C++20 coroutines and librados::AioCompletion
///
/// \param dpp DoutPrefixProvider, may be null.
/// \param executor The executor on which to call the function
/// \param coro An awaitable, must return boost::system::error_code
/// \param c The AioCompletion
template <typename AwaitExecutor>
void coro_aiocomplete(const DoutPrefixProvider* dpp, auto executor,
		      boost::asio::awaitable<boost::system::error_code,
		                             AwaitExecutor>&& coro,
		      librados::AioCompletion* c)
{
  namespace asio = boost::asio;
  namespace sys = boost::system;
  asio::co_spawn(
    executor, std::move(coro),
    [dpp, c](std::exception_ptr eptr, sys::error_code ec) {
      detail::do_complete_aiocompletion(dpp, c, eptr,
					ceph::from_error_code(ec));
    });
}
}
