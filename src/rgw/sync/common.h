// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/system/error_code.hpp>

namespace rgw::sync {

using error_code = boost::system::error_code;

namespace asio = boost::asio;

using default_executor = asio::strand<asio::io_context::executor_type>;

template <typename T>
using awaitable = asio::awaitable<T, default_executor>;

using use_awaitable_t = asio::use_awaitable_t<default_executor>;
static constexpr use_awaitable_t use_awaitable{};

} // namespace rgw::sync
