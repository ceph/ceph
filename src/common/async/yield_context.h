// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <boost/asio/io_context.hpp>

#include "acconfig.h"

#ifndef HAVE_BOOST_CONTEXT

// hide the dependencies on boost::context and boost::coroutines
namespace boost::asio {
struct yield_context;
}

#else // HAVE_BOOST_CONTEXT
#ifndef BOOST_COROUTINES_NO_DEPRECATION_WARNING
#define BOOST_COROUTINES_NO_DEPRECATION_WARNING
#endif
#include <boost/asio/spawn.hpp>

#endif // HAVE_BOOST_CONTEXT


/// optional-like wrapper for a boost::asio::yield_context and its associated
/// boost::asio::io_context. operations that take an optional_yield argument
/// will, when passed a non-empty yield context, suspend this coroutine instead
/// of the blocking the thread of execution
class optional_yield {
  boost::asio::io_context *c = nullptr;
  boost::asio::yield_context *y = nullptr;
 public:
  /// construct with a valid io and yield_context
  explicit optional_yield(boost::asio::io_context& c,
                          boost::asio::yield_context& y) noexcept
    : c(&c), y(&y) {}

  /// type tag to construct an empty object
  struct empty_t {};
  optional_yield(empty_t) noexcept {}

  /// implicit conversion to bool, returns true if non-empty
  operator bool() const noexcept { return y; }

  /// return a reference to the associated io_context. only valid if non-empty
  boost::asio::io_context& get_io_context() const noexcept { return *c; }

  /// return a reference to the yield_context. only valid if non-empty
  boost::asio::yield_context& get_yield_context() const noexcept { return *y; }
};

// type tag object to construct an empty optional_yield
static constexpr optional_yield::empty_t null_yield{};
