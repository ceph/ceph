// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

#include <boost/range/begin.hpp>
#include <boost/range/end.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>

/// optional-like wrapper for a boost::asio::yield_context. operations that take
/// an optional_yield argument will, when passed a non-empty yield context,
/// suspend this coroutine instead of the blocking the thread of execution
class optional_yield {
  boost::asio::yield_context *y = nullptr;
 public:
  /// construct with a valid io and yield_context
  optional_yield(boost::asio::yield_context& y) noexcept : y(&y) {}

  /// type tag to construct an empty object
  struct empty_t {};
  optional_yield(empty_t) noexcept {}

  /// implicit conversion to bool, returns true if non-empty
  operator bool() const noexcept { return y; }

  /// return a reference to the yield_context. only valid if non-empty
  boost::asio::yield_context& get_yield_context() const noexcept { return *y; }

  using executor_type = boost::asio::any_io_executor;

  /// Return the executor associated with the `yield_context` or a
  /// strand in the system executor.
  ///
  /// \note In the case of `null_yield` this creates a new strand on
  /// every call. If it is important that two things share an
  /// executor, copy it rather than calling twice.
  executor_type
  get_executor() const
  {
    return y ? y->get_executor()
             : boost::asio::make_strand(boost::asio::system_executor{});
  }
};

// type tag object to construct an empty optional_yield
static constexpr optional_yield::empty_t null_yield{};
