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

  /// return the cancellation type of the yield_context, or none
  boost::asio::cancellation_type cancelled() const {
    if (y) {
      return y->cancelled();
    } else {
      return boost::asio::cancellation_type::none;
    }
  }
};

// type tag object to construct an empty optional_yield
static constexpr optional_yield::empty_t null_yield{};
