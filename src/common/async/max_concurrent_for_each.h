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
#include <iterator>
#include <ranges>
#include <utility>
#include <boost/asio/spawn.hpp>
#include "cancel_on_error.h"
#include "yield_context.h"
#include "spawn_throttle.h"

namespace ceph::async {

/// Call a coroutine with each element in the given range then wait for all of
/// them to complete. The first exception is rethrown to the caller. The
/// cancel_on_error option controls whether these exceptions trigger the
/// cancellation of other children. The number of outstanding coroutines
/// is limited by the max_concurrent argument.
///
/// Example:
/// \code
/// void child(task& t, boost::asio::yield_context yield);
///
/// void parent(std::span<task> tasks, optional_yield y)
/// {
///   // process all tasks, up to 10 at a time
///   max_concurrent_for_each(tasks, 10, y, child);
/// }
/// \endcode
template <typename Iterator, typename Sentinel, typename Func,
          typename Reference = std::iter_reference_t<Iterator>>
    requires (std::input_iterator<Iterator> &&
              std::sentinel_for<Sentinel, Iterator> &&
              std::invocable<Func, Reference, boost::asio::yield_context>)
void max_concurrent_for_each(Iterator begin,
                             Sentinel end,
                             size_t max_concurrent,
                             optional_yield y,
                             Func&& func,
                             cancel_on_error on_error = cancel_on_error::none)
{
  const size_t count = std::ranges::distance(begin, end);
  if (!count) {
    return;
  }
  auto throttle = spawn_throttle{y, max_concurrent, on_error};
  for (Iterator i = begin; i != end; ++i) {
    boost::asio::spawn(throttle.get_executor(),
                       [&func, &val = *i] (boost::asio::yield_context yield) {
                         func(val, yield);
                       }, throttle);
  }
  throttle.wait();
}

/// \overload
template <typename Range, typename Func,
          typename Reference = std::ranges::range_reference_t<Range>>
    requires (std::ranges::range<Range> &&
              std::invocable<Func, Reference, boost::asio::yield_context>)
auto max_concurrent_for_each(Range&& range,
                             size_t max_concurrent,
                             optional_yield y,
                             Func&& func,
                             cancel_on_error on_error = cancel_on_error::none)
{
  return max_concurrent_for_each(std::begin(range), std::end(range),
                                 max_concurrent, y, std::forward<Func>(func),
                                 on_error);
}

// TODO: overloads for co_spawn()

} // namespace ceph::async
