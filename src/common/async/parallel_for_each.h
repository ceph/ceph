// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
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
#include <type_traits>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/execution/executor.hpp>
#include <boost/asio/this_coro.hpp>
#include "spawn_group.h"

namespace ceph::async {

/// Call a coroutine with each element in the given range then wait for all
/// of them to complete. The first exception is rethrown to the caller. The
/// cancel_on_error option controls whether these exceptions trigger the
/// cancellation of other children.
///
/// Example:
/// \code
/// awaitable<void> child(task& t);
///
/// awaitable<void> parent(std::span<task> tasks)
/// {
///   co_await parallel_for_each(tasks.begin(), tasks.end(), child);
/// }
/// \endcode
template <typename Iterator, typename Sentinel, typename VoidAwaitableFactory,
          typename Value = std::iter_reference_t<Iterator>,
          typename VoidAwaitable = std::invoke_result_t<
              VoidAwaitableFactory, Value>,
          typename AwaitableT = typename VoidAwaitable::value_type,
          typename AwaitableExecutor = typename VoidAwaitable::executor_type>
    requires (std::input_iterator<Iterator> &&
              std::sentinel_for<Sentinel, Iterator> &&
              std::same_as<AwaitableT, void> &&
              boost::asio::execution::executor<AwaitableExecutor>)
auto parallel_for_each(Iterator begin, Sentinel end,
                       VoidAwaitableFactory&& factory,
                       cancel_on_error on_error = cancel_on_error::none)
    -> boost::asio::awaitable<void, AwaitableExecutor>
{
  const size_t count = std::ranges::distance(begin, end);
  if (!count) {
    co_return;
  }
  auto ex = co_await boost::asio::this_coro::executor;
  auto group = spawn_group{ex, count, on_error};
  for (Iterator i = begin; i != end; ++i) {
    boost::asio::co_spawn(ex, factory(*i), group);
  }
  co_await group.wait();
}

/// \overload
template <typename Range, typename VoidAwaitableFactory,
          typename Value = std::ranges::range_reference_t<Range>,
          typename VoidAwaitable = std::invoke_result_t<
              VoidAwaitableFactory, Value>,
          typename AwaitableT = typename VoidAwaitable::value_type,
          typename AwaitableExecutor = typename VoidAwaitable::executor_type>
    requires (std::ranges::range<Range> &&
              std::same_as<AwaitableT, void> &&
              boost::asio::execution::executor<AwaitableExecutor>)
auto parallel_for_each(Range&& range, VoidAwaitableFactory&& factory,
                       cancel_on_error on_error = cancel_on_error::none)
    -> boost::asio::awaitable<void, AwaitableExecutor>
{
  return parallel_for_each(std::begin(range), std::end(range),
                           std::move(factory), on_error);
}

} // namespace ceph::async
