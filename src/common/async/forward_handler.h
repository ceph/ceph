// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#ifndef CEPH_ASYNC_FORWARD_HANDLER_H
#define CEPH_ASYNC_FORWARD_HANDLER_H

#include <boost/asio/associated_allocator.hpp>
#include <boost/asio/associated_executor.hpp>

namespace ceph::async {

/**
 * A forwarding completion handler for use with boost::asio.
 *
 * A completion handler wrapper that invokes the handler's operator() as an
 * rvalue, regardless of whether the wrapper is invoked as an lvalue or rvalue.
 * This operation is potentially destructive to the wrapped handler, so is only
 * suitable for single-use handlers.
 *
 * This is useful when combined with bind_handler() and move-only arguments,
 * because executors will always call the lvalue overload of operator().
 *
 * The original Handler's associated allocator and executor are maintained.
 *
 * @see forward_handler
 */
template <typename Handler>
struct ForwardingHandler {
  Handler handler;

  ForwardingHandler(Handler&& handler)
    : handler(std::move(handler))
  {}

  template <typename ...Args>
  void operator()(Args&& ...args) {
    std::move(handler)(std::forward<Args>(args)...);
  }

  using allocator_type = boost::asio::associated_allocator_t<Handler>;
  allocator_type get_allocator() const noexcept {
    return boost::asio::get_associated_allocator(handler);
  }

  using cancellation_slot_type = boost::asio::associated_cancellation_slot_t<Handler>;
  cancellation_slot_type get_cancellation_slot() const noexcept {
    return boost::asio::get_associated_cancellation_slot(handler);
  }
};

} // namespace ceph::async

namespace boost::asio {

// specialize boost::asio::associated_executor<> for ForwardingHandler
template <typename Handler, typename Executor>
struct associated_executor<ceph::async::ForwardingHandler<Handler>, Executor> {
  using type = boost::asio::associated_executor_t<Handler, Executor>;

  static type get(const ceph::async::ForwardingHandler<Handler>& handler,
                  const Executor& ex = Executor()) noexcept {
    return boost::asio::get_associated_executor(handler.handler, ex);
  }
};

} // namespace boost::asio

namespace ceph::async {

/**
 * Returns a single-use completion handler that always forwards on operator().
 *
 * Wraps a completion handler such that it is always invoked as an rvalue. This
 * is necessary when combining executors and bind_handler() with move-only
 * argument types.
 *
 * Example use:
 *
 *   auto callback = [] (std::unique_ptr<int>&& p) {};
 *   auto bound_handler = bind_handler(callback, std::make_unique<int>(5));
 *   auro handler = forward_handler(std::move(bound_handler));
 *
 *   // execute the forwarding handler on an io_context:
 *   boost::asio::io_context context;
 *   boost::asio::post(context, std::move(handler));
 *   context.run();
 *
 * @see ForwardingHandler
 */
template <typename Handler>
auto forward_handler(Handler&& h)
{
  return ForwardingHandler{std::forward<Handler>(h)};
}

} // namespace ceph::async

#endif // CEPH_ASYNC_FORWARD_HANDLER_H
