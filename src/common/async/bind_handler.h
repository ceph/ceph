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

#ifndef CEPH_ASYNC_BIND_HANDLER_H
#define CEPH_ASYNC_BIND_HANDLER_H

#include <tuple>
#include <boost/asio/associator.hpp>

namespace ceph::async {

/**
 * A bound completion handler for use with boost::asio.
 *
 * A completion handler wrapper that allows a tuple of arguments to be forwarded
 * to the original Handler. This is intended for use with boost::asio functions
 * like defer(), dispatch() and post() which expect handlers which are callable
 * with no arguments.
 *
 * The original Handler's associated allocator and executor are maintained.
 *
 * @see bind_handler
 */
template <typename Handler, typename Tuple>
struct CompletionHandler {
  Handler handler;
  Tuple args;

  CompletionHandler(Handler&& handler, Tuple&& args)
    : handler(std::move(handler)),
      args(std::move(args))
  {}

  void operator()() & {
    std::apply(handler, args);
  }
  void operator()() const & {
    std::apply(handler, args);
  }
  void operator()() && {
    std::apply(std::move(handler), std::move(args));
  }
};

} // namespace ceph::async

namespace boost::asio {

// forward the handler's associated executor, allocator, cancellation slot, etc
template <template <typename, typename> class Associator,
          typename Handler, typename Tuple, typename DefaultCandidate>
struct associator<Associator,
    ceph::async::CompletionHandler<Handler, Tuple>, DefaultCandidate>
  : Associator<Handler, DefaultCandidate>
{
  static auto get(const ceph::async::CompletionHandler<Handler, Tuple>& h) noexcept {
    return Associator<Handler, DefaultCandidate>::get(h.handler);
  }
  static auto get(const ceph::async::CompletionHandler<Handler, Tuple>& h,
                  const DefaultCandidate& c) noexcept {
    return Associator<Handler, DefaultCandidate>::get(h.handler, c);
  }
};

} // namespace boost::asio

namespace ceph::async {

/**
 * Returns a wrapped completion handler with bound arguments.
 *
 * Binds the given arguments to a handler, and returns a CompletionHandler that
 * is callable with no arguments. This is similar to std::bind(), except that
 * all arguments must be provided. Move-only argument types are supported as
 * long as the CompletionHandler's 'operator() &&' overload is used, i.e.
 * std::move(handler)().
 *
 * Example use:
 *
 *   // bind the arguments (5, "hello") to a callback lambda:
 *   auto callback = [] (int a, std::string b) {};
 *   auto handler = bind_handler(callback, 5, "hello");
 *
 *   // execute the bound handler on an io_context:
 *   boost::asio::io_context context;
 *   boost::asio::post(context, std::move(handler));
 *   context.run();
 *
 * @see CompletionHandler
 */
template <typename Handler, typename ...Args>
auto bind_handler(Handler&& h, Args&& ...args)
{
  return CompletionHandler{std::forward<Handler>(h),
                           std::make_tuple(std::forward<Args>(args)...)};
}

} // namespace ceph::async

#endif // CEPH_ASYNC_BIND_HANDLER_H
