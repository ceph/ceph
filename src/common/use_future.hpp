// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

//
// use_future.hpp
// ~~~~~~~~~~~~~~
//
// Copyright (c) 2003-2016 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (c) 2017 Casey Bodley <cbodley@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef CEPH_USE_FUTURE_HPP
#define CEPH_USE_FUTURE_HPP

#include "future.hpp"
#include <boost/asio/async_result.hpp>

/// boost/asio/use_future.hpp adapted for boost::future
namespace ceph {

/// Class used to specify that an asynchronous operation should return a future.
/**
 * The use_future_t class is used to indicate that an asynchronous operation
 * should return a ceph::future object. A use_future_t object may be passed as a
 * handler to an asynchronous operation, typically using the special value @c
 * ceph::use_future. For example:
 *
 * @code ceph::future<std::size_t> my_future
 *   = my_socket.async_read_some(my_buffer, ceph::use_future); @endcode
 *
 * The initiating function (async_read_some in the above example) returns a
 * future that will receive the result of the operation. If the operation
 * completes with an error_code indicating failure, it is converted into a
 * system_error and passed back to the caller via the future.
 */
template <typename Allocator = std::allocator<void> >
class use_future_t
{
public:
  /// The allocator type. The allocator is used when constructing the
  /// @c ceph::promise object for a given asynchronous operation.
  typedef Allocator allocator_type;

  /// Construct using default-constructed allocator.
  BOOST_ASIO_CONSTEXPR use_future_t()
  {
  }

  /// Construct using specified allocator.
  explicit use_future_t(const Allocator& allocator)
    : allocator_(allocator)
  {
  }

  /// Specify an alternate allocator.
  template <typename OtherAllocator>
  use_future_t<OtherAllocator> operator[](const OtherAllocator& allocator) const
  {
    return use_future_t<OtherAllocator>(allocator);
  }

  /// Obtain allocator.
  allocator_type get_allocator() const
  {
    return allocator_;
  }

private:
  Allocator allocator_;
};

/// A special value, similar to std::nothrow.
/**
 * See the documentation for boost::asio::use_future_t for a usage example.
 */
constexpr use_future_t<> use_future;

namespace detail {

  // Completion handler to adapt a promise as a completion handler.
  template <typename T>
  class promise_handler
  {
  public:
    // Construct from use_future special value.
    template <typename Alloc>
    promise_handler(use_future_t<Alloc> uf)
      : promise_(std::allocate_shared<ceph::promise<T> >(
            typename Alloc::template rebind<char>::other(uf.get_allocator()),
            std::allocator_arg,
            typename Alloc::template rebind<char>::other(uf.get_allocator())))
    {
    }

    void operator()(T t)
    {
      promise_->set_value(t);
    }

    void operator()(const boost::system::error_code& ec, T t)
    {
      if (ec)
        promise_->set_exception(
            boost::copy_exception(
              boost::system::system_error(ec)));
      else
        promise_->set_value(t);
    }

  //private:
    std::shared_ptr<ceph::promise<T> > promise_;
  };

  // Completion handler to adapt a void promise as a completion handler.
  template <>
  class promise_handler<void>
  {
  public:
    // Construct from use_future special value. Used during rebinding.
    template <typename Alloc>
    promise_handler(use_future_t<Alloc> uf)
      : promise_(std::allocate_shared<ceph::promise<void> >(
            typename Alloc::template rebind<char>::other(uf.get_allocator()),
            std::allocator_arg,
            typename Alloc::template rebind<char>::other(uf.get_allocator())))
    {
    }

    void operator()()
    {
      promise_->set_value();
    }

    void operator()(const boost::system::error_code& ec)
    {
      if (ec)
        promise_->set_exception(
            boost::copy_exception(
              boost::system::system_error(ec)));
      else
        promise_->set_value();
    }

  //private:
    std::shared_ptr<ceph::promise<void> > promise_;
  };

  // Ensure any exceptions thrown from the handler are propagated back to the
  // caller via the future.
  template <typename Function, typename T>
  void asio_handler_invoke(Function f, promise_handler<T>* h)
  {
    std::shared_ptr<ceph::promise<T> > p(h->promise_);
    try
    {
      f();
    }
    catch (...)
    {
      p->set_exception(boost::current_exception());
    }
  }

} // namespace detail
} // namespace ceph

namespace boost {
namespace asio {

// Handler traits specialisation for promise_handler.
template <typename T>
class async_result<ceph::detail::promise_handler<T> >
{
public:
  // The initiating function will return a future.
  typedef ceph::future<T> type;

  // Constructor creates a new promise for the async operation, and obtains the
  // corresponding future.
  explicit async_result(ceph::detail::promise_handler<T>& h)
  {
    value_ = h.promise_->get_future();
  }

  // Obtain the future to be returned from the initiating function.
  type get() { return std::move(value_); }

private:
  type value_;
};

// Handler type specialisation for use_future.
template <typename Allocator, typename ReturnType>
struct handler_type<ceph::use_future_t<Allocator>, ReturnType()>
{
  typedef ceph::detail::promise_handler<void> type;
};

// Handler type specialisation for use_future.
template <typename Allocator, typename ReturnType, typename Arg1>
struct handler_type<ceph::use_future_t<Allocator>, ReturnType(Arg1)>
{
  typedef ceph::detail::promise_handler<Arg1> type;
};

// Handler type specialisation for use_future.
template <typename Allocator, typename ReturnType>
struct handler_type<ceph::use_future_t<Allocator>,
    ReturnType(boost::system::error_code)>
{
  typedef ceph::detail::promise_handler<void> type;
};

// Handler type specialisation for use_future.
template <typename Allocator, typename ReturnType, typename Arg2>
struct handler_type<ceph::use_future_t<Allocator>,
    ReturnType(boost::system::error_code, Arg2)>
{
  typedef ceph::detail::promise_handler<Arg2> type;
};

} // namespace asio
} // namespace boost

#endif // CEPH_USE_FUTURE_HPP
