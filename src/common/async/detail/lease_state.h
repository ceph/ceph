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

#include <optional>
#include <utility>
#include <boost/asio/append.hpp>
#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include "common/ceph_time.h"

namespace ceph::async::detail {

using lease_clock = ceph::coarse_mono_clock;
using lease_timer = boost::asio::basic_waitable_timer<lease_clock>;

// base class for the lease completion state. this contains everything that
// doesn't depend on the coroutine's return type
class lease_state {
  lease_timer timer;
  boost::asio::cancellation_signal signal;
  std::exception_ptr eptr;

 public:
  lease_state(boost::asio::any_io_executor ex) : timer(ex) {}

  lease_timer& get_timer() { return timer; }

  boost::asio::any_io_executor get_executor() { return timer.get_executor(); }
  boost::asio::cancellation_slot get_cancellation_slot() { return signal.slot(); }

  void complete()
  {
    timer.cancel(); // wake the renewal loop
  }

  void abort(std::exception_ptr e)
  {
    if (!eptr) { // only the first exception is reported
      eptr = e;
      cancel();
    }
  }

  void rethrow()
  {
    if (eptr) {
      std::rethrow_exception(eptr);
    }
  }

  void cancel()
  {
    signal.emit(boost::asio::cancellation_type::terminal);
    timer.cancel();
  }
};

// capture and return the arguments to cr's completion handler
template <typename T>
class lease_completion_state : public lease_state,
    public boost::intrusive_ref_counter<lease_completion_state<T>,
        boost::thread_unsafe_counter>
{
  using result_type = std::pair<std::exception_ptr, T>;
  std::optional<result_type> result;
 public:
  lease_completion_state(boost::asio::any_io_executor ex) : lease_state(ex) {}

  bool completed() const { return result.has_value(); }

  auto completion_handler()
  {
    return bind_cancellation_slot(get_cancellation_slot(),
                                  bind_executor(get_executor(),
        [self = boost::intrusive_ptr{this}] (std::exception_ptr eptr, T val) {
          self->result.emplace(eptr, std::move(val));
          self->complete();
        }));
  }

  T get() // precondition: completed()
  {
    rethrow(); // rethrow exceptions from renewal
    if (auto eptr = std::get<0>(*result); eptr) {
      std::rethrow_exception(eptr);
    }
    return std::get<1>(std::move(*result));
  }
};

// specialization for void return type
template<>
class lease_completion_state<void> : public lease_state,
    public boost::intrusive_ref_counter<lease_completion_state<void>,
        boost::thread_unsafe_counter>
{
  using result_type = std::exception_ptr;
  std::optional<result_type> result;
 public:
  lease_completion_state(boost::asio::any_io_executor ex) : lease_state(ex) {}

  bool completed() const { return result.has_value(); }

  auto completion_handler()
  {
    return bind_cancellation_slot(get_cancellation_slot(),
                                  bind_executor(get_executor(),
        [self = boost::intrusive_ptr{this}] (std::exception_ptr eptr) {
          self->result = eptr;
          self->complete();
        }));
  }

  void get() // precondition: completed()
  {
    rethrow(); // rethrow exceptions from renewal
    if (*result) {
      std::rethrow_exception(*result);
    }
  }
};

template <typename T>
auto make_lease_completion_state(boost::asio::any_io_executor ex)
{
  return boost::intrusive_ptr{new lease_completion_state<T>(ex)};
}

} // namespace ceph::async::detail
