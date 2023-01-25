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
#include <boost/asio/async_result.hpp>
#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include "common/async/co_waiter.h"
#include "common/async/service.h"
#include "common/ceph_time.h"
#include "sync/common.h"

namespace rgw::sync::detail {

using lease_clock = ceph::coarse_mono_clock;
using lease_timer = asio::basic_waitable_timer<lease_clock,
      asio::wait_traits<lease_clock>, default_executor>;

// base class for the lease completion state. this contains everything that
// doesn't depend on the coroutine's return type
class lease_state : public ceph::async::service_list_base_hook {
  ceph::async::service<lease_state>* svc = nullptr;
  lease_timer timer;
  asio::cancellation_signal signal;
  std::exception_ptr eptr;
  ceph::async::co_waiter<void, default_executor> waiter;

 public:
  lease_state(default_executor ex) : timer(ex) {}
  ~lease_state()
  {
    if (svc) {
      svc->remove(*this);
    }
  }

  lease_timer& get_timer() { return timer; }

  asio::cancellation_slot get_cancellation_slot() { return signal.slot(); }

  awaitable<void> wait()
  {
    if (!svc) {
      // register for service_shutdown() notifications
      svc = &asio::use_service<ceph::async::service<lease_state>>(
          asio::query(co_await asio::this_coro::executor,
                      asio::execution::context));
      svc->add(*this);
    }
    co_await waiter.get();
  }

  void complete()
  {
    if (waiter.waiting()) {
      waiter.complete(nullptr);
    } else {
      timer.cancel(); // wake the renewal loop
    }
  }

  bool aborted() const { return !!eptr; }

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
    signal.emit(asio::cancellation_type::terminal);
    timer.cancel();
  }

  void service_shutdown()
  {
    waiter.shutdown();
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
  lease_completion_state(default_executor ex) : lease_state(ex) {}

  bool completed() const { return result.has_value(); }

  auto completion_handler()
  {
    return asio::bind_cancellation_slot(get_cancellation_slot(),
        [self = boost::intrusive_ptr{this}] (std::exception_ptr eptr, T val) {
          self->result.emplace(eptr, std::move(val));
          self->complete();
        });
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

// specialization for awaitable<void>
template<>
class lease_completion_state<void> : public lease_state,
    public boost::intrusive_ref_counter<lease_completion_state<void>,
        boost::thread_unsafe_counter>
{
  using result_type = std::exception_ptr;
  std::optional<result_type> result;
 public:
  lease_completion_state(default_executor ex) : lease_state(ex) {}

  bool completed() const { return result.has_value(); }

  auto completion_handler()
  {
    return asio::bind_cancellation_slot(get_cancellation_slot(),
        [self = boost::intrusive_ptr{this}] (std::exception_ptr eptr) {
          self->result = eptr;
          self->complete();
        });
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
auto make_lease_completion_state(default_executor ex)
{
  return boost::intrusive_ptr{new lease_completion_state<T>(ex)};
}

} // namespace rgw::sync::detail
