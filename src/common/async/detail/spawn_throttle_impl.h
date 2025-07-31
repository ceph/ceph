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

#include <exception>
#include <optional>
#include <memory>
#include <utility>
#include <boost/asio/append.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/execution/context.hpp>
#include <boost/asio/query.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include "common/async/cancel_on_error.h"
#include "common/async/service.h"

namespace ceph::async::detail {

struct spawn_throttle_handler;

// Reference-counted spawn throttle implementation.
class spawn_throttle_impl :
    public boost::intrusive_ref_counter<spawn_throttle_impl,
        boost::thread_unsafe_counter>,
    public service_list_base_hook
{
 public:
  spawn_throttle_impl(boost::asio::yield_context yield,
                      size_t limit, cancel_on_error on_error)
    : svc(boost::asio::use_service<service<spawn_throttle_impl>>(
              boost::asio::query(yield.get_executor(),
                                 boost::asio::execution::context))),
      yield(yield), limit(limit), on_error(on_error),
      children(std::make_unique<child[]>(limit))
  {
    // register for service_shutdown() notifications
    svc.add(*this);

    // initialize the free list
    for (size_t i = 0; i < limit; i++) {
      free.push_back(children[i]);
    }
  }

  ~spawn_throttle_impl()
  {
    svc.remove(*this);
  }

  // return the completion handler for a new child. may block due to throttling
  // or rethrow an exception from a previously-spawned child
  spawn_throttle_handler get();

  // track each spawned coroutine for cancellation. these are stored in an
  // array, and recycled after each use via the free list
  struct child : boost::intrusive::list_base_hook<> {
    std::optional<boost::asio::cancellation_signal> signal;
  };

  using executor_type = boost::asio::any_io_executor;
  executor_type get_executor()
  {
    return yield.get_executor();
  }

  // wait until count <= target_count
  void wait_for(size_t target_count)
  {
    if (count > target_count) {
      wait_for_count = target_count;

      boost::asio::async_initiate<boost::asio::yield_context, WaitSignature>(
          [this] (auto handler) {
            auto slot = get_associated_cancellation_slot(handler);
            if (slot.is_connected()) {
              slot.template emplace<op_cancellation>(this);
            }
            waiter.emplace(std::move(handler));
          }, yield);
      // this is a coroutine, so the wait has completed by this point
    }

    report_exception(); // throw unreported exception
  }

  // cancel outstanding coroutines
  void cancel()
  {
    cancel_outstanding_from(outstanding.begin());
    if (waiter) {
      wait_complete(make_error_code(boost::asio::error::operation_aborted));
    }
  }

  // complete the given child coroutine
  void on_complete(child& c, std::exception_ptr eptr)
  {
    --count;

    // move back to the free list
    auto next = outstanding.erase(outstanding.iterator_to(c));
    c.signal.reset();
    free.push_back(c);

    if (eptr && !unreported_exception) {
      // hold on to the first child exception until we can report it in wait()
      // or completion()
      unreported_exception = eptr;

      // handle cancel_on_error
      auto cancel_from = outstanding.end();
      if (on_error == cancel_on_error::after) {
        cancel_from = next;
      } else if (on_error == cancel_on_error::all) {
        cancel_from = outstanding.begin();
      }
      cancel_outstanding_from(cancel_from);
    }

    if (waiter && count <= wait_for_count) {
      wait_complete({});
    }
  }


  void service_shutdown()
  {
    waiter.reset();
  }

 private:
  service<spawn_throttle_impl>& svc;
  boost::asio::yield_context yield;
  const size_t limit;
  const cancel_on_error on_error;
  size_t count = 0;

  void report_exception()
  {
    if (unreported_exception) {
      std::rethrow_exception(std::exchange(unreported_exception, nullptr));
    }
  }

  std::exception_ptr unreported_exception;
  std::unique_ptr<child[]> children;

  using child_list = boost::intrusive::list<child,
        boost::intrusive::constant_time_size<false>>;
  child_list outstanding;
  child_list free;

  void cancel_outstanding_from(child_list::iterator i)
  {
    while (i != outstanding.end()) {
      // increment before cancellation, which may invoke on_complete()
      // directly and remove the child from this list
      child& c = *i++;
      c.signal->emit(boost::asio::cancellation_type::terminal);
    }
  }

  using WaitSignature = void(boost::system::error_code);
  struct wait_state {
    using Work = boost::asio::executor_work_guard<
        boost::asio::any_io_executor>;
    using Handler = typename boost::asio::async_result<
        boost::asio::yield_context, WaitSignature>::handler_type;

    Work work;
    Handler handler;

    explicit wait_state(Handler&& h)
      : work(make_work_guard(h)),
        handler(std::move(h))
    {}
  };
  std::optional<wait_state> waiter;
  size_t wait_for_count = 0;

  struct op_cancellation {
    spawn_throttle_impl* self;
    explicit op_cancellation(spawn_throttle_impl* self) noexcept
      : self(self) {}
    void operator()(boost::asio::cancellation_type type) {
      if (type != boost::asio::cancellation_type::none) {
        self->cancel();
      }
    }
  };

  void wait_complete(boost::system::error_code ec)
  {
    auto w = std::move(*waiter);
    waiter.reset();
    boost::asio::dispatch(boost::asio::append(std::move(w.handler), ec));
  }
};

// A cancellable spawn() completion handler that notifies the spawn_throttle
// upon completion. This holds a reference to the implementation in order to
// extend its lifetime. This is required for per-op cancellation because the
// cancellation_signals must outlive these coroutine stacks.
struct spawn_throttle_handler {
  boost::intrusive_ptr<spawn_throttle_impl> impl;
  spawn_throttle_impl::child& c;
  boost::asio::cancellation_slot slot;

  spawn_throttle_handler(boost::intrusive_ptr<spawn_throttle_impl> impl,
                         spawn_throttle_impl::child& c)
    : impl(std::move(impl)), c(c), slot(c.signal->slot())
  {}

  using executor_type = spawn_throttle_impl::executor_type;
  executor_type get_executor() const noexcept
  {
    return impl->get_executor();
  }

  using cancellation_slot_type = boost::asio::cancellation_slot;
  cancellation_slot_type get_cancellation_slot() const noexcept
  {
    return slot;
  }

  void operator()(std::exception_ptr eptr)
  {
    impl->on_complete(c, eptr);
  }
};

inline spawn_throttle_handler spawn_throttle_impl::get()
{
  report_exception(); // throw unreported exception

  if (count >= limit) {
    wait_for(limit - 1);
  }

  ++count;

  // move a free child to the outstanding list
  child& c = free.front();
  free.pop_front();
  outstanding.push_back(c);

  // spawn the coroutine with its associated cancellation signal
  c.signal.emplace();
  return {this, c};
}

} // namespace ceph::async::detail
