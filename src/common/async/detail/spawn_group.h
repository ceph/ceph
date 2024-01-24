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

#include <exception>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/execution/executor.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include "common/async/cancel_on_error.h"
#include "common/async/co_waiter.h"
#include "common/async/service.h"
#include "include/scope_guard.h"

namespace ceph::async::detail {

template <boost::asio::execution::executor Executor>
class spawn_group_impl;

// A cancellable co_spawn() completion handler that notifies the spawn_group
// upon completion. This holds a reference to the implementation in order to
// extend its lifetime. This is required for per-op cancellation because the
// cancellation_signals must outlive these coroutine frames.
template <typename Executor>
class spawn_group_handler {
  using impl_type = spawn_group_impl<Executor>;
  using size_type = typename impl_type::size_type;
  boost::intrusive_ptr<impl_type> impl;
  boost::asio::cancellation_slot slot;
  size_type index;
 public:
  spawn_group_handler(boost::intrusive_ptr<impl_type> impl,
                      boost::asio::cancellation_slot slot, size_type index)
      : impl(std::move(impl)), slot(std::move(slot)), index(index)
  {}

  using executor_type = typename impl_type::executor_type;
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
    impl->child_complete(index, eptr);
  }
};

// Reference-counted spawn group implementation.
template <boost::asio::execution::executor Executor>
class spawn_group_impl :
    public boost::intrusive_ref_counter<spawn_group_impl<Executor>,
        boost::thread_unsafe_counter>,
    public service_list_base_hook
{
 public:
  using size_type = uint16_t;

  spawn_group_impl(Executor ex, size_type limit,
                   cancel_on_error on_error)
    : svc(boost::asio::use_service<service<spawn_group_impl>>(
            boost::asio::query(ex, boost::asio::execution::context))),
      ex(ex),
      signals(std::make_unique<boost::asio::cancellation_signal[]>(limit)),
      limit(limit), on_error(on_error)
  {
    // register for service_shutdown() notifications
    svc.add(*this);
  }
  ~spawn_group_impl()
  {
    svc.remove(*this);
  }

  using executor_type = Executor;
  executor_type get_executor() const noexcept
  {
    return ex;
  }

  spawn_group_handler<executor_type> completion()
  {
    if (spawned >= limit) {
      throw std::length_error("spawn group maximum size exceeded");
    }
    const size_type index = spawned++;
    return {boost::intrusive_ptr{this}, signals[index].slot(), index};
  }

  void child_complete(size_type index, std::exception_ptr e)
  {
    if (e) {
      if (!eptr) {
        eptr = e;
      }
      if (on_error == cancel_on_error::all) {
        cancel_from(0);
      } else if (on_error == cancel_on_error::after) {
        cancel_from(index + 1);
      }
    }
    if (++completed == spawned) {
      complete();
    }
  }

  boost::asio::awaitable<void, executor_type> wait()
  {
    if (completed < spawned) {
      co_await waiter.get();
    }

    // clear for reuse
    completed = 0;
    spawned = 0;

    if (eptr) {
      std::rethrow_exception(std::exchange(eptr, nullptr));
    }
  }

  void cancel()
  {
    cancel_from(0);
  }

  void service_shutdown()
  {
    waiter.shutdown();
  }

 private:
  service<spawn_group_impl>& svc;
  co_waiter<void, executor_type> waiter;
  executor_type ex;
  std::unique_ptr<boost::asio::cancellation_signal[]> signals;
  std::exception_ptr eptr;
  const size_type limit;
  size_type spawned = 0;
  size_type completed = 0;
  const cancel_on_error on_error;

  void cancel_from(size_type begin)
  {
    for (size_type i = begin; i < spawned; i++) {
      signals[i].emit(boost::asio::cancellation_type::terminal);
    }
  }

  void complete()
  {
    if (waiter.waiting()) {
      waiter.complete(nullptr);
    }
  }
};

} // namespace ceph::async::detail
