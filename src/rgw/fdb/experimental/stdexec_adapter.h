// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_RGW_FDB_EXPERIMENTAL_STDEXEC_ADAPTER_H
 #define CEPH_RGW_FDB_EXPERIMENTAL_STDEXEC_ADAPTER_H

#include "common/async/yield_context.h"
#include "asio_completion.h"
#include "concurrency.h"

#include <concepts>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <tuple>
#include <type_traits>
#include <utility>

#include <boost/asio/associated_allocator.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/executor_work_guard.hpp>

#include <stdexec/execution.hpp>

namespace ceph::libfdb::experimental {

namespace detail {

template <typename ...ValueTs>
struct completion_value
{
 using type = std::tuple<ValueTs...>;
};

template <typename ValueT>
struct completion_value<ValueT>
{
 using type = ValueT;
};

template <typename ...ValueTs>
using completion_value_t = typename completion_value<ValueTs...>::type;

using stop_environment_t = decltype(stdexec::env {stdexec::prop {
  stdexec::get_stop_token, std::declval<std::stop_token>()
}});

template <typename SenderT>
using wait_value_t = stdexec::value_types_of_t<
  std::remove_cvref_t<SenderT>, stop_environment_t,
  completion_value_t, std::type_identity_t>;

template <typename SenderT>
using single_error_t = stdexec::error_types_of_t<
  std::remove_cvref_t<SenderT>, stop_environment_t, std::type_identity_t>;

template <typename SenderT>
concept asio_waitable_sender =
  stdexec::sender_in<std::remove_cvref_t<SenderT>, stop_environment_t> &&
  std::same_as<single_error_t<SenderT>, std::exception_ptr> &&
  std::is_nothrow_move_constructible_v<wait_value_t<SenderT>>;

template <typename SenderT, typename HandlerT>
class asio_wait_operation final
{
 using value_t = wait_value_t<SenderT>;
 using executor_t = boost::asio::associated_executor_t<HandlerT>;
 using work_guard_t = boost::asio::executor_work_guard<executor_t>;

 HandlerT handler;
 work_guard_t work_guard;
 std::stop_source stop_source;
 std::mutex completion_mutex;
 std::exception_ptr error;
 std::optional<value_t> value;

 struct receiver final
 {
  using receiver_concept = stdexec::receiver_tag;

  // This operation owns its connected receiver. A non-owning back-reference
  // avoids an ownership cycle and is consumed by the sender's terminal signal:
  asio_wait_operation& operation;

  template <typename ...ValueTs>
  requires std::is_nothrow_constructible_v<value_t, ValueTs&&...>
  void set_value(ValueTs&& ...values) && noexcept
  {
   operation.value.emplace(std::forward<ValueTs>(values)...);
   operation.schedule_completion();
  }

  void set_error(std::exception_ptr error) && noexcept
  {
   operation.error = std::move(error);
   operation.schedule_completion();
  }

  void set_stopped() && noexcept
  {
   operation.error = operation_aborted_exception();
   operation.schedule_completion();
  }

  [[nodiscard]] auto get_env() const noexcept
  {
   return stdexec::env {stdexec::prop {
     stdexec::get_stop_token, operation.stop_source.get_token()
   }};
  }

 };

 using sender_operation_t = stdexec::connect_result_t<SenderT, receiver>;

 sender_operation_t sender_operation;

 // This keeps an incomplete sender alive; completion transfers ownership to
 // the executor queue. Cancellation retains only the independent stop state:
 std::shared_ptr<asio_wait_operation> ownership;

 public:
 asio_wait_operation(SenderT sender, HandlerT completion_handler)
  : handler(std::move(completion_handler)),
    work_guard(boost::asio::make_work_guard(boost::asio::get_associated_executor(handler))),
    stop_source(boost::asio::get_associated_cancellation_slot(handler).is_connected()
      ? std::stop_source {} : std::stop_source {std::nostopstate}),
    sender_operation(stdexec::connect(std::move(sender), receiver {*this}))
 {}

 static void launch(SenderT sender, HandlerT handler)
 {
  auto operation = std::allocate_shared<asio_wait_operation>(
    boost::asio::get_associated_allocator(handler),
    std::move(sender), std::move(handler));
  auto slot = boost::asio::get_associated_cancellation_slot(operation->handler);

  if (slot.is_connected()) {
   slot.template emplace<stop_on_cancellation<boost::asio::cancellation_type::all>>(
     operation->stop_source);
  }

  // Slot setup can throw; publish self-ownership only after it succeeds.
  auto *active = operation.get();
  active->ownership = std::move(operation);

  // Completion may be immediate, so don't touch "operation" after start():
  stdexec::start(active->sender_operation);
 }

 private:
 static void finish(std::shared_ptr<asio_wait_operation> operation)
 {
  auto completion_handler = std::move(operation->handler);
  auto completion_error = std::move(operation->error);
  auto completion_value = std::move(operation->value);
  auto work = std::move(operation->work_guard);
  operation.reset();

  std::move(completion_handler)(std::move(completion_error), std::move(completion_value));
 }

 void schedule_completion() noexcept
 {
  post_completion<&asio_wait_operation::completion_mutex>(work_guard.get_executor(),
    boost::asio::get_associated_allocator(handler), std::move(ownership),
    [](auto operation, std::exception_ptr failure) {
     if (failure) {
      operation->error = std::move(failure);
      operation->value.reset();
     }
     finish(std::move(operation));
    });
 }

};

} // namespace detail

// Adapts a sender with one successful completion shape to an Asio completion
// token. Multiple values are collected in a tuple; one value remains unwrapped.
// Completion is posted through the associated executor, and Asio cancellation
// is forwarded through the sender's stop token:
template <detail::asio_waitable_sender SenderT, typename CompletionTokenT>
auto async_wait(SenderT&& sender, CompletionTokenT&& token)
{
 using sender_t = std::remove_cvref_t<SenderT>;
 using value_t = detail::wait_value_t<sender_t>;
 using signature_t = void(std::exception_ptr, std::optional<value_t>);

 return boost::asio::async_initiate<CompletionTokenT, signature_t>(
   [](auto handler, sender_t work) {
    using handler_t = decltype(handler);

    detail::asio_wait_operation<sender_t, handler_t>::launch(std::move(work), std::move(handler));
   }, token, std::forward<SenderT>(sender));
}

// With a yield context, suspend its coroutine; without one, wait on this
// thread. Errors retain their original exception types in both cases:
template <detail::asio_waitable_sender SenderT>
auto wait(SenderT&& sender, optional_yield y)
  -> detail::wait_value_t<SenderT>
{
 if (y) {
  auto result = async_wait(std::forward<SenderT>(sender), y.get_yield_context());

  return std::move(*result);
 }

 auto result = stdexec::sync_wait(std::forward<SenderT>(sender));

 if (not result) {
  std::rethrow_exception(detail::operation_aborted_exception());
 }

 return std::make_from_tuple<detail::wait_value_t<SenderT>>(std::move(*result));
}

} // namespace ceph::libfdb::experimental

#endif
