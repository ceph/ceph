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

#ifndef CEPH_FDB_EXECUTION_ADAPTER_H
#define CEPH_FDB_EXECUTION_ADAPTER_H

#include "common/async/yield_context.h"
#include "fdb/execution.h"

#include <tuple>
#include <memory>
#include <utility>
#include <concepts>
#include <optional>
#include <exception>
#include <type_traits>

#include <boost/asio/post.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/bind_allocator.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/associated_allocator.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/system/system_error.hpp>

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
  stdexec::get_stop_token, std::declval<stdexec::inplace_stop_token>()
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

inline std::exception_ptr make_operation_aborted_exception() noexcept
{
 try {
  throw boost::system::system_error {boost::asio::error::operation_aborted};
 } catch (...) {
  return std::current_exception();
 }
}

template <typename SenderT, typename HandlerT>
class asio_wait_operation final
{
 using value_t = wait_value_t<SenderT>;
 using executor_t = boost::asio::associated_executor_t<HandlerT>;
 using work_guard_t = boost::asio::executor_work_guard<executor_t>;

 struct stop_request final
 {
  std::weak_ptr<asio_wait_operation> operation;

  void operator()(const boost::asio::cancellation_type type) const noexcept
  {
   if (boost::asio::cancellation_type::none == type) {
    return;
   }

   if (auto locked = operation.lock()) {
    locked->stop_source.request_stop();
   }
  }
 };

 struct receiver final
 {
  using receiver_concept = stdexec::receiver_tag;

  // This operation owns its connected receiver. A non-owning back-reference
  // avoids an ownership cycle and remains valid through posted completion:
  asio_wait_operation& operation;
  stdexec::inplace_stop_token stop_token;

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
   operation.error = make_operation_aborted_exception();
   operation.schedule_completion();
  }

  [[nodiscard]] auto get_env() const noexcept
  {
   return stdexec::env {stdexec::prop {
     stdexec::get_stop_token, stop_token
   }};
  }
 };

 using sender_operation_t = stdexec::connect_result_t<SenderT, receiver>;

 HandlerT handler;
 work_guard_t work_guard;
 stdexec::inplace_stop_source stop_source;
 std::exception_ptr error;
 std::optional<value_t> value;
 sender_operation_t sender_operation;

 // This keeps an incomplete sender alive; completion transfers ownership to
 // the executor queue, while its cancellation slot retains only a weak view:
 std::shared_ptr<asio_wait_operation> ownership;

 public:
 asio_wait_operation(SenderT sender, HandlerT completion_handler)
  : handler(std::move(completion_handler)),
    work_guard(boost::asio::make_work_guard(
      boost::asio::get_associated_executor(handler))),
    sender_operation(stdexec::connect(
      std::move(sender), receiver {*this, stop_source.get_token()}))
 {}

 static void launch(SenderT sender, HandlerT handler)
 {
  auto operation = std::allocate_shared<asio_wait_operation>(
    boost::asio::get_associated_allocator(handler),
    std::move(sender), std::move(handler));
  operation->ownership = operation;

  auto slot = boost::asio::get_associated_cancellation_slot(operation->handler);

  if (slot.is_connected()) {
   slot.template emplace<stop_request>(operation);
  }

  // Completion may be immediate, so don't touch "operation" after start():
  stdexec::start(operation->sender_operation);
 }

 private:
 void finish()
 {
  auto completion_handler = std::move(handler);
  auto completion_error = std::move(error);
  auto completion_value = std::move(value);

  std::move(completion_handler)(
    std::move(completion_error), std::move(completion_value));
 }

 void schedule_completion() noexcept
 {
  // Keep this state alive even if another executor thread runs or discards the
  // posted handler before post() returns:
  auto completion_owner = std::move(ownership);

  try {
   boost::asio::post(
     work_guard.get_executor(),
     boost::asio::bind_allocator(
       boost::asio::get_associated_allocator(handler),
       [operation = completion_owner] { operation->finish(); }));
  } catch (...) {
   error = std::current_exception();
   value.reset();

   try {
    finish();
   } catch (...) {
    std::terminate();
   }
  }
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

    detail::asio_wait_operation<sender_t, handler_t>::launch(
      std::move(work), std::move(handler));
   }, token, std::forward<SenderT>(sender));
}

// With a yield context, suspend its coroutine; without one, wait on this
// thread. Errors retain their original exception types in both cases:
template <detail::asio_waitable_sender SenderT>
auto wait(SenderT&& sender, optional_yield y)
  -> detail::wait_value_t<SenderT>
{
 if (y) {
  auto result = async_wait(
    std::forward<SenderT>(sender), y.get_yield_context());

  return std::move(*result);
 }

 auto result = stdexec::sync_wait(std::forward<SenderT>(sender));

 if (not result) {
  std::rethrow_exception(detail::make_operation_aborted_exception());
 }

 return std::make_from_tuple<detail::wait_value_t<SenderT>>(std::move(*result));
}

} // namespace ceph::libfdb::experimental

#endif
