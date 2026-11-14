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

// Experimental Asio backend. Native completion tokens stay behind operation.
#ifndef CEPH_RGW_FDB_EXPERIMENTAL_ASIO_H
 #define CEPH_RGW_FDB_EXPERIMENTAL_ASIO_H

#if !defined(CEPH_LIBFDB_EXPERIMENTAL_ASIO)
 #error "enable LIBFDB_EXPERIMENTAL_CONCURRENCY=ASIO and link rgw_fdb_execution"
#endif

#include "asio_completion.h"
#include "operation.h"
#include "requests.h"

#include <atomic>
#include <exception>
#include <functional>
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
#include <boost/asio/bind_allocator.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/post.hpp>

namespace ceph::libfdb::experimental {

namespace detail {

template <typename RequestT, typename HandlerT>
class asio_future_operation final
{
 using value_t = typename RequestT::value_type;
 using executor_t = boost::asio::associated_executor_t<HandlerT>;

 struct stop_future final
 {
  asio_future_operation& operation;

  void operator()() const noexcept
  {
   operation.cancellation_requested.store(true, std::memory_order_release);
   RequestT::cancel(operation.state);
  }

 };

 HandlerT handler;
 boost::asio::executor_work_guard<executor_t> work;

 RequestT request;
 typename RequestT::state_type state;

 std::stop_source stop_source {std::nostopstate};
 std::optional<std::stop_callback<stop_future>> stop_callback;
 std::atomic_bool cancellation_requested = false;

 std::exception_ptr error;
 std::optional<value_t> value;

 std::shared_ptr<asio_future_operation> ownership;
 std::mutex completion_mutex;

 static void on_ready(FDBFuture *, void *context) noexcept
 {
  static_cast<asio_future_operation *>(context)->complete();
 }

 void await_future() noexcept
 {
  try {
   auto *future = request.pending_future(state);

   if (nullptr == future) {
    complete();
    return;
   }

   if constexpr (RequestT::cancellable) {
    if (stop_source.stop_possible()) {
     stop_callback.emplace(stop_source.get_token(), stop_future {*this});
    }
   }

   const auto registration_error =
    fdb_future_set_callback(future, on_ready, this);

   if (0 != registration_error) {
    complete(registration_error);
   }
  } catch (...) {
   stop_callback.reset();
   error = std::current_exception();
   deliver();
  }
 }

 void complete(const fdb_error_t wait_error = 0) noexcept
 {
  // Synchronize with a racing cancellation before moving a future into a result.
  stop_callback.reset();

  try {
   auto ready = ::ceph::libfdb::experimental::detail::advance(request, state, wait_error);

   if (not ready) {
    await_future();
    return;
   }
   value.emplace(std::move(*ready));
  } catch (const libfdb_exception& failure) {
   error = std::current_exception();

   if (fdb_error_code::operation_cancelled == failure.code() &&
       cancellation_requested.load(std::memory_order_acquire)) {
    error = operation_aborted_exception();
   }
  } catch (...) {
   error = std::current_exception();
  }

  deliver();
 }

 static void finish(std::shared_ptr<asio_future_operation> owner)
 {
  auto completion = std::move(owner->handler);
  auto error = std::move(owner->error);
  auto value = std::move(owner->value);
  auto work = std::move(owner->work);
  owner.reset();
  std::move(completion)(std::move(error), std::move(value));
 }

 void deliver() noexcept
 {
  auto owner = std::move(ownership);

  post_completion<&asio_future_operation::completion_mutex>(work.get_executor(),
    boost::asio::get_associated_allocator(handler), std::move(owner),
    [](auto owner, std::exception_ptr failure) {
     if (failure) {
      owner->error = std::move(failure);
      owner->value.reset();
     }
     finish(std::move(owner));
    });
 }

 public:
 asio_future_operation(RequestT request, HandlerT handler)
  : handler(std::move(handler)),
    work(boost::asio::make_work_guard(boost::asio::get_associated_executor(this->handler))),
    request(std::move(request))
 {}

 static void launch(RequestT request, HandlerT handler)
 {
  const auto allocator = boost::asio::get_associated_allocator(handler);
  auto owner = std::allocate_shared<asio_future_operation>(
    allocator, std::move(request), std::move(handler));

  if constexpr (RequestT::cancellable) {
   auto slot = boost::asio::get_associated_cancellation_slot(owner->handler);

   if (slot.is_connected()) {
    owner->stop_source = std::stop_source {};
    // Reads may have installed conflicts; total cancellation is not promised.
    slot.template emplace<stop_on_cancellation<>>(owner->stop_source);
   }
  }

  // Publish self-ownership only after cancellation setup has succeeded.
  auto *active = owner.get();
  active->ownership = std::move(owner);

  try {
   active->request.start(active->state);
  } catch (...) {
   active->error = std::current_exception();
   active->deliver();
   return;
  }

  active->await_future();
 }

};

template <typename RequestT>
auto make_future_operation(RequestT request)
{
 using signature_t = void(std::exception_ptr, std::optional<typename RequestT::value_type>);
 auto token = boost::asio::deferred;

 return operation {boost::asio::async_initiate<decltype(token), signature_t>(
   [](auto handler, RequestT request) {
    asio_future_operation<RequestT, decltype(handler)>::launch(std::move(request), std::move(handler));
   }, token, std::move(request))};
}

template <typename SignatureT>
struct asio_value;

template <typename ValueT>
struct asio_value<void(std::exception_ptr, std::optional<ValueT>)> final
{
 using type = ValueT;
};

template <typename OperationT>
using asio_value_t = typename asio_value<boost::asio::completion_signature_of_t<
  typename std::remove_cvref_t<OperationT>::native_handle_type>>::type;

template <typename FnT, typename ...ArgTs>
concept transaction_invocation =
 std::invocable<FnT&, transaction_handle&, ArgTs&...> &&
 requires {
  typename asio_value_t<std::invoke_result_t<FnT&, transaction_handle&, ArgTs&...>>;
 };

template <typename InvocationT, typename HandlerT, typename PolicyT>
class asio_transaction_operation final
 : public std::enable_shared_from_this<asio_transaction_operation<InvocationT, HandlerT, PolicyT>>
{
 using body_t = std::invoke_result_t<InvocationT&, transaction_handle&>;
 using value_t = asio_value_t<body_t>;
 using executor_t = boost::asio::associated_executor_t<HandlerT>;

 static_assert(std::is_nothrow_move_constructible_v<value_t>,
               "ASIO transaction results must own their values and be nothrow movable");

 struct stop_transaction final
 {
  asio_transaction_operation& operation;

  void operator()() const noexcept
  {
   resume(operation.shared_from_this(), [](auto& active) {
    if (not active.commit_submitted) {
     active.child_cancellation.emit(boost::asio::cancellation_type::partial);
    }
   });
  }

 };

 HandlerT handler;
 boost::asio::executor_work_guard<executor_t> work;

 database_handle dbh;
 std::optional<transaction_options> options;

 InvocationT invocation;
 [[no_unique_address]] retry_if<PolicyT> policy;

 transaction_handle txn;
 std::optional<value_t> value;

 boost::asio::cancellation_signal child_cancellation;
 bool commit_submitted = false;
 std::recursive_mutex completion_mutex;
 std::stop_source stop_source {std::nostopstate};
 std::optional<std::stop_callback<stop_transaction>> stop_callback;
 unsigned active_callbacks = 0;

 // Active callbacks retain ownership; an engaged empty error is pending success.
 std::optional<std::exception_ptr> deferred_completion;

 template <typename FnT, typename ...ArgTs>
 static void resume(std::shared_ptr<asio_transaction_operation> owner, FnT fn, ArgTs&& ...args)
 {
  std::unique_lock lock {owner->completion_mutex};
  ++owner->active_callbacks;
  try {
   std::invoke(fn, *owner, std::forward<ArgTs>(args)...);
  } catch (...) {
   owner->finish(std::current_exception());
  }
  --owner->active_callbacks;
  if (0 == owner->active_callbacks and owner->deferred_completion) {
   auto pending = std::exchange(owner->deferred_completion, std::nullopt);
   lock.unlock();
   complete(std::move(owner), std::move(*pending));
   return;
  }
  owner.reset();
  lock.unlock();
 }

 auto bind_child(auto callback)
 {
  const auto slot = boost::asio::get_associated_cancellation_slot(handler).is_connected()
   ? child_cancellation.slot() : boost::asio::cancellation_slot {};

  return boost::asio::bind_allocator(boost::asio::get_associated_allocator(handler),
    boost::asio::bind_executor(work.get_executor(),
      boost::asio::bind_cancellation_slot(slot,
       [owner = this->shared_from_this(), callback = std::move(callback)](auto ...args) mutable {
        resume(std::move(owner), std::move(callback), std::move(args)...);
       })));
 }

 static void complete(std::shared_ptr<asio_transaction_operation> owner, std::exception_ptr error)
 {
  owner->stop_callback.reset();
  auto completion = std::move(owner->handler);
  auto work = std::move(owner->work);
  auto value = std::move(owner->value);
  owner.reset();
  std::move(completion)(std::move(error), std::move(value));
 }

 void finish(std::exception_ptr error = {}) noexcept
 {
  if (error) {
   if (txn) {
    ::ceph::libfdb::detail::finish_version_stamps(
     *txn, std::make_error_code(std::errc::operation_canceled));
   }

   value.reset();
  }

  auto owner = this->shared_from_this();
  post_completion<&asio_transaction_operation::completion_mutex>(work.get_executor(),
    boost::asio::get_associated_allocator(handler), std::move(owner),
    [error = std::move(error)](auto owner, std::exception_ptr failure) mutable {
     if (failure) {
      owner->value.reset();
      error = std::move(failure);
     }
     std::unique_lock terminal {owner->completion_mutex};
     // Reentrant delivery must let its initiating callback unwind first.
     if (owner->active_callbacks) {
      owner->deferred_completion.emplace(std::move(error));
      terminal.unlock();
      return;
     }
     terminal.unlock();
     complete(std::move(owner), std::move(error));
    });
 }

 void replay(const fdb_error_t error)
 {
  value.reset();
  commit_submitted = false;

  if (not stop_source.stop_requested()) {
   start_attempt();
   return;
  }

  finish(retryable_not_committed(std::error_code(error, ::ceph::libfdb::fdb_error_category()))
          ? operation_aborted_exception() : make_fdb_exception_ptr(error));
 }

 void body_failed(std::exception_ptr error)
 {
  value.reset();

  try {
   if (const auto replay_error = ::ceph::libfdb::detail::retryable_error(error, txn, policy)) {
    prepare_replay(*replay_error);
    return;
   }
  } catch (...) {
   finish(std::current_exception());
   return;
  }

  finish(std::move(error));
 }

 void prepare_replay(const fdb_error_t replay_error)
 {
  value.reset();

  if (stop_source.stop_requested()) {
   replay(replay_error);
   return;
  }

  try {
   auto preparation = make_future_operation(replay_request {
    {}, txn, std::error_code(replay_error, ::ceph::libfdb::fdb_error_category())
   });
   std::move(preparation).native_handle()(bind_child(
     [replay_error](auto& active, std::exception_ptr failure, auto) {
      if (failure) {
       active.finish(std::move(failure));
       return;
      }

      active.replay(replay_error);
     }));
  } catch (...) {
   finish(std::current_exception());
  }
 }

 void body_completed(std::exception_ptr error, std::optional<value_t> result)
 {
  if (error) {
   body_failed(std::move(error));
   return;
  }

  if (not result) {
   finish(operation_aborted_exception());
   return;
  }

  value.emplace(std::move(*result));

  if (stop_source.stop_requested()) {
   finish(operation_aborted_exception());
   return;
  }

  try {
   auto commit = make_future_operation(commit_request<true> {txn});
   commit_submitted = true;
   std::move(commit).native_handle()(bind_child(
     [](auto& active, std::exception_ptr error, auto result) {
      if (error) {
       active.finish(std::move(error));
       return;
      }

      active.commit_completed(*result);
     }));
  } catch (...) {
   finish(std::current_exception());
  }
 }

 void commit_completed(const std::expected<void, std::error_code> result)
 {
  if (result) {
   ::ceph::libfdb::detail::publish_invocation(invocation);
   finish();
   return;
  }

  try {
   if (const auto replay_error = ::ceph::libfdb::detail::retryable_error(
         transaction_failure {result.error(), transaction_phase::commit}, txn, policy)) {
    prepare_replay(*replay_error);
    return;
   }
  } catch (...) {
   finish(std::current_exception());
   return;
  }

  finish(make_fdb_exception_ptr(result.error().value()));
 }

 void start_attempt()
 {
  if (stop_source.stop_requested()) {
   finish(operation_aborted_exception());
   return;
  }

  try {
   value.reset();
   ::ceph::libfdb::detail::prepare_invocation(invocation);
   auto body = invocation(txn);
   std::move(body).native_handle()(bind_child(
     [](auto& active, std::exception_ptr error, auto result) {
      active.body_completed(std::move(error), std::move(result));
     }));
  } catch (...) {
   body_failed(std::current_exception());
  }
 }

 public:
 asio_transaction_operation(database_handle dbh, std::optional<transaction_options> options,
                            InvocationT invocation, retry_if<PolicyT> retry, HandlerT handler)
  : handler(std::move(handler)),
    work(boost::asio::make_work_guard(boost::asio::get_associated_executor(this->handler))),
    dbh(std::move(dbh)),
    options(std::move(options)),
    invocation(std::move(invocation)),
    policy(std::move(retry))
 {}

 static void launch(database_handle dbh, std::optional<transaction_options> options,
                    InvocationT invocation, retry_if<PolicyT> policy, HandlerT handler)
 {
  const auto allocator = boost::asio::get_associated_allocator(handler);
  auto owner = std::allocate_shared<asio_transaction_operation>(
    allocator, std::move(dbh), std::move(options), std::move(invocation),
    std::move(policy), std::move(handler));
  auto slot = boost::asio::get_associated_cancellation_slot(owner->handler);

  if (slot.is_connected()) {
   owner->stop_source = std::stop_source {};
   slot.template emplace<stop_on_cancellation<>>(owner->stop_source);
   owner->stop_callback.emplace(owner->stop_source.get_token(), stop_transaction {*owner});
  }

  const auto executor = owner->work.get_executor();
  std::unique_lock initiating {owner->completion_mutex};
  boost::asio::post(executor, boost::asio::bind_allocator(allocator, [owner]() mutable {
   resume(std::move(owner), [](auto& active) {
    if (active.stop_source.stop_requested()) {
     active.finish(operation_aborted_exception());
     return;
    }

    try {
     active.txn = active.options ? make_transaction(std::move(active.dbh), std::move(*active.options))
                                 : make_transaction(std::move(active.dbh));
     active.options.reset();
    } catch (...) {
     active.finish(std::current_exception());
     return;
    }

    active.start_attempt();
   });
  }));
  owner.reset();
  initiating.unlock();
 }

};

template <typename InvocationT, typename PolicyT = void>
auto make_transaction_operation(database_handle dbh, std::optional<transaction_options> options,
                                 InvocationT invocation, retry_if<PolicyT> policy = {})
{
 using body_t = std::invoke_result_t<InvocationT&, transaction_handle&>;
 using signature_t = void(std::exception_ptr, std::optional<asio_value_t<body_t>>);
 auto token = boost::asio::deferred;

 return operation {boost::asio::async_initiate<decltype(token), signature_t>(
   [](auto handler, database_handle dbh, std::optional<transaction_options> options,
      InvocationT invocation, retry_if<PolicyT> policy) {
    asio_transaction_operation<InvocationT, decltype(handler), PolicyT>::launch(
      std::move(dbh), std::move(options), std::move(invocation),
      std::move(policy), std::move(handler));
   }, token, std::move(dbh), std::move(options), std::move(invocation), std::move(policy))};
}

} // namespace detail

} // namespace ceph::libfdb::experimental

#include "operations.h"

#endif
