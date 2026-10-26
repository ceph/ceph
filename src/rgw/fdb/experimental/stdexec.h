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

#ifndef CEPH_RGW_FDB_EXPERIMENTAL_STDEXEC_H
 #define CEPH_RGW_FDB_EXPERIMENTAL_STDEXEC_H

#if !defined(CEPH_LIBFDB_EXPERIMENTAL_NVIDIA_STDEXEC)

static_assert(false,
 "libfdb experimental concurrency requires LIBFDB_EXPERIMENTAL_CONCURRENCY=NVIDIA_STDEXEC");

#else

#include "operation.h"
#include "requests.h"

#include <exec/completion_signatures.hpp>
#include <exec/trampoline_scheduler.hpp>

#include <stdexec/execution.hpp>

#include <atomic>
#include <concepts>
#include <exception>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>

namespace ceph::libfdb::experimental {

namespace detail {

template <typename RequestT, typename ReceiverT>
class future_operation final
{
 using value_t = typename RequestT::value_type;

 struct stop_request final
 {
  future_operation& operation;

  void operator()() const noexcept
  {
   operation.cancellation_requested.store(true, std::memory_order_release);

   // Cancellation may complete inline and destroy the operation:
   RequestT::cancel(operation.state);
  }

 };

 using stop_token_t = std::conditional_t<RequestT::cancellable,
   stdexec::stop_token_of_t<stdexec::env_of_t<ReceiverT>>, stdexec::never_stop_token>;
 using stop_callback_t = stdexec::stop_callback_for_t<stop_token_t, stop_request>;

 RequestT request;

 ReceiverT receiver;

 typename RequestT::state_type state;

 std::optional<stop_callback_t> stop_callback;
 std::atomic_bool cancellation_requested = false;

 static void on_ready(FDBFuture *, void *context) noexcept
 {
  static_cast<future_operation *>(context)->complete();
 }

 void await_future() noexcept
 {
  FDBFuture *future = nullptr;

  try {
   future = request.pending_future(state);

   // An empty selection completes without manufacturing a database request.
   if (nullptr == future) {
    complete();
    return;
   }

   if constexpr (RequestT::cancellable) {
    stop_callback.emplace(stdexec::get_stop_token(stdexec::get_env(receiver)),
                          stop_request {*this});
   }
  } catch (...) {
   stop_callback.reset();
   stdexec::set_error(std::move(receiver), std::current_exception());
   return;
  }

  // Successful registration may complete inline and destroy this state:
  const auto error = fdb_future_set_callback(future, on_ready, this);

  if (0 != error) {
   complete(error);
  }
 }

 void complete(const fdb_error_t wait_error = 0) noexcept
 {
  // Synchronize cancellation before transferring future ownership to a result:
  stop_callback.reset();

  try {
   auto value = ::ceph::libfdb::experimental::detail::advance(request, state, wait_error);

   if (not value) {
    await_future();
    return;
   }

   if constexpr (std::same_as<value_t, std::tuple<>>) {
    stdexec::set_value(std::move(receiver));
    return;
   }

   if constexpr (not std::same_as<value_t, std::tuple<>>) {
    stdexec::set_value(std::move(receiver), std::move(*value));
   }
  } catch (const libfdb_exception& error) {
   if (fdb_error_code::operation_cancelled == error.code() and
       cancellation_requested.load(std::memory_order_acquire)) {
    stdexec::set_stopped(std::move(receiver));
    return;
   }

   stdexec::set_error(std::move(receiver), std::current_exception());
  } catch (...) {
   stdexec::set_error(std::move(receiver), std::current_exception());
  }
 }

 public:
 using operation_state_concept = stdexec::operation_state_tag;

 future_operation(RequestT request, ReceiverT receiver)
  : request(std::move(request)),
    receiver(std::move(receiver))
 {}

 future_operation(future_operation&&) = delete;

 future_operation& operator=(future_operation&&) = delete;

 public:
 void start() & noexcept
 {
  const auto token = stdexec::get_stop_token(stdexec::get_env(receiver));

  if (token.stop_requested()) {
   request.cancel_before_start();
   stdexec::set_stopped(std::move(receiver));
   return;
  }

  try {
   request.start(state);
  } catch (...) {
   stdexec::set_error(std::move(receiver), std::current_exception());
   return;
  }

  // Submitted commits and replay preparation retain their actual disposition:
  await_future();
 }

};

template <typename RequestT>
class future_sender final
{
 RequestT request;

 public:
 using sender_concept = stdexec::sender_tag;
 using value_t = typename RequestT::value_type;
 using completion_signatures = stdexec::completion_signatures<
   std::conditional_t<std::same_as<value_t, std::tuple<>>,
                      stdexec::set_value_t(), stdexec::set_value_t(value_t)>,
   stdexec::set_error_t(std::exception_ptr),
   stdexec::set_stopped_t()>;

 explicit future_sender(RequestT request)
  : request(std::move(request))
 {}

 public:
 template <stdexec::receiver_of<completion_signatures> ReceiverT>
 auto connect(ReceiverT receiver) &&
 {
  return future_operation<RequestT, ReceiverT> {std::move(request), std::move(receiver)};
 }

 [[nodiscard]] stdexec::env<> get_env() const noexcept { return {}; }

};

template <typename>
struct transaction_value_traits final
{
 static constexpr bool valid_shape = false;
 static constexpr bool supported = false;

 using stored_t = std::tuple<>;
};

template <typename ...ValueTs>
struct transaction_value_traits<std::variant<std::tuple<ValueTs...>>> final
{
 static constexpr bool valid_shape = true;
 static constexpr bool supported =
  ((not std::is_reference_v<ValueTs> and
    std::move_constructible<std::decay_t<ValueTs>>) and ...);

 using stored_t = std::tuple<std::decay_t<ValueTs>...>;
};

template <typename RequestT>
auto make_future_operation(RequestT request)
{
 return operation {future_sender {std::move(request)}};
}

template <typename FnT, typename ...ArgTs>
concept transaction_invocation =
 std::invocable<FnT&, transaction_handle&, ArgTs&...> and
 stdexec::sender<std::remove_cvref_t<
   std::invoke_result_t<FnT&, transaction_handle&, ArgTs&...>>>;

template <typename ...EnvTs>
struct transaction_environment final
{
 static_assert(sizeof...(EnvTs) <= 1);
 using type = stdexec::env<>;
};

template <typename EnvT>
struct transaction_environment<EnvT> final
{
 using type = EnvT;
};

template <typename ...EnvTs>
using transaction_environment_t =
 typename transaction_environment<EnvTs...>::type;

template <typename BodySenderT, typename ...EnvTs>
consteval auto transaction_completion_signatures()
{
 using value_variants_t = stdexec::value_types_of_t<
  BodySenderT, transaction_environment_t<EnvTs...>,
  std::tuple, std::variant>;
 using value_traits_t = transaction_value_traits<value_variants_t>;

 static_assert(value_traits_t::valid_shape,
               "transaction bodies need exactly one successful completion shape");
 static_assert(not value_traits_t::valid_shape or value_traits_t::supported,
               "transaction sender results must be owning and movable");

 auto transform_values = []<typename ...ValueTs>() {
  return stdexec::completion_signatures<
    stdexec::set_value_t(std::decay_t<ValueTs>...)> {};
 };

 return ::exec::transform_completion_signatures(
   stdexec::get_completion_signatures<BodySenderT, EnvTs...>(),
   transform_values,
   ::exec::keep_completion<stdexec::set_error_t> {},
   ::exec::keep_completion<stdexec::set_stopped_t> {},
   stdexec::completion_signatures<
     stdexec::set_error_t(std::exception_ptr),
     stdexec::set_stopped_t()> {});
}

template <typename InvocationT, typename ReceiverT, typename PolicyT>
class transaction_operation final
{
 using body_sender_t = std::remove_cvref_t<
   std::invoke_result_t<InvocationT&, transaction_handle&>>;
 using receiver_env_t = stdexec::env_of_t<ReceiverT>;
 using value_traits_t =
  transaction_value_traits<stdexec::value_types_of_t<
    body_sender_t, receiver_env_t, std::tuple, std::variant>>;
 using stored_values_t = typename value_traits_t::stored_t;

 ReceiverT receiver;

 transaction_handle txn;

 database_handle dbh;
 std::optional<transaction_options> opts;

 InvocationT invocation;
 [[no_unique_address]] retry_if<PolicyT> policy;

 std::optional<stored_values_t> pending_values;

 class attempt;

 struct body_receiver final
 {
  using receiver_concept = stdexec::receiver_tag;

  transaction_operation& operation;
  attempt& transaction_attempt;

  template <typename ...ValueTs>
  void set_value(ValueTs&& ...values) noexcept
  {
   operation.body_succeeded(transaction_attempt, std::forward<ValueTs>(values)...);
  }

  template <typename ErrorT>
  void set_error(ErrorT&& error) noexcept
  {
   operation.body_failed(std::forward<ErrorT>(error));
  }

  void set_stopped() noexcept { operation.body_stopped(); }

  [[nodiscard]] auto get_env() const noexcept
  {
   return stdexec::get_env(operation.receiver);
  }

 };

 struct commit_receiver final
 {
  using receiver_concept = stdexec::receiver_tag;

  transaction_operation& operation;

  void set_value(const std::expected<void, std::error_code> result) noexcept
  {
   operation.commit_completed(result);
  }

  void set_error(std::exception_ptr error) noexcept
  {
   operation.finish_error(std::move(error));
  }

  void set_stopped() noexcept { operation.body_stopped(); }

  [[nodiscard]] auto get_env() const noexcept
  {
   return stdexec::get_env(operation.receiver);
  }

 };

 using body_operation_t =
  stdexec::connect_result_t<body_sender_t, body_receiver>;
 using commit_operation_t =
  stdexec::connect_result_t<future_sender<commit_request<true>>, commit_receiver>;

 struct replay_receiver final
 {
  using receiver_concept = stdexec::receiver_tag;

  transaction_operation& operation;
  fdb_error_t error;

  void set_value() noexcept { operation.replay(error); }

  void set_error(std::exception_ptr failure) noexcept
  {
   operation.finish_error(std::move(failure));
  }

  void set_stopped() noexcept { operation.body_stopped(); }

  [[nodiscard]] auto get_env() const noexcept
  {
   // Finish on_error even when stopping: its disposition decides whether an
   // uncertain commit must be preserved instead of reporting cancellation.
   return stdexec::env {
    stdexec::prop {stdexec::get_stop_token, stdexec::never_stop_token {}},
    stdexec::get_env(operation.receiver)
   };
  }

 };

 class replay_operation_t final
 {
  using state_type = stdexec::connect_result_t<
   decltype(stdexec::starts_on(::exec::trampoline_scheduler {},
                              std::declval<future_sender<replay_request>>())),
   replay_receiver>;

  state_type state;

  public:
  explicit replay_operation_t(transaction_operation& parent, const fdb_error_t error)
   : state(stdexec::connect(
     stdexec::starts_on(::exec::trampoline_scheduler {},
                       future_sender {replay_request {{}, parent.txn,
                         std::error_code(error, ::ceph::libfdb::fdb_error_category())}}),
     replay_receiver {parent, error}))
  {}

  void start() & noexcept { stdexec::start(state); }

 };

 class attempt final
 {
  body_operation_t body;
  commit_operation_t commit;

  public:
  // connect() is lazy: the self-reference cannot be observed before start().
  explicit attempt(transaction_operation& parent)
   : body(stdexec::connect(parent.invocation(parent.txn),
                           body_receiver {parent, *this})),
     commit(stdexec::connect(future_sender {commit_request<true> {parent.txn}},
                             commit_receiver {parent}))
  {}

  attempt(const attempt&) = delete;
  attempt(attempt&&) = delete;

  attempt& operator=(const attempt&) = delete;
  attempt& operator=(attempt&&) = delete;

  public:
  void start() noexcept { stdexec::start(body); }

  void start_commit() noexcept { stdexec::start(commit); }

 };

 // Completion may destroy a child. Preserve body results in the parent before
 // replacing its operation; the trampoline bounds inline retry recursion.
 std::variant<std::monostate, attempt, replay_operation_t> active_operation;

 [[nodiscard]] bool stop_requested() const noexcept
 {
  return stdexec::get_stop_token(stdexec::get_env(receiver)).stop_requested();
 }

 void start_attempt() noexcept
 {
  pending_values.reset();

  try {
   ::ceph::libfdb::detail::prepare_invocation(invocation);
   active_operation.template emplace<attempt>(*this).start();
  } catch (...) {
   body_failed(std::current_exception());
  }
 }

 template <typename ...ValueTs>
 void body_succeeded(attempt& active_attempt, ValueTs&& ...values) noexcept
 {
  try {
   pending_values.emplace(std::forward<ValueTs>(values)...);
  } catch (...) {
   finish_error(std::current_exception());
   return;
  }

  if (stop_requested()) {
   body_stopped();
   return;
  }

  active_attempt.start_commit();
 }

 template <typename ErrorT>
 void body_failed(ErrorT&& error) noexcept
 {
  pending_values.reset();

  if constexpr (std::same_as<std::remove_cvref_t<ErrorT>, std::exception_ptr> or
                std::same_as<std::remove_cvref_t<ErrorT>, libfdb_exception>) {
   try {
    if (const auto replay_error = ::ceph::libfdb::detail::retryable_error(error, txn, policy)) {
     prepare_replay(*replay_error);
     return;
    }
   } catch (...) {
    finish_error(std::current_exception());
    return;
   }
  }

  finish_error(std::forward<ErrorT>(error));
 }

 void prepare_replay(const fdb_error_t error) noexcept
 {
  pending_values.reset();

  if (stop_requested()) {
   replay(error);
   return;
  }

  try {
   active_operation.template emplace<replay_operation_t>(*this, error).start();
  } catch (...) {
   finish_error(std::current_exception());
  }
 }

 void body_stopped() noexcept
 {
  ::ceph::libfdb::detail::finish_version_stamps(
   *txn, std::make_error_code(std::errc::operation_canceled));
  pending_values.reset();
  stdexec::set_stopped(std::move(receiver));
 }

 void commit_completed(const std::expected<void, std::error_code> commit) noexcept
 {
  if (commit) {
   ::ceph::libfdb::detail::publish_invocation(invocation);

   std::apply([this](auto&& ...values) noexcept {
     stdexec::set_value(std::move(receiver), std::forward<decltype(values)>(values)...);
   }, std::move(*pending_values));
   return;
  }

  try {
   if (const auto replay_error = ::ceph::libfdb::detail::retryable_error(
         transaction_failure {commit.error(), transaction_phase::commit}, txn, policy)) {
    prepare_replay(*replay_error);
    return;
   }
  } catch (...) {
   finish_error(std::current_exception());
   return;
  }

  finish_error(make_fdb_exception_ptr(commit.error().value()));
 }

 void replay(const fdb_error_t error) noexcept
 {
  pending_values.reset();

  if (not stop_requested()) {
   start_attempt();
   return;
  }

  if (retryable_not_committed(std::error_code(error, ::ceph::libfdb::fdb_error_category()))) {
   stdexec::set_stopped(std::move(receiver));
   return;
  }

  finish_error(make_fdb_exception_ptr(error));
 }

 template <typename ErrorT>
 void finish_error(ErrorT&& error) noexcept
 {
  if (txn) {
   ::ceph::libfdb::detail::finish_version_stamps(
    *txn, std::make_error_code(std::errc::operation_canceled));
  }

  pending_values.reset();
  stdexec::set_error(std::move(receiver), std::forward<ErrorT>(error));
 }

 public:
 using operation_state_concept = stdexec::operation_state_tag;

 transaction_operation(database_handle database, std::optional<transaction_options> options,
                       InvocationT bound_invocation, retry_if<PolicyT> retry,
                       ReceiverT transaction_receiver)
  : receiver(std::move(transaction_receiver)),
    dbh(std::move(database)),
    opts(std::move(options)),
    invocation(std::move(bound_invocation)),
    policy(std::move(retry))
 {}

 transaction_operation(transaction_operation&&) = delete;

 transaction_operation& operator=(transaction_operation&&) = delete;

 public:
 void start() & noexcept
 {
  if (stop_requested()) {
   stdexec::set_stopped(std::move(receiver));
   return;
  }

  try {
   txn = opts ? ::ceph::libfdb::make_transaction(std::move(dbh), std::move(*opts))
              : ::ceph::libfdb::make_transaction(std::move(dbh));
   opts.reset();
  } catch (...) {
   finish_error(std::current_exception());
   return;
  }

  start_attempt();
 }

};

template <typename InvocationT, typename PolicyT>
class transaction_sender final
{
 using body_sender_t = std::remove_cvref_t<
   std::invoke_result_t<InvocationT&, transaction_handle&>>;

 database_handle dbh;
 std::optional<transaction_options> opts;

 InvocationT invocation;
 [[no_unique_address]] retry_if<PolicyT> policy;

 public:
 using sender_concept = stdexec::sender_tag;

 template <typename SelfT, typename ...EnvTs>
 static consteval auto get_completion_signatures()
 {
  return transaction_completion_signatures<body_sender_t, EnvTs...>();
 }

 transaction_sender(database_handle database, std::optional<transaction_options> options,
                    InvocationT bound_invocation, retry_if<PolicyT> retry)
  : dbh(std::move(database)),
    opts(std::move(options)),
    invocation(std::move(bound_invocation)),
    policy(std::move(retry))
 {}

 public:
 template <stdexec::receiver ReceiverT>
 auto connect(ReceiverT receiver) &&
 {
  return transaction_operation<InvocationT, ReceiverT, PolicyT> {
   std::move(dbh), std::move(opts),
   std::move(invocation), std::move(policy), std::move(receiver)
  };
 }

 [[nodiscard]] stdexec::env<> get_env() const noexcept { return {}; }

};

template <typename InvocationT, typename PolicyT = void>
auto make_transaction_operation(database_handle dbh, std::optional<transaction_options> options,
                                 InvocationT invocation, retry_if<PolicyT> policy = {})
{
 return operation {transaction_sender {
  std::move(dbh), std::move(options), std::move(invocation), std::move(policy)
 }};
}

} // namespace detail

} // namespace ceph::libfdb::experimental

#include "operations.h"

#endif // CEPH_LIBFDB_EXPERIMENTAL_NVIDIA_STDEXEC

#endif
