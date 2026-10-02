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

#ifndef CEPH_RGW_FDB_EXECUTION_H
#define CEPH_RGW_FDB_EXECUTION_H

#if !defined(CEPH_EXPERIMENTAL_NVIDIA_STDEXEC)

static_assert(false,
 "libfdb experimental concurrency requires EXPERIMENTAL_NVIDIA_STDEXEC=ON");

#else

#include "scan.h"

#include <exec/completion_signatures.hpp>

#include <stdexec/execution.hpp>

#include <span>
#include <tuple>
#include <atomic>
#include <memory>
#include <ranges>
#include <string>
#include <vector>
#include <cstdint>
#include <utility>
#include <variant>
#include <concepts>
#include <iterator>
#include <optional>
#include <exception>
#include <stdexcept>
#include <type_traits>
#include <string_view>

namespace ceph::libfdb::experimental {

namespace detail {

template <typename ReceiverT>
class point_read_operation;

template <typename ReceiverT>
class watch_operation;

template <typename ReceiverT>
class commit_operation;

template <typename ReceiverT>
class scan_read_operation;

} // namespace detail

// Owns the FoundationDB storage behind bytes(), which remain valid until this
// result is destroyed or moved from. A missing key is a successful empty result:
class point_read_result final
{
 transaction_handle txn;
 ceph::libfdb::detail::future_value future;
 std::optional<std::span<const std::uint8_t>> value;

 point_read_result(transaction_handle transaction,
                   ceph::libfdb::detail::future_value future_owner,
                   std::optional<std::span<const std::uint8_t>> bytes) noexcept
  : txn(std::move(transaction)),
    future(std::move(future_owner)),
    value(bytes)
 {}

 public:
 point_read_result(point_read_result&& other) noexcept
  : txn(std::move(other.txn)),
    future(std::move(other.future)),
    value(std::exchange(other.value, std::nullopt))
 {}

 point_read_result(const point_read_result&) = delete;

 point_read_result& operator=(point_read_result&& other) noexcept
 {
  if (this == std::addressof(other)) {
   return *this;
  }

  // The old future must be released before its transaction:
  future = std::move(other.future);
  txn = std::move(other.txn);
  value = std::exchange(other.value, std::nullopt);

  return *this;
 }

 point_read_result& operator=(const point_read_result&) = delete;

 [[nodiscard]] bool has_value() const noexcept { return value.has_value(); }
 explicit operator bool() const noexcept { return has_value(); }

 [[nodiscard]] std::optional<std::span<const std::uint8_t>> bytes() const& noexcept
 {
  return value;
 }

 std::optional<std::span<const std::uint8_t>> bytes() const&& = delete;

 private:
 template <typename ReceiverT>
 friend class detail::point_read_operation;
};

struct scan_row final
{
 std::span<const std::uint8_t> key;
 std::span<const std::uint8_t> value;
};

/* Tracks progress through one compiled selection. The immutable query says
 * which keys to read; this move-only cursor records where its scan has reached. */
class scan_cursor final
{
 ceph::libfdb::detail::query_cursor progress;

 public:
 template <query::expression SelectionT>
 explicit scan_cursor(SelectionT selection)
  : progress(std::move(selection))
 {}

 scan_cursor(scan_cursor&&) noexcept = default;

 scan_cursor(const scan_cursor&) = delete;

 scan_cursor& operator=(scan_cursor&&) noexcept = default;

 scan_cursor& operator=(const scan_cursor&) = delete;

 [[nodiscard]] explicit operator bool() const noexcept
 {
  return static_cast<bool>(progress);
 }

 private:
 template <typename ReceiverT>
 friend class detail::scan_read_operation;
};

// Owns the ready FDB future behind rows(); its byte spans expire with it:
class scan_window final
{
 transaction_handle txn;
 std::optional<ceph::libfdb::detail::future_value> future;
 std::span<const FDBKeyValue> result_rows;
 scan_cursor cursor;

 scan_window(transaction_handle transaction,
             ceph::libfdb::detail::future_value future_owner,
             const std::span<const FDBKeyValue> rows,
             scan_cursor next_cursor) noexcept
  : txn(std::move(transaction)),
    future(std::move(future_owner)),
    result_rows(rows),
    cursor(std::move(next_cursor))
 {}

 public:
 scan_window(scan_window&& other) noexcept
  : txn(std::move(other.txn)),
    future(std::move(other.future)),
    result_rows(std::exchange(other.result_rows, {})),
    cursor(std::move(other.cursor))
 {}

 scan_window(const scan_window&) = delete;

 scan_window& operator=(scan_window&& other) noexcept
 {
  if (this == std::addressof(other)) {
   return *this;
  }

  // Release the old FDB result before its transaction:
  result_rows = {};
  future.reset();
  txn.reset();

  txn = std::move(other.txn);
  future = std::move(other.future);
  result_rows = std::exchange(other.result_rows, {});
  cursor = std::move(other.cursor);

  return *this;
 }

 scan_window& operator=(const scan_window&) = delete;

 [[nodiscard]] auto rows() const& noexcept
 {
  return result_rows |
         std::views::transform([](const FDBKeyValue& row) noexcept {
          return scan_row {
           .key = ceph::libfdb::detail::key_bytes(row),
           .value = ceph::libfdb::detail::value_bytes(row)
          };
         });
 }

 auto rows() const&& = delete;

 [[nodiscard]] scan_cursor next() && noexcept
 {
  auto next_cursor = std::move(cursor);

  result_rows = {};
  future.reset();
  txn.reset();

  return next_cursor;
 }

 scan_cursor next() & = delete;

 private:
 template <typename ReceiverT>
 friend class detail::scan_read_operation;
};

namespace detail {

inline std::exception_ptr make_fdb_exception_ptr(const fdb_error_t error) noexcept
{
 try {
  throw libfdb_exception {error};
 } catch (...) {
  return std::current_exception();
 }
}

inline void set_fdb_error(auto&& receiver, const fdb_error_t error) noexcept
{
 stdexec::set_error(
   std::forward<decltype(receiver)>(receiver), make_fdb_exception_ptr(error));
}

inline bool was_cancelled_by_request(const fdb_error_t error,
                                     const std::atomic_bool& cancellation_requested) noexcept
{
 return ceph::libfdb::detail::operation_cancelled_error == error and
        cancellation_requested.load(std::memory_order_acquire);
}

template <typename ReceiverT>
class point_read_operation final
{
 struct stop_request final
 {
  point_read_operation& operation;

  void operator()() const noexcept
  {
   operation.cancellation_requested.store(true, std::memory_order_release);

   // Cancellation may complete inline and destroy "operation", so this
   // FoundationDB call must remain last:
   operation.future->cancel();
  }
 };

 using stop_token_t = stdexec::stop_token_of_t<stdexec::env_of_t<ReceiverT>>;
 using stop_callback_t = stdexec::stop_callback_for_t<stop_token_t, stop_request>;

 transaction_handle txn;
 std::string key;
 ReceiverT receiver;
 std::optional<ceph::libfdb::detail::future_value> future;
 std::optional<stop_callback_t> stop_callback;
 std::atomic_bool cancellation_requested = false;

 // The raw parameters are prescribed by FoundationDB's C callback interface:
 static void on_ready(FDBFuture *, void *context) noexcept
 {
  static_cast<point_read_operation *>(context)->complete();
 }

 void complete() noexcept
 {
  stop_callback.reset();

  auto *ready_future = future->raw_handle();
  fdb_bool_t present = false;
  const std::uint8_t *data = nullptr;
  int size = 0;

  if (const auto error = fdb_future_get_value(
        ready_future, &present, &data, &size); 0 != error) {
   if (was_cancelled_by_request(error, cancellation_requested)) {
    stdexec::set_stopped(std::move(receiver));
    return;
   }

   set_fdb_error(std::move(receiver), error);
   return;
  }

  std::optional<std::span<const std::uint8_t>> bytes;

  try {
   // Validate the complete C result even when the key is absent:
   const auto result = ceph::libfdb::detail::result_bytes(data, size);

   if (present) {
    bytes.emplace(result);
   }
  } catch (...) {
   stdexec::set_error(std::move(receiver), std::current_exception());
   return;
  }

  auto result = point_read_result {
   std::move(txn), std::move(*future), bytes
  };

  stdexec::set_value(std::move(receiver), std::move(result));
 }

 public:
 using operation_state_concept = stdexec::operation_state_tag;

 point_read_operation(transaction_handle transaction, std::string read_key,
                      ReceiverT read_receiver)
  : txn(std::move(transaction)),
    key(std::move(read_key)),
    receiver(std::move(read_receiver))
 {}

 point_read_operation(point_read_operation&&) = delete;

 point_read_operation& operator=(point_read_operation&&) = delete;

 void start() & noexcept
 {
  const auto token = stdexec::get_stop_token(stdexec::get_env(receiver));

  if (token.stop_requested()) {
   stdexec::set_stopped(std::move(receiver));
   return;
  }

  try {
   if (not txn or not *txn) {
    throw std::invalid_argument("invalid FoundationDB transaction");
   }

   const auto key_bytes = ceph::libfdb::detail::as_byte_view(
     std::string_view {key});

   if (not std::in_range<int>(std::size(key_bytes))) {
    throw std::length_error("FoundationDB key is too large");
   }

   constexpr fdb_bool_t is_snapshot = false;
   auto *raw_future = fdb_transaction_get(
     txn->raw_handle(), key_bytes.data(),
     static_cast<int>(std::size(key_bytes)), is_snapshot);

   if (nullptr == raw_future) {
    throw std::invalid_argument("invalid FoundationDB future");
   }

   future.emplace(raw_future);
   stop_callback.emplace(token, stop_request {*this});
  } catch (...) {
   stdexec::set_error(std::move(receiver), std::current_exception());
   return;
  }

  // FoundationDB may invoke on_ready() inline. On successful registration this
  // operation could therefore already be destroyed when the call returns:
  const auto registration_error = future->set_callback(on_ready, this);

  if (0 != registration_error) {
   stop_callback.reset();
   set_fdb_error(std::move(receiver), registration_error);
  }
 }
};

class point_read_sender final
{
 transaction_handle txn;
 std::string key;

 public:
 using sender_concept = stdexec::sender_tag;
 using completion_signatures = stdexec::completion_signatures<
   stdexec::set_value_t(point_read_result),
   stdexec::set_error_t(std::exception_ptr),
   stdexec::set_stopped_t()>;

 point_read_sender(transaction_handle transaction, std::string read_key)
  : txn(std::move(transaction)),
    key(std::move(read_key))
 {}

 template <stdexec::receiver_of<completion_signatures> ReceiverT>
 auto connect(ReceiverT receiver) &&
 {
  return point_read_operation<ReceiverT> {
   std::move(txn), std::move(key), std::move(receiver)
  };
 }

 [[nodiscard]] stdexec::env<> get_env() const noexcept { return {}; }
};

template <typename ReceiverT>
class watch_operation final
{
 struct stop_request final
 {
  watch_operation& operation;

  void operator()() const noexcept
  {
   operation.cancellation_requested.store(true, std::memory_order_release);

   // Cancellation may complete inline and destroy "operation", so this
   // FoundationDB call must remain last:
   operation.future.cancel();
  }
 };

 using stop_token_t = stdexec::stop_token_of_t<stdexec::env_of_t<ReceiverT>>;
 using stop_callback_t = stdexec::stop_callback_for_t<stop_token_t, stop_request>;

 ceph::libfdb::detail::future_value future;
 ReceiverT receiver;
 std::optional<stop_callback_t> stop_callback;
 std::atomic_bool cancellation_requested = false;

 // The raw parameters are prescribed by FoundationDB's C callback interface:
 static void on_ready(FDBFuture *, void *context) noexcept
 {
  static_cast<watch_operation *>(context)->complete();
 }

 void complete() noexcept
 {
  stop_callback.reset();

  if (const auto error = fdb_future_get_error(future.raw_handle());
      0 != error) {
   if (was_cancelled_by_request(error, cancellation_requested)) {
    stdexec::set_stopped(std::move(receiver));
    return;
   }

   set_fdb_error(std::move(receiver), error);
   return;
  }

  stdexec::set_value(std::move(receiver));
 }

 public:
 using operation_state_concept = stdexec::operation_state_tag;

 watch_operation(ceph::libfdb::detail::future_value watch_future,
                 ReceiverT watch_receiver)
  : future(std::move(watch_future)),
    receiver(std::move(watch_receiver))
 {}

 watch_operation(watch_operation&&) = delete;

 watch_operation& operator=(watch_operation&&) = delete;

 void start() & noexcept
 {
  const auto token = stdexec::get_stop_token(stdexec::get_env(receiver));

  if (token.stop_requested()) {
   future.cancel();

   stdexec::set_stopped(std::move(receiver));
   return;
  }

  try {
   if (nullptr == future.raw_handle()) {
    throw std::invalid_argument("invalid FoundationDB watch");
   }

   stop_callback.emplace(token, stop_request {*this});
  } catch (...) {
   stdexec::set_error(std::move(receiver), std::current_exception());
   return;
  }

  // FoundationDB may invoke on_ready() inline. On successful registration this
  // operation could therefore already be destroyed when the call returns:
  const auto registration_error = future.set_callback(on_ready, this);

  if (0 != registration_error) {
   stop_callback.reset();
   set_fdb_error(std::move(receiver), registration_error);
  }
 }
};

class watch_sender final
{
 ceph::libfdb::detail::future_value future;

 public:
 using sender_concept = stdexec::sender_tag;
 using completion_signatures = stdexec::completion_signatures<
   stdexec::set_value_t(),
   stdexec::set_error_t(std::exception_ptr),
   stdexec::set_stopped_t()>;

 explicit watch_sender(ceph::libfdb::detail::future_value watch_future)
  : future(std::move(watch_future))
 {}

 template <stdexec::receiver_of<completion_signatures> ReceiverT>
 auto connect(ReceiverT receiver) &&
 {
  return watch_operation<ReceiverT> {
   std::move(future), std::move(receiver)
  };
 }

 [[nodiscard]] stdexec::env<> get_env() const noexcept { return {}; }
};

template <typename ReceiverT>
class commit_operation final
{
 transaction_handle txn;
 ReceiverT receiver;
 std::optional<ceph::libfdb::detail::commit_attempt> attempt;

 // The raw parameters are prescribed by FoundationDB's C callback interface:
 static void on_ready(FDBFuture *, void *context) noexcept
 {
  static_cast<commit_operation *>(context)->complete();
 }

 void complete_error(std::exception_ptr error) noexcept
 {
  stdexec::set_error(std::move(receiver), std::move(error));
 }

 void await_pending_future() noexcept
 {
  FDBFuture *future = nullptr;

  try {
   future = attempt->pending_future();
  } catch (...) {
   complete_error(std::current_exception());
   return;
  }

  // FoundationDB may invoke on_ready() inline. On successful registration this
  // operation could therefore already be destroyed when the call returns:
  const auto registration_error =
   fdb_future_set_callback(future, on_ready, this);

  if (0 != registration_error) {
   set_fdb_error(std::move(receiver), registration_error);
  }
 }

 void complete() noexcept
 {
  try {
   const auto progress = attempt->advance();

   switch (progress) {
    case ceph::libfdb::detail::commit_progress::pending:
     await_pending_future();
     return;

    case ceph::libfdb::detail::commit_progress::committed:
     stdexec::set_value(std::move(receiver), commit_result {
       .committed = true
     });
     return;

    case ceph::libfdb::detail::commit_progress::replay_ready:
     stdexec::set_value(std::move(receiver), commit_result {
       .committed = false,
       .replay_error = attempt->replay_error()
     });
     return;
   }

   std::unreachable();
  } catch (...) {
   complete_error(std::current_exception());
   return;
  }
 }

 public:
 using operation_state_concept = stdexec::operation_state_tag;

 commit_operation(transaction_handle transaction, ReceiverT commit_receiver)
  : txn(std::move(transaction)),
    receiver(std::move(commit_receiver))
 {}

 commit_operation(commit_operation&&) = delete;

 commit_operation& operator=(commit_operation&&) = delete;

 void start() & noexcept
 {
  const auto token = stdexec::get_stop_token(stdexec::get_env(receiver));

  if (token.stop_requested()) {
   stdexec::set_stopped(std::move(receiver));
   return;
  }

  try {
   if (not txn or not *txn) {
    throw std::invalid_argument("invalid FoundationDB transaction");
   }

   attempt.emplace(*txn);
  } catch (...) {
   complete_error(std::current_exception());
   return;
  }

  // Stop is deliberately ignored after commit submission: cancellation cannot
  // establish whether FoundationDB committed the transaction.
  await_pending_future();
 }
};

class commit_sender final
{
 transaction_handle txn;

 public:
 using sender_concept = stdexec::sender_tag;
 using completion_signatures = stdexec::completion_signatures<
  stdexec::set_value_t(commit_result),
  stdexec::set_error_t(std::exception_ptr),
  stdexec::set_stopped_t()>;

 explicit commit_sender(transaction_handle transaction)
  : txn(std::move(transaction))
 {}

 template <stdexec::receiver_of<completion_signatures> ReceiverT>
 auto connect(ReceiverT receiver) &&
 {
  return commit_operation<ReceiverT> {
   std::move(txn), std::move(receiver)
  };
 }

 [[nodiscard]] stdexec::env<> get_env() const noexcept { return {}; }
};

template <typename>
struct transaction_value_traits
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

template <typename FnT, typename ...ArgTs>
using transaction_invocation_t =
 ceph::libfdb::detail::bound_invocation<FnT, ArgTs...>;

template <typename FnT, typename ...ArgTs>
using transaction_body_sender_t = std::remove_cvref_t<
 std::invoke_result_t<FnT&, transaction_handle&, ArgTs&...>>;

template <typename FnT, typename ...ArgTs>
concept transaction_sender_invocation =
 std::invocable<FnT&, transaction_handle&, ArgTs&...> and
 stdexec::sender<transaction_body_sender_t<FnT, ArgTs...>>;

template <typename ...EnvTs>
struct transaction_environment
{
 static_assert(sizeof...(EnvTs) <= 1);
 using type = stdexec::env<>;
};

template <typename EnvT>
struct transaction_environment<EnvT>
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

 return exec::transform_completion_signatures(
   stdexec::get_completion_signatures<BodySenderT, EnvTs...>(),
   transform_values,
   exec::keep_completion<stdexec::set_error_t> {},
   exec::keep_completion<stdexec::set_stopped_t> {},
   stdexec::completion_signatures<
     stdexec::set_error_t(std::exception_ptr),
     stdexec::set_stopped_t()> {});
}

template <typename FnT, typename ReceiverT, typename ...ArgTs>
requires transaction_sender_invocation<FnT, ArgTs...>
class transaction_operation final
{
 using invocation_t = transaction_invocation_t<FnT, ArgTs...>;
 using body_sender_t = transaction_body_sender_t<FnT, ArgTs...>;
 using receiver_env_t = stdexec::env_of_t<ReceiverT>;
 using value_traits_t =
  transaction_value_traits<stdexec::value_types_of_t<
    body_sender_t, receiver_env_t, std::tuple, std::variant>>;
 using stored_values_t = typename value_traits_t::stored_t;

 ReceiverT receiver;
 transaction_handle txn;
 database_handle dbh;
 std::optional<transaction_options> opts;
 invocation_t invocation;
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
   operation.body_succeeded(
     transaction_attempt, std::forward<ValueTs>(values)...);
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

  void set_value(commit_result result) noexcept
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
  stdexec::connect_result_t<commit_sender, commit_receiver>;

 class replay_preparation final
 {
  transaction_operation& operation;
  ceph::libfdb::detail::future_value future;
  fdb_error_t error;

  // The raw parameters are prescribed by FoundationDB's C callback interface:
  static void on_ready(FDBFuture *, void *context) noexcept
  {
   static_cast<replay_preparation *>(context)->complete();
  }

  void complete() noexcept
  {
   const auto on_error_error = fdb_future_get_error(future.raw_handle());

   if (0 != on_error_error) {
    // Match synchronous replay: the original operation remains the cause when
    // FoundationDB cannot prepare its transaction for another attempt.
    operation.finish_error(make_fdb_exception_ptr(error));
    return;
   }

   ceph::libfdb::detail::transaction_replay_ready(*operation.txn);
   operation.replay(error);
  }

  public:
  replay_preparation(transaction_operation& parent,
                     const fdb_error_t replay_error)
  : operation(parent),
     future(fdb_transaction_on_error(operation.txn->raw_handle(),
                                     replay_error)),
     error(replay_error)
  {
   future.raw_ptr_or_throw();
  }

  replay_preparation(const replay_preparation&) = delete;
  replay_preparation(replay_preparation&&) = delete;

  replay_preparation& operator=(const replay_preparation&) = delete;
  replay_preparation& operator=(replay_preparation&&) = delete;

  void start() noexcept
  {
   // The callback may run inline and complete the entire operation. Do not
   // access this preparation after successful registration:
   const auto registration_error = future.set_callback(on_ready, this);

   if (0 != registration_error) {
    operation.finish_error(make_fdb_exception_ptr(registration_error));
   }
  }
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
     commit(stdexec::connect(commit_sender {parent.txn},
                             commit_receiver {parent}))
  {}

  attempt(const attempt&) = delete;
  attempt(attempt&&) = delete;

  attempt& operator=(const attempt&) = delete;
  attempt& operator=(attempt&&) = delete;

  void start() noexcept { stdexec::start(body); }

  void start_commit() noexcept { stdexec::start(commit); }
 };

 std::optional<attempt> first_attempt;
 std::vector<std::unique_ptr<attempt>> replay_attempts;
 std::vector<std::unique_ptr<replay_preparation>> replay_preparations;
 std::size_t attempt_count = 0;

 [[nodiscard]] bool stop_requested() const noexcept
 {
  return stdexec::get_stop_token(
    stdexec::get_env(receiver)).stop_requested();
 }

 [[nodiscard]] static std::optional<fdb_error_t> retryable_error(
   const std::exception_ptr& error) noexcept
 {
  if (not error) {
   return std::nullopt;
  }

  try {
   std::rethrow_exception(error);
  } catch (const libfdb_exception& failure) {
   if (failure.retryable()) {
    return failure.fdb_error_value;
   }
  } catch (...) {
  }

  return std::nullopt;
 }

 [[nodiscard]] static std::optional<fdb_error_t> retryable_error(
   const libfdb_exception& error) noexcept
 {
  if (error.retryable()) {
   return error.fdb_error_value;
  }

  return std::nullopt;
 }

 attempt& make_attempt()
 {
  if (not first_attempt) {
   return first_attempt.emplace(*this);
  }

  // Older attempts remain alive until terminal completion so an inline FDB
  // callback can never destroy itself:
  replay_attempts.push_back(std::make_unique<attempt>(*this));

  return *replay_attempts.back();
 }

 void start_attempt() noexcept
 {
  pending_values.reset();

  try {
   ceph::libfdb::detail::prepare_invocation(invocation);
   ++attempt_count;
   make_attempt().start();
  } catch (...) {
   auto error = std::current_exception();

   if (const auto replay_error = retryable_error(error)) {
    prepare_replay(*replay_error);
    return;
   }

   finish_error(std::move(error));
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
   if (const auto replay_error = retryable_error(error)) {
    prepare_replay(*replay_error);
    return;
   }
  }

  finish_error(std::forward<ErrorT>(error));
 }

 void prepare_replay(const fdb_error_t error) noexcept
 {
  pending_values.reset();

  try {
   replay_preparations.push_back(
     std::make_unique<replay_preparation>(*this, error));
  } catch (...) {
   finish_error(std::current_exception());
   return;
  }

  replay_preparations.back()->start();
 }

 void body_stopped() noexcept
 {
  pending_values.reset();
  stdexec::set_stopped(std::move(receiver));
 }

 void commit_completed(const commit_result commit) noexcept
 {
  if (commit.committed) {
   ceph::libfdb::detail::publish_invocation(invocation);

   std::apply([this](auto&& ...values) noexcept {
     stdexec::set_value(std::move(receiver),
                        std::forward<decltype(values)>(values)...);
   }, std::move(*pending_values));
   return;
  }

  replay(commit.replay_error);
 }

 void replay(const fdb_error_t error) noexcept
 {
  pending_values.reset();

  if (stop_requested()) {
   if (fdb_error_predicate(FDB_ERROR_PREDICATE_MAYBE_COMMITTED, error)) {
    finish_error(make_fdb_exception_ptr(error));
    return;
   }

   stdexec::set_stopped(std::move(receiver));
   return;
  }

  if (ceph::libfdb::detail::transaction_retry_attempts <= attempt_count) {
   finish_error(make_fdb_exception_ptr(error));
   return;
  }

  start_attempt();
 }

 template <typename ErrorT>
 void finish_error(ErrorT&& error) noexcept
 {
  pending_values.reset();
  stdexec::set_error(std::move(receiver), std::forward<ErrorT>(error));
 }

 public:
 using operation_state_concept = stdexec::operation_state_tag;

 transaction_operation(database_handle database,
                       std::optional<transaction_options> options,
                       invocation_t bound_invocation,
                       ReceiverT transaction_receiver)
  : receiver(std::move(transaction_receiver)),
    dbh(std::move(database)),
    opts(std::move(options)),
    invocation(std::move(bound_invocation))
 {}

 transaction_operation(transaction_operation&&) = delete;

 transaction_operation& operator=(transaction_operation&&) = delete;

 void start() & noexcept
 {
  if (stop_requested()) {
   stdexec::set_stopped(std::move(receiver));
   return;
  }

  try {
   txn = opts ? ceph::libfdb::make_transaction(std::move(dbh), *opts)
              : ceph::libfdb::make_transaction(std::move(dbh));
  } catch (...) {
   finish_error(std::current_exception());
   return;
  }

  start_attempt();
 }
};

template <typename FnT, typename ...ArgTs>
requires transaction_sender_invocation<FnT, ArgTs...>
class transaction_sender final
{
 using invocation_t = transaction_invocation_t<FnT, ArgTs...>;
 using body_sender_t = transaction_body_sender_t<FnT, ArgTs...>;

 database_handle dbh;
 std::optional<transaction_options> opts;
 invocation_t invocation;

 public:
 using sender_concept = stdexec::sender_tag;

 template <typename SelfT, typename ...EnvTs>
 static consteval auto get_completion_signatures()
 {
  return transaction_completion_signatures<body_sender_t, EnvTs...>();
 }

 transaction_sender(database_handle database,
                    std::optional<transaction_options> options,
                    invocation_t bound_invocation)
  : dbh(std::move(database)),
    opts(std::move(options)),
    invocation(std::move(bound_invocation))
 {}

 template <stdexec::receiver ReceiverT>
 auto connect(ReceiverT receiver) &&
 {
  return transaction_operation<FnT, ReceiverT, ArgTs...> {
   std::move(dbh), std::move(opts), std::move(invocation),
   std::move(receiver)
  };
 }

 [[nodiscard]] stdexec::env<> get_env() const noexcept { return {}; }
};

template <typename ReceiverT>
class scan_read_operation final
{
 struct stop_request final
 {
  scan_read_operation& operation;

  void operator()() const noexcept
  {
   operation.cancellation_requested.store(true, std::memory_order_release);

   // Cancellation may complete inline and destroy "operation", so this
   // FoundationDB call must remain last:
   operation.future->cancel();
  }
 };

 using stop_token_t = stdexec::stop_token_of_t<stdexec::env_of_t<ReceiverT>>;
 using stop_callback_t = stdexec::stop_callback_for_t<stop_token_t, stop_request>;

 transaction_handle txn;
 scan_cursor cursor;
 ReceiverT receiver;
 std::optional<ceph::libfdb::detail::future_value> future;
 std::optional<stop_callback_t> stop_callback;
 std::atomic_bool cancellation_requested = false;

 // The raw parameters are prescribed by FoundationDB's C callback interface:
 static void on_ready(FDBFuture *, void *context) noexcept
 {
  static_cast<scan_read_operation *>(context)->complete();
 }

 void complete() noexcept
 {
  stop_callback.reset();

  const FDBKeyValue *data = nullptr;
  int count = 0;
  fdb_bool_t more_available = false;

  if (const auto error = fdb_future_get_keyvalue_array(
        future->raw_handle(), &data, &count, &more_available); 0 != error) {
   if (was_cancelled_by_request(error, cancellation_requested)) {
    stdexec::set_stopped(std::move(receiver));
    return;
   }

   set_fdb_error(std::move(receiver), error);
   return;
  }

  std::span<const FDBKeyValue> rows;

  try {
   rows = ceph::libfdb::detail::result_span(data, count);
   cursor.progress.advance(rows, more_available);
  } catch (...) {
   stdexec::set_error(std::move(receiver), std::current_exception());
   return;
  }

  auto result = scan_window {
   std::move(txn), std::move(*future), rows, std::move(cursor)
  };

  stdexec::set_value(std::move(receiver), std::move(result));
 }

 public:
 using operation_state_concept = stdexec::operation_state_tag;

 scan_read_operation(transaction_handle transaction, scan_cursor read_cursor,
                     ReceiverT read_receiver)
  : txn(std::move(transaction)),
    cursor(std::move(read_cursor)),
    receiver(std::move(read_receiver))
 {}

 scan_read_operation(scan_read_operation&&) = delete;

 scan_read_operation& operator=(scan_read_operation&&) = delete;

 void start() & noexcept
 {
  const auto token = stdexec::get_stop_token(stdexec::get_env(receiver));

  if (token.stop_requested()) {
   stdexec::set_stopped(std::move(receiver));
   return;
  }

  try {
   if (not txn or not *txn) {
    throw std::invalid_argument("invalid FoundationDB transaction");
   }

   if (not cursor) {
    throw std::invalid_argument("cannot read from an exhausted scan cursor");
   }

   future.emplace(ceph::libfdb::detail::get_range_future_from_transaction(
     *txn, cursor.progress.current(), cursor.progress.fdb_iteration()));

   if (nullptr == future->raw_handle()) {
    throw std::invalid_argument("invalid FoundationDB future");
   }

   stop_callback.emplace(token, stop_request {*this});
  } catch (...) {
   stdexec::set_error(std::move(receiver), std::current_exception());
   return;
  }

  // FoundationDB may invoke on_ready() inline. On successful registration this
  // operation could therefore already be destroyed when the call returns:
  const auto registration_error = future->set_callback(on_ready, this);

  if (0 != registration_error) {
   stop_callback.reset();
   set_fdb_error(std::move(receiver), registration_error);
  }
 }
};

class scan_read_sender final
{
 transaction_handle txn;
 scan_cursor cursor;

 public:
 using sender_concept = stdexec::sender_tag;
 using completion_signatures = stdexec::completion_signatures<
   stdexec::set_value_t(scan_window),
   stdexec::set_error_t(std::exception_ptr),
   stdexec::set_stopped_t()>;

 scan_read_sender(transaction_handle transaction, scan_cursor read_cursor)
  : txn(std::move(transaction)),
    cursor(std::move(read_cursor))
 {}

 template <stdexec::receiver_of<completion_signatures> ReceiverT>
 auto connect(ReceiverT receiver) &&
 {
  return scan_read_operation<ReceiverT> {
   std::move(txn), std::move(cursor), std::move(receiver)
  };
 }

 [[nodiscard]] stdexec::env<> get_env() const noexcept { return {}; }
};

} // namespace detail

// Lazily runs one sender-producing transaction body, committing only after its
// successful completion and replaying when FoundationDB requests it:
template <typename FnT, typename ...ArgTs>
requires detail::transaction_sender_invocation<
 std::decay_t<FnT>, std::decay_t<ArgTs>...>
[[nodiscard]] auto transact(database_handle dbh, FnT&& fn, ArgTs&& ...args)
{
 using sender_t = detail::transaction_sender<
  std::decay_t<FnT>, std::decay_t<ArgTs>...>;

 return sender_t {
  std::move(dbh), std::nullopt,
  ceph::libfdb::detail::bind_transaction_invocation(
    std::forward<FnT>(fn), std::forward<ArgTs>(args)...)
 };
}

template <typename FnT, typename ...ArgTs>
requires detail::transaction_sender_invocation<
 std::decay_t<FnT>, std::decay_t<ArgTs>...>
[[nodiscard]] auto transact(database_handle dbh,
                            const transaction_options& options,
                            FnT&& fn, ArgTs&& ...args)
{
 using sender_t = detail::transaction_sender<
  std::decay_t<FnT>, std::decay_t<ArgTs>...>;

 return sender_t {
  std::move(dbh), options,
  ceph::libfdb::detail::bind_transaction_invocation(
    std::forward<FnT>(fn), std::forward<ArgTs>(args)...)
 };
}

// Creates a lazy raw point read on an existing transaction. The operation
// starts only when its connected operation state is started:
[[nodiscard]] inline auto get(raw_t, transaction_handle txn, const concepts::libfdb_key auto& key)
{
 return detail::point_read_sender {
  std::move(txn), std::string {ceph::libfdb::detail::as_libfdb_key_view(key)}
 };
}

// Lazily waits for one already-created FoundationDB watch to report a change:
[[nodiscard]] inline auto when_changed(watch_handle watch)
{
 return detail::watch_sender {
  ceph::libfdb::detail::take_watch_future(std::move(watch))
 };
}

// Creates a lazy commit attempt. Cancellation is honored before start; once
// submitted, the sender reports the actual commit or replay disposition:
[[nodiscard]] inline auto commit(transaction_handle txn)
{
 return detail::commit_sender {std::move(txn)};
}

// Reads one zero-copy result window and returns the cursor for the next one:
[[nodiscard]] inline auto read_window(raw_t, transaction_handle txn, scan_cursor cursor)
{
 return detail::scan_read_sender {std::move(txn), std::move(cursor)};
}

} // namespace ceph::libfdb::experimental

#endif // CEPH_EXPERIMENTAL_NVIDIA_STDEXEC

#endif
