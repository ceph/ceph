// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025-2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_FDB_TRANSACTION_H
#define CEPH_FDB_TRANSACTION_H

#include "base.h"

#include <array>
#include <tuple>
#include <optional>
#include <string_view>

#include <algorithm>

#include <memory>
#include <cstddef>
#include <utility>
#include <concepts>
#include <stdexcept>
#include <functional>
#include <type_traits>

namespace ceph::libfdb {

namespace detail {

template <typename T>
inline constexpr bool is_staged_proxy = false;

enum struct staged_initialization { copy_target, in_place };

struct staged_access;

} // namespace detail

namespace concepts {

// Publication must not fail after the database commits:
template <typename T>
concept stageable =
 std::copy_constructible<T> and
 std::is_nothrow_move_assignable_v<T>;

template <typename T>
concept in_place_stageable =
 std::move_constructible<T> and std::default_initializable<T> and
 std::is_nothrow_move_assignable_v<T>;

} // namespace concepts

namespace detail {

template <typename T, staged_initialization Initialization>
concept stageable_with =
 (staged_initialization::copy_target == Initialization and
  concepts::stageable<T>) or
 (staged_initialization::in_place == Initialization and
  concepts::in_place_stageable<T>);

} // namespace detail

template <typename T,
          detail::staged_initialization Initialization =
            detail::staged_initialization::copy_target>
requires detail::stageable_with<T, Initialization>
class staged_proxy;

template <concepts::stageable T>
[[nodiscard]] staged_proxy<T> staged(T& target);

template <concepts::in_place_stageable T>
[[nodiscard]] auto staged(T& target, std::in_place_t)
 -> staged_proxy<T, detail::staged_initialization::in_place>;

// A transaction-attempt-local view of a value supplied through staged(). It is
// also the compile-time binding stored by a transactor invocation.
template <typename T, detail::staged_initialization Initialization>
requires detail::stageable_with<T, Initialization>
class staged_proxy final
{
 std::reference_wrapper<T> target;
 std::optional<T> attempt_value;

 T& materialize()
 {
  if (not attempt_value) {
   if constexpr (detail::staged_initialization::copy_target == Initialization) {
    attempt_value.emplace(target.get());
   }

   if constexpr (detail::staged_initialization::in_place == Initialization) {
    attempt_value.emplace();
   }
  }

  return *attempt_value;
 }

 void prepare_attempt()
 noexcept(detail::staged_initialization::copy_target == Initialization or
          std::is_nothrow_default_constructible_v<T>)
 {
  attempt_value.reset();

  if constexpr (detail::staged_initialization::in_place == Initialization) {
   attempt_value.emplace();
  }
 }

 explicit staged_proxy(T& value)
  : target(value)
 {}

 public:
 staged_proxy(const staged_proxy&) = delete;
 staged_proxy(staged_proxy&&) noexcept(std::is_nothrow_move_constructible_v<T>) = default;

 staged_proxy& operator=(const staged_proxy&) = delete;
 staged_proxy& operator=(staged_proxy&&) = delete;

 staged_proxy& operator=(T replacement)
 {
  attempt_value.emplace(std::move(replacement));

  return *this;
 }

 template <typename U>
 requires requires(T& active_target, U&& value) {
  active_target += std::forward<U>(value);
 }
 staged_proxy& operator+=(U&& value)
 {
  materialize() += std::forward<U>(value);

  return *this;
 }

 void clear()
 requires requires(T& active_target) { active_target.clear(); }
 {
  materialize().clear();
 }

 template <typename U>
 requires requires(T& active_target, U&& value) {
  active_target.push_back(std::forward<U>(value));
 }
 void push_back(U&& value)
 {
  materialize().push_back(std::forward<U>(value));
 }

 template <typename ...ArgTs>
 requires requires(T& active_target, ArgTs&& ...args) {
  active_target.emplace_back(std::forward<ArgTs>(args)...);
 }
 void emplace_back(ArgTs&& ...args)
 {
  materialize().emplace_back(std::forward<ArgTs>(args)...);
 }

 template <typename KeyT, typename ValueT>
 requires requires(T& active_target, KeyT&& key, ValueT&& value) {
  active_target.insert_or_assign(std::forward<KeyT>(key),
                                 std::forward<ValueT>(value));
 }
 void insert_or_assign(KeyT&& key, ValueT&& value)
 {
  materialize().insert_or_assign(std::forward<KeyT>(key),
                                 std::forward<ValueT>(value));
 }

 // Escape hatch for an operation outside the intentionally small proxy API;
 // this returns attempt-local state, not the original target:
 [[nodiscard]] T& get_target() &
 {
  return materialize();
 }

 T& get_target() && = delete;

 private:
 friend struct detail::staged_access;

 template <concepts::stageable U>
 friend staged_proxy<U> staged(U& target);

 template <concepts::in_place_stageable U>
 friend auto staged(U& target, std::in_place_t)
  -> staged_proxy<U, detail::staged_initialization::in_place>;
};

namespace detail {

template <typename T, staged_initialization Initialization>
requires stageable_with<T, Initialization>
inline constexpr bool is_staged_proxy<staged_proxy<T, Initialization>> = true;

template <typename T>
concept staged_argument = is_staged_proxy<std::remove_cvref_t<T>>;

struct staged_access final
{
 template <staged_argument ProxyT>
 static void prepare_attempt(ProxyT& proxy)
 noexcept(noexcept(proxy.prepare_attempt()))
 {
  proxy.prepare_attempt();
 }

 template <staged_argument ProxyT>
 static void publish(ProxyT& proxy) noexcept
 {
  if (not proxy.attempt_value) {
   return;
  }

  proxy.target.get() = std::move(*proxy.attempt_value);
  proxy.attempt_value.reset();
 }

 template <staged_argument ProxyT>
 [[nodiscard]] static const void *target_address(
   const ProxyT& proxy) noexcept
 {
  return std::addressof(proxy.target.get());
 }
};

} // namespace detail

namespace concepts {

template <typename T>
concept supported_invocation_result =
 std::is_void_v<T> or
 (not detail::is_staged_proxy<std::remove_cvref_t<T>> and
  not std::is_reference_v<T> and
  std::constructible_from<std::remove_cvref_t<T>, T> and
  std::move_constructible<std::remove_cvref_t<T>>);

} // namespace concepts

namespace detail {

inline const void *staged_target_address(const auto&) noexcept
{
 return nullptr;
}

template <staged_argument ProxyT>
const void *staged_target_address(const ProxyT& proxy) noexcept
{
 return staged_access::target_address(proxy);
}

template <typename ...ArgTs>
void validate_staged_arguments(const ArgTs& ...arguments)
{
 if constexpr ((is_staged_proxy<std::remove_cvref_t<ArgTs>> or ...)) {
  std::array addresses {staged_target_address(arguments)...};
  std::ranges::sort(addresses);
  const auto same_target = [](const void *lhs, const void *rhs) {
   return nullptr != lhs and lhs == rhs;
  };

  if (std::ranges::adjacent_find(addresses, same_target) != addresses.end()) {
   throw std::invalid_argument("cannot stage one target more than once");
  }
 }
}

inline void prepare_bound_argument(auto&) noexcept
{}

template <staged_argument ProxyT>
void prepare_bound_argument(ProxyT& proxy)
noexcept(noexcept(staged_access::prepare_attempt(proxy)))
{
 staged_access::prepare_attempt(proxy);
}

inline void publish_bound_argument(auto&) noexcept
{}

template <staged_argument ProxyT>
void publish_bound_argument(ProxyT& proxy) noexcept
{
 staged_access::publish(proxy);
}

template <typename FnT, typename ...ArgTs>
struct bound_invocation final
{
 FnT fn;
 std::tuple<ArgTs...> arguments;

 void prepare_attempt()
 noexcept((noexcept(prepare_bound_argument(std::declval<ArgTs&>())) and ...))
 {
  std::apply([](auto& ...argument) {
    (prepare_bound_argument(argument), ...);
  }, arguments);
 }

 void publish() noexcept
 {
  std::apply([](auto& ...argument) noexcept {
    (publish_bound_argument(argument), ...);
  }, arguments);
 }

 decltype(auto) operator()(transaction_handle& txn)
 {
  return std::apply([this, &txn](auto& ...argument) -> decltype(auto) {
    return std::invoke(fn, txn, argument...);
  }, arguments);
 }
};

template <typename FnT>
void prepare_invocation(FnT&) noexcept
{}

template <typename FnT, typename ...ArgTs>
void prepare_invocation(bound_invocation<FnT, ArgTs...>& invocation)
noexcept(noexcept(invocation.prepare_attempt()))
{
 invocation.prepare_attempt();
}

template <typename FnT>
void publish_invocation(FnT&) noexcept
{}

template <typename FnT, typename ...ArgTs>
void publish_invocation(bound_invocation<FnT, ArgTs...>& invocation) noexcept
{
 invocation.publish();
}

template <typename T>
using bound_argument_t = std::decay_t<T>&;

template <typename FnT, typename ...ArgTs>
using bound_transaction_result_t =
 std::invoke_result_t<std::decay_t<FnT>&,
                      transaction_handle&,
                      bound_argument_t<ArgTs>...>;

} // namespace detail

// Mark a bound transactor argument as local output. Each attempt gets isolated
// state; the target is updated only after a confirmed commit:
template <concepts::stageable T>
[[nodiscard]] staged_proxy<T> staged(T& target)
{
 return staged_proxy<T> {target};
}

// Construct fresh attempt-local state instead of copying the target:
template <concepts::in_place_stageable T>
[[nodiscard]] auto staged(T& target, std::in_place_t)
 -> staged_proxy<T, detail::staged_initialization::in_place>
{
 return staged_proxy<T, detail::staged_initialization::in_place> {target};
}

// Overload tag for transactors that should report replay/commit metadata:
struct with_result_t final {};
inline constexpr with_result_t with_result;

// Describes replay/commit work performed by result-reporting transactors.
struct transaction_result final
{
 bool committed = false;
 std::size_t attempts = 0;

 // Failures followed by another attempt; excludes the final exhausted attempt:
 std::size_t replay_count = 0;

 fdb_error_t last_error = 0;
};

// Describes a commit attempt when the caller owns transaction replay.
struct commit_result final
{
 bool committed = false;
 fdb_error_t replay_error = 0;
};

inline transaction_handle make_transaction(database_handle dbh)
{
 return std::make_shared<transaction>(std::move(dbh));
}

inline transaction_handle make_transaction(database_handle dbh, const transaction_options& opts)
{
 return std::make_shared<transaction>(std::move(dbh), opts);
}

// Note: only rarely is a direct call to this needed. You can use transactors or
// pass database handles to operations for automatic transaction management.
// After a transaction is committed, it cannot be used again. A false result
// means that the client should replay the transaction:
[[nodiscard]] inline bool commit(transaction_handle& txn)
{
 return txn->commit();
}

[[nodiscard]] inline commit_result commit(with_result_t, transaction_handle& txn)
{
 commit_result result;
 result.committed = txn->commit(&result.replay_error);

 return result;
}

[[nodiscard]] inline bool commit(transaction_handle& txn, const versionstamp& stamp)
{
 txn->mark_version(stamp);
 return commit(txn);
}

// Prepare a transaction to replay after a retryable operation-body error:
inline void prepare_replay(transaction_handle& txn, fdb_error_t error);

[[nodiscard]] inline watch_handle make_watch(transaction_handle txn, std::string_view key)
{
 return txn->make_watch(detail::as_byte_view(key));
}

namespace detail {

template <typename FnT, typename ...ArgTs>
using transaction_invocation_result_t =
 std::invoke_result_t<FnT&, transaction_handle&, ArgTs&...>;

template <typename FnT, typename ...ArgTs>
concept transaction_op =
 std::invocable<FnT&, transaction_handle&, ArgTs&...> &&
 concepts::supported_invocation_result<
   transaction_invocation_result_t<FnT, ArgTs...>>;

template <typename FnT, typename ...ArgTs>
concept result_reporting_transaction_op =
 transaction_op<FnT, ArgTs...> &&
 std::is_void_v<transaction_invocation_result_t<FnT, ArgTs...>>;

template <typename FnT, typename ...ArgTs>
concept bound_transaction_op =
 std::constructible_from<std::decay_t<FnT>, FnT> &&
 (std::constructible_from<std::decay_t<ArgTs>, ArgTs> && ...) &&
 std::invocable<std::decay_t<FnT>&,
                transaction_handle&,
                bound_argument_t<ArgTs>...> &&
 concepts::supported_invocation_result<bound_transaction_result_t<FnT, ArgTs...>>;

template <typename FnT, typename ...ArgTs>
concept bound_result_reporting_transaction_op =
 bound_transaction_op<FnT, ArgTs...> &&
 std::is_void_v<bound_transaction_result_t<FnT, ArgTs...>>;

template <typename FnT>
using operation_result_t =
 std::conditional_t<std::is_void_v<transaction_invocation_result_t<FnT>>,
                    void,
                    std::remove_cvref_t<transaction_invocation_result_t<FnT>>>;

template <transaction_op FnT>
auto maybe_retry(transaction_handle txn, FnT&& fn) -> operation_result_t<FnT>;

template <result_reporting_transaction_op FnT>
transaction_result maybe_retry_with_result(transaction_handle txn, FnT&& fn);

template <transaction_op FnT>
auto commit_noreplay(transaction_handle txn,
                     commit_after_op commit_after,
                     FnT&& fn) -> operation_result_t<FnT>;

template <transaction_op FnT>
auto in_transaction(database_handle dbh, FnT&& fn) -> operation_result_t<FnT>;

} // namespace detail

/* A "transactor" is a function-like wrapper for running replayable transactions.
 * It defers transaction creation until called, commits after the user function
 * returns, and replays when FoundationDB requests replay. Ordinary calls throw
 * when recovery fails or retries are exhausted; with_result calls report retry
 * exhaustion. Arguments wrapped with staged() publish only after a confirmed
 * commit. Plus, the name is pretty cool. */
class transactor final
{
 database_handle dbh;
 std::optional<transaction_options> opts;

 private:
 explicit transactor(database_handle database)
  : dbh(std::move(database))
 {}

 transactor(database_handle database, const transaction_options& options)
  : dbh(std::move(database)),
    opts(options)
 {}

 // Bind the callable and arguments once so replays see stable state:
 template <typename FnT, typename ...ArgTs>
 static auto bind_invocation(FnT&& fn, ArgTs&& ...args)
 {
  detail::validate_staged_arguments(args...);

  return detail::bound_invocation<std::decay_t<FnT>,
                                  std::decay_t<ArgTs>...> {
   .fn = std::forward<FnT>(fn),
   .arguments = {
    std::forward<ArgTs>(args)...
   }
  };
 }

 transaction_handle make_transaction_for_call() const
 {
  return opts ? make_transaction(dbh, *opts) : make_transaction(dbh);
 }

 public:
 template <typename FnT>
 requires detail::transaction_op<FnT>
 decltype(auto) operator()(FnT&& fn) const
 {
  return detail::maybe_retry(make_transaction_for_call(),
                             std::forward<FnT>(fn));
 }

 template <typename FnT, typename ...ArgTs>
 requires (0 < sizeof...(ArgTs) && detail::bound_transaction_op<FnT, ArgTs...>)
 decltype(auto) operator()(FnT&& fn, ArgTs&& ...args) const
 {
  auto bound = bind_invocation(std::forward<FnT>(fn),
                               std::forward<ArgTs>(args)...);

  return (*this)(bound);
 }

 template <typename FnT>
 requires detail::result_reporting_transaction_op<FnT>
 transaction_result operator()(with_result_t, FnT&& fn) const
 {
  return detail::maybe_retry_with_result(make_transaction_for_call(),
                                         std::forward<FnT>(fn));
 }

 template <typename FnT, typename ...ArgTs>
 requires (0 < sizeof...(ArgTs) &&
           detail::bound_result_reporting_transaction_op<FnT, ArgTs...>)
 transaction_result operator()(with_result_t, FnT&& fn, ArgTs&& ...args) const
 {
  auto bound = bind_invocation(std::forward<FnT>(fn),
                               std::forward<ArgTs>(args)...);

  return (*this)(with_result, bound);
 }

 private:
 friend inline transactor make_transactor(database_handle dbh);
 friend inline transactor make_transactor(database_handle dbh, const transaction_options& opts);
};

inline transactor make_transactor(database_handle dbh)
{
 return transactor(std::move(dbh));
}

inline transactor make_transactor(database_handle dbh, const transaction_options& opts)
{
 return transactor(std::move(dbh), opts);
}

namespace detail {

inline commit_result commit_or_throw(transaction_handle& txn)
{
 const auto result = ceph::libfdb::commit(with_result, txn);

 if (result.committed) {
  return result;
 }

 if (0 == result.replay_error) {
  throw libfdb_exception("transactor commit did not start");
 }

 throw libfdb_exception(result.replay_error);
}

inline bool retry_after_error(transaction_handle& txn, const fdb_error_t error)
{
 if (0 == error) {
  return false;
 }

 if (not fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, error)) {
  // Non-retryable errors cannot be repaired by on_error():
  throw libfdb_exception(error);
 }

 const auto on_error_result =
  get_future_error(wait_for_on_error(txn->raw_handle(), error));

 if (0 != on_error_result) {
  throw libfdb_exception(error);
 }

 return true;
}

} // namespace detail

inline void prepare_replay(transaction_handle& txn, const fdb_error_t error)
{
 if (not detail::retry_after_error(txn, error)) {
  throw libfdb_exception(error);
 }
}

namespace detail {

enum struct invocation_failure_policy { no_retry, retry };

struct no_invocation_result final {};

// Keep the existing transactor retry behavior in one place:
inline constexpr std::size_t transaction_retry_attempts = 10;

template <typename ResultT>
struct invocation_result_traits final
{
 using stored_t = std::remove_cvref_t<ResultT>;

 template <typename FnT>
 static stored_t store(transaction_handle& txn, FnT&& fn)
 {
  return std::invoke(std::forward<FnT>(fn), txn);
 }

 static stored_t take(std::optional<stored_t>&& result)
 {
  return *std::move(result);
 }
};

template <>
struct invocation_result_traits<void> final
{
 using stored_t = no_invocation_result;

 template <typename FnT>
 static stored_t store(transaction_handle& txn, FnT&& fn)
 {
  std::invoke(std::forward<FnT>(fn), txn);

  return {};
 }

 static void take(std::optional<stored_t>&&)
 {}
};

template <typename ResultT>
using stored_invocation_result_t =
 typename invocation_result_traits<ResultT>::stored_t;

template <typename ResultT>
struct invocation_attempt final
{
 std::optional<stored_invocation_result_t<ResultT>> value;
 fdb_error_t replay_error = 0;
};

template <typename ResultT>
struct invocation_outcome final
{
 std::optional<stored_invocation_result_t<ResultT>> value;
 std::size_t attempts = 0;
 std::size_t replay_count = 0;
 fdb_error_t last_error = 0;
};

template <invocation_failure_policy FailurePolicy,
          typename FnT,
          typename CommitFnT,
          typename ResultT = std::invoke_result_t<FnT&, transaction_handle&>>
auto attempt_invocation(transaction_handle& txn,
                        FnT&& fn,
                        CommitFnT&& commit_fn)
 -> invocation_attempt<ResultT>
{
 using stored_result_t = stored_invocation_result_t<ResultT>;

 std::optional<stored_result_t> result;

 try {
  result.emplace(invocation_result_traits<ResultT>::store(txn, fn));
 } catch (const libfdb_exception& e) {
  if constexpr (invocation_failure_policy::no_retry == FailurePolicy) {
   throw;
  }

  if (not e.retryable()) {
   throw;
  }

  prepare_replay(txn, e.fdb_error_value);
  return invocation_attempt<ResultT> {
   .value = std::nullopt,
   .replay_error = e.fdb_error_value
  };
 }

 const auto commit_state = std::invoke(commit_fn, txn);

 if (not commit_state.committed) {
  if (0 == commit_state.replay_error) {
   throw libfdb_exception("transactor commit did not start");
  }

  return invocation_attempt<ResultT> {
   .value = std::nullopt,
   .replay_error = commit_state.replay_error
  };
 }

 return {.value = std::move(result)};
}

template <invocation_failure_policy FailurePolicy,
          typename FnT,
          typename CommitFnT,
          typename ResultT = std::invoke_result_t<FnT&, transaction_handle&>>
auto invoke_with_retry(transaction_handle& txn,
                       FnT&& fn,
                       CommitFnT&& commit_fn) -> invocation_outcome<ResultT>
{
 invocation_outcome<ResultT> outcome;

 for (auto tries = transaction_retry_attempts; tries; --tries) {
  ++outcome.attempts;
  prepare_invocation(fn);

  auto attempt = attempt_invocation<FailurePolicy>(txn, fn, commit_fn);

  if (attempt.value) {
   publish_invocation(fn);
   outcome.value = std::move(attempt.value);

   return outcome;
  }

  outcome.last_error = attempt.replay_error;

  if (1 < tries) {
   ++outcome.replay_count;
  }
 }

 return outcome;
}

template <typename ResultT>
decltype(auto) take_invocation_outcome(invocation_outcome<ResultT>&& outcome)
{
 if (not outcome.value) {
  throw libfdb_exception("transaction retry limit exceeded");
 }

 return invocation_result_traits<ResultT>::take(std::move(outcome.value));
}

template <transaction_op FnT>
auto maybe_retry(transaction_handle txn, FnT&& fn) -> operation_result_t<FnT>
{
 using result_t = transaction_invocation_result_t<FnT>;

 auto outcome = invoke_with_retry<invocation_failure_policy::retry>(
   txn, std::forward<FnT>(fn),
   [](transaction_handle& active_txn) {
     return ceph::libfdb::commit(with_result, active_txn);
   });

 return take_invocation_outcome<result_t>(std::move(outcome));
}

template <transaction_op FnT>
auto retry_without_commit(transaction_handle txn,
                          FnT&& fn) -> operation_result_t<FnT>
{
 using result_t = transaction_invocation_result_t<FnT>;

 auto outcome = invoke_with_retry<invocation_failure_policy::retry>(
   txn, std::forward<FnT>(fn),
   [](transaction_handle&) {
     return commit_result {.committed = true};
   });

 return take_invocation_outcome<result_t>(std::move(outcome));
}

template <result_reporting_transaction_op FnT>
transaction_result maybe_retry_with_result(transaction_handle txn, FnT&& fn)
{
 auto outcome = invoke_with_retry<invocation_failure_policy::retry>(
   txn, std::forward<FnT>(fn),
   [](transaction_handle& active_txn) {
     return ceph::libfdb::commit(with_result, active_txn);
   });

 return {
  .committed = outcome.value.has_value(),
  .attempts = outcome.attempts,
  .replay_count = outcome.replay_count,
  .last_error = outcome.last_error
 };
}

template <transaction_op FnT>
auto in_transaction(database_handle dbh, FnT&& fn) -> operation_result_t<FnT>
{
 return maybe_retry(make_transaction(std::move(dbh)),
                    std::forward<FnT>(fn));
}

// Commit only once; the caller is responsible for transaction replay:
template <transaction_op FnT>
auto commit_noreplay(transaction_handle txn,
                     const commit_after_op commit_after,
                     FnT&& fn) -> operation_result_t<FnT>
{
 if (commit_after_op::commit != commit_after) {
  return std::invoke(fn, txn);
 }

 using result_t = transaction_invocation_result_t<FnT>;

 auto outcome = invoke_with_retry<invocation_failure_policy::no_retry>(
   txn, std::forward<FnT>(fn), commit_or_throw);

 return take_invocation_outcome<result_t>(std::move(outcome));
}

} // namespace detail

} // namespace ceph::libfdb

#endif
