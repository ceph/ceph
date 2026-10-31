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
#include <optional>
#include <string_view>
#include <tuple>

#include <algorithm>
#include <iterator>

#include <concepts>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

namespace ceph::libfdb {

enum struct transaction_phase { body, commit };

struct transaction_failure final
{
 std::error_code code;
 transaction_phase phase;
};

// Own one decision callable per logical invocation. Class template argument
// deduction keeps retry_if(callable) typed, including mutable/move-only state.
template <typename FnT = void>
class retry_if final
{
 [[no_unique_address]] FnT fn;

 public:
 explicit retry_if(FnT fn)
 requires std::invocable<FnT&, const transaction_failure&> and
          std::same_as<bool, std::invoke_result_t<FnT&, const transaction_failure&>>
  : fn(std::move(fn))
 {}

 public:
 bool operator()(const transaction_failure& failure)
 noexcept(std::is_nothrow_invocable_v<FnT&, const transaction_failure&>)
 {
  return std::invoke(fn, failure);
 }

};

// The ordinary path stores no policy state; native preparation decides eligibility.
template <>
struct retry_if<void> final
{
 bool operator()(const transaction_failure&) const noexcept
 {
  return true;
 }

};

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

 private:
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
  if constexpr (detail::staged_initialization::copy_target == Initialization) {
   attempt_value.reset();
  }

  if constexpr (detail::staged_initialization::in_place == Initialization) {
   attempt_value.emplace();
  }
 }

 private:
 explicit staged_proxy(T& value)
  : target(value)
 {}

 public:
 staged_proxy(const staged_proxy&) = delete;
 staged_proxy(staged_proxy&&) noexcept(std::is_nothrow_move_constructible_v<T>) = default;

 staged_proxy& operator=(const staged_proxy&) = delete;
 staged_proxy& operator=(staged_proxy&&) = delete;

 public:
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

template <typename T>
concept storable_invocation_result =
 not std::is_void_v<T> and supported_invocation_result<T>;

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

  if (std::ranges::adjacent_find(addresses, same_target) != std::ranges::end(addresses)) {
   throw libfdb_exception(std::errc::invalid_argument, "cannot stage one target more than once");
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

 public:
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

inline transaction_handle make_transaction(database_handle dbh)
{
 return std::make_shared<transaction>(std::move(dbh));
}

inline transaction_handle make_transaction(database_handle dbh, transaction_options opts)
{
 return std::make_shared<transaction>(std::move(dbh), std::move(opts));
}

namespace detail {

// Owns everything needed to create consistently configured transactions:
class transaction_source final
{
 database_handle dbh;
 transaction_options options;

 public:
 explicit transaction_source(database_handle database)
  : dbh(std::move(database))
 {}

 transaction_source(database_handle database, const transaction_options& opts)
  : dbh(std::move(database)),
    options(opts)
 {}

 public:
 [[nodiscard]] transaction_handle make() const
 {
  return make_transaction(dbh, options);
 }

};

} // namespace detail

// Submit once, without retry preparation. Native failures are returned as values;
// precondition and local failures may throw. Use reset_for_replay before replay.
[[nodiscard]] inline auto commit(with_result_t, const transaction_handle& txn)
 -> std::expected<void, std::error_code>
{
 if (not txn or not *txn) {
  throw libfdb_exception(std::errc::invalid_argument, "commit() requires a transaction");
 }

 return txn->commit(with_result);
}

inline void commit(const transaction_handle& txn)
{
 if (const auto result = commit(with_result, txn); not result) {
  throw libfdb_exception(result.error());
 }
}

[[nodiscard]] inline auto commit(with_result_t, const transaction_handle& txn, const versionstamp& stamp)
 -> std::expected<void, std::error_code>
{
 if (not txn or not *txn) {
  throw libfdb_exception(std::errc::invalid_argument, "commit() requires a transaction");
 }

 txn->mark_version(stamp);

 return commit(with_result, txn);
}

inline void commit(const transaction_handle& txn, const versionstamp& stamp)
{
 if (const auto result = commit(with_result, txn, stamp); not result) {
  throw libfdb_exception(result.error());
 }
}

[[nodiscard]] inline std::string get_key(transaction_handle txn, const key_selector& selector,
                                       const read_mode mode = read_mode::serializable)
{
 return detail::extract_byte_string(
          detail::block_until_ready(detail::transaction_get_key(txn, selector, mode)));
}

[[nodiscard]] inline std::int64_t committed_version(const transaction_handle& txn)
{
 return txn->committed_version();
}

[[nodiscard]] inline std::int64_t read_version(const transaction_handle& txn)
{
 return txn->read_version();
}

[[nodiscard]] inline std::int64_t approximate_commit_bytes(const transaction_handle& txn)
{
 return txn->approximate_commit_bytes();
}

inline void set_read_version(const transaction_handle& txn, const std::int64_t version)
{
 txn->set_read_version(version);
}

// Prepare an uncommitted transaction for replay after a native failure:
inline void reset_for_replay(const transaction_handle& txn, std::error_code error);

[[nodiscard]] inline watch_handle make_watch(transaction_handle txn,
                                             const concepts::libfdb_key auto& key)
{
 return txn->make_watch(detail::as_byte_view(key));
}

namespace detail {

// Recovery decisions are independent of blocking, callback, or sender delivery.
// Native on_error remains authoritative once the policy requests preparation.
template <typename PolicyT>
std::optional<fdb_error_t> retryable_error(const transaction_failure failure,
                                         const transaction_handle& txn,
                                         retry_if<PolicyT>& policy)
noexcept(std::is_nothrow_invocable_v<retry_if<PolicyT>&, const transaction_failure&>)
{
 if (transaction_phase::body == failure.phase) {
  finish_version_stamps(*txn, failure.code);
 }

 if (failure.code.category() != ::ceph::libfdb::fdb_error_category() or
     not policy(failure)) {
  return std::nullopt;
 }

 return failure.code.value();
}

template <typename PolicyT>
std::optional<fdb_error_t> retryable_error(const libfdb_exception& error,
                                         const transaction_handle& txn,
                                         retry_if<PolicyT>& policy)
noexcept(std::is_nothrow_invocable_v<retry_if<PolicyT>&, const transaction_failure&>)
{
 return retryable_error(transaction_failure {error.code(), transaction_phase::body},
                       txn, policy);
}

template <typename PolicyT>
std::optional<fdb_error_t> retryable_error(const std::exception_ptr& error,
                                         const transaction_handle& txn,
                                         retry_if<PolicyT>& policy)
noexcept(std::is_nothrow_invocable_v<retry_if<PolicyT>&, const transaction_failure&>)
try
{
 if (not error) {
  return std::nullopt;
 }

 std::rethrow_exception(error);
}
catch (const libfdb_exception& failure)
{
 return retryable_error(failure, txn, policy);
}
catch (...)
{
 return std::nullopt;
}

template <typename FnT, typename ...ArgTs>
using transaction_invocation_result_t =
 std::invoke_result_t<FnT&, transaction_handle&, ArgTs&...>;

template <typename FnT, typename ...ArgTs>
concept transaction_op =
 std::invocable<FnT&, transaction_handle&, ArgTs&...> &&
 concepts::supported_invocation_result<
   transaction_invocation_result_t<FnT, ArgTs...>>;

template <typename FnT, typename ...ArgTs>
concept bound_transaction_op =
 std::constructible_from<std::decay_t<FnT>, FnT> &&
 (std::constructible_from<std::decay_t<ArgTs>, ArgTs> && ...) &&
 std::invocable<std::decay_t<FnT>&,
                transaction_handle&,
                bound_argument_t<ArgTs>...> &&
 concepts::supported_invocation_result<bound_transaction_result_t<FnT, ArgTs...>>;

// Bind the callable and arguments once so replays see stable state:
template <typename FnT, typename ...ArgTs>
auto bind_transaction_invocation(FnT&& fn, ArgTs&& ...args)
{
 validate_staged_arguments(args...);

 return bound_invocation<std::decay_t<FnT>, std::decay_t<ArgTs>...> {
  .fn = std::forward<FnT>(fn),
  .arguments = {
   std::forward<ArgTs>(args)...
  }
 };
}

template <typename FnT>
using operation_result_t =
 std::conditional_t<std::is_void_v<transaction_invocation_result_t<FnT>>,
                    void,
                    std::remove_cvref_t<transaction_invocation_result_t<FnT>>>;

// Only native libfdb_exception needs an origin marker to bypass recovery.
// Other thrown types propagate without allocating an extra exception object:
struct user_callback_failure final
{
 std::exception_ptr cause;
};

template <typename FnT, typename ...ArgTs>
requires std::invocable<FnT&, ArgTs...>
decltype(auto) invoke_user_callback(FnT& fn, ArgTs&& ...arguments)
try
{
 return std::invoke(fn, std::forward<ArgTs>(arguments)...);
}
catch (const libfdb_exception& error)
{
 if (error.code().category() != ::ceph::libfdb::fdb_error_category()) {
  throw;
 }

 throw user_callback_failure {std::current_exception()};
}

template <typename FnT>
auto invoke_user_callback(std::in_place_t, FnT fn)
{
 return [fn = std::move(fn)](auto&& ...arguments) mutable -> decltype(auto)
 requires std::invocable<FnT&, decltype(arguments)...>
 {
  return invoke_user_callback(fn, std::forward<decltype(arguments)>(arguments)...);
 };
}

template <transaction_op FnT, typename PolicyT = void>
auto maybe_retry(transaction_handle txn, FnT&& fn, retry_if<PolicyT> policy = {})
 -> operation_result_t<FnT>;

template <transaction_op FnT>
auto commit_noreplay(transaction_handle txn,
                     commit_after_op commit_after,
                     FnT&& fn) -> operation_result_t<FnT>;

template <transaction_op FnT>
auto in_transaction(database_handle dbh, FnT&& fn) -> operation_result_t<FnT>;

template <transaction_op FnT>
auto in_transaction(database_handle dbh, const transaction_options& options, FnT&& fn)
 -> operation_result_t<FnT>;

template <transaction_op FnT>
auto in_transaction(const transaction_source& source, FnT&& fn) -> operation_result_t<FnT>;

template <transaction_op FnT>
auto in_read_transaction(database_handle dbh, FnT&& fn)
 -> operation_result_t<FnT>;

template <transaction_op FnT>
auto in_read_transaction(database_handle dbh, const transaction_options& options, FnT&& fn)
 -> operation_result_t<FnT>;

template <transaction_op FnT>
auto in_read_transaction(const transaction_source& source, FnT&& fn) -> operation_result_t<FnT>;

} // namespace detail

/* A "transactor" is a function-like wrapper for running replayable transactions.
 * It defers transaction creation until called, commits after the user function
 * returns, and replays when FoundationDB requests replay. Calls throw when
 * recovery fails or retries are exhausted. Arguments wrapped with staged()
 * publish only after a confirmed commit. Plus, the name is pretty cool. */
class transactor final
{
 detail::transaction_source source;

 private:
 explicit transactor(database_handle database)
  : source(std::move(database))
 {}

 transactor(database_handle database, const transaction_options& options)
  : source(std::move(database), options)
 {}

 public:
 template <typename FnT>
 requires detail::transaction_op<FnT>
 decltype(auto) operator()(FnT&& fn) const
 {
  return detail::maybe_retry(source.make(), std::forward<FnT>(fn));
 }

 template <typename FnT, typename ...ArgTs>
 requires (0 < sizeof...(ArgTs) &&
           detail::bound_transaction_op<std::reference_wrapper<std::remove_reference_t<FnT>>,
                                        ArgTs...>)
 decltype(auto) operator()(FnT&& fn, ArgTs&& ...args) const
 {
  auto bound = detail::bind_transaction_invocation(
    std::ref(fn), std::forward<ArgTs>(args)...);

  return (*this)(bound);
 }

 template <typename PolicyT, typename FnT>
 requires detail::transaction_op<FnT>
 decltype(auto) operator()(retry_if<PolicyT> policy, FnT&& fn) const
 {
  return detail::maybe_retry(source.make(), std::forward<FnT>(fn), std::move(policy));
 }

 template <typename PolicyT, typename FnT, typename ...ArgTs>
 requires (0 < sizeof...(ArgTs) &&
           detail::bound_transaction_op<std::reference_wrapper<std::remove_reference_t<FnT>>,
                                        ArgTs...>)
 decltype(auto) operator()(retry_if<PolicyT> policy, FnT&& fn, ArgTs&& ...args) const
 {
  auto bound = detail::bind_transaction_invocation(
   std::ref(fn), std::forward<ArgTs>(args)...);

  return (*this)(std::move(policy), bound);
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

inline void reset_for_replay(const transaction_handle& txn, const std::error_code error)
{
 auto preparation = detail::block_until_ready(detail::reset_for_replay_if_needed(txn, error));
 detail::transaction_replay_ready(*txn);
}

namespace detail {

template <typename ResultT>
struct invocation_result_traits final
{
 using stored_t = std::remove_cvref_t<ResultT>;

 template <typename FnT>
 static stored_t store(transaction_handle& txn, FnT&& fn)
 {
  return std::invoke(std::forward<FnT>(fn), txn);
 }

 static stored_t take(stored_t&& result)
 {
  return std::move(result);
 }

};

template <>
struct invocation_result_traits<void> final
{
 using stored_t = std::tuple<>;

 template <typename FnT>
 static stored_t store(transaction_handle& txn, FnT&& fn)
 {
  std::invoke(std::forward<FnT>(fn), txn);

  return {};
 }

 static void take(stored_t&&)
 {}

};

template <typename FnT, typename CommitFnT, typename PolicyT = void,
          typename ResultT = std::invoke_result_t<FnT&, transaction_handle&>>
auto invoke_with_retry(transaction_handle& txn, FnT&& fn, CommitFnT&& commit_fn,
                       retry_if<PolicyT> policy = {})
 -> operation_result_t<FnT>
try
{
 using stored_result_t = typename invocation_result_traits<ResultT>::stored_t;

 for (;;) {
  std::optional<stored_result_t> result;

  try {
   prepare_invocation(fn);
   result.emplace(invocation_result_traits<ResultT>::store(txn, fn));
  } catch (const libfdb_exception& e) {
   if (const auto replay_error = retryable_error(e, txn, policy)) {
    reset_for_replay(txn, std::error_code(*replay_error, ::ceph::libfdb::fdb_error_category()));
    continue;
   }

   throw;
  } catch (...) {
   finish_version_stamps(*txn, std::make_error_code(std::errc::operation_canceled));

   throw;
  }

  const auto commit_state = std::invoke(commit_fn, txn);

  if (not commit_state) {
   if (const auto replay_error = retryable_error(
         transaction_failure {commit_state.error(), transaction_phase::commit}, txn, policy)) {
    reset_for_replay(txn, std::error_code(*replay_error, ::ceph::libfdb::fdb_error_category()));
    continue;
   }

   throw libfdb_exception(commit_state.error());
  }

  publish_invocation(fn);

  return invocation_result_traits<ResultT>::take(std::move(*result));
 }
}
catch (const user_callback_failure& failure)
{
 std::rethrow_exception(failure.cause);
}

template <transaction_op FnT, typename PolicyT>
auto maybe_retry(transaction_handle txn, FnT&& fn, retry_if<PolicyT> policy)
 -> operation_result_t<FnT>
{
 return invoke_with_retry(txn, std::forward<FnT>(fn),
          [](transaction_handle& active_txn) {
           return ::ceph::libfdb::commit(with_result, active_txn);
          }, std::move(policy));
}

template <transaction_op FnT>
auto retry_without_commit(transaction_handle txn, FnT&& fn) -> operation_result_t<FnT>
{
 return invoke_with_retry(txn, std::forward<FnT>(fn),
          [](transaction_handle&) { return std::expected<void, std::error_code> {}; });
}

template <transaction_op FnT>
auto in_transaction(database_handle dbh, FnT&& fn) -> operation_result_t<FnT>
{
 return maybe_retry(make_transaction(std::move(dbh)), std::forward<FnT>(fn));
}

template <transaction_op FnT>
auto in_transaction(database_handle dbh, const transaction_options& options, FnT&& fn)
 -> operation_result_t<FnT>
{
 return maybe_retry(make_transaction(std::move(dbh), options), std::forward<FnT>(fn));
}

template <transaction_op FnT>
auto in_transaction(const transaction_source& source, FnT&& fn) -> operation_result_t<FnT>
{
 return maybe_retry(source.make(), std::forward<FnT>(fn));
}

template <transaction_op FnT>
auto in_read_transaction(database_handle dbh, FnT&& fn)
 -> operation_result_t<FnT>
{
 return retry_without_commit(make_transaction(std::move(dbh)), std::forward<FnT>(fn));
}

template <transaction_op FnT>
auto in_read_transaction(database_handle dbh, const transaction_options& options, FnT&& fn)
 -> operation_result_t<FnT>
{
 return retry_without_commit(make_transaction(std::move(dbh), options), std::forward<FnT>(fn));
}

template <transaction_op FnT>
auto in_read_transaction(const transaction_source& source, FnT&& fn) -> operation_result_t<FnT>
{
 // Successful read-only transactions can simply be destroyed without commit:
 return retry_without_commit(source.make(), std::forward<FnT>(fn));
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

 prepare_invocation(fn);
 auto result = invocation_result_traits<operation_result_t<FnT>>::store(txn, fn);
 ::ceph::libfdb::commit(txn);
 publish_invocation(fn);

 return invocation_result_traits<operation_result_t<FnT>>::take(std::move(result));
}

} // namespace detail

} // namespace ceph::libfdb

#endif
