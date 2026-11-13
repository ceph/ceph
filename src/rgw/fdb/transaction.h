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

// Own one decision callable per logical invocation. Class template argument
// deduction keeps retry_if(callable) typed, including mutable/move-only state.
template <typename FnT = void>
class retry_if final
{
 [[no_unique_address]] FnT fn;

 public:
 explicit retry_if(FnT fn)
 requires std::invocable<FnT&, const transaction_failure<>&> and
          std::same_as<bool, std::invoke_result_t<FnT&, const transaction_failure<>&>>
  : fn(std::move(fn))
 {}

 public:
 bool operator()(const transaction_failure<>& failure)
 noexcept(std::is_nothrow_invocable_v<FnT&, const transaction_failure<>&>)
 {
  return std::invoke(fn, failure);
 }

};

// The ordinary path stores no policy state; native preparation decides eligibility.
template <>
struct retry_if<void> final
{
 bool operator()(const transaction_failure<>&) const noexcept
 {
  return true;
 }

};

namespace detail {

template <typename FnT, typename ...ArgTs>
requires std::invocable<FnT&, ArgTs...>
decltype(auto) invoke_user_callback(FnT& fn, ArgTs&& ...arguments)
try
 {
  return std::invoke(fn, std::forward<ArgTs>(arguments)...);
 }
catch (const libfdb_exception& error)
 {
  if (error.code().category() != libfdb::fdb_error_category()) {
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
[[nodiscard]] auto staged(std::in_place_t, T& target)
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
 template <typename ...ArgTs>
 void reconstruct(ArgTs&& ...args)
 {
  auto construct = [&] { attempt_value.emplace(std::forward<ArgTs>(args)...); };
  detail::invoke_user_callback(construct);
 }

 T& materialize()
 {
  if (not attempt_value) {
   if constexpr (detail::staged_initialization::copy_target == Initialization) {
    reconstruct(target.get());
   }

   if constexpr (detail::staged_initialization::in_place == Initialization) {
    reconstruct();
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
   reconstruct();
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
  reconstruct(std::move(replacement));

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
 friend auto staged(std::in_place_t, U& target)
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

namespace detail {

template <typename ResultT>
struct invocation_result_traits final
{
 using value_type = ResultT;
 using error_type = void;
 using stored_t = std::expected<ResultT, std::monostate>;
 static constexpr bool controlled = false;
};

template <typename ValueT, typename ErrorT>
struct invocation_result_traits<std::expected<ValueT, ErrorT>> final
{
 using value_type = ValueT;
 using error_type = ErrorT;
 using stored_t = std::expected<ValueT, ErrorT>;
 static constexpr bool controlled = true;
};

// A bare error tag needs an explicit expected<T, E> body contract.
template <typename ErrorT>
struct invocation_result_traits<std::unexpected<ErrorT>> final {};

template <typename T>
concept safely_deliverable = std::is_void_v<T> or
 (std::is_object_v<T> and not is_staged_proxy<std::remove_cvref_t<T>> and
  std::is_nothrow_move_constructible_v<T> and std::destructible<T>);

} // namespace detail

namespace concepts {

template <typename T>
concept supported_invocation_result =
 not std::is_reference_v<T> and
 detail::safely_deliverable<typename detail::invocation_result_traits<std::remove_cv_t<T>>::value_type> and
 detail::safely_deliverable<typename detail::invocation_result_traits<std::remove_cv_t<T>>::error_type> and
 (std::is_void_v<T> or
  std::constructible_from<typename detail::invocation_result_traits<std::remove_cv_t<T>>::stored_t, T>);

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
[[nodiscard]] auto staged(std::in_place_t, T& target)
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

// Submit once. Recovery and whole-body replay remain with the transaction owner.
[[nodiscard]] inline transaction_result<void> commit(const transaction_handle& txn) noexcept
{
 if (not txn or not *txn) {
  return detail::fail_transaction(fdb_error {std::make_error_code(std::errc::invalid_argument), {}},
                                  transaction_phase::setup, {});
 }
 return txn->commit();
}

[[nodiscard]] inline transaction_result<void> commit(const transaction_handle& txn,
                                                     const versionstamp& stamp) noexcept
{
 if (not txn or not *txn) {
  return detail::fail_transaction(fdb_error {std::make_error_code(std::errc::invalid_argument), {}},
                                  transaction_phase::setup, {});
 }
 try
  {
   txn->mark_version(stamp);
   return txn->commit();
  }
 catch (...)
  {
   return detail::fail_transaction(
    detail::exception_failure(std::current_exception(), transaction_phase::setup, {}));
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

// Native on_error remains authoritative once the policy requests preparation.
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
auto in_read_transaction(database_handle dbh, FnT&& fn)
 -> operation_result_t<FnT>;

template <transaction_op FnT>
auto in_read_transaction(const transaction_source& source, FnT&& fn) -> operation_result_t<FnT>;

} // namespace detail

namespace detail {

template <typename FnT>
using managed_result_t = transaction_result<
 typename invocation_result_traits<std::remove_cv_t<transaction_invocation_result_t<FnT>>>::value_type,
 typename invocation_result_traits<std::remove_cv_t<transaction_invocation_result_t<FnT>>>::error_type>;

template <typename PolicyT, typename FnT, typename ...ArgTs>
auto transact(const transaction_source&, retry_policy, retry_if<PolicyT>&, FnT&&, ArgTs&&...) noexcept;

} // namespace detail

// A reusable configuration. Every invocation owns its transaction and native replay.
// Results retain commit knowledge, including earlier uncertain attempts.
class transactor final
{
 detail::transaction_source source;
 retry_policy retry;

 private:
 transactor(database_handle database, const transaction_options& options, const retry_policy policy)
  : source(std::move(database), options), retry(policy)
 {}

 public:
 template <typename FnT, typename ...ArgTs>
 requires detail::bound_transaction_op<std::reference_wrapper<std::remove_reference_t<FnT>>, ArgTs...>
 [[nodiscard]] auto operator()(FnT&& fn, ArgTs&& ...args) const noexcept
 {
  retry_if<> policy;
  return detail::transact(source, retry, policy, std::forward<FnT>(fn), std::forward<ArgTs>(args)...);
 }

 template <typename PolicyT, typename FnT, typename ...ArgTs>
 requires detail::bound_transaction_op<std::reference_wrapper<std::remove_reference_t<FnT>>, ArgTs...>
 [[nodiscard]] auto operator()(retry_if<PolicyT> policy, FnT&& fn, ArgTs&& ...args) const noexcept
 {
  return detail::transact(source, retry, policy, std::forward<FnT>(fn), std::forward<ArgTs>(args)...);
 }

 private:
 friend transactor make_transactor(database_handle, const transaction_options&, retry_policy);
};

[[nodiscard]] inline transactor make_transactor(database_handle dbh,
                                                const transaction_options& options = {},
                                                const retry_policy policy = retry_policy::retryable)
{
 return transactor(std::move(dbh), options, policy);
}

inline void reset_for_replay(const transaction_handle& txn, const std::error_code error)
{
 auto preparation = detail::block_until_ready(detail::reset_for_replay_if_needed(txn, error));
 detail::transaction_replay_ready(*txn);
}

namespace detail {

// Exception-based leaf/adapter boundaries share the same failure extraction.
[[noreturn]] inline void rethrow_failure(const transaction_failure<>& failure)
{
 if (const auto *native = std::get_if<fdb_error>(&failure.cause)) {
  if (const auto *recovery = std::get_if<std::error_code>(&native->retry_error)) {
   throw libfdb_exception(*recovery);
  }
  if (const auto *recovery = std::get_if<std::exception_ptr>(&native->retry_error)) {
   std::rethrow_exception(*recovery);
  }
  if (native->exception) {
   std::rethrow_exception(native->exception);
  }
  throw libfdb_exception(native->code);
 }

 std::rethrow_exception(std::get<std::exception_ptr>(failure.cause));
}

inline std::exception_ptr failure_exception(const transaction_failure<>& failure) noexcept
try
 {
  rethrow_failure(failure);
 }
catch (...)
 {
  return std::current_exception();
 }

// This boundary serves existing leaf helpers whose public contract is a value or
// exception. Transaction owners use the structured result directly.
template <typename ValueT>
ValueT take_operation_result(transaction_result<ValueT> result)
{
 if (not result) {
  rethrow_failure(result.error());
 }
 if constexpr (not std::is_void_v<ValueT>) {
  return std::move(result->value);
 }
}

// Decide without blocking; optional execution backends prepare replay themselves.
template <typename PolicyT>
std::optional<fdb_error_t> retryable_error(const transaction_failure<>& failure,
                                          retry_if<PolicyT>& policy,
                                          const retry_policy retry = retry_policy::retryable)
noexcept(std::is_nothrow_invocable_v<retry_if<PolicyT>&, const transaction_failure<>&>)
{
 if (transaction_phase::setup == failure.phase or
     transaction_phase::commit_observation == failure.phase) {
  return std::nullopt;
 }
 if (retry_policy::retryable_not_committed == retry and failure.status.has_unknown_commit()) {
  return std::nullopt;
 }

 const auto *native = std::get_if<fdb_error>(&failure.cause);
 if (not native or libfdb::fdb_error_category() != native->code.category() or not policy(failure)) {
  return std::nullopt;
 }

 return native->code.value();
}

// Resolve pending stamps on a failed attempt without asserting database rollback.
inline void finish_failed_attempt(const transaction_handle& txn, const transaction_failure<>& failure) noexcept
{
 if (txn) {
  const auto *native = std::get_if<fdb_error>(&failure.cause);
  finish_version_stamps(*txn, native ? native->code : std::make_error_code(std::errc::operation_canceled));
 }
}

template <typename PolicyT>
auto prepare_replay(const transaction_handle& txn, transaction_failure<> failure,
                    retry_if<PolicyT>& policy, const retry_policy retry) noexcept
 -> std::optional<transaction_failure<>>
{
 try
  {
   if (not retryable_error(failure, policy, retry)) {
    return failure;
   }
  }
 catch (...)
  {
   std::get<fdb_error>(failure.cause).retry_error = std::current_exception();
   return failure;
  }

 auto& native = std::get<fdb_error>(failure.cause);
 try
  {
   reset_for_replay(txn, native.code);
   return std::nullopt;
  }
 catch (const libfdb_exception& error)
  {
   native.retry_error = error.code();
  }
 catch (...)
  {
   native.retry_error = std::current_exception();
  }

 return failure;
}

template <typename FnT, typename CommitFnT, typename PolicyT = void>
auto invoke_with_retry(transaction_handle& txn, FnT&& fn, CommitFnT&& commit_fn,
                       retry_if<PolicyT>& policy,
                       const retry_policy retry = retry_policy::retryable) noexcept
 -> managed_result_t<FnT>
{
 using raw_result = transaction_invocation_result_t<FnT>;
 using traits = invocation_result_traits<std::remove_cv_t<raw_result>>;
 using value_type = typename traits::value_type;
 using error_type = typename traits::error_type;
 using result_type = managed_result_t<FnT>;
 static_assert(std::is_nothrow_move_constructible_v<result_type>);

 transaction_status status;
 auto phase = transaction_phase::body;
 try
  {
   for (;;) {
    next_attempt(status);
    phase = transaction_phase::body;
    std::optional<typename traits::stored_t> attempt;
    try
     {
      prepare_invocation(fn);
      if constexpr (std::is_void_v<raw_result>) {
       std::invoke(fn, txn);
       attempt.emplace();
      }
      if constexpr (not std::is_void_v<raw_result>) {
       attempt.emplace(std::invoke(fn, txn));
      }
     }
    catch (...)
     {
      auto failure = exception_failure(std::current_exception(), phase, status);
      finish_failed_attempt(txn, failure);
      if (auto stopped = prepare_replay(txn, std::move(failure), policy, retry)) {
       return fail_transaction<error_type>(std::move(*stopped));
      }
      continue;
     }

    if constexpr (traits::controlled) {
     if (not *attempt) {
      finish_version_stamps(*txn, std::make_error_code(std::errc::operation_canceled));
      return fail_transaction<error_type>(application_error<error_type> {std::move(attempt->error())},
                                          phase, status);
     }
    }

    if constexpr (not std::same_as<std::remove_cvref_t<CommitFnT>, std::nullopt_t>) {
     phase = transaction_phase::commit;
     auto completion = std::invoke(commit_fn, txn);
     observe_commit(status, completion ? completion->status : completion.error().status);
     if (not completion) {
      auto failure = std::move(completion.error());
      phase = failure.phase;
      failure.status = status;
      if (auto stopped = prepare_replay(txn, std::move(failure), policy, retry)) {
       return fail_transaction<error_type>(std::move(*stopped));
      }
      continue;
     }
    }

    publish_invocation(fn);
    if constexpr (std::is_void_v<value_type>) {
     return result_type {std::in_place, status};
    }
    if constexpr (not std::is_void_v<value_type>) {
     return result_type {std::in_place, std::move(**attempt), status};
    }
   }
  }
 catch (...)
  {
   auto failure = exception_failure(std::current_exception(), phase, status);
   finish_failed_attempt(txn, failure);
   return fail_transaction<error_type>(std::move(failure));
  }
}

template <typename PolicyT, typename FnT, typename ...ArgTs>
auto transact(const transaction_source& source, const retry_policy retry, retry_if<PolicyT>& policy,
               FnT&& fn, ArgTs&& ...args) noexcept
{
 using invocation_t = decltype(bind_transaction_invocation(std::ref(fn), std::forward<ArgTs>(args)...));
 using traits = invocation_result_traits<std::remove_cv_t<transaction_invocation_result_t<invocation_t>>>;
 using error_type = typename traits::error_type;
 using result_type = managed_result_t<invocation_t>;
 try
  {
   auto invocation = bind_transaction_invocation(std::ref(fn), std::forward<ArgTs>(args)...);
   auto txn = source.make();
   return invoke_with_retry(txn, invocation,
    [](const transaction_handle& current) { return ceph::libfdb::commit(current); }, policy, retry);
  }
 catch (...)
  {
   return result_type {fail_transaction<error_type>(
    exception_failure(std::current_exception(), transaction_phase::setup, {}))};
  }
}

template <transaction_op FnT, typename PolicyT>
auto maybe_retry(transaction_handle txn, FnT&& fn, retry_if<PolicyT> policy)
 -> operation_result_t<FnT>
{
 return take_operation_result(invoke_with_retry(txn, std::forward<FnT>(fn),
  [](const transaction_handle& current) { return ceph::libfdb::commit(current); }, policy));
}

template <transaction_op FnT>
auto retry_without_commit(transaction_handle txn, FnT&& fn) -> operation_result_t<FnT>
{
 retry_if<> policy;
 return take_operation_result(invoke_with_retry(txn, std::forward<FnT>(fn),
  std::nullopt, policy));
}

template <transaction_op FnT>
auto in_transaction(database_handle dbh, FnT&& fn) -> operation_result_t<FnT>
{
 return maybe_retry(make_transaction(std::move(dbh)), std::forward<FnT>(fn));
}

template <transaction_op FnT>
auto in_read_transaction(database_handle dbh, FnT&& fn)
 -> operation_result_t<FnT>
{
 return retry_without_commit(make_transaction(std::move(dbh)), std::forward<FnT>(fn));
}

template <transaction_op FnT>
auto in_read_transaction(const transaction_source& source, FnT&& fn) -> operation_result_t<FnT>
{
 return retry_without_commit(source.make(), std::forward<FnT>(fn));
}

// Leaf helpers do not own whole-body replay when supplied a transaction.
template <transaction_op FnT>
auto commit_noreplay(transaction_handle txn,
                     const commit_after_op commit_after,
                     FnT&& fn) -> operation_result_t<FnT>
{
 if (commit_after_op::commit != commit_after) {
  return std::invoke(fn, txn);
 }

 // A body failure leaves this caller-owned transaction usable. A replay owner's
 // attempt cleanup would prematurely finalize its pending versionstamps.
 prepare_invocation(fn);
 if constexpr (std::is_void_v<operation_result_t<FnT>>) {
  std::invoke(fn, txn);
  take_operation_result(ceph::libfdb::commit(txn));
  publish_invocation(fn);
 }
 if constexpr (not std::is_void_v<operation_result_t<FnT>>) {
  auto value = std::invoke(fn, txn);
  take_operation_result(ceph::libfdb::commit(txn));
  publish_invocation(fn);
  return value;
 }
}

} // namespace detail

} // namespace ceph::libfdb

#endif
