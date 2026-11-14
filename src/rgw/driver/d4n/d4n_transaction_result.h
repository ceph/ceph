/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */

#ifndef CEPH_RGW_D4N_TRANSACTION_RESULT_H
 #define CEPH_RGW_D4N_TRANSACTION_RESULT_H

#include <concepts>
#include <exception>
#include <functional>
#include <optional>
#include <system_error>
#include <type_traits>

#include "common/inline_variant.h"
#include "rgw/fdb/transaction_result.h"

#include <utility>
#include <variant>
#include <expected>

namespace rgw::d4n {

enum class TransactionStatus {
  success = 0,
  uncertain = 2,
  failed = 3,
  retries_exhausted = 4
};

namespace detail {

// Holds a backend execution result and retry-limit state, retaining all native result details.
// TransactionResult below selects the result types used by D4N's backends:
template <typename ...Executions>
struct DBResult
{
 private:
 std::variant<Executions...> execution_;

 bool exhausted_ = false;

 public:
 DBResult() = default;

 template <typename Execution>
 requires (std::same_as<Execution, Executions> || ...)
 explicit DBResult(Execution&& result, const bool exhausted = false)
  noexcept(std::is_nothrow_move_constructible_v<Execution>)
  : execution_(std::move(result)),
    exhausted_(exhausted)
 {}

 public:
 explicit operator bool() const
  noexcept((noexcept(std::declval<const Executions&>().has_value()) && ...))
 {
   return std::visit([](const auto& result) -> bool { return result.has_value(); }, execution_);
 }

 bool retry_limit_reached() const noexcept { return exhausted_; }

 decltype(auto) inspect(auto&& fn) const&
 {
   return std::visit(std::forward<decltype(fn)>(fn), execution_);
 }

 void inspect(auto&&) const&& = delete;

};

} // namespace detail

using errno_result = std::expected<void, int>;
using fdb_result = ceph::libfdb::transaction_result<void, int>;
using fdb_failure = ceph::libfdb::transaction_failure<int>;
using TransactionResult = detail::DBResult<errno_result, fdb_result>;

struct ResultInfo
{
 TransactionStatus status = TransactionStatus::success;

 std::optional<int> application_error;

 std::error_code source_error;
};

namespace detail {

inline ResultInfo describe_failure(const int error, const bool exhausted) noexcept
{
 return {exhausted ? TransactionStatus::retries_exhausted : TransactionStatus::failed, error, {}};
}

inline ResultInfo describe_failure(const fdb_failure& failure, const bool exhausted) noexcept
{
 // Handle FoundationDB unknown_commit: uncertainty takes precedence over exhaustion:
 ResultInfo result {
   failure.status.has_unknown_commit() ? TransactionStatus::uncertain
     : exhausted ? TransactionStatus::retries_exhausted : TransactionStatus::failed,
   {}, {}
 };

 std::visit(overloaded {
   [&result](const ceph::libfdb::application_error<int>& cause) { result.application_error = cause.error; },
   [&result](const ceph::libfdb::fdb_error& cause) { result.source_error = cause.code; },
   [](const std::exception_ptr&) {}
 }, failure.cause);

 return result;
}

template <typename Handler>
concept uncertainty_handler =
  std::invocable<Handler&, const fdb_failure&> and
  std::same_as<std::invoke_result_t<Handler&, const fdb_failure&>, int>;

} // namespace detail

inline ResultInfo describe(const TransactionResult& result) noexcept
{
 return result.inspect([&](const auto& execution) {
   return execution ? ResultInfo {}
     : detail::describe_failure(execution.error(), result.retry_limit_reached());
 });
}

inline bool rejected_with(const TransactionResult& result, const int error) noexcept
{
 const auto info = describe(result);

 return TransactionStatus::failed == info.status && error == info.application_error;
}

} // namespace rgw::d4n

#endif // CEPH_RGW_D4N_TRANSACTION_RESULT_H
