// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Ceph - scalable distributed file system
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 * SPDX-License-Identifier: LGPL-2.1-or-later
 */

#ifndef CEPH_FDB_TRANSACTION_RESULT_H
 #define CEPH_FDB_TRANSACTION_RESULT_H

#include "fdb_errors.h"

#include <cstddef>

#include <exception>
#include <system_error>

#include <expected>
#include <optional>
#include <variant>

#include <type_traits>
#include <utility>

namespace ceph::libfdb {

enum struct transaction_phase { setup, body, commit, commit_observation };

enum struct commit_status { not_submitted, not_committed, committed, unknown_result, unconfirmed };

// Unknown includes unconfirmed submission and uncertainty retained from earlier attempts.
enum struct retry_policy { no_replay_unknown, replay_unknown };

struct transaction_status final
{
 std::size_t attempts = 0;

 commit_status commit = commit_status::not_submitted;

 std::optional<commit_status> prior_unknown_commit;

 std::optional<std::error_code> first_unknown_commit_error;

 // Expert causal-write options weaken guarantees even after observed success.
 bool duplicate_commit_possible = false;

 public:
 [[nodiscard]] bool has_unknown_commit() const noexcept
 {
  return duplicate_commit_possible or prior_unknown_commit.has_value() or
         commit_status::unknown_result == commit or commit_status::unconfirmed == commit;
 }

 [[nodiscard]] bool may_commit_later() const noexcept
 {
  return duplicate_commit_possible or prior_unknown_commit == commit_status::unconfirmed or
         commit_status::unconfirmed == commit;
 }

};

struct fdb_error final
{
 std::error_code code;

 // Recovery failure must not overwrite the operation/commit that triggered it.
 std::variant<std::monostate, std::error_code, std::exception_ptr> retry_error;

 // Present when the original failure was thrown; preserves its diagnostic context and identity.
 std::exception_ptr exception {};
};

template <typename ErrorT>
struct application_error final
{
 ErrorT error;
};

template <typename ErrorT = void>
struct transaction_failure final
{
 using cause_type = std::conditional_t<
  std::is_void_v<ErrorT>, std::variant<fdb_error, std::exception_ptr>,
  std::variant<application_error<ErrorT>, fdb_error, std::exception_ptr>>;

 cause_type cause;

 transaction_phase phase;

 transaction_status status;
};

template <typename ValueT>
struct committed_value final
{
 ValueT value;

 transaction_status status;
};

template <>
struct committed_value<void> final
{
 transaction_status status;
};

template <typename ValueT, typename ErrorT = void>
using transaction_result = std::expected<committed_value<ValueT>, transaction_failure<ErrorT>>;

namespace detail {

// Preserve user origin when application code throws a native-looking error:
struct user_callback_failure final
{
 std::exception_ptr cause;
};

inline transaction_failure<> exception_failure(const std::exception_ptr& error,
                                              const transaction_phase phase,
                                              const transaction_status& status) noexcept
try
 {
  if (error) {
   std::rethrow_exception(error);
  }

  return {error, phase, status};
 }
catch (const user_callback_failure& failure)
 {
  return {failure.cause, phase, status};
 }
catch (const libfdb_exception& failure)
 {
  return {fdb_error {failure.code(), {}, error}, phase, status};
 }
catch (...)
 {
  return {error, phase, status};
 }

inline void next_attempt(transaction_status& status) noexcept
{
 if (commit_status::unconfirmed == status.commit) {
  status.prior_unknown_commit = commit_status::unconfirmed;
 }

 if (commit_status::unknown_result == status.commit and not status.prior_unknown_commit) {
  status.prior_unknown_commit = commit_status::unknown_result;
 }

 status.commit = commit_status::not_submitted;
 ++status.attempts;
}

inline void uncertain_commit(transaction_status& status, const commit_status state,
                             const std::optional<std::error_code> code = std::nullopt) noexcept
{
 status.commit = state;

 if (not status.first_unknown_commit_error and code) {
  status.first_unknown_commit_error = code;
 }
}

inline void observe_commit(transaction_status& status, const transaction_status& completion) noexcept
{
 status.commit = completion.commit;
 status.duplicate_commit_possible |= completion.duplicate_commit_possible;

 if (not status.first_unknown_commit_error) {
  status.first_unknown_commit_error = completion.first_unknown_commit_error;
 }
}

template <typename ErrorT = void>
auto fail_transaction(auto&& cause, const transaction_phase phase,
                     const transaction_status& status) noexcept
 -> std::unexpected<transaction_failure<ErrorT>>
{
 return std::unexpected(transaction_failure<ErrorT> {
             typename transaction_failure<ErrorT>::cause_type {std::forward<decltype(cause)>(cause)},
             phase, status
         });
}

template <typename ErrorT = void>
auto fail_transaction(transaction_failure<> failure) noexcept
 -> std::unexpected<transaction_failure<ErrorT>>
{
 return std::visit([&failure](auto&& cause) {
  return fail_transaction<ErrorT>(std::move(cause), failure.phase, failure.status);
 }, std::move(failure.cause));
}

} // namespace detail

static_assert(std::is_nothrow_move_constructible_v<transaction_result<void>>);

} // namespace ceph::libfdb

#endif
