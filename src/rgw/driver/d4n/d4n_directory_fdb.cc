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

#include <algorithm>
#include <concepts>
#include <limits>
#include <type_traits>
#include <utility>

#include <expected>
#include <exception>
#include <functional>
#include <memory>
#include <ranges>
#include <variant>

#include <source_location>
#include <string_view>
#include <system_error>

#include <boost/algorithm/string.hpp>
#include <boost/asio/consign.hpp>

#include "common/async/blocked_completion.h"
#include "common/dout.h" 
#include "common/dout_fmt.h"
#include "common/error_code.h"
#include "common/inline_variant.h"
#include "d4n_directory_fdb.h"
#include "d4n_log.h"
#include "d4n_backend.h"

namespace rgw::d4n {

using std::map;
using std::string;

using detail::report_fdb_exception;
using detail::to_errno;

namespace fdbc = lfdb::layer::content;
namespace q    = lfdb::query;

namespace {

int handle_fdb_error(const DoutPrefixProvider *dpp, const std::error_code error,
                     const std::string_view message, const std::source_location whence,
                     const std::string_view resource)
{
  detail::log_message(dpp, whence,
    "ERROR: {} [{}:{}] resource={}", message, error.category().name(), error.value(), resource);

  const auto result = to_errno(error);

  return 0 > result ? result : -EIO;
}

int handle_fdb_failure(const DoutPrefixProvider *dpp, const lfdb::fdb_error& cause,
                       const std::source_location whence, const std::string_view resource)
{
  // Report the original failure before a replay preparation failure:
  const auto status = cause.exception
    ? report_fdb_exception(dpp, cause.exception, whence, resource)
    : handle_fdb_error(dpp, cause.code, cause.code.message(), whence, resource);

  return std::visit(overloaded {
    [status](std::monostate) { return status; },

    [&](const std::error_code& recovery) {
      return handle_fdb_error(dpp, recovery, "FDB replay preparation failed", whence, resource);
    },

    [&](const std::exception_ptr& recovery) {
      return report_fdb_exception(dpp, recovery, whence, resource);
    }
  }, cause.retry_error);
}

} // namespace

int detail::report_fdb_exception(const DoutPrefixProvider *dpp, const std::exception_ptr failure,
                                 const std::source_location whence,
                                 const std::string_view resource)
try
 {
   std::rethrow_exception(failure);
 }
catch(const std::system_error& error)
 {
   return handle_fdb_error(dpp, error.code(), error.what(), whence, resource);
 }
catch(...)
 {
   std::string message;

   const auto result = ceph::from_exception(failure, &message);

   detail::log_message(dpp, whence, "ERROR: {} resource={}", message, resource);

   return 0 > result ? result : -EIO;
 }

template <typename ErrorT>
requires (std::same_as<ErrorT, int> or std::same_as<ErrorT, void>)
int detail::return_d4n_error(const DoutPrefixProvider *dpp,
                             const lfdb::transaction_failure<ErrorT>& failure,
                             const std::source_location whence, const std::string_view resource)
{
  // Handle FoundationDB unknown_commit while the native completion facts are available.
  // Reconciliation can be added here before the failure becomes a D4N error:
  if (failure.status.has_unknown_commit()) {
    detail::log_message(dpp, whence,
      "FDB commit outcome unknown; may finish later={} attempts={} resource={}",
      failure.status.may_commit_later(), failure.status.attempts, resource);
    if (failure.status.first_unknown_commit_error) {
      handle_fdb_error(dpp, *failure.status.first_unknown_commit_error,
                        "first unconfirmed commit error", whence, resource);
    }
  }

  const auto result = std::visit(overloaded{
    [](const lfdb::application_error<int>& cause) {
      return cause.error;
    },
    [&](const std::exception_ptr& cause) {
      return report_fdb_exception(dpp, cause, whence, resource);
    },
    [&](const lfdb::fdb_error& cause) {
      return handle_fdb_failure(dpp, cause, whence, resource);
    }
  }, failure.cause);

  return failure.status.has_unknown_commit() || 0 <= result ? -EIO : result;
}

template int detail::return_d4n_error<void>(const DoutPrefixProvider *,
    const lfdb::transaction_failure<>&, std::source_location, std::string_view);
template int detail::return_d4n_error<int>(const DoutPrefixProvider *,
    const lfdb::transaction_failure<int>&, std::source_location, std::string_view);

namespace {

int return_d4n_error(const DoutPrefixProvider *dpp, const std::optional<Transaction> txn,
                     const std::exception_ptr failure,
                     const std::source_location whence = std::source_location::current(),
                     const std::string_view resource = {})
{
  // Borrowed native failures belong to the owning libfdb Transactor.
  if (txn && std::holds_alternative<fdb_txn_ref>(*txn)) {
    std::rethrow_exception(failure);
  }
  return report_fdb_exception(dpp, failure, whence, resource);
}

// Borrowed calls publish working data; owning calls publish confirmed committed data:
template <typename ValueT>
requires std::is_nothrow_move_assignable_v<ValueT>
int handle_fdb_result(const DoutPrefixProvider *, ValueT&& value, ValueT& output,
                      const std::string_view, const std::source_location)
{
  output = std::move(value);
  return 0;
}

template <typename ValueT>
int handle_fdb_result(const DoutPrefixProvider *dpp,
                      std::expected<ValueT, int>&& result,
                      ValueT& output,
                      const std::string_view resource = {},
                      const std::source_location whence = std::source_location::current())
{
  if (!result) {
    return result.error();
  }

  return handle_fdb_result(dpp, std::move(*result), output, resource, whence);
}

template <typename ValueT, typename ErrorT>
int handle_fdb_result(const DoutPrefixProvider *dpp, lfdb::transaction_result<ValueT, ErrorT>&& result,
                      ValueT& output,
                      const std::string_view resource = {},
                      const std::source_location whence = std::source_location::current())
{
  if (!result) {
    return detail::return_d4n_error(dpp, result.error(), whence, resource);
  }

  return handle_fdb_result(dpp, std::move(result->value), output, resource, whence);
}

int handle_fdb_result(const DoutPrefixProvider *, const int value,
                      const std::string_view, const std::source_location)
{
  return value;
}

template <typename ValueT>
int handle_fdb_result(const DoutPrefixProvider *, const std::expected<ValueT, int>& result,
                      const std::string_view, const std::source_location)
{
  if (!result) {
    return result.error();
  }
  if constexpr (!std::is_void_v<ValueT>) {
    return *result;
  }
  return 0;
}

template <typename ValueT, typename ErrorT>
int handle_fdb_result(const DoutPrefixProvider *dpp, const lfdb::transaction_result<ValueT, ErrorT>& result,
                      const std::string_view resource = {},
                      const std::source_location whence = std::source_location::current())
{
  if (!result) {
    return detail::return_d4n_error(dpp, result.error(), whence, resource);
  }
  if constexpr (!std::is_void_v<ValueT>) {
    return result->value;
  }
  return 0;
}

template <typename Func, typename ...OutputTs>
requires (sizeof...(OutputTs) <= 1)
int fdb_invoke(const lfdb::database_handle& db, const DoutPrefixProvider *dpp,
               std::optional<Transaction> txn,
               const std::string_view resource, const std::source_location whence,
               Func&& fn, OutputTs& ...outputs)
try
 {
  if (txn) {
    const auto *view = std::get_if<fdb_txn_ref>(&*txn);
    if (!view || !view->get()) {
      return handle_fdb_error(dpp, std::make_error_code(std::errc::invalid_argument),
                               "invalid D4N FDB transaction", whence, resource);
    }

    auto& fdb_txn = view->get();
    using result_type = std::invoke_result_t<Func, lfdb::transaction_handle&>;

    // Preserve exceptions and their origin until the owning Transactor observes them.
    if constexpr (std::is_void_v<result_type>) {
      std::invoke(std::forward<Func>(fn), fdb_txn);
      return 0;
    }

    if constexpr (!std::is_void_v<result_type>) {
      return handle_fdb_result(dpp, std::invoke(std::forward<Func>(fn), fdb_txn),
                                outputs..., resource, whence);
    }
  }

  const auto txr = lfdb::make_transactor(db);
  return handle_fdb_result(dpp, txr(std::forward<Func>(fn)), outputs..., resource, whence);
 }
catch (...)
 {
   return return_d4n_error(dpp, txn, std::current_exception(), whence, resource);
 }

template <typename Func>
int fdb_invoke(const lfdb::database_handle& db, const DoutPrefixProvider *dpp,
               std::optional<Transaction> txn,
               Func&& fn,
               const std::source_location whence = std::source_location::current())
{
  return fdb_invoke(db, dpp, txn, {}, whence, std::forward<Func>(fn));
}

template <typename Func, typename OutputT>
int fdb_invoke(const lfdb::database_handle& db, const DoutPrefixProvider *dpp,
               std::optional<Transaction> txn, Func&& fn, OutputT& output,
               const std::source_location whence = std::source_location::current())
{
  return fdb_invoke(db, dpp, txn, {}, whence, std::forward<Func>(fn), output);
}

template <typename ValueT>
struct DirectoryPage final
{
 std::vector<ValueT> entries;
 std::string continuation_token;
};

auto collect_fdb_page(std::ranges::input_range auto&& rows, const uint64_t count)
{
  // Preserve the existing INT_MAX row cap for counts beyond libfdb's page limit.
  if (lfdb::page::max_size < count) {
    return lfdb::collect(
        std::ranges::ref_view{rows} | std::views::take(std::numeric_limits<int>::max()),
        lfdb::page{});
  }

  return lfdb::collect(rows, lfdb::page{count});
}

auto read_fields(lfdb::transaction_handle& tr, const std::string_view key)
{
  std::expected<std::map<std::string, std::string>, int> fields {std::in_place};
  if (!lfdb::get(tr, key, *fields)) {
    fields = std::unexpected(-ENOENT);
  }
  return fields;
}

} // anonymous namespace

static std::string encode_score(int64_t score)
{
  return fmt::format("{:019d}", score);
}

// Helper: Convert seconds to nanoseconds for TTL specification
constexpr uint64_t seconds_to_nanoseconds(uint64_t seconds) {
  // Check for overflow: max uint64_t / 1e9 ≈ 18.4e9 seconds ≈ 584 years
  if (seconds > std::numeric_limits<uint64_t>::max() / 1000000000ULL) {
    return std::numeric_limits<uint64_t>::max();
  }
  return seconds * 1000000000ULL;
}

// Helper: Convert milliseconds to nanoseconds for TTL specification
constexpr uint64_t milliseconds_to_nanoseconds(uint64_t milliseconds) {
  if (milliseconds > std::numeric_limits<uint64_t>::max() / 1000000ULL) {
    return std::numeric_limits<uint64_t>::max();
  }
  return milliseconds * 1000000ULL;
}

// Lease metadata structure
struct LeaseData {
  uint64_t expiry = 0;          // When the lease expires (epoch nanoseconds)
  std::string holder_id;
  std::string token;
  uint64_t tick_count = 0;
  std::string last_renewal_id;  // ID of last renewal - for replay detection

  bool is_active(uint64_t now) const {
    return expiry > now;
  }
};

// Build FDB key for a lease on a resource using FDB directory layer
// Uses compiled_key directly to avoid string conversion overhead
// Parses hierarchical resource names into FDB tuple components
// Format: "bucket/object/version/operation" or "bucket/object/version/operation/uuid"
// Note: Components are URL-encoded by the caller to handle '/' in S3 object names
auto make_lease_key(const std::string& resource_name)
{
  auto root = fdbc::keyspace("d4n") / "leases";

  std::vector<std::string> parts;
  boost::split(parts, resource_name, boost::is_any_of("/"));

  auto key = root;
  for (const auto& part : parts) {
    if (!part.empty()) {
      key = key / part;
    }
  }
  return key;
}

// Get current time in nanoseconds since epoch
uint64_t current_time_nanoseconds()
{
  const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  return static_cast<uint64_t>(std::max<int64_t>(now, 0));
}

// Calculate expiry time from TTL in nanoseconds
uint64_t calculate_expiry(uint64_t ttl_nanoseconds)
{
  const auto now = current_time_nanoseconds();
  if (ttl_nanoseconds > std::numeric_limits<uint64_t>::max() - now) {
    return std::numeric_limits<uint64_t>::max();
  }
  return now + ttl_nanoseconds;
}

int FDBLease::acquire(const DoutPrefixProvider* dpp, const std::string& resource_name, const std::string& holder_id, const std::string& token,
    uint64_t ttl_nanoseconds,
    std::optional<Transaction> txn)
try
 {
  if (ttl_nanoseconds == 0) {
    ldpp_dout(dpp, 10) << "FDBLease::" << __func__ << " invalid TTL=0 for resource=" << resource_name << dendl;
    return -EINVAL;
  }

  if (resource_name.empty() || holder_id.empty() || token.empty()) {
    ldpp_dout(dpp, 10) << "FDBLease::" << __func__ << " empty resource_name/holder_id/token" << dendl;
    return -EINVAL;
  }

  const auto fdb_key = make_lease_key(resource_name);

  return fdb_invoke(FDBdb, dpp, txn, resource_name, std::source_location::current(), [&](auto& tr) {
    std::expected<void, int> outcome;
    const auto now = current_time_nanoseconds();

    // Check if resource already has an active lease
    LeaseData existing_lease;
    if (lfdb::get(tr, fdb_key, existing_lease)) {
      if (existing_lease.is_active(now)) {
        // Repeated acquisition with the same ownership is already satisfied.
        if (existing_lease.holder_id == holder_id && existing_lease.token == token) {
          ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                             << " lease already acquired by us (replay): resource=" << resource_name
                             << dendl;
          return outcome; // Idempotent - already acquired by us
        }
        ldpp_dout(dpp, 10) << "FDBLease::" << __func__ << " resource already leased by holder=" << existing_lease.holder_id << " resource=" << resource_name << dendl;
        outcome = std::unexpected(-EEXIST);
        return outcome;
      }
      // Expired, can reuse
      ldpp_dout(dpp, 20) << "FDBLease::" << __func__ << " reusing expired lease on resource=" << resource_name << dendl;
    }

    // Acquire the lease
    LeaseData new_lease{
        .expiry = calculate_expiry(ttl_nanoseconds),
        .holder_id = holder_id,
        .token = token,
        .tick_count = 0,
        .last_renewal_id = "" // No renewals yet
    };
    lfdb::set(tr, fdb_key, new_lease);

    // NOTE: Log before commit() - may print multiple times on transaction replay
    ldpp_dout(dpp, 20) << "FDBLease::" << __func__ << " acquired lease: resource=" << resource_name
                       << " holder=" << holder_id << " expiry_ns=" << new_lease.expiry << dendl;
    return outcome;
  });

 }
catch(...)
 {
  return return_d4n_error(dpp, txn, std::current_exception(), std::source_location::current(), resource_name);
 }

int FDBLease::renew(const DoutPrefixProvider* dpp,
                           const std::string& resource_name,
                           const std::string& holder_id,
                           const std::string& token,
                           uint64_t ttl_nanoseconds,
                           uint64_t max_ticks)
try
 {
  if (ttl_nanoseconds == 0) {
    ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                       << " invalid TTL=0 for resource=" << resource_name << dendl;
    return -EINVAL;
  }

  if (resource_name.empty() || holder_id.empty() || token.empty()) {
    ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                       << " empty resource_name/holder_id/token" << dendl;
    return -EINVAL;
  }

  // Generate unique renewal_id OUTSIDE transaction lambda
  // This ensures the same ID is used on transaction replay
  // Format: timestamp_nanoseconds:holder_id:token_hash
  const uint64_t renewal_timestamp = current_time_nanoseconds();
  const std::string renewal_id = std::to_string(renewal_timestamp) + ":" +
                                  holder_id + ":" +
                                  std::to_string(std::hash<std::string>{}(token));

  const auto fdb_key = make_lease_key(resource_name);

  const auto txr = lfdb::make_transactor(FDBdb);
  const auto result = txr([&](auto& tr) {
    std::expected<int, int> outcome{0};
    LeaseData existing_lease;
    const auto now = current_time_nanoseconds();

    if (!lfdb::get(tr, fdb_key, existing_lease)) {
      ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                         << " lease not found: resource=" << resource_name << dendl;
      outcome = std::unexpected(-ENOENT);
      return outcome;
    }

    // REPLAY DETECTION: Check if this exact renewal was already applied
    if (existing_lease.last_renewal_id == renewal_id) {
      ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                         << " renewal already applied (transaction replay detected)"
                         << " renewal_id=" << renewal_id << " resource=" << resource_name << dendl;
      return outcome; // Idempotent - this renewal already happened
    }

    if (!existing_lease.is_active(now)) {
      // Commit the cleanup, then return the negative domain outcome as a value.
      lfdb::erase(tr, fdb_key);
      ldpp_dout(dpp, 10) << "FDBLease::" << __func__ << " lease expired: resource=" << resource_name
                         << dendl;
      outcome = -ENOENT;
      return outcome;
    }

    // Validate ownership
    if (existing_lease.holder_id != holder_id || existing_lease.token != token) {
      ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                         << " ownership validation failed: resource=" << resource_name
                         << " expected_holder=" << existing_lease.holder_id
                         << " provided_holder=" << holder_id << dendl;
      outcome = std::unexpected(-EACCES);
      return outcome;
    }

    // Check if max_ticks limit reached
    if (max_ticks > 0 && existing_lease.tick_count >= max_ticks) {
      ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                         << " max_ticks limit reached: resource=" << resource_name
                         << " tick_count=" << existing_lease.tick_count
                         << " max_ticks=" << max_ticks << dendl;
      outcome = std::unexpected(-EINVAL);
      return outcome; // Lease still held - caller decides what to do
    }

    // Renew the lease with incremented tick count and this renewal_id
    LeaseData renewed_lease{
        .expiry = calculate_expiry(ttl_nanoseconds),
        .holder_id = holder_id,
        .token = token,
        .tick_count = existing_lease.tick_count + 1,
        .last_renewal_id = renewal_id  // Mark this renewal as applied
    };
    lfdb::set(tr, fdb_key, renewed_lease);

    // NOTE: Log before commit() - may print multiple times on transaction replay
    ldpp_dout(dpp, 20) << "FDBLease::" << __func__ << " renewed lease: resource=" << resource_name
                       << " holder=" << holder_id << " new_expiry_ns=" << renewed_lease.expiry
                       << " tick_count=" << renewed_lease.tick_count << " renewal_id=" << renewal_id
                       << dendl;
    return outcome;
  });

  return handle_fdb_result(dpp, result, resource_name);
 }
catch(...)
 {
  return report_fdb_exception(dpp, std::current_exception(), std::source_location::current(), resource_name);
 }

int FDBLease::release(const DoutPrefixProvider* dpp,
                             const std::string& resource_name,
                             const std::string& holder_id,
                             const std::string& token)
try
 {
  if (resource_name.empty() || holder_id.empty() || token.empty()) {
    ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                       << " empty resource_name/holder_id/token" << dendl;
    return -EINVAL;
  }

  const auto fdb_key = make_lease_key(resource_name);

  const auto txr = lfdb::make_transactor(FDBdb);
  const auto result = txr([&](auto& tr) {
    std::expected<void, int> outcome;
    LeaseData existing_lease;
    if (!lfdb::get(tr, fdb_key, existing_lease)) {
      // Make release() idempotent for transaction replay
      // If lease not found, it was already released (possibly by replay)
      ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                         << " lease not found (already released or replay): resource="
                         << resource_name << dendl;
      return outcome; // Idempotent - already released
    }

    // Validate ownership
    if (existing_lease.holder_id != holder_id || existing_lease.token != token) {
      ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                         << " ownership validation failed: resource=" << resource_name
                         << " expected_holder=" << existing_lease.holder_id
                         << " provided_holder=" << holder_id << dendl;
      outcome = std::unexpected(-EACCES);
      return outcome;
    }

    // Release the lease
    lfdb::erase(tr, fdb_key);

    // NOTE: Log before commit() - may print multiple times on transaction replay
    ldpp_dout(dpp, 20) << "FDBLease::" << __func__ << " released lease: resource=" << resource_name
                       << " holder=" << holder_id << dendl;
    return outcome;
  });

  return handle_fdb_result(dpp, result, resource_name);
 }
catch(...)
 {
  return report_fdb_exception(dpp, std::current_exception(), std::source_location::current(), resource_name);
 }

LeaseCheckResult FDBLease::any_active(const DoutPrefixProvider* dpp,
                                      const std::string& resource_prefix)
try
 {
  if (resource_prefix.empty()) {
    ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                       << " empty resource_prefix" << dendl;
    return std::unexpected(-EINVAL);
  }

  // Build prefix for scanning all leases matching this resource
  // With hierarchical keys, prefix matching now works correctly:
  // "bucket" prefix will match "bucket/object1", "bucket/object2", etc.
  // "bucket/obj/v1/GET" prefix will match all "bucket/obj/v1/GET/*" (all uuids)
  const auto prefix_key = make_lease_key(resource_prefix);

  const auto txr = lfdb::make_transactor(FDBdb);
  const auto completed = txr([&](auto& tr) -> bool {
    const auto rows = lfdb::collect<LeaseData>(tr, q::prefix(prefix_key));

    if (rows.empty()) {
      ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                         << " no leases found for resource prefix: " << resource_prefix << dendl;
      return false;
    }

    // Check if any lease is still active and opportunistically cleanup expired ones
    const auto now = current_time_nanoseconds();
    int total_leases = rows.size();
    int active_leases = 0;
    std::vector<std::string> expired_keys;

    for (const auto& [key, lease] : rows) {
      if (lease.is_active(now)) {
        active_leases++;
        ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                           << " found active lease: holder=" << lease.holder_id
                           << " expiry_ns=" << lease.expiry << dendl;
      } else {
        // Collect expired leases for opportunistic cleanup
        expired_keys.push_back(key);
      }
    }

    // Opportunistic cleanup: delete expired leases in the same transaction
    for (const auto& key : expired_keys) {
      lfdb::erase(tr, key);
    }

    if (!expired_keys.empty()) {
      ldpp_dout(dpp, 20) << "FDBLease::" << __func__ << " cleaned up " << expired_keys.size()
                         << " expired leases" << dendl;
    }

    bool result = (active_leases > 0);
    ldpp_dout(dpp, 20) << "FDBLease::" << __func__ << " resource_prefix=" << resource_prefix
                       << " total_leases=" << total_leases << " active_leases=" << active_leases
                       << " has_active=" << result << dendl;
    return result;
  });

  const auto result = handle_fdb_result(dpp, completed, resource_prefix);
  if (0 > result) {
    return std::unexpected(result);
  }
  return 0 < result;
 }
catch(...)
 {
  return std::unexpected(report_fdb_exception(dpp, std::current_exception(),
    std::source_location::current(), resource_prefix));
 }

LeaseCheckResult FDBLease::is_active(const DoutPrefixProvider* dpp,
                                     const std::string& resource_name,
                                     const std::string& holder_id,
                                     const std::string& token)
try
 {
  if (resource_name.empty() || holder_id.empty() || token.empty()) {
    ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                       << " empty resource_name/holder_id/token" << dendl;
    return std::unexpected(-EINVAL);
  }

  const auto fdb_key = make_lease_key(resource_name);

  // Atomic check-and-cleanup in single transaction to avoid TOCTOU race
  // NOTE: If callers use is_active() to authorize a later FDB update,
  // consider sharing the transaction: read the lease key in the update
  // transaction itself - FDB will retry if the lease changes
  const auto txr = lfdb::make_transactor(FDBdb);
  const auto completed = txr([&](auto& tr) -> bool {
    LeaseData lease;
    if (!lfdb::get(tr, fdb_key, lease)) {
      ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                         << " lease not found: resource=" << resource_name << dendl;
      return false;
    }

    // Check expiry and opportunistically cleanup if expired
    const auto now = current_time_nanoseconds();
    if (!lease.is_active(now)) {
      ldpp_dout(dpp, 20) << "FDBLease::" << __func__ << " lease expired: resource=" << resource_name
                         << " expiry_ns=" << lease.expiry << " - deleting" << dendl;
      // Opportunistic cleanup: delete in same transaction (atomic)
      lfdb::erase(tr, fdb_key);
      return false;
    }

    // Validate ownership
    if (lease.holder_id != holder_id || lease.token != token) {
      ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                         << " ownership mismatch: resource=" << resource_name
                         << " expected_holder=" << lease.holder_id
                         << " provided_holder=" << holder_id << dendl;
      return false;
    }

    ldpp_dout(dpp, 20) << "FDBLease::" << __func__ << " lease is active: resource=" << resource_name
                       << " holder=" << holder_id << " expiry_ns=" << lease.expiry << dendl;
    return true;
  });

  const auto result = handle_fdb_result(dpp, completed, resource_name);
  if (0 > result) {
    return std::unexpected(result);
  }
  return 0 < result;
 }
catch(...)
 {
  return std::unexpected(report_fdb_exception(dpp, std::current_exception(),
    std::source_location::current(), resource_name));
 }
// Returns count+1 so callers can detect whether a continuation token is needed:
// if FDB returns count+1 rows, there are more; if fewer, the range is exhausted.
// Returns 0 when count==0 (unbounded).
static int fdb_page_read_limit(uint64_t count)
{
  if (count == 0) {
    return 0;
  }

  constexpr auto max = static_cast<uint64_t>(std::numeric_limits<int>::max());
  return static_cast<int>(count >= max ? max : count + 1);
}


TransactionResult FDBBackend::execute(const DoutPrefixProvider *dpp, optional_yield y,
    const boost::compat::function_ref<int(Transaction&)> fn, const unsigned max_attempts)
{
  if (0 == max_attempts) {
    return TransactionResult(errno_result {std::unexpected(-EAGAIN)}, true);
  }

  bool exhausted = false;
  auto limit = lfdb::retry_if([&exhausted, max_attempts](const lfdb::transaction_failure<>& failure) {
    if (failure.status.attempts < max_attempts) {
      return true;
    }
    exhausted = lfdb::retryable(std::get<lfdb::fdb_error>(failure.cause).code);
    return false;
  });

  // libfdb owns native eligibility, preparation, backoff, completion, and replay.
  const auto txr = lfdb::make_transactor(objects.FDBdb);
  auto result = txr(std::move(limit), [&fn](auto& fdb_txn) {
    Transaction txn(std::ref(fdb_txn));
    std::expected<void, int> attempt;
    if (const auto ret = fn(txn); 0 > ret) {
      attempt = std::unexpected(ret);
    }
    return attempt;
  });

  return TransactionResult(std::move(result), exhausted);
}

int FDBDirectory::get_kv(const DoutPrefixProvider* dpp,
                         optional_yield y,
                         const std::string& key,
                         const std::string& field,
                         std::string& out_val,
                         std::optional<Transaction> txn)
try
{
  std::map<std::string, std::string> kvs;
  if (const auto result = fdb_invoke(FDBdb, dpp, txn,
        [&](auto& tr) { return read_fields(tr, key); }, kvs); 0 > result) {
    return result;
  }

  const auto it = kvs.find(field);
  if (it == kvs.end()) {
    return -ENOENT;
  }

  out_val = std::move(it->second);
  return 0;
}
catch(...)
{
  return return_d4n_error(dpp, txn, std::current_exception());
}

int FDBDirectory::set_kv(const DoutPrefixProvider* dpp, optional_yield y,
		    const std::string& key, const std::string& field,
                    const std::string& val, std::optional<Transaction> txn)
{
  return fdb_invoke(FDBdb, dpp, txn, [&](auto& tr) {
    std::map<std::string, std::string> existing;
    lfdb::get(tr, key, existing);
    existing[field] = val;
    lfdb::set(tr, key, existing);
  });
}

int FDBDirectory::get_kv_multi(const DoutPrefixProvider* dpp, optional_yield,
		    const std::string& key, const std::vector<std::string>& fields,
                    std::map<std::string, std::string>& out_vals, std::optional<Transaction> txn)
try
{
  std::map<std::string, std::string> kvs;
  if (const auto result = fdb_invoke(FDBdb, dpp, txn,
        [&](auto& tr) { return read_fields(tr, key); }, kvs); 0 > result) {
    return result;
  }

  std::map<std::string, std::string> selected;
  for (const auto& field : fields) {
    const auto it = kvs.find(field);
    if (it == kvs.end()) {
      return -ENOENT;
    }
    selected.try_emplace(it->first, std::move(it->second));
  }

  out_vals = std::move(selected);
  return 0;
}
catch(...)
{
  return return_d4n_error(dpp, txn, std::current_exception());
}

int FDBDirectory::set_kv_multi(const DoutPrefixProvider* dpp, optional_yield y,
    const std::string& key,
    const std::map<std::string, std::string>& vals,
    std::optional<Transaction> txn)
{
  return fdb_invoke(FDBdb, dpp, txn, [&](auto& tr) {
    std::map<std::string, std::string> existing;
    lfdb::get(tr, key, existing);
    for (const auto& [field, value] : vals) {
      existing[field] = value;
    }
    lfdb::set(tr, key, existing);
  });
}

int FDBDirectory::set_kv_if_not_exists(const DoutPrefixProvider* dpp, optional_yield y,
                                        const std::string& key,
                                        const std::string& field,
                                        const std::string& val,
                                        std::optional<Transaction> txn)
{
  return fdb_invoke(FDBdb, dpp, txn, [&](auto& tr) {
    std::map<std::string, std::string> existing;
    lfdb::get(tr, key, existing);
    if (existing.find(field) == existing.end()) {
      existing[field] = val;
      lfdb::set(tr, key, existing);
    }
  });
}

int FDBBucketDirectory::del(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, std::optional<Transaction> txn)
{
  return fdb_invoke(FDBdb, dpp, txn, [&](auto& tr) {
    lfdb::erase(tr, bucket_id);
  });
}

int FDBBucketDirectory::add_object(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& object_name, std::optional<CacheObject> params, std::optional<Transaction> txn)
{
  if (!params) {
    return -EINVAL;
  }

  return fdb_invoke(FDBdb, dpp, txn, [&](auto& tr) {
    auto member_key = build_object_index(bucket_id, object_name);
    lfdb::set(tr, member_key, *params);
  });
}

int FDBBucketDirectory::remove_object(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& object_name, std::optional<Transaction> txn)
{
  return fdb_invoke(FDBdb, dpp, txn, [&](auto& tr) {
    auto member_key = build_object_index(bucket_id, object_name);
    lfdb::erase(tr, member_key);
  });
}

int FDBBucketDirectory::list_objects(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& start_token, const std::string& prefix, const std::string& marker, [[maybe_unused]] const std::string_view end_marker, uint64_t count, bool marker_inclusive, std::vector<CacheObject>& objs_info, std::string& continuation_token, std::optional<Transaction> txn)
try
 {
  auto object_query = object_listing_query(bucket_id, prefix, marker, {}, marker_inclusive);

  if (q::is_empty(object_query)) {
    return -ENOENT;
  }

  detail::log_message<20>(dpp, std::source_location::current(), "prefix: {} marker: {}", prefix, marker);

  const auto page_query = q::with_options(
      std::move(object_query),
      q::query_options{
          .result_limit = fdb_page_read_limit(count)});

  const auto read_object_page = [&](auto& tr) {
    std::expected<DirectoryPage<CacheObject>, int> page{std::in_place};
    auto& objects = page->entries;
    auto& next_token = page->continuation_token;
    auto gathered = collect_fdb_page(
        lfdb::scan<CacheObject>(tr, page_query) | std::views::values, count);

    if (gathered.empty()) {
      page = std::unexpected(-ENOENT);
      return page;
    }

    objects = std::move(gathered.rows);
    if (gathered.has_more) {
      next_token = objects.back().objName;
    }

    return page;
  };

  DirectoryPage<CacheObject> page;
  if (const auto result = fdb_invoke(FDBdb, dpp, txn, read_object_page, page); 0 > result) {
    return result;
  }

  objs_info = std::move(page.entries);
  continuation_token = std::move(page.continuation_token);
  return 0;
 }
catch(...)
 {
  return return_d4n_error(dpp, txn, std::current_exception());
 }

fdbc::compiled_key FDBBucketDirectory::object_listing_subspace(const std::string_view bucket_id)
{
  return fdbc::key(bucket_id, object_listing_segment);
}

// Key form is <bucket-id>/objects/<object-name>.
fdbc::compiled_key FDBBucketDirectory::build_object_index(
    const std::string_view bucket_id, const std::string_view obj_name)
{
  return fdbc::key(bucket_id, object_listing_segment, obj_name);
}

fdbc::compiled_key FDBBucketDirectory::build_object_index(
    fdbc::compiled_key parent, const std::string_view obj_name)
{
  return std::move(parent) / obj_name;
}

lfdb::query::interval FDBBucketDirectory::object_listing_query(
    const std::string_view bucket_id, const std::string_view prefix,
    const std::string_view marker, const std::string_view end_marker,
    const bool marker_inclusive)
{
  const auto parent = object_listing_subspace(bucket_id);
  const std::string base{libfdb_key_view(parent)};
  const auto prefix_begin = base + std::string{prefix};
  const auto marker_key = base + std::string{marker};
  return marker.empty() ? q::prefix(prefix_begin)
      : marker_inclusive ? q::prefix_starting_at(prefix_begin, marker_key)
                          : q::prefix_starting_after(prefix_begin, marker_key);
}

int FDBBucketDirectory::exist_key(const DoutPrefixProvider* dpp, optional_yield y,
                                  const std::string& bucket_id, std::optional<Transaction> txn)
{
  return fdb_invoke(FDBdb, dpp, txn, [&](auto& tr) {
    return lfdb::key_exists(tr, bucket_id);
  });
}

/*
  Key formats:
  <bucket-id>#<object-name>/versions/<score>/<version> --> stores versions in order
  <bucket-id>#<object-name>/score/<version> --> for reverse lookup of a version key using its score
*/
fdbc::compiled_key FDBObjectDirectory::object_keyspace(
    const std::string& bucket_id, const std::string& obj_name)
{
  return fdbc::keyspace(ObjectDirectory::build_index(bucket_id, obj_name));
}

std::pair<fdbc::compiled_key, fdbc::compiled_key> FDBObjectDirectory::version_subspaces(
    const std::string& bucket_id, const std::string& obj_name)
{
  auto root = object_keyspace(bucket_id, obj_name);

  return {get_versions_subspace(root), get_score_subspace(std::move(root))};
}

fdbc::compiled_key FDBObjectDirectory::get_versions_subspace(fdbc::compiled_key object_index)
{
  return std::move(object_index) / "versions";
}

fdbc::compiled_key FDBObjectDirectory::get_score_subspace(fdbc::compiled_key object_index)
{
  return std::move(object_index) / "score";
}

fdbc::compiled_key FDBObjectDirectory::build_versions_score_prefix(
    fdbc::compiled_key versions_subspace, const std::string_view score)
{
  return std::move(versions_subspace) / score;
}

fdbc::compiled_key FDBObjectDirectory::build_versions_index(fdbc::compiled_key versions_subspace,
                                                          const std::string_view score,
                                                          const std::string_view version)
{
  return build_versions_score_prefix(std::move(versions_subspace), score) / version;
}

fdbc::compiled_key FDBObjectDirectory::build_version_score_index(fdbc::compiled_key score_subspace,
                                                               const std::string_view version)
{
  return std::move(score_subspace) / version;
}

lfdb::query::interval FDBObjectDirectory::version_score_query(
    const fdbc::compiled_key& versions_subspace, const int64_t min, const int64_t max)
{
  const std::string base{libfdb_key_view(versions_subspace)};
  return q::prefix(base) & q::between(base + encode_score(min), base + encode_score(max) + "\xff");
}

int FDBObjectDirectory::exist_key(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, std::optional<Transaction> txn)
{
  return fdb_invoke(FDBdb, dpp, txn, [&](auto& tr) {
    std::string key = build_index(bucket_id, obj_name);
    return lfdb::key_exists(tr, key);
  });
}


int FDBObjectDirectory::del(const DoutPrefixProvider* dpp, optional_yield y, CacheObj* object, std::optional<Transaction> txn)
{
  return fdb_invoke(FDBdb, dpp, txn, [&](auto& tr) {
    std::string key = build_index(object->bucketName, object->objName);
    lfdb::erase(tr, key);
  });
}

int FDBObjectDirectory::add_version(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& version, ceph::real_time& creation_time, std::optional<CacheObjectVersion> params, std::optional<Transaction> txn)
{
  auto score = std::chrono::duration_cast<std::chrono::nanoseconds>(creation_time.time_since_epoch()).count();

  ldpp_dout(dpp, 10) << "FDBObjectDirectory::" << __func__ << "(): Score of object name: "<< obj_name << " version: " << version << " is: "  << score << dendl;

  try
   {
    if (!params) {
      return -EINVAL;
    }

    const auto update_version_indices = [&](auto& tr) {
      ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " :bucket_id " << bucket_id << dendl;
      ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " :obj_name " << obj_name << dendl;

      const auto [versions_subspace, score_subspace] = version_subspaces(bucket_id, obj_name);

      std::string encoded_score = encode_score(score);
      auto score_key = build_version_score_index(score_subspace, version);

      ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " score_key " << std::string(libfdb_key_view(score_key)) << dendl;

      std::string existing;
      if (lfdb::get(tr, score_key, existing)) {
        auto existing_versions_key = build_versions_index(versions_subspace, existing, version);
        lfdb::erase(tr, existing_versions_key);
      }

      auto versions_key = build_versions_index(versions_subspace, encoded_score, version);
      lfdb::set(tr, versions_key, *params);
      lfdb::set(tr, score_key, encoded_score);
      ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " versions_key: " << std::string(libfdb_key_view(versions_key)) << dendl;
    };

    return fdb_invoke(FDBdb, dpp, txn, update_version_indices);
   }
  catch(...)
   {
    return return_d4n_error(dpp, txn, std::current_exception());
   }
}

int FDBObjectDirectory::remove_version(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& version, std::optional<Transaction> txn)
{
  return fdb_invoke(FDBdb, dpp, txn, [&](auto& tr) {
    std::expected<void, int> outcome;
    const auto [versions_subspace, score_subspace] = version_subspaces(bucket_id, obj_name);

    auto score_key = build_version_score_index(score_subspace, version);
    std::string existing_score;

    if (!lfdb::get(tr, score_key, existing_score)) {
      outcome = std::unexpected(-ENOENT);
      return outcome;
    }

    auto version_key = build_versions_index(versions_subspace, existing_score, version);
    lfdb::erase(tr, version_key);
    lfdb::erase(tr, score_key);
    return outcome;
  });
}

int FDBObjectDirectory::remove_version_by_creation_time(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, ceph::real_time creation_time, std::optional<Transaction> txn)
{
  auto score = std::chrono::duration_cast<std::chrono::nanoseconds>(creation_time.time_since_epoch()).count();

  try
   {
    const auto versions_subspace = get_versions_subspace(object_keyspace(bucket_id, obj_name));
    const auto score_range = version_score_query(versions_subspace, score, score);

    if (q::is_empty(score_range)) {
      return -ENOENT;
    }

    const auto operation = [&](auto& tr) {
      lfdb::erase(tr, score_range);
    };

    return fdb_invoke(FDBdb, dpp, txn, operation);
   }
  catch (...)
   {
    return return_d4n_error(dpp, txn, std::current_exception());
   }
}

int FDBObjectDirectory::list_versions(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& marker_version, uint64_t count, std::vector<CacheObjectVersion>& obj_versions, std::string& continuation_token, std::optional<Transaction> txn)
{
  ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " obj_name: " << obj_name << dendl;
  ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " marker_version: " << marker_version << dendl;

  try
   {
    const auto [versions_subspace, score_subspace] = version_subspaces(bucket_id, obj_name);

    ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << "() versions_subspace: " << std::string(libfdb_key_view(versions_subspace)) << dendl;

    const auto read_version_page = [&](auto& tr) {
      std::expected<DirectoryPage<CacheObjectVersion>, int> page{std::in_place};
      auto& versions = page->entries;
      auto& next_token = page->continuation_token;
      q::interval versions_query = q::prefix(versions_subspace);

      if (!marker_version.empty()) {
        std::string marker_score;
        const auto score_key = build_version_score_index(score_subspace, marker_version);

        if (!lfdb::get(tr, score_key, marker_score)) {
          ldpp_dout(dpp, 10) << "FDBObjectDirectory::" << __func__
                             << "() marker version not found: " << marker_version << dendl;
          page = std::unexpected(-ENOENT);
          return page;
        }

        const auto marker_key = build_versions_index(versions_subspace, marker_score, marker_version);
        versions_query &= q::before(marker_key);
      }

      if (q::is_empty(versions_query)) {
        page = std::unexpected(-ENOENT);
        return page;
      }

      const auto page_query = q::with_options(std::move(versions_query), q::query_options{
          .result_limit = fdb_page_read_limit(count),
          .reverse_order = true});

      auto gathered = collect_fdb_page(
          lfdb::scan<CacheObjectVersion>(tr, page_query) | std::views::values, count);

      if (gathered.empty()) {
        page = std::unexpected(-ENOENT);
        return page;
      }

      versions = std::move(gathered.rows);
      if (gathered.has_more) {
        next_token = versions.back().version;
      }

      detail::log_message<20>(dpp, std::source_location::current(), "count: {}", count);
      for (const auto& version : versions) {
        detail::log_message<20>(dpp, std::source_location::current(), "version: {}", version.version);
        detail::log_message<20>(dpp, std::source_location::current(), "user_id: {}", version.user_id);
        detail::log_message<20>(dpp, std::source_location::current(), "display_name: {}", version.display_name);
      }

      return page;
    };

    DirectoryPage<CacheObjectVersion> page;
    if (const auto result = fdb_invoke(FDBdb, dpp, txn, read_version_page, page); 0 > result) {
      return result;
    }

    obj_versions = std::move(page.entries);
    continuation_token = std::move(page.continuation_token);
    return 0;
   }
  catch(...)
   {
    return return_d4n_error(dpp, txn, std::current_exception());
   }
}

int FDBBlockDirectory::exist_key(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, std::optional<Transaction> txn)
{
  return fdb_invoke(FDBdb, dpp, txn, [&](auto& tr) {
    std::string key = build_index(block);
    ldpp_dout(dpp, 20) << "FDBBlockDirectory::" << __func__ << " key: " << key << dendl;
    return lfdb::key_exists(tr, key);
  });
}

int FDBBlockDirectory::set(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, std::optional<Transaction> txn)
try
 {
  if (!block) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__
                      << "() ERROR: null block pointer" << dendl;
    return -EINVAL;
  }

  const auto write_block = [&](auto& tr) {
    std::string key = build_index(block);
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): index is: " << key << dendl;

    const auto fdbValues = encode_fields(*block);

    lfdb::set(tr, key, fdbValues);
  };

  return fdb_invoke(FDBdb, dpp, txn, write_block);
 }
catch(...)
 {
  return return_d4n_error(dpp, txn, std::current_exception());
 }

/* In this function, if a transaction is supplied, all the blocks will be set in the same transaction */
int FDBBlockDirectory::set(const DoutPrefixProvider* dpp, optional_yield y, std::vector<CacheBlock>& blocks, std::optional<Transaction> txn)
{
  return fdb_invoke(FDBdb, dpp, txn, [&](auto& tr) {
    for (auto& block : blocks) {
      std::string key = build_index(&block);
      ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): index is: " << key << dendl;
      const auto fdbValues = encode_fields(block);

      lfdb::set(tr, key, fdbValues);
    }
  });
}

int FDBBlockDirectory::populate_block(CacheBlock& block, const std::map<std::string, std::string>& kvs) const
{
  block.blockID = std::stoull(kvs.at("blockID"));
  block.version = kvs.at("version");
  block.deleteMarker = (kvs.at("deleteMarker") == "1");
  block.invalid = (kvs.at("invalid") == "1");
  block.size = std::stoull(kvs.at("size"));
  block.globalWeight = std::stoull(kvs.at("globalWeight"));
  block.cacheObj.objName = kvs.at("objName");
  block.cacheObj.bucketName = kvs.at("bucketName");
  block.cacheObj.creationTime = kvs.at("creationTime");
  block.cacheObj.dirty = (kvs.at("dirty") == "1");
  block.cacheObj.hostsList.clear();
  boost::split(block.cacheObj.hostsList, kvs.at("hosts"), boost::is_any_of("_"));
  block.cacheObj.etag = kvs.at("etag");
  block.cacheObj.size = std::stoull(kvs.at("objSize"));
  block.cacheObj.user_id = kvs.at("userId");
  block.cacheObj.display_name = kvs.at("displayName");
  block.cacheObj.acl = kvs.at("acl");
  block.cacheObj.attrs.clear();

  const size_t attrs_count = std::stoull(kvs.at("attrsCount"));
  size_t found_attrs = 0;

  for (const auto& [field, value] : kvs) {
    if (!field.starts_with("attr_")) continue;
    ceph::buffer::list bl;
    bl.append(value);
    block.cacheObj.attrs[field.substr(5)] = std::move(bl);
    ++found_attrs;
  }

  if (found_attrs != attrs_count) {
    return -EINVAL;
  }

  return 0;
}

std::optional<CacheBlock>
FDBBlockDirectory::read_block(const lfdb::transaction_handle& txn, const std::string& key) const
{
  std::map<std::string, std::string> fields;
  if (!lfdb::get(txn, key, fields)) {
    return std::nullopt;
  }

  CacheBlock decoded {};
  if (const int ret = populate_block(decoded, fields); 0 > ret) {
    throw std::system_error(-ret, std::generic_category(), "invalid D4N block record");
  }

  return decoded;
}

std::expected<std::optional<CacheBlock>, int>
FDBBlockDirectory::find(const DoutPrefixProvider *dpp, optional_yield y, const CacheBlock& block,
                        std::optional<Transaction> txn)
try
 {
  const auto key = build_index(&block);
  std::optional<CacheBlock> found;
  const auto find_block = [this, &key](auto& tr) { return read_block(tr, key); };
  if (const int ret = fdb_invoke(FDBdb, dpp, txn, key, std::source_location::current(),
                                 find_block, found); 0 > ret) {
    return std::unexpected(ret);
  }

  return found;
 }
catch(...)
 {
  return std::unexpected(return_d4n_error(dpp, txn, std::current_exception()));
 }

int FDBBlockDirectory::get(const DoutPrefixProvider* dpp, optional_yield y, std::vector<CacheBlock>& blocks, std::optional<Transaction> txn)
try
 {
  if (blocks.empty()) {
    return 0;
  }

  std::vector<std::optional<CacheBlock>> found;
  const auto read_blocks = [this, &blocks](auto& tr) {
    std::vector<std::optional<CacheBlock>> values;
    values.reserve(blocks.size());
    for (const auto& block : blocks) {
      values.push_back(read_block(tr, build_index(&block)));
    }

    return values;
  };

  if (const int ret = fdb_invoke(FDBdb, dpp, txn, read_blocks, found); 0 > ret) {
    return ret;
  }

  for (auto&& [block, value] : std::views::zip(blocks, found)) {
    if (value) {
      block = std::move(*value);
    }
  }

  return 0;
 }
catch(...)
 {
  return return_d4n_error(dpp, txn, std::current_exception());
 }

int FDBBlockDirectory::copy(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, const std::string& copyName, const std::string& copyBucketName, std::optional<Transaction> txn)
{
  if (block == nullptr) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__ << "(): null block pointer" << dendl;
    return -EINVAL;
  }


  auto ret = this->get(dpp, y, block, txn);
  if (ret < 0){
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Could not retrive the object." << dendl;
    return ret;
  }

  auto copyBlock = CacheBlock{ .cacheObj = { .objName = copyName, .bucketName = copyBucketName }, .blockID = 0 };

  copyBlock.version = block->version;
  copyBlock.deleteMarker = block->deleteMarker;
  copyBlock.size = block->size;
  copyBlock.globalWeight = block->globalWeight;
  copyBlock.cacheObj.dirty = block->cacheObj.dirty;
  copyBlock.cacheObj.creationTime = block->cacheObj.creationTime;
  copyBlock.cacheObj.hostsList = block->cacheObj.hostsList;
  copyBlock.cacheObj.etag = block->cacheObj.etag;
  copyBlock.cacheObj.size = block->cacheObj.size;
  copyBlock.cacheObj.user_id = block->cacheObj.user_id;
  copyBlock.cacheObj.display_name = block->cacheObj.display_name;
  copyBlock.cacheObj.acl = block->cacheObj.acl;
  copyBlock.cacheObj.attrs = block->cacheObj.attrs;

  return this->set(dpp, y, &copyBlock, txn);
}

int FDBBlockDirectory::del(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, std::optional<Transaction> txn)
{
  if (block == nullptr) {
    return -EINVAL;
  }

  return fdb_invoke(FDBdb, dpp, txn, [&](auto& tr) {
    std::string key = build_index(block);
    lfdb::erase(tr, key);
  });
}


int FDBBlockDirectory::update_field(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, const std::string& field, std::string& value, std::optional<Transaction> txn)
{
  if (block == nullptr) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__
                      << "(): null block pointer" << dendl;
    return -EINVAL;
  }

  if (const auto ret = this->get(dpp, y, block, txn); ret < 0){
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Could not retrive the object." << dendl;
    return ret;
  }

  if (field == "blockID") {
    block->blockID = std::stoull(value);
  }
  else if (field == "version") {
    block->version = value;
  }
  else if (field == "deleteMarker") {
    block->deleteMarker = (value == "1");
  }
  else if (field == "size") {
    block->size = std::stoull(value);
  }
  else if (field == "globalWeight") {
    block->globalWeight = std::stoull(value);
  }
  else if (field == "invalid") {
    block->invalid = (value == "1" || value == "true");
  }
  else if (field == "objName") {
    block->cacheObj.objName = value;
  }
  else if (field == "bucketName") {
    block->cacheObj.bucketName = value;
  }
  else if (field == "dirty") {
    block->cacheObj.dirty = (value == "1");
  }
  else if (field == "creationTime") {
    block->cacheObj.creationTime = value;
  }
  else if (field == "hosts") {
    block->cacheObj.hostsList.insert(value);
  }
  else if (field == "etag") {
    block->cacheObj.etag = value;
  }
  else if (field == "objSize") {
    block->cacheObj.size = std::stoull(value);
  }
  else if (field == "userId") {
    block->cacheObj.user_id = value;
  }
  else if (field == "displayName") {
    block->cacheObj.display_name = value;
  }

  return this->set(dpp, y, block, txn);

}

int FDBBlockDirectory::remove_host(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, const std::string& value, std::optional<Transaction> txn)
{
  int ret = this->get(dpp, y, block, txn);
  if (ret < 0) {
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Could not retrieve the object." << dendl;
    return ret;
  }

  block->cacheObj.hostsList.erase(value);

  ret = this->set(dpp, y, block, txn);
  if (ret < 0) {
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Could not update the block." << dendl;
    return ret;
  }

  return 0;
}

} // namespace rgw::d4n
