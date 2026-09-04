// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- // vim: ts=8 sw=2 smarttab ft=cpp
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

#ifndef CEPH_FDB_INTERFACE_H
#define CEPH_FDB_INTERFACE_H

#include "conversion.h"
#include "transaction.h"

#include <span>
#include <tuple>
#include <string>
#include <vector>
#include <string_view>

#include <ranges>
#include <iterator>
#include <algorithm>

#include <memory>
#include <cstdint>
#include <utility>
#include <concepts>
#include <functional>
#include <stop_token>
#include <filesystem>
#include <type_traits>

namespace ceph::libfdb {

namespace concepts {

template <typename IteratorT>
concept key_value_iterator =
 std::input_iterator<IteratorT> and
 requires(std::iter_reference_t<IteratorT> kv) {
  requires libfdb_key<decltype(kv.first)>;
  requires std::is_object_v<std::remove_reference_t<decltype(kv.second)>>;
 };

template <typename RangeT>
concept key_value_range =
 std::ranges::input_range<RangeT> and
 key_value_iterator<std::ranges::iterator_t<RangeT>>;

template <typename RangeT>
concept key_value_forward_range =
 std::ranges::forward_range<RangeT> and
 key_value_iterator<std::ranges::iterator_t<RangeT>>;

} // namespace concepts

/* This should be called when the application is all done with FoundationDB: */
inline void shutdown_libfdb()
{
 ceph::libfdb::detail::database_system::shutdown_fdb();
}

// By default, libfdb applies its internal database defaults:
inline database_handle create_database()
{
 return std::make_shared<database>();
}

inline database_handle create_database(connection_source source)
{
 return std::make_shared<database>(std::move(source), database_options {});
}

inline database_handle create_database(connection_source source,
                                       const database_options& dbopts,
                                       const network_options& netopts)
{
 return std::make_shared<database>(std::move(source), dbopts, netopts);
}

inline database_handle create_database(const database_options& dbopts,
                                       const network_options& netopts)
{
 return std::make_shared<database>(dbopts, netopts);
}

inline database_handle create_database(const database_options& opts)
{
 return create_database(opts, network_options{});
}

inline database_handle create_database(connection_source source,
                                       const database_options& dbopts)
{
 return create_database(std::move(source), dbopts, network_options{});
}

namespace api {

[[nodiscard]] inline std::string client_version()
{
 const auto *version = fdb_get_client_version();

 if (nullptr == version) {
  throw libfdb_exception("invalid FDB client version");
 }

 return std::string(version);
}

[[nodiscard]] inline int max_version() noexcept
{
 return fdb_get_max_api_version();
}

} // namespace api

namespace system {

[[nodiscard]] inline double client_network_load(database_handle dbh)
{
 return detail::database_or_throw(dbh).client_network_load();
}

[[nodiscard]] inline std::string client_status_json(database_handle dbh)
{
 return detail::database_or_throw(dbh).client_status_json();
}

[[nodiscard]] inline std::uint64_t server_protocol(
  database_handle dbh,
  const std::uint64_t expected_version = 0)
{
 return detail::database_or_throw(dbh).server_protocol(expected_version);
}

inline void reboot_worker(database_handle dbh,
                          std::string_view address,
                          const bool check,
                          const int duration)
{
 detail::database_or_throw(dbh).reboot_worker(address, check, duration);
}

inline void force_recovery_with_data_loss(database_handle dbh,
                                          std::string_view dcid)
{
 detail::database_or_throw(dbh).force_recovery_with_data_loss(dcid);
}

inline void create_snapshot(database_handle dbh,
                            std::string_view uid,
                            std::string_view snap_command)
{
 detail::database_or_throw(dbh).create_snapshot(uid, snap_command);
}

} // namespace system

namespace detail {

template <typename OutValuesT>
struct value_collector_t final
{
 OutValuesT& out_values;

 void operator()(std::span<const std::uint8_t> out_data) const;
};

template <typename OutValuesT>
auto value_collector(OutValuesT& out_values) -> value_collector_t<OutValuesT>;

template <typename OutputTargetOrFnT>
requires concepts::value_callback<std::remove_reference_t<OutputTargetOrFnT>>
decltype(auto) get_output_for(OutputTargetOrFnT&& output_target_or_fn)
{
 return std::forward<OutputTargetOrFnT>(output_target_or_fn);
}

template <typename OutputTargetOrFnT>
requires (not concepts::value_callback<std::remove_reference_t<OutputTargetOrFnT>>)
auto get_output_for(OutputTargetOrFnT&& output_target_or_fn)
{
 return value_collector(output_target_or_fn);
}

} // namespace detail

[[nodiscard]] inline watch_handle make_watch(database_handle dbh, std::string_view key)
{
 return detail::in_transaction(dbh,
          [key](transaction_handle& txn) {
            return make_watch(txn, key);
          });
}

[[nodiscard]] inline std::string get_key(database_handle dbh,
                                         const key_selector& selector,
                                         const read_mode mode = read_mode::serializable)
{
 return detail::in_read_transaction(dbh,
          [selector, mode](transaction_handle& txn) {
            return get_key(txn, selector, mode);
          });
}

template <typename FnT>
concept watch_callback =
 std::invocable<FnT&, std::string_view> &&
 std::is_void_v<std::invoke_result_t<FnT&, std::string_view>>;

template <typename FnT>
requires watch_callback<FnT>
void watched_loop(database_handle dbh, std::string_view key, std::stop_token stop_token, FnT&& fn)
{
 std::string watched_key(key);

 while (not stop_token.stop_requested() &&
        watch_event::changed == make_watch(dbh, watched_key).wait_for_event(stop_token)) {
  std::invoke(fn, std::string_view(watched_key));
 }
}

/* watched_loop() runs until the watch is cancelled or an exception escapes.
 * For more complex stop behavior, see make_watch(), ready(), cancel(), and
 * wait_for_event(): */
template <typename FnT>
requires watch_callback<FnT>
void watched_loop(database_handle dbh, std::string_view key, FnT&& fn)
{
 return watched_loop(dbh, key, std::stop_token{}, std::forward<FnT>(fn));
}

inline void set(transaction_handle txn,
                const concepts::libfdb_key auto& k, const auto& v,
                const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [key = detail::as_byte_view(k), &v](const transaction_handle& active_txn) {
            return detail::transaction_set_kv_bytes(active_txn, key, ceph::libfdb::to::convert(v));
          });
}

// If someone gives us an explicit transaction handle, they almost certainly don't want to commit 
// it (though they can always specify otherwise):
inline void set(transaction_handle txn,
                const concepts::libfdb_key auto& k,
                const auto& v)
{
 return set(txn, k, v, commit_after_op::no_commit);
}

// ...conversely, with a database handle given, we can assume they DO want to auto-commit:
inline void set(database_handle dbh,
                const concepts::libfdb_key auto& k, const auto& v)
{
 return detail::in_transaction(dbh,
          [k, &v](transaction_handle& txn) {
            return set(txn, k, v, commit_after_op::no_commit);
          });
}

template <concepts::key_value_iterator IteratorT>
inline void set(transaction_handle txn,
                IteratorT b, IteratorT e,
                const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [&b, &e](const transaction_handle& active_txn) {
            std::vector<std::uint8_t> fixed_buffer;

            std::ranges::for_each(std::ranges::subrange(b, e),
                      [&active_txn, &fixed_buffer](const auto& kv) {
                        detail::transaction_set_kv_bytes(active_txn,
                                  detail::as_byte_view(kv.first),
                                  ceph::libfdb::to::convert(kv.second, fixed_buffer));
                      });
          });
}

template <concepts::key_value_iterator IteratorT>
requires std::forward_iterator<IteratorT>
inline void set(database_handle dbh, IteratorT b, IteratorT e)
{
 return detail::in_transaction(dbh,
          [b, e](transaction_handle& txn) {
            return set(txn, b, e, commit_after_op::no_commit);
          });
}

inline void set(transaction_handle txn,
                concepts::key_value_range auto&& kvs,
                const commit_after_op commit_after)
{
 return set(txn,
            std::ranges::begin(kvs),
            std::ranges::end(kvs),
            commit_after);
}

inline void set(transaction_handle txn, concepts::key_value_range auto&& kvs)
{
 return set(txn, kvs, commit_after_op::no_commit);
}

inline void set(database_handle dbh, concepts::key_value_forward_range auto&& kvs)
{
 // Database-handle operations may replay on retry, so the range must be multipass.
 return detail::in_transaction(dbh,
          [&kvs](transaction_handle& txn) {
            return set(txn, kvs, commit_after_op::no_commit);
          });
}

// Note that we force things into a span so that byte streams get the proper encoding expected by zpp_bits:
inline void set(transaction_handle txn,
                const concepts::libfdb_key auto& k,
                const ceph::libfdb::concepts::stringview_convertible auto& v,
                const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [key = detail::as_byte_view(k), value = std::string_view(v)](const transaction_handle& active_txn) {
            return detail::transaction_set_kv_bytes(active_txn, key, ceph::libfdb::to::convert(value));
          });
}

inline void set(transaction_handle txn,
                const concepts::libfdb_key auto& k,
                const ceph::libfdb::concepts::stringview_convertible auto& v)
{
 return set(txn, k, v, commit_after_op::no_commit);
}

inline void set(database_handle dbh,
                const concepts::libfdb_key auto& k,
                const ceph::libfdb::concepts::stringview_convertible auto& v)
{
 return detail::in_transaction(dbh,
          [k, value = std::string_view(v)](transaction_handle& txn) {
            return set(txn, k, value, commit_after_op::no_commit);
          });
}

inline void set(transaction_handle txn,
                const versioned_bytes& k,
                const auto& v,
                const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [&k, &v](const transaction_handle& active_txn) {
            return active_txn->set(k, ceph::libfdb::to::convert(v));
          });
}

inline void set(transaction_handle txn,
                const versioned_bytes& k,
                const auto& v)
{
 return set(txn, k, v, commit_after_op::no_commit);
}

inline void set(database_handle dbh,
                const versioned_bytes& k,
                const auto& v)
{
 return detail::in_transaction(dbh,
          [&k, &v](transaction_handle& txn) {
            return set(txn, k, v, commit_after_op::no_commit);
          });
}

// Version-stamped keys and values are strictly an either/or choice for a single set():
inline void set(transaction_handle,
                const versioned_bytes&,
                const versioned_bytes&,
                const commit_after_op) = delete;

inline void set(transaction_handle,
                const versioned_bytes&,
                const versioned_bytes&) = delete;

inline void set(database_handle,
                const versioned_bytes&,
                const versioned_bytes&) = delete;

inline void set(transaction_handle txn,
                const concepts::libfdb_key auto& k,
                const versioned_bytes& v,
                const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [key = detail::as_byte_view(k), &v](const transaction_handle& active_txn) {
            return active_txn->set(key, v);
          });
}

inline void set(transaction_handle txn,
                const concepts::libfdb_key auto& k,
                const versioned_bytes& v)
{
 return set(txn, k, v, commit_after_op::no_commit);
}

inline void set(database_handle dbh,
                const concepts::libfdb_key auto& k,
                const versioned_bytes& v)
{
 return detail::in_transaction(dbh,
          [k, &v](transaction_handle& txn) {
            return set(txn, k, v, commit_after_op::no_commit);
          });
}

} // namespace ceph::libfdb

namespace ceph::libfdb::detail {

template <typename ValueT>
concept fdb_integer_value =
 std::integral<ValueT> and
 not std::same_as<std::remove_cv_t<ValueT>, bool>;

template <fdb_integer_value ValueT>
constexpr auto little_endian_integer(const std::span<const std::uint8_t> bytes)
{
 if (sizeof(ValueT) < std::size(bytes)) {
  throw std::invalid_argument {"FoundationDB integer value is too large"};
 }

 using unsigned_t = std::make_unsigned_t<ValueT>;

 unsigned_t out = 0;

 for (auto byte : bytes | std::views::reverse) {
  out <<= 8;
  out |= static_cast<unsigned_t>(byte);
 }

 return static_cast<ValueT>(out);
}

} // namespace ceph::libfdb::detail

namespace ceph::libfdb {

// Atomic operations enqueue FoundationDB server-side value mutations:
namespace atomic {

namespace detail {

inline auto byte_span(const std::string_view bytes)
{
 return ceph::libfdb::detail::as_byte_view(bytes);
}

template <typename FDBBytesT>
requires (not concepts::stringview_convertible<FDBBytesT>)
inline auto byte_span(const FDBBytesT& bytes)
{
 return std::span<const std::uint8_t>(bytes);
}

inline constexpr auto integral_param =
 []<typename ValueT>(const ValueT value)
 requires ceph::libfdb::detail::fdb_integer_value<ValueT>
 {
  return ceph::libfdb::detail::little_endian_bytes(value);
 };

inline constexpr auto byte_param =
 []<typename FDBBytesT>(const FDBBytesT& value)
 requires requires(const FDBBytesT& x) { byte_span(x); }
 {
  return byte_span(value);
 };

template <FDBMutationType MutationKind, auto MaterializeParam, typename ParamT>
requires requires(const ParamT& value) { MaterializeParam(value); }
inline void atomic_op(transaction_handle txn,
                      const concepts::libfdb_key auto& k,
                      const ParamT& param,
                      const commit_after_op commit_after)
{
 return ceph::libfdb::detail::commit_noreplay(txn, commit_after,
          [key = ceph::libfdb::detail::as_byte_view(k),
           value = MaterializeParam(param)](const transaction_handle& active_txn) {
            return ceph::libfdb::detail::transaction_atomic_op(
             active_txn, key, std::span<const std::uint8_t>(value), MutationKind);
          });
}

template <FDBMutationType MutationKind, auto MaterializeParam, typename ParamT>
requires requires(const ParamT& value) { MaterializeParam(value); }
inline void atomic_op(database_handle dbh,
                      const concepts::libfdb_key auto& k,
                      const ParamT& param)
{
 return ceph::libfdb::detail::in_transaction(dbh,
          [k, &param](transaction_handle& txn) {
            return atomic_op<MutationKind, MaterializeParam>(
             txn, k, param, commit_after_op::no_commit);
          });
}

} // namespace detail

template <typename ValueT>
requires ceph::libfdb::detail::fdb_integer_value<ValueT>
inline void add(transaction_handle txn,
                const concepts::libfdb_key auto& k,
                const ValueT value,
                const commit_after_op commit_after = commit_after_op::no_commit)
{
 return detail::atomic_op<FDB_MUTATION_TYPE_ADD, detail::integral_param>(
  txn, k, value, commit_after);
}

template <typename ValueT>
requires ceph::libfdb::detail::fdb_integer_value<ValueT>
inline void add(database_handle dbh,
                const concepts::libfdb_key auto& k,
                const ValueT value)
{
 return detail::atomic_op<FDB_MUTATION_TYPE_ADD, detail::integral_param>(dbh, k, value);
}

template <typename ValueT>
requires (std::unsigned_integral<ValueT> && not std::same_as<ValueT, bool>)
inline void min(transaction_handle txn,
                const concepts::libfdb_key auto& k,
                const ValueT value,
                const commit_after_op commit_after = commit_after_op::no_commit)
{
 return detail::atomic_op<FDB_MUTATION_TYPE_MIN, detail::integral_param>(
  txn, k, value, commit_after);
}

template <typename ValueT>
requires (std::unsigned_integral<ValueT> && not std::same_as<ValueT, bool>)
inline void min(database_handle dbh,
                const concepts::libfdb_key auto& k,
                const ValueT value)
{
 return detail::atomic_op<FDB_MUTATION_TYPE_MIN, detail::integral_param>(dbh, k, value);
}

template <typename ValueT>
requires (std::unsigned_integral<ValueT> && not std::same_as<ValueT, bool>)
inline void max(transaction_handle txn,
                const concepts::libfdb_key auto& k,
                const ValueT value,
                const commit_after_op commit_after = commit_after_op::no_commit)
{
 return detail::atomic_op<FDB_MUTATION_TYPE_MAX, detail::integral_param>(
  txn, k, value, commit_after);
}

template <typename ValueT>
requires (std::unsigned_integral<ValueT> && not std::same_as<ValueT, bool>)
inline void max(database_handle dbh,
                const concepts::libfdb_key auto& k,
                const ValueT value)
{
 return detail::atomic_op<FDB_MUTATION_TYPE_MAX, detail::integral_param>(dbh, k, value);
}

template <typename ValueT>
requires ceph::libfdb::detail::fdb_integer_value<ValueT>
inline void bit_and(transaction_handle txn,
                    const concepts::libfdb_key auto& k,
                    const ValueT value,
                    const commit_after_op commit_after = commit_after_op::no_commit)
{
 return detail::atomic_op<FDB_MUTATION_TYPE_BIT_AND, detail::integral_param>(
  txn, k, value, commit_after);
}

template <typename ValueT>
requires ceph::libfdb::detail::fdb_integer_value<ValueT>
inline void bit_and(database_handle dbh,
                    const concepts::libfdb_key auto& k,
                    const ValueT value)
{
 return detail::atomic_op<FDB_MUTATION_TYPE_BIT_AND, detail::integral_param>(dbh, k, value);
}

template <typename ValueT>
requires ceph::libfdb::detail::fdb_integer_value<ValueT>
inline void bit_or(transaction_handle txn,
                   const concepts::libfdb_key auto& k,
                   const ValueT value,
                   const commit_after_op commit_after = commit_after_op::no_commit)
{
 return detail::atomic_op<FDB_MUTATION_TYPE_BIT_OR, detail::integral_param>(
  txn, k, value, commit_after);
}

template <typename ValueT>
requires ceph::libfdb::detail::fdb_integer_value<ValueT>
inline void bit_or(database_handle dbh,
                   const concepts::libfdb_key auto& k,
                   const ValueT value)
{
 return detail::atomic_op<FDB_MUTATION_TYPE_BIT_OR, detail::integral_param>(dbh, k, value);
}

template <typename ValueT>
requires ceph::libfdb::detail::fdb_integer_value<ValueT>
inline void bit_xor(transaction_handle txn,
                    const concepts::libfdb_key auto& k,
                    const ValueT value,
                    const commit_after_op commit_after = commit_after_op::no_commit)
{
 return detail::atomic_op<FDB_MUTATION_TYPE_BIT_XOR, detail::integral_param>(
  txn, k, value, commit_after);
}

template <typename ValueT>
requires ceph::libfdb::detail::fdb_integer_value<ValueT>
inline void bit_xor(database_handle dbh,
                    const concepts::libfdb_key auto& k,
                    const ValueT value)
{
 return detail::atomic_op<FDB_MUTATION_TYPE_BIT_XOR, detail::integral_param>(dbh, k, value);
}

template <typename FDBBytesT>
requires requires(const FDBBytesT& value) { detail::byte_span(value); }
inline void byte_min(transaction_handle txn,
                     const concepts::libfdb_key auto& k,
                     const FDBBytesT& value,
                     const commit_after_op commit_after = commit_after_op::no_commit)
{
 return detail::atomic_op<FDB_MUTATION_TYPE_BYTE_MIN, detail::byte_param>(
  txn, k, value, commit_after);
}

template <typename FDBBytesT>
requires requires(const FDBBytesT& value) { detail::byte_span(value); }
inline void byte_min(database_handle dbh,
                     const concepts::libfdb_key auto& k,
                     const FDBBytesT& value)
{
 return detail::atomic_op<FDB_MUTATION_TYPE_BYTE_MIN, detail::byte_param>(dbh, k, value);
}

template <typename FDBBytesT>
requires requires(const FDBBytesT& value) { detail::byte_span(value); }
inline void byte_max(transaction_handle txn,
                     const concepts::libfdb_key auto& k,
                     const FDBBytesT& value,
                     const commit_after_op commit_after = commit_after_op::no_commit)
{
 return detail::atomic_op<FDB_MUTATION_TYPE_BYTE_MAX, detail::byte_param>(
  txn, k, value, commit_after);
}

template <typename FDBBytesT>
requires requires(const FDBBytesT& value) { detail::byte_span(value); }
inline void byte_max(database_handle dbh,
                     const concepts::libfdb_key auto& k,
                     const FDBBytesT& value)
{
 return detail::atomic_op<FDB_MUTATION_TYPE_BYTE_MAX, detail::byte_param>(dbh, k, value);
}

template <typename FDBBytesT>
requires requires(const FDBBytesT& value) { detail::byte_span(value); }
inline void append_if_fits(transaction_handle txn,
                           const concepts::libfdb_key auto& k,
                           const FDBBytesT& value,
                           const commit_after_op commit_after = commit_after_op::no_commit)
{
 return detail::atomic_op<FDB_MUTATION_TYPE_APPEND_IF_FITS, detail::byte_param>(
  txn, k, value, commit_after);
}

template <typename FDBBytesT>
requires requires(const FDBBytesT& value) { detail::byte_span(value); }
inline void append_if_fits(database_handle dbh,
                           const concepts::libfdb_key auto& k,
                           const FDBBytesT& value)
{
 return detail::atomic_op<FDB_MUTATION_TYPE_APPEND_IF_FITS, detail::byte_param>(dbh, k, value);
}

template <typename FDBBytesT>
requires requires(const FDBBytesT& value) { detail::byte_span(value); }
inline void compare_and_clear(transaction_handle txn,
                              const concepts::libfdb_key auto& k,
                              const FDBBytesT& expected,
                              const commit_after_op commit_after = commit_after_op::no_commit)
{
 return detail::atomic_op<FDB_MUTATION_TYPE_COMPARE_AND_CLEAR, detail::byte_param>(
  txn, k, expected, commit_after);
}

template <typename FDBBytesT>
requires requires(const FDBBytesT& value) { detail::byte_span(value); }
inline void compare_and_clear(database_handle dbh,
                              const concepts::libfdb_key auto& k,
                              const FDBBytesT& expected)
{
 return detail::atomic_op<FDB_MUTATION_TYPE_COMPARE_AND_CLEAR, detail::byte_param>(
  dbh, k, expected);
}

} // namespace atomic

} // namespace ceph::libfdb

namespace ceph::libfdb {
// erase() in libfdb is clear() in FDB parlance:
inline void erase(ceph::libfdb::transaction_handle txn,
                  const query::expression auto& selection,
                  const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [&selection](const transaction_handle& active_txn) {
            query::for_each_interval(selection, [&active_txn](const ceph::libfdb::select& interval) {
              detail::transaction_clear_range(active_txn, interval);
            });
          });
}

inline void erase(ceph::libfdb::transaction_handle txn, const query::expression auto& selection)
{
 return erase(txn, selection, commit_after_op::no_commit);
}

inline void erase(ceph::libfdb::database_handle dbh,
                  const query::expression auto& selection)
{
 return detail::in_transaction(dbh,
          [&selection](transaction_handle& txn) {
            return erase(txn, selection, commit_after_op::no_commit);
          });
}

inline void erase(ceph::libfdb::transaction_handle txn,
                  const concepts::libfdb_key auto& k,
                  const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [key = detail::as_byte_view(k)](const transaction_handle& active_txn) {
            return detail::transaction_clear_key_bytes(active_txn, key);
          });
}

inline void erase(ceph::libfdb::transaction_handle txn, const concepts::libfdb_key auto& k)
{
 return erase(txn, k, commit_after_op::no_commit);
}

inline void erase(ceph::libfdb::database_handle dbh, const concepts::libfdb_key auto& k)
{
 return detail::in_transaction(dbh,
          [k](transaction_handle& txn) {
            return erase(txn, k, commit_after_op::no_commit);
          });
}

namespace detail {

inline void mark_conflict(transaction_handle txn,
                          const query::expression auto& selection,
                          const FDBConflictRangeType type)
{
 bool marked = false;

 query::for_each_interval(selection, [&](const ceph::libfdb::select& interval) {
  transaction_mark_conflict_range(txn, interval, type);
  marked = true;
 });

 if (not marked) {
  throw std::invalid_argument(
    "conflict expression must contain a non-empty range");
 }
}

} // namespace detail

// Mark explicit conflict ranges when transaction correctness depends on data
// that was not read or written through the ordinary libfdb operation path:
inline void mark_conflict_read(transaction_handle txn,
                               const query::expression auto& selection)
{
 return detail::mark_conflict(txn, selection, FDB_CONFLICT_RANGE_TYPE_READ);
}

inline void mark_conflict_write(transaction_handle txn,
                                const query::expression auto& selection)
{
 return detail::mark_conflict(txn, selection, FDB_CONFLICT_RANGE_TYPE_WRITE);
}

inline void mark_conflict_read(transaction_handle txn,
                               const concepts::libfdb_key auto& begin,
                               const concepts::libfdb_key auto& end)
{
 return mark_conflict_read(txn, query::between(begin, end));
}

inline void mark_conflict_write(transaction_handle txn,
                                const concepts::libfdb_key auto& begin,
                                const concepts::libfdb_key auto& end)
{
 return mark_conflict_write(txn, query::between(begin, end));
}

inline void mark_conflict_read(transaction_handle txn,
                               const concepts::libfdb_key auto& key)
{
 return mark_conflict_read(txn, query::singleton(key));
}

inline void mark_conflict_write(transaction_handle txn,
                                const concepts::libfdb_key auto& key)
{
 return mark_conflict_write(txn, query::singleton(key));
}

template <typename OutputTargetOrFnT>
requires concepts::value_callback<std::remove_reference_t<OutputTargetOrFnT>> or
         concepts::decoded_value_sink<OutputTargetOrFnT&&>
inline bool get(ceph::libfdb::transaction_handle txn,
                const concepts::libfdb_key auto& key,
                OutputTargetOrFnT&& output_target_or_fn,
                const read_mode mode,
                const commit_after_op commit_after)
{
 return detail::commit_noreplay(
   txn, commit_after,
   [key = detail::as_byte_view(key),
    &output_target_or_fn,
    mode](const transaction_handle& active_txn) {
     return active_txn->get(
       key, detail::get_output_for(output_target_or_fn), mode);
   });
}

template <typename OutputTargetOrFnT>
requires concepts::value_callback<std::remove_reference_t<OutputTargetOrFnT>> or
         concepts::decoded_value_sink<OutputTargetOrFnT&&>
inline bool get(ceph::libfdb::transaction_handle txn,
                const concepts::libfdb_key auto& key,
                OutputTargetOrFnT&& output_target_or_fn,
                const commit_after_op commit_after)
{
 return get(txn, key, std::forward<OutputTargetOrFnT>(output_target_or_fn),
            read_mode::serializable, commit_after);
}

template <typename OutputTargetOrFnT>
requires concepts::value_callback<std::remove_reference_t<OutputTargetOrFnT>> or
         concepts::decoded_value_sink<OutputTargetOrFnT&&>
inline bool get(ceph::libfdb::transaction_handle txn,
                const concepts::libfdb_key auto& key,
                OutputTargetOrFnT&& output_target_or_fn,
                const read_mode mode = read_mode::serializable)
{
 return get(txn, key, std::forward<OutputTargetOrFnT>(output_target_or_fn),
            mode, commit_after_op::no_commit);
}

template <typename OutputTargetOrFnT>
requires concepts::value_callback<std::remove_reference_t<OutputTargetOrFnT>> or
         concepts::decoded_value_sink<OutputTargetOrFnT&&>
inline bool get(ceph::libfdb::database_handle dbh,
                const concepts::libfdb_key auto& key,
                OutputTargetOrFnT&& output_target_or_fn,
                const read_mode mode = read_mode::serializable)
try
{
 return detail::in_read_transaction(dbh,
          [key, &output_target_or_fn, mode](transaction_handle& txn) {
            auto&& output = detail::get_output_for(output_target_or_fn);

            return get(txn, key,
                       [&output](const std::span<const std::uint8_t> value) {
                         detail::invoke_user_callback(output, value);
                       },
                       mode,
                       commit_after_op::no_commit);
          });
}
catch (const detail::user_callback_failure& failure)
{
 std::rethrow_exception(failure.cause);
}

// Adapt "out" to FDB's raw little-endian representation used by numeric atomic
// mutations, bypassing libfdb's ordinary decoding. With get(), a missing key
// leaves "out" unchanged; a value wider than ValueT throws:
template <detail::fdb_integer_value ValueT>
[[nodiscard]] constexpr auto as_fdb_integer(ValueT& out) noexcept
{
 return [&out](const std::span<const std::uint8_t> bytes) {
  out = detail::little_endian_integer<ValueT>(bytes);
 };
}

} // namespace ceph::libfdb

namespace ceph::libfdb {

// Does a key exist?
inline bool key_exists(transaction_handle txn,
                       const concepts::libfdb_key auto& k,
                       const read_mode mode,
                       const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [key = detail::as_byte_view(k), mode](const transaction_handle& active_txn) {
            return active_txn->key_exists(key, mode);
          });
}

inline bool key_exists(transaction_handle txn,
                       const concepts::libfdb_key auto& k,
                       const commit_after_op commit_after)
{
 return key_exists(txn, k, read_mode::serializable, commit_after);
}

inline bool key_exists(transaction_handle txn,
                       const concepts::libfdb_key auto& k,
                       const read_mode mode = read_mode::serializable)
{
 return key_exists(txn, k, mode, commit_after_op::no_commit);
}

inline bool key_exists(database_handle dbh,
                       const concepts::libfdb_key auto& k,
                       const read_mode mode = read_mode::serializable)
{
 return detail::in_read_transaction(dbh,
          [k, mode](transaction_handle& txn) {
            return key_exists(txn, k, mode, commit_after_op::no_commit);
          });
}

namespace detail {

template <typename OutValuesT>
void value_collector_t<OutValuesT>::operator()(std::span<const std::uint8_t> out_data) const
{
 ceph::libfdb::from::convert(out_data, out_values);
}

template <typename OutValuesT>
auto value_collector(OutValuesT& out_values) -> value_collector_t<OutValuesT>
{
 return { out_values };
}

} // namespace detail

} // namespace ceph::libfdb

#endif
