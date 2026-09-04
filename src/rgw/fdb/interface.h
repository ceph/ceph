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

inline database_handle create_database(const std::filesystem::path dbfile)
{
 return std::make_shared<database>(dbfile);
}

inline database_handle create_database(const std::filesystem::path dbfile,
                                       const database_options& dbopts,
                                       const network_options& netopts)
{
 return std::make_shared<database>(dbfile, dbopts, netopts);
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

inline database_handle create_database(const std::filesystem::path dbfile,
                                       const database_options& dbopts)
{
 return create_database(dbfile, dbopts, network_options{});
}

} // namespace ceph::libfdb

namespace ceph::libfdb::detail {

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

} // namespace ceph::libfdb::detail

namespace ceph::libfdb {

[[nodiscard]] inline watch_handle make_watch(database_handle dbh, std::string_view key)
{
 return detail::in_transaction(dbh,
          [key](transaction_handle& txn) {
            return make_watch(txn, key);
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

} // namespace ceph::libfdb

namespace ceph::libfdb {

inline void set(transaction_handle txn,
                const concepts::libfdb_key auto& k, const auto& v,
                const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [key = detail::as_fdb_span(k), &v](const transaction_handle& active_txn) {
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
                                  detail::as_fdb_span(kv.first),
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
          [key = detail::as_fdb_span(k), value = std::string_view(v)](const transaction_handle& active_txn) {
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
          [&k, &v](const transaction_handle& txn) {
            return txn->set(k, ceph::libfdb::to::convert(v));
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
          [key = detail::as_fdb_span(k), &v](const transaction_handle& txn) {
            return txn->set(key, v);
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
          [key = detail::as_fdb_span(k)](const transaction_handle& active_txn) {
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

} // namespace ceph::libfdb

namespace ceph::libfdb {

template <typename OutputTargetOrFnT>
requires concepts::value_callback<std::remove_reference_t<OutputTargetOrFnT>> or
         concepts::decoded_value_sink<OutputTargetOrFnT&&>
inline bool get(ceph::libfdb::transaction_handle txn,
                const concepts::libfdb_key auto& key,
                OutputTargetOrFnT&& output_target_or_fn,
                const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [key = detail::as_fdb_span(key), &output_target_or_fn](const transaction_handle& active_txn) {
            return active_txn->get(key,
                                   detail::get_output_for(output_target_or_fn));
          });
}

template <typename OutputTargetOrFnT>
requires concepts::value_callback<std::remove_reference_t<OutputTargetOrFnT>> or
         concepts::decoded_value_sink<OutputTargetOrFnT&&>
inline bool get(ceph::libfdb::transaction_handle txn,
                const concepts::libfdb_key auto& key,
                OutputTargetOrFnT&& output_target_or_fn)
{
 return get(txn, key, std::forward<OutputTargetOrFnT>(output_target_or_fn), commit_after_op::no_commit);
}

template <typename OutputTargetOrFnT>
requires concepts::value_callback<std::remove_reference_t<OutputTargetOrFnT>> or
         concepts::decoded_value_sink<OutputTargetOrFnT&&>
inline bool get(ceph::libfdb::database_handle dbh,
                const concepts::libfdb_key auto& key,
                OutputTargetOrFnT&& output_target_or_fn)
{
 return detail::in_transaction(dbh,
          [key, &output_target_or_fn](transaction_handle& txn) {
            return get(txn, key, output_target_or_fn, commit_after_op::no_commit);
          });
}

} // namespace ceph::libfdb

namespace ceph::libfdb {

// Does a key exist?
inline bool key_exists(transaction_handle txn,
                       const concepts::libfdb_key auto& k,
                       const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [key = detail::as_libfdb_key_view(k)](const transaction_handle& active_txn) {
            return active_txn->key_exists(key);
          });
}

inline bool key_exists(transaction_handle txn, const concepts::libfdb_key auto& k)
{
 return key_exists(txn, k, commit_after_op::no_commit);
}

inline bool key_exists(database_handle dbh, const concepts::libfdb_key auto& k)
{
 return detail::in_transaction(dbh,
          [k](transaction_handle& txn) {
            return key_exists(txn, k, commit_after_op::no_commit);
          });
}

} // namespace ceph::libfdb

namespace ceph::libfdb::detail {

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

} // namespace ceph::libfdb::detail

#endif
