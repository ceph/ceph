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

#ifndef CEPH_FDB_BINDINGS_H
 #define CEPH_FDB_BINDINGS_H

#include "base.h"
#include "conversion.h"

#include <limits>

namespace ceph::libfdb {

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

inline transaction_handle make_transaction(database_handle dbh)
{
 return std::make_shared<transaction>(dbh);
}

inline transaction_handle make_transaction(database_handle dbh, const transaction_options& opts)
{
 return std::make_shared<transaction>(dbh, opts);
}

// Note: only rarely is a direct call to this needed. You can use transactors or pass database_handles
// to get automagic.
// Note: after a transaction is committed, it cannot be used for more database
// work. Post-commit observation helpers such as committed_version() are okay.
// On false, the client should retry the transaction:
[[nodiscard]] inline bool commit(transaction_handle& txn)
{
 return txn->commit(); 
}

[[nodiscard]] inline bool commit(transaction_handle& txn,
                                 const versionstamp& stamp)
{
 txn->mark_version(stamp);
 return commit(txn);
}

[[nodiscard]] inline std::int64_t committed_version(const transaction_handle& txn)
{
 return txn->committed_version();
}

[[nodiscard]] inline std::int64_t read_version(const transaction_handle& txn)
{
 return txn->read_version();
}

inline void set_read_version(const transaction_handle& txn, const std::int64_t version)
{
 txn->set_read_version(version);
}

[[nodiscard]] inline watch_handle make_watch(transaction_handle txn, std::string_view key)
{
 return txn->make_watch(detail::as_fdb_span(key));
}

} // namespace ceph::libfdb

namespace ceph::libfdb::detail {

// Forward declarations:
template <typename FnT>
using transaction_invocation_result_t =
 std::invoke_result_t<FnT&, transaction_handle&>;

template <typename FnT>
concept supported_transaction_invocation =
 concepts::supported_invocation_result<transaction_invocation_result_t<FnT>>;

template <typename FnT>
using operation_result_t =
 std::conditional_t<std::is_void_v<transaction_invocation_result_t<FnT>>,
                    void,
                    std::remove_cvref_t<transaction_invocation_result_t<FnT>>>;

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

template <supported_transaction_invocation FnT>
auto maybe_retry(transaction_handle txn, FnT&& fn) -> operation_result_t<FnT>;

template <supported_transaction_invocation FnT>
auto commit_noreplay(transaction_handle txn, const commit_after_op commit_after, FnT&& fn)
 -> operation_result_t<FnT>;

template <supported_transaction_invocation FnT>
auto in_transaction(database_handle dbh, FnT&& fn)
 -> operation_result_t<FnT>;

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
requires std::invocable<FnT&, std::string_view>
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
requires std::invocable<FnT&, std::string_view>
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

namespace detail {

inline select select_from_initializer_list(std::initializer_list<std::string_view> keys)
{
 const auto first = std::begin(keys);

 if (1 == std::size(keys)) {
  return select(*first);
 }

 if (2 == std::size(keys)) {
  return select(*first, *std::next(first));
 }

 throw libfdb_exception("range selection initializer list requires one or two keys");
}

template <typename OutT>
concept string_pair_output =
 concepts::string_pair_output_iterator<OutT> ||
 concepts::string_pair_output_range<OutT>;

template <typename OutT>
struct materialized_string_pair_output final
{
 OutT values;
 std::size_t nread = 0;
};

template <typename RangeT>
inline auto move_range(RangeT& range)
{
 return std::ranges::subrange(std::make_move_iterator(std::begin(range)),
                              std::make_move_iterator(std::end(range)));
}

template <typename ContainerT>
inline void publish_string_pair_results(ContainerT& out, ContainerT&& tmp)
{
 if constexpr (ceph::concepts::has_empty<ContainerT> &&
               std::assignable_from<ContainerT&, ContainerT&&>) {
  if (out.empty()) {
   out = std::move(tmp);
   return;
  }
 }

 if constexpr (concepts::has_merge<ContainerT>) {
  out.merge(tmp);
  return;
 }

 ceph::util::append_range(out, move_range(tmp));
}

template <query::expression SelectionT, string_pair_output OutT>
inline std::size_t get_value_selection_from_transaction(transaction& txn,
                                                        const SelectionT& selection,
                                                        OutT& out)
{
 std::size_t nread = 0;

 query::for_each_interval(selection, [&](const ceph::libfdb::select& interval) {
  nread += detail::get_value_range_from_transaction(txn, interval, out);
 });

 return nread;
}

template <typename OutT, query::expression SelectionT>
requires concepts::materializable_string_pair_output_range<OutT>
inline auto materialize_string_pair_selection(transaction& txn,
                                              const SelectionT& selection)
 -> materialized_string_pair_output<OutT>
{
 OutT tmp;
 const auto nread = get_value_selection_from_transaction(txn, selection, tmp);

 return {
  .values = std::move(tmp),
  .nread = nread
 };
}

} // namespace detail

// get() with a selector writes key/value pairs to out_iter and returns the
// number of pairs emitted:
// JFW: Satisfying output_iterator is not as straightforward as it appears, I need to look at this mechanism again; meanwhile, the template doesn't
// /prevent/ future type narrowing, but I'm forcing it to std::string for now:
inline std::size_t get(ceph::libfdb::transaction_handle txn,
                       const query::expression auto& selection,
                       concepts::string_pair_output_iterator auto out_iter,
                       const ceph::libfdb::commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [&selection, out_iter](const transaction_handle& active_txn) mutable {
            return detail::get_value_selection_from_transaction(*active_txn, selection, out_iter);
          });
}

inline std::size_t get(ceph::libfdb::transaction_handle txn,
                       const query::expression auto& selection,
                       concepts::string_pair_output_iterator auto out_iter)
{
 return get(txn, selection, out_iter, commit_after_op::no_commit);
}

inline std::size_t get(ceph::libfdb::database_handle dbh,
                       const query::expression auto& selection,
                       concepts::string_pair_output_iterator auto out_iter)
{
 auto result = detail::in_transaction(dbh,
          [&selection](transaction_handle& txn) {
            using out_t = std::vector<std::pair<std::string, std::string>>;
            return detail::materialize_string_pair_selection<out_t>(*txn, selection);
          });

 std::ranges::move(result.values, out_iter);
 return result.nread;
}

inline std::size_t get(ceph::libfdb::transaction_handle txn,
                       const query::expression auto& selection,
                       concepts::string_pair_output_range auto& out,
                       const ceph::libfdb::commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [&selection, &out](const transaction_handle& active_txn) {
            return detail::get_value_selection_from_transaction(*active_txn, selection, out);
          });
}

inline std::size_t get(ceph::libfdb::transaction_handle txn,
                       const query::expression auto& selection,
                       concepts::string_pair_output_range auto& out)
{
 return get(txn, selection, out, commit_after_op::no_commit);
}

inline std::size_t get(ceph::libfdb::database_handle dbh,
                       const query::expression auto& selection,
                       concepts::materializable_string_pair_output_range auto& out)
{
 using out_t = std::remove_cvref_t<decltype(out)>;

 auto result = detail::in_transaction(dbh,
          [&selection](transaction_handle& txn) {
            return detail::materialize_string_pair_selection<out_t>(*txn, selection);
          });

 detail::publish_string_pair_results(out, std::move(result.values));
 return result.nread;
}

inline std::size_t get(ceph::libfdb::transaction_handle txn,
                       std::initializer_list<std::string_view> keys,
                       concepts::string_pair_output_range auto& out,
                       const ceph::libfdb::commit_after_op commit_after)
{
 return get(txn, detail::select_from_initializer_list(keys), out, commit_after);
}

inline std::size_t get(ceph::libfdb::transaction_handle txn,
                       std::initializer_list<std::string_view> keys,
                       concepts::string_pair_output_range auto& out)
{
 return get(txn, keys, out, commit_after_op::no_commit);
}

inline std::size_t get(ceph::libfdb::database_handle dbh,
                       std::initializer_list<std::string_view> keys,
                       concepts::materializable_string_pair_output_range auto& out)
{
 return get(dbh, detail::select_from_initializer_list(keys), out);
}

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

namespace ceph::libfdb {

/* A "transactor" is a function-like wrapper for running replayable transactions.
 * It defers transaction creation until called, commits after the user function
 * returns, retries when FoundationDB requests replay, and throws when recovery
 * fails or retry attempts are exhausted. Plus, the name is pretty cool. */
class transactor final
{
 database_handle dbh;

 std::optional<transaction_options> opts;

 private:
 explicit transactor(database_handle dbh_)
  : dbh(dbh_)
 {}

 transactor(database_handle dbh_, const transaction_options& opts_)
  : dbh(dbh_),
    opts(opts_)
 {}

 public:
 template <typename FnT>
 requires std::invocable<FnT&, transaction_handle&>
 decltype(auto) operator()(FnT&& fn) const
 {
  auto txn = opts ? make_transaction(dbh, *opts)
                  : make_transaction(dbh);

  return detail::maybe_retry(txn, std::forward<FnT>(fn));
 }

 private:
 friend inline transactor make_transactor(database_handle dbh);
 friend inline transactor make_transactor(database_handle dbh, const transaction_options& opts);
};

inline transactor make_transactor(database_handle dbh)
{
 return transactor(dbh);
}

inline transactor make_transactor(database_handle dbh, const transaction_options& opts)
{
 return transactor(dbh, opts);
}

} // namespace ceph::libfdb

// Scan and block traversal:
namespace ceph::libfdb {

namespace detail {

inline auto intervals(ceph::libfdb::select selection)
{
 // Raw selectors keep select compatibility but still execute in ordinary FDB keyspace:
 return std::views::single(query::intersection(std::move(selection), query::universal()))
      | std::views::filter([](const ceph::libfdb::select& range) {
         return not query::is_empty(range);
        });
}

template <query::non_interval_expression QueryT>
inline auto intervals(const QueryT& query)
{
 std::vector<ceph::libfdb::select> out;

 query::for_each_interval(query, [&out](ceph::libfdb::select interval) {
  out.push_back(std::move(interval));
 });

 return out;
}

template <typename AssocT, typename RangeT>
inline AssocT collect_range(RangeT&& range)
{
 return ceph::util::collect_as<AssocT>(std::forward<RangeT>(range));
}

template <typename ValueT = std::string>
inline auto scan_selector(ceph::libfdb::transaction_handle txn, ceph::libfdb::select key_range)
  -> std::generator<std::pair<std::string, ValueT>>
{
 auto decoded_pairs = ceph::libfdb::detail::generate_FDB_pairs(*txn, key_range)
                    | std::views::join
                    | std::views::transform(ceph::libfdb::detail::to_decoded_kv_pair<ValueT>);

 co_yield std::ranges::elements_of(decoded_pairs);
}

template <typename ValueT, typename BlockRangeT>
inline auto flatten_blocks(BlockRangeT block_range)
  -> std::generator<std::pair<std::string, ValueT>>
{
 for (auto block : block_range) {
  for (auto& pair : block) {
   co_yield std::move(pair);
  }
 }
}

} // namespace detail

// For ordinary range scans inside one explicit transaction, scan() is usually
// the right default:
template <typename ValueT = std::string,
          query::expression SelectionT>
inline auto scan(ceph::libfdb::transaction_handle txn, SelectionT selection)
  -> std::generator<std::pair<std::string, ValueT>>
{
 for (auto& interval : detail::intervals(selection)) {
  co_yield std::ranges::elements_of(
   detail::scan_selector<ValueT>(txn, std::move(interval)));
 }
}

// Compatibility name retained for existing callers:
template <typename ValueT = std::string,
          query::expression SelectionT>
inline auto pair_generator(ceph::libfdb::transaction_handle txn,
                           SelectionT selection)
  -> std::generator<std::pair<std::string, ValueT>>
{
 return scan<ValueT>(txn, std::move(selection));
}

struct page final
{
 static constexpr uint64_t max_size =
  static_cast<uint64_t>(std::numeric_limits<int>::max()) - 1;

 // FDB range limit convention: 0 means unlimited.
 uint64_t size = 0;

 constexpr page() noexcept = default;

 explicit constexpr page(uint64_t size_)
  : size(size_)
 {
  if (max_size < size) {
   throw libfdb_exception("page size exceeds FoundationDB range limit");
  }
 }
};

template <typename RowT>
struct page_result final
{
 std::vector<RowT> rows;
 bool has_more = false;

 auto begin() noexcept { return std::begin(rows); }
 auto begin() const noexcept { return std::begin(rows); }
 auto end() noexcept { return std::end(rows); }
 auto end() const noexcept { return std::end(rows); }

 bool empty() const noexcept
 {
  return std::empty(rows);
 }

 std::size_t size() const noexcept
 {
  return std::size(rows);
 }
};

namespace detail {

inline int range_limit_for(page p)
{
 if (0 == p.size) {
  return 0;
 }

 return static_cast<int>(p.size + 1);
}

template <typename ValueT>
using row_t = std::pair<std::string, ValueT>;

template <typename ValueT, typename FnT>
using row_transform_result_t =
 std::invoke_result_t<FnT&, row_t<ValueT>&&>;

template <typename FnT, typename ValueT>
concept row_invocable = std::invocable<FnT&, row_t<ValueT>&&>;

template <typename PredT, typename ValueT>
concept row_predicate = std::predicate<PredT&, const row_t<ValueT>&>;

} // namespace detail

template <typename ValueT = std::string, typename FnT, query::expression SelectionT>
requires detail::row_invocable<FnT, ValueT>
inline void for_each(ceph::libfdb::transaction_handle txn,
                     SelectionT selection,
                     FnT&& fn)
{
 for (auto&& row : scan<ValueT>(std::move(txn), std::move(selection))) {
  std::invoke(fn, std::move(row));
 }
}

// Database-handle functional helpers run inside the managed transaction loop.
// Keep callbacks replay-safe; use an explicit transaction for side effects that
// must not be repeated.
template <typename ValueT = std::string, typename FnT, query::expression SelectionT>
requires detail::row_invocable<FnT, ValueT>
inline void for_each(ceph::libfdb::database_handle dbh,
                     SelectionT selection,
                     FnT&& fn)
{
 detail::in_transaction(dbh,
  [selection = std::move(selection), fn = std::forward<FnT>(fn)](auto& txn) mutable {
   for_each<ValueT>(txn, selection, fn);
  });
}

template <typename ValueT = std::string,
          typename FnT,
          typename OutIterT,
          query::expression SelectionT>
requires detail::row_invocable<FnT, ValueT> &&
         concepts::storable_invocation_result<detail::row_transform_result_t<ValueT, FnT>> &&
         std::output_iterator<OutIterT, detail::row_transform_result_t<ValueT, FnT>>
inline OutIterT transform(ceph::libfdb::transaction_handle txn,
                          SelectionT selection,
                          FnT&& fn,
                          OutIterT out)
{
 for_each<ValueT>(std::move(txn), std::move(selection),
                  [&fn, &out](auto&& row) mutable {
                   *out++ = std::invoke(fn, std::move(row));
                  });

 return out;
}

template <typename ValueT = std::string, typename FnT, query::expression SelectionT>
requires detail::row_invocable<FnT, ValueT> &&
         concepts::storable_invocation_result<detail::row_transform_result_t<ValueT, FnT>>
[[nodiscard]] auto transform(ceph::libfdb::transaction_handle txn,
                             SelectionT selection,
                             FnT&& fn)
{
 using result_t =
  std::remove_cvref_t<detail::row_transform_result_t<ValueT, FnT>>;

 std::vector<result_t> out;
 transform<ValueT>(std::move(txn), std::move(selection),
                   std::forward<FnT>(fn), std::back_inserter(out));

 return out;
}

template <typename ValueT = std::string, typename FnT, query::expression SelectionT>
requires detail::row_invocable<FnT, ValueT> &&
         concepts::storable_invocation_result<detail::row_transform_result_t<ValueT, FnT>>
[[nodiscard]] auto transform(ceph::libfdb::database_handle dbh,
                             SelectionT selection,
                             FnT&& fn)
{
 return detail::in_transaction(dbh,
  [selection = std::move(selection), fn = std::forward<FnT>(fn)](auto& txn) mutable {
   return transform<ValueT>(txn, selection, fn);
  });
}

template <typename ValueT = std::string,
          typename FnT,
          typename OutIterT,
          query::expression SelectionT>
requires detail::row_invocable<FnT, ValueT> &&
         concepts::storable_invocation_result<detail::row_transform_result_t<ValueT, FnT>> &&
         std::output_iterator<OutIterT, detail::row_transform_result_t<ValueT, FnT>>
inline OutIterT transform(ceph::libfdb::database_handle dbh,
                          SelectionT selection,
                          FnT&& fn,
                          OutIterT out)
{
 auto transformed = transform<ValueT>(dbh, std::move(selection),
                                      std::forward<FnT>(fn));

 for (auto& value : transformed) {
  *out++ = std::move(value);
 }

 return out;
}

template <typename ValueT = std::string, typename PredT, query::expression SelectionT>
requires detail::row_predicate<PredT, ValueT>
inline std::size_t erase_if(ceph::libfdb::transaction_handle txn,
                            SelectionT selection,
                            PredT&& pred)
{
 std::size_t removed = 0;

 for (const auto& row : scan<ValueT>(txn, std::move(selection))) {
  if (std::invoke(pred, row)) {
   erase(txn, row.first);
   ++removed;
  }
 }

 return removed;
}

template <typename ValueT = std::string, typename PredT, query::expression SelectionT>
requires detail::row_predicate<PredT, ValueT>
inline std::size_t erase_if(ceph::libfdb::database_handle dbh,
                            SelectionT selection,
                            PredT&& pred)
{
 return detail::in_transaction(dbh,
  [selection = std::move(selection), pred = std::forward<PredT>(pred)](auto& txn) mutable {
   return erase_if<ValueT>(txn, selection, pred);
  });
}

template <std::ranges::input_range RangeT>
[[nodiscard]] auto collect(RangeT&& rows, page p)
{
 using row_type = std::ranges::range_value_t<RangeT>;

 page_result<row_type> out;
 if (p.size) {
  out.rows.reserve(p.size);
 }

 for (auto&& row : rows) {
  if (p.size && std::size(out.rows) == p.size) {
   out.has_more = true;
   break;
  }

  out.rows.emplace_back(std::forward<decltype(row)>(row));
 }

 return out;
}

template <typename ValueT = std::string>
[[nodiscard]] auto scan(ceph::libfdb::transaction_handle txn,
                        ceph::libfdb::select selector,
                        page p)
{
 using row_type = std::pair<std::string, ValueT>;

 if (0 == p.size) {
  return collect(scan<ValueT>(std::move(txn), std::move(selector)), p);
 }

 selector.options.result_limit = detail::range_limit_for(p);
 auto window = detail::read_query_window(*txn, selector, 1);

 page_result<row_type> out;
 out.rows.reserve(p.size);
 out.has_more = p.size < std::size(window.result_pairs) ||
                window.more_available;

 for (const auto& raw_pair : window.result_pairs | std::views::take(p.size)) {
  out.rows.emplace_back(detail::to_decoded_kv_pair<ValueT>(raw_pair));
 }

 return out;
}

template <typename ValueT = std::string>
[[nodiscard]] auto scan(ceph::libfdb::database_handle dbh,
                        ceph::libfdb::select selector,
                        page p)
{
 return make_transactor(dbh)([selector = std::move(selector), p](auto& txn) {
  return scan<ValueT>(txn, selector, p);
 });
}

// blocks() is for truly large scans that benefit from split planning:
// it trades direct streaming for block-at-a-time processing, bounded
// transaction windows, and lower risk of one transaction getting too old.
// Prefer scan(txn, ...) for ordinary caller-owned transaction scans.
namespace detail {

template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>>
auto blocks_selector(ceph::libfdb::database_handle dbh, ceph::libfdb::select selector)
 -> std::generator<AssocT>
{
 if (0 == selector.options.result_limit) {
  selector.options.result_limit = 4096;
 }

 // JFW: Although this is tunable, in my measurement it isn't a large factor so far-- likely 
 // these can move into the select instance itself (this is hard to measure in tests-- N
 // has to be pretty big; I've adjusted chunk_size to where I guesstimate it needs to be,
 // we need real-world experience to tweak this further):
 const auto chunk_size = 4 * 1024 * 1024;

 auto split_ranges = detail::plan_split_ranges(dbh, selector, chunk_size);

 auto read_blocks = [txr = make_transactor(dbh)](this auto& self, ceph::libfdb::select range, const int iteration)
 -> std::generator<AssocT> {
  auto read_result = txr([range, iteration](auto& txn) {
   return detail::materialize_query_window<ValueT, AssocT>(*txn, range, iteration);
  });

  auto next_range = std::move(read_result.next_range);

  if (read_result.result_block.empty()) {
   co_return;
  }

  co_yield std::move(read_result.result_block);

  if (next_range) {
   co_yield std::ranges::elements_of(self(std::move(*next_range), iteration + 1));
  }
 };

 auto expand_range = [&read_blocks](ceph::libfdb::select range) {
  return read_blocks(std::move(range), 1);
 };

 co_yield std::ranges::elements_of(split_ranges
                                 | std::views::transform(expand_range)
                                 | std::views::join);
}

} // namespace detail

template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>,
          query::expression SelectionT>
auto blocks(ceph::libfdb::database_handle dbh, SelectionT selection)
 -> std::generator<AssocT>
{
 for (auto& interval : detail::intervals(selection)) {
  co_yield std::ranges::elements_of(
   detail::blocks_selector<ValueT, AssocT>(dbh, std::move(interval)));
 }
}

// Compatibility name retained for existing callers:
template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>,
          query::expression SelectionT>
auto block_generator(ceph::libfdb::database_handle dbh, SelectionT selection)
 -> std::generator<AssocT>
{
 return blocks<ValueT, AssocT>(dbh, std::move(selection));
}

// Managed scans flatten the blocks() stream into key/value pairs.
template <typename ValueT = std::string,
          query::expression SelectionT>
inline auto scan(ceph::libfdb::database_handle dbh, SelectionT selection)
  -> std::generator<std::pair<std::string, ValueT>>
{
 return detail::flatten_blocks<ValueT>(blocks<ValueT>(dbh, std::move(selection)));
}

template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>,
          query::expression SelectionT>
inline AssocT collect(ceph::libfdb::transaction_handle txn, SelectionT selection)
{
 return detail::collect_range<AssocT>(scan<ValueT>(txn, std::move(selection)));
}

template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>,
          query::expression SelectionT>
inline AssocT collect(ceph::libfdb::database_handle dbh, SelectionT selection)
{
 return detail::collect_range<AssocT>(scan<ValueT>(dbh, std::move(selection)));
}

} // namespace ceph::libfdb

namespace ceph::libfdb::detail {

// Helper implementations:
inline fdb_error_t do_commit(transaction_handle& txn)
{
 if (fdb_error_t r = 0; !txn->commit(&r)) {
  return r;
 }

 return 0;
}

inline bool commit_or_throw(transaction_handle& txn)
{
 if (fdb_error_t r = do_commit(txn); 0 != r) {
  throw ceph::libfdb::libfdb_exception(r);
 }

 return true;
}

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

enum struct invocation_failure_policy { no_retry, retry };

struct no_invocation_result final {};

template <typename ResultT>
struct invocation_result_traits final {
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
struct invocation_result_traits<void> final {
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
using stored_invocation_result_t = typename invocation_result_traits<ResultT>::stored_t;

template <typename ResultT, typename FnT>
requires concepts::supported_invocation_result<ResultT>
auto store_invocation_result(transaction_handle& txn, FnT&& fn)
 -> stored_invocation_result_t<ResultT>
{
 return invocation_result_traits<ResultT>::store(txn, std::forward<FnT>(fn));
}

template <typename ResultT, typename StoredT>
requires std::is_void_v<ResultT> || concepts::storable_invocation_result<ResultT>
decltype(auto) invocation_value_from_result(std::optional<StoredT>&& result)
{
 return invocation_result_traits<ResultT>::take(std::move(result));
}

template <invocation_failure_policy FailurePolicy,
          typename FnT,
          typename CommitFnT,
          typename ResultT = std::invoke_result_t<FnT&, transaction_handle&>>
requires concepts::supported_invocation_result<ResultT>
auto attempt_invocation(transaction_handle& txn, FnT&& fn, CommitFnT&& commit_fn)
 -> std::optional<stored_invocation_result_t<ResultT>>
{
 using stored_result_t = stored_invocation_result_t<ResultT>;

 std::optional<stored_result_t> result;

 try {
     result.emplace(store_invocation_result<ResultT>(txn, fn));
 }
 catch (const libfdb_exception& e) {
     // Figure out how to recover from invocation failure:

     if constexpr (invocation_failure_policy::no_retry == FailurePolicy) {
      throw;
     }

     if (not e.retryable()) {
      throw;
     }

     retry_after_error(txn, e.fdb_error_value);
     return std::nullopt;
 }

 if (!std::invoke(commit_fn, txn)) {
  return std::nullopt;
 }

 return result;
}

template <invocation_failure_policy FailurePolicy,
          typename FnT,
          typename CommitFnT,
          typename ResultT = std::invoke_result_t<FnT&, transaction_handle&>>
requires concepts::supported_invocation_result<ResultT>
decltype(auto) invoke_with_retry(transaction_handle& txn, FnT&& fn, CommitFnT&& commit_fn)
{
 for (auto tries = 10; tries; --tries) {
  if (auto result = attempt_invocation<FailurePolicy>(txn, fn, commit_fn)) {
   return invocation_value_from_result<ResultT>(std::move(result));
  }
 }

 throw libfdb_exception("transaction retry limit exceeded");
}

template <supported_transaction_invocation FnT>
auto maybe_retry(transaction_handle txn, FnT&& fn) -> operation_result_t<FnT>
{
 return invoke_with_retry<invocation_failure_policy::retry>(
          txn, std::forward<FnT>(fn),
          [](transaction_handle& active_txn) {
            return ceph::libfdb::commit(active_txn);
          });
}

template <supported_transaction_invocation FnT>
auto in_transaction(database_handle dbh, FnT&& fn)
 -> operation_result_t<FnT>
{
 return maybe_retry(make_transaction(dbh), std::forward<FnT>(fn));
}

// Commit only once; the caller is responsible for transaction replay:
template <supported_transaction_invocation FnT>
auto commit_noreplay(transaction_handle txn, const commit_after_op commit_after, FnT&& fn)
 -> operation_result_t<FnT>
{
 if (commit_after_op::commit != commit_after) {
  return std::invoke(fn, txn);
 }

 return invoke_with_retry<invocation_failure_policy::no_retry>(
          txn, std::forward<FnT>(fn), commit_or_throw);
}

} // namespace ceph::libfdb::detail

#endif
