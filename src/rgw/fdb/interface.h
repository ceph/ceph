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

#include <tuple>

namespace ceph::libfdb {

// Overload tag for transactors that should report replay/commit metadata:
struct with_result_t final {};
inline constexpr with_result_t with_result;

// Describes replay/commit work performed by result-reporting transactors.
struct transaction_result final {
 bool committed = false;
 std::size_t attempts = 0;

 // Replays prepared after retryable failures; excludes a final exhausted attempt:
 std::size_t replay_count = 0;

 fdb_error_t last_error = 0;
};

// Describes a commit attempt when the caller owns transaction replay.
struct commit_result final {
 bool committed = false;
 fdb_error_t replay_error = 0;
};

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
 if (!dbh) {
  throw std::invalid_argument("make_transaction() requires database handle");
 }

 return std::make_shared<transaction>(dbh);
}

inline transaction_handle make_transaction(database_handle dbh, const transaction_options& opts)
{
 return std::make_shared<transaction>(dbh, opts);
}

// Note: only rarely is a direct call to this needed. You can use transactors or pass database_handles
// to get automagic.
// Note: after a transaction is committed, it cannot be used again.
// On false, the client should retry the transaction:
[[nodiscard]] inline bool commit(transaction_handle& txn)
{
 return txn->commit();
}

// Prepare a transaction to replay after a retryable operation-body error:
void prepare_replay(transaction_handle& txn, fdb_error_t error);

[[nodiscard]] commit_result commit(with_result_t, transaction_handle& txn);

[[nodiscard]] inline bool commit(transaction_handle& txn,
                                 const versionstamp& stamp)
{
 txn->mark_version(stamp);
 return commit(txn);
}

[[nodiscard]] inline watch_handle make_watch(transaction_handle txn, std::string_view key)
{
 return txn->make_watch(detail::as_fdb_span(key));
}

} // namespace ceph::libfdb

namespace ceph::libfdb::detail {

// Forward declarations:
template <typename FnT, typename ...ArgTs>
using transaction_invocation_result_t =
 std::invoke_result_t<FnT&, transaction_handle&, ArgTs&...>;

template <typename FnT, typename ...ArgTs>
concept transaction_op =
 std::invocable<FnT&, transaction_handle&, ArgTs&...> &&
 concepts::supported_invocation_result<transaction_invocation_result_t<FnT, ArgTs...>>;

template <typename FnT, typename ...ArgTs>
concept result_reporting_transaction_op =
 transaction_op<FnT, ArgTs...> &&
 std::is_void_v<transaction_invocation_result_t<FnT, ArgTs...>>;

template <typename FnT, typename ...ArgTs>
concept bound_transaction_op =
 std::constructible_from<std::decay_t<FnT>, FnT> &&
 (std::constructible_from<std::decay_t<ArgTs>, ArgTs> && ...) &&
 transaction_op<std::decay_t<FnT>, std::decay_t<ArgTs>...>;

template <typename FnT>
using operation_result_t =
 std::conditional_t<std::is_void_v<transaction_invocation_result_t<FnT>>,
                    void,
                    std::remove_cvref_t<transaction_invocation_result_t<FnT>>>;

template <result_reporting_transaction_op FnT>
transaction_result maybe_retry_with_result(transaction_handle txn, FnT&& fn);

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

template <transaction_op FnT>
auto maybe_retry(transaction_handle txn, FnT&& fn) -> operation_result_t<FnT>;

template <transaction_op FnT>
auto commit_noreplay(transaction_handle txn, const commit_after_op commit_after, FnT&& fn)
 -> operation_result_t<FnT>;

template <transaction_op FnT>
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

 if constexpr (requires { out.merge(tmp); }) {
  out.merge(tmp);
  return;
 }

 ceph::util::append_range(out, move_range(tmp));
}

template <query::expression SelectionT, typename OutT>
requires concepts::string_pair_output_iterator<OutT> ||
         concepts::string_pair_output_range<OutT>
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
 * returns, replays when FoundationDB requests replay, and throws when recovery
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

 // Bind the callable and arguments once so replays see stable state:
 template <typename FnT, typename ...ArgTs>
 static auto bind_invocation(FnT&& fn, ArgTs&& ...args)
 {
  return [frame = std::tuple<std::decay_t<FnT>, std::decay_t<ArgTs>...> {
            std::forward<FnT>(fn), std::forward<ArgTs>(args)...
          }](transaction_handle& txn) mutable -> decltype(auto) {
    return std::apply([&txn](auto& active_fn, auto& ...active_args) -> decltype(auto) {
      return std::invoke(active_fn, txn, active_args...);
    }, frame);
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
  return detail::maybe_retry(make_transaction_for_call(), std::forward<FnT>(fn));
 }

 template <typename FnT, typename ...ArgTs>
 requires (sizeof...(ArgTs) > 0 && detail::bound_transaction_op<FnT, ArgTs...>)
 decltype(auto) operator()(FnT&& fn, ArgTs&& ...args) const
 {
  auto bound = bind_invocation(std::forward<FnT>(fn), std::forward<ArgTs>(args)...);

  return (*this)(bound);
 }

 template <typename FnT>
 requires detail::result_reporting_transaction_op<FnT>
 transaction_result operator()(with_result_t, FnT&& fn) const
 {
  return detail::maybe_retry_with_result(make_transaction_for_call(), std::forward<FnT>(fn));
 }

 template <typename FnT, typename ...ArgTs>
 requires (sizeof...(ArgTs) > 0 &&
           detail::bound_transaction_op<FnT, ArgTs...> &&
           detail::result_reporting_transaction_op<
             std::decay_t<FnT>, std::decay_t<ArgTs>...>)
 transaction_result operator()(with_result_t, FnT&& fn, ArgTs&& ...args) const
 {
  auto bound = bind_invocation(std::forward<FnT>(fn), std::forward<ArgTs>(args)...);

  return (*this)(with_result, bound);
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
 AssocT out;
 std::ranges::copy(std::forward<RangeT>(range),
                   std::inserter(out, std::end(out)));
 return out;
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

// Legacy name retained for compatibility:
template <typename ValueT = std::string,
          query::expression SelectionT>
inline auto pair_generator(ceph::libfdb::transaction_handle txn,
                           SelectionT selection)
  -> std::generator<std::pair<std::string, ValueT>>
{
 return scan<ValueT>(txn, std::move(selection));
}

// Note: blocks() uses split planning to tackle large sets; use scan(txn, ...)
// for direct scans in a caller-owned transaction.
//
// What blocks() gives you:
// - avoids one huge transaction getting too old
// - gives caller block-at-a-time processing
// - can bound memory and transaction duration better than a monolithic scan
//
// Note: blocks() was originally parallel, and could be again, but preliminary
// benchmarking showed it to be a significant performance impediment. The
// database must be truly large to see benefits.
//
// Note: This is meant to be straightforward and easy-to-understand-- hence, there's not
// a recovery strategy or other things (you can replay the entire query)-- as new needs arise, this
// can be made more flexible via selector options, dynamic range-splitting, etc., but so far there
// has been no need:
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
  auto read_result = txr([](auto& txn, ceph::libfdb::select range, const int iteration) {
   return detail::materialize_query_window<ValueT, AssocT>(*txn, std::move(range), iteration);
  }, std::move(range), iteration);

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

// Legacy name retained for compatibility:
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

} // namespace ceph::libfdb::detail

namespace ceph::libfdb {

inline void prepare_replay(transaction_handle& txn, const fdb_error_t error)
{
 if (not detail::retry_after_error(txn, error)) {
  throw libfdb_exception(error);
 }
}

[[nodiscard]] inline commit_result commit(with_result_t, transaction_handle& txn)
{
 fdb_error_t replay_error = 0;
 const auto committed = txn->commit(&replay_error);

 return {
  .committed = committed,
  .replay_error = replay_error,
 };
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

enum struct invocation_failure_policy { no_retry, retry };

struct no_invocation_result final {};

// Keep the existing transactor retry behavior in one place.
constexpr std::size_t transaction_retry_attempts = 10;

inline void record_transaction_replay(transaction_result& result,
                                      const fdb_error_t r,
                                      const bool can_replay)
{
 result.last_error = r;

 if (can_replay) {
  ++result.replay_count;
 }
}

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
auto store_invocation_result(transaction_handle& txn, FnT&& fn)
 -> stored_invocation_result_t<ResultT>
{
 return invocation_result_traits<ResultT>::store(txn, std::forward<FnT>(fn));
}

template <typename ResultT, typename StoredT>
decltype(auto) invocation_value_from_result(std::optional<StoredT>&& result)
{
 return invocation_result_traits<ResultT>::take(std::move(result));
}

template <invocation_failure_policy FailurePolicy,
          typename FnT,
          typename CommitFnT,
          typename ResultT = std::invoke_result_t<FnT&, transaction_handle&>>
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

  prepare_replay(txn, e.fdb_error_value);
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
decltype(auto) invoke_with_retry(transaction_handle& txn, FnT&& fn, CommitFnT&& commit_fn)
{
 for (auto tries = transaction_retry_attempts; tries; --tries) {
  if (auto result = attempt_invocation<FailurePolicy>(txn, fn, commit_fn)) {
   return invocation_value_from_result<ResultT>(std::move(result));
  }
 }

 throw libfdb_exception("transaction retry limit exceeded");
}

template <transaction_op FnT>
auto maybe_retry(transaction_handle txn, FnT&& fn) -> operation_result_t<FnT>
{
 return invoke_with_retry<invocation_failure_policy::retry>(
          txn, std::forward<FnT>(fn),
          [](transaction_handle& active_txn) {
            return ceph::libfdb::commit(active_txn);
          });
}

template <result_reporting_transaction_op FnT>
transaction_result maybe_retry_with_result(transaction_handle txn, FnT&& fn)
{
 transaction_result result;

 for (auto attempts_left = transaction_retry_attempts; attempts_left; --attempts_left) {
  ++result.attempts;

  try {
   std::invoke(fn, txn);
  } catch (const libfdb_exception& e) {
   if (not e.retryable()) {
    throw;
   }

   prepare_replay(txn, e.fdb_error_value);
   record_transaction_replay(result, e.fdb_error_value, 1 < attempts_left);

   continue;
  }

  const auto commit_state = commit(with_result, txn);
  if (not commit_state.committed and 0 == commit_state.replay_error) {
   throw libfdb_exception("transactor commit did not start");
  }

  if (not commit_state.committed) {
   record_transaction_replay(result, commit_state.replay_error, 1 < attempts_left);

   continue;
  }

  result.committed = true;
  return result;
 }

 return result;
}

template <transaction_op FnT>
auto in_transaction(database_handle dbh, FnT&& fn)
 -> operation_result_t<FnT>
{
 return maybe_retry(make_transaction(dbh), std::forward<FnT>(fn));
}

// Commit only once; the caller is responsible for transaction replay:
template <transaction_op FnT>
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
