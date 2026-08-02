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
namespace ceph::libfdb {

/* This should be called when the application is all done with FoundationDB: */
inline void shutdown_libfdb()
{
 ceph::libfdb::detail::database_system::shutdown_fdb();
}

// By default, libfdb will in turn use FDB's own defaults (see FDB docs):
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
 return std::make_shared<database>(opts, network_options{});
}

inline database_handle create_database(const std::filesystem::path dbfile,
                                       const database_options& dbopts)
{
 return std::make_shared<database>(dbfile, dbopts, network_options{});
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
// Note: after a transaction is committed, it cannot be used again; but right now, that is NOT
// an error with respect to the object. So, don't do operations on the object after you've committed
// it or the behavior could be surprising.
// On false, the client should retry the transaction:
[[nodiscard]] inline bool commit(transaction_handle& txn)
{
 return txn->commit(); 
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

template <supported_transaction_invocation FnT>
auto maybe_retry(transaction_handle txn, FnT&& fn) -> operation_result_t<FnT>;

template <supported_transaction_invocation FnT>
auto maybe_commit(transaction_handle txn, const commit_after_op commit_after, FnT&& fn)
 -> operation_result_t<FnT>;

template <supported_transaction_invocation FnT>
auto commit_noreplay(transaction_handle txn, const commit_after_op commit_after, FnT&& fn)
 -> operation_result_t<FnT>;

template <supported_transaction_invocation FnT>
auto commit_in_new_transaction(database_handle dbh, FnT&& fn)
 -> operation_result_t<FnT>;

} // namespace ceph::libfdb::detail

namespace ceph::libfdb {

inline void set(transaction_handle txn,
                std::string_view k, const auto& v,
                const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [key = detail::as_fdb_span(k), &v](const transaction_handle& txn) {
            return txn->set(key, ceph::libfdb::to::convert(v));
          });
}

// If someone gives us an explicit transaction handle, they almost certainly don't want to commit 
// it (though they can always specify otherwise):
inline void set(transaction_handle txn, std::string_view k, const auto& v)
{
 return set(txn, k, v, commit_after_op::no_commit);
}

// ...conversely, with a database handle given, we can assume they DO want to auto-commit:
inline void set(database_handle dbh,
                std::string_view k, const auto& v)
{
 return detail::commit_in_new_transaction(dbh,
          [key = detail::as_fdb_span(k), &v](const transaction_handle& txn) {
            return txn->set(key, ceph::libfdb::to::convert(v));
          });
}

template <template <typename ...> typename AssocT = flat_map,
          concepts::key_value_iterator IteratorT>
inline void set(transaction_handle txn,
                IteratorT b, IteratorT e,
                const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [&b, &e](const transaction_handle& txn) {
            std::vector<std::uint8_t> fixed_buffer;

            std::ranges::for_each(std::ranges::subrange(b, e),
                      [&txn, &fixed_buffer](const auto& kv) {
                        detail::transaction_set_kv_bytes(txn,
                                  detail::as_fdb_span(kv.first),
                                  ceph::libfdb::to::convert(kv.second, fixed_buffer));
                      });
          });
}

template <template <typename ...> typename AssocT = flat_map,
          concepts::key_value_iterator IteratorT>
inline void set(database_handle dbh, IteratorT b, IteratorT e)
{
 return detail::commit_in_new_transaction(dbh,
          [b, e](const transaction_handle& txn) {
            return set(txn, b, e, commit_after_op::no_commit);
          });
}

// Note that we force things into a span so that byte streams get the proper encoding expected by zpp_bits:
inline void set(transaction_handle txn,
                std::string_view k, const ceph::libfdb::concepts::stringview_convertible auto& v,
                const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [key = detail::as_fdb_span(k), value = std::string_view(v)](const transaction_handle& txn) {
            return txn->set(key, ceph::libfdb::to::convert(value));
          });
}

inline void set(transaction_handle txn,
                std::string_view k, const ceph::libfdb::concepts::stringview_convertible auto& v)
{
 return set(txn, k, v, commit_after_op::no_commit);
}

inline void set(database_handle dbh,
                std::string_view k, const ceph::libfdb::concepts::stringview_convertible auto& v)
{
 return detail::commit_in_new_transaction(dbh,
          [key = detail::as_fdb_span(k), value = std::string_view(v)](const transaction_handle& txn) {
            return txn->set(key, ceph::libfdb::to::convert(value));
          });
}

} // namespace ceph::libfdb

namespace ceph::libfdb {

// erase() in libfdb is clear() in FDB parlance:
inline void erase(ceph::libfdb::transaction_handle txn,
                  const ceph::libfdb::select& key_range,
                  const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [&key_range](const transaction_handle& txn) {
            return txn->erase(key_range);
          });
}

inline void erase(ceph::libfdb::transaction_handle txn, const ceph::libfdb::select& key_range)
{
 return erase(txn, key_range, commit_after_op::no_commit);
}

inline void erase(ceph::libfdb::database_handle dbh,
                  const ceph::libfdb::select& key_range)
{
 return detail::commit_in_new_transaction(dbh,
          [&key_range](const transaction_handle& txn) {
            return txn->erase(key_range);
          });
}

inline void erase(ceph::libfdb::transaction_handle txn,
                  std::string_view k,
                  const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [key = detail::as_fdb_span(k)](const transaction_handle& txn) {
            return txn->erase(key);
          });
}

inline void erase(ceph::libfdb::transaction_handle txn, std::string_view k)
{
 return erase(txn, k, commit_after_op::no_commit);
}

inline void erase(ceph::libfdb::database_handle dbh, std::string_view k)
{
 return detail::commit_in_new_transaction(dbh,
          [key = detail::as_fdb_span(k)](const transaction_handle& txn) {
            return txn->erase(key);
          });
}

} // namespace ceph::libfdb

namespace ceph::libfdb {

// get() with selector:
// JFW: Satisfying output_iterator is not as straightforward as it appears, I need to look at this mechanism again; meanwhile, the template doesn't
// /prevent/ future type narrowing, but I'm forcing it to std::string for now:
inline bool get(ceph::libfdb::transaction_handle txn,
                const ceph::libfdb::select& key_range, auto out_iter,
                const ceph::libfdb::commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [&key_range, out_iter](const transaction_handle& txn) {
            return txn->get(key_range, out_iter);
          });
}

inline bool get(ceph::libfdb::transaction_handle txn,
                const ceph::libfdb::select& key_range, auto out_iter)
{ 
 return get(txn, key_range, out_iter, commit_after_op::no_commit);
}

inline bool get(ceph::libfdb::database_handle dbh,
                const ceph::libfdb::select& key_range, auto out_iter)
{
 return detail::maybe_retry(ceph::libfdb::make_transaction(dbh),
          [&key_range, out_iter](transaction_handle& txn) {
            return get(txn, key_range, out_iter, commit_after_op::no_commit);
          });
}

template <typename OutputTargetOrFnT>
requires concepts::value_callback<std::remove_reference_t<OutputTargetOrFnT>> ||
         concepts::value_output<OutputTargetOrFnT&&>
inline bool get(ceph::libfdb::transaction_handle txn,
                std::string_view key,
                OutputTargetOrFnT&& output_target_or_fn,
                const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [key = detail::as_fdb_span(key), &output_target_or_fn](const transaction_handle& txn) {
            if constexpr (concepts::value_callback<std::remove_reference_t<OutputTargetOrFnT>>) {
              return txn->get(key, output_target_or_fn);
            }

            if constexpr (concepts::value_output<OutputTargetOrFnT&&>) {
              return txn->get(key,
                              detail::value_collector(output_target_or_fn));
            }
          });
}

template <typename OutputTargetOrFnT>
requires concepts::value_callback<std::remove_reference_t<OutputTargetOrFnT>> ||
         concepts::value_output<OutputTargetOrFnT&&>
inline bool get(ceph::libfdb::transaction_handle txn,
                std::string_view key, OutputTargetOrFnT&& output_target_or_fn)
{
 return get(txn, key, std::forward<OutputTargetOrFnT>(output_target_or_fn), commit_after_op::no_commit);
}

template <typename OutputTargetOrFnT>
requires concepts::value_callback<std::remove_reference_t<OutputTargetOrFnT>> ||
         concepts::value_output<OutputTargetOrFnT&&>
inline bool get(ceph::libfdb::database_handle dbh,
                std::string_view key, OutputTargetOrFnT&& output_target_or_fn)
{
 return detail::maybe_retry(ceph::libfdb::make_transaction(dbh),
          [key, &output_target_or_fn](transaction_handle& txn) {
            return get(txn, key, output_target_or_fn, commit_after_op::no_commit);
          });
}

} // namespace ceph::libfdb

namespace ceph::libfdb {

// Does a key exist?
inline bool key_exists(transaction_handle txn,
                       std::string_view k,
                       const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [k](const transaction_handle& txn) {
            return txn->key_exists(k);
          });
}

inline bool key_exists(transaction_handle txn, std::string_view k)
{
 return key_exists(txn, k, commit_after_op::no_commit);
}

inline bool key_exists(database_handle dbh, std::string_view k)
{
 return detail::maybe_retry(ceph::libfdb::make_transaction(dbh),
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

// Basic Generators:
namespace ceph::libfdb {

// For ordinary range scans inside one explicit transaction, pair_generator() is
// usually the right default:
template <typename ValueT = std::string>
inline auto pair_generator(ceph::libfdb::transaction_handle txn, ceph::libfdb::select key_range) 
  -> std::generator<std::pair<std::string, ValueT>>
{
 auto decoded_pairs = ceph::libfdb::detail::generate_FDB_pairs(*txn, key_range)
                    | std::views::join
                    | std::views::transform(ceph::libfdb::detail::to_decoded_kv_pair<ValueT>);

 co_yield std::ranges::elements_of(decoded_pairs);
}

// Note: block_generator() uses split planning to tackle large sets; use pair_generator() for
// direct scans.
//
// What block_generator() gives you:
// - avoids one huge transaction getting too old
// - gives caller block-at-a-time processing
// - can bound memory and transaction duration better than a monolithic scan
//
// Note: block_generator() was originally parallel, and could be again, but preliminary benchmarking
// showed it to be a significant performance impediment. The database must be truly large to see benefits.
//
// Note: This is meant to be straightforward and easy-to-understand-- hence, there's not
// a recovery strategy or other things (you can replay the entire query)-- as new needs arise, this
// can be made more flexible via selector options, dynamic range-splitting, etc., but so far there
// has been no need:
template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>>
auto block_generator(ceph::libfdb::database_handle dbh, ceph::libfdb::select selector)
-> std::generator<AssocT>
{
 if (0 == selector.options.stride) {
  selector.options.stride = 4096;
 }

 // JFW: Although this is tunable, in my measurement it isn't a large factor so far-- likely 
 // these can move into the select instance itself (this is hard to measure in tests-- N
 // has to be pretty big; I've adjusted chunk_size to where I guesstimate it needs to be,
 // we need real-world experience to tweak this further):
 const auto chunk_size = 4 * 1024 * 1024;

 auto split_ranges = detail::plan_split_ranges(dbh, selector, chunk_size);

 auto read_blocks = [txr = make_transactor(dbh)](this auto& read_blocks, ceph::libfdb::select range, const int iteration)
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
   co_yield std::ranges::elements_of(read_blocks(std::move(*next_range), 2));
  }
 };

 auto expand_range = [&read_blocks](ceph::libfdb::select range) {
  return read_blocks(std::move(range), 1);
 };

 co_yield std::ranges::elements_of(split_ranges
                                 | std::views::transform(expand_range)
                                 | std::views::join);
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
using stored_invocation_result_t =
 std::conditional_t<std::is_void_v<ResultT>,
                    no_invocation_result,
                    std::remove_cvref_t<ResultT>>;

template <typename ResultT, typename FnT>
requires concepts::supported_invocation_result<ResultT>
auto store_invocation_result(transaction_handle& txn, FnT&& fn)
 -> stored_invocation_result_t<ResultT>
{
 if constexpr (std::is_void_v<ResultT>) {
  return (std::invoke(fn, txn), stored_invocation_result_t<ResultT>{});
 }

 if constexpr (not std::is_void_v<ResultT>) {
  return std::invoke(fn, txn);
 }
}

template <typename ResultT, typename StoredT>
requires std::is_void_v<ResultT>
void invocation_value_from_result(std::optional<StoredT>&&)
{}

template <typename ResultT, typename StoredT>
requires concepts::storable_invocation_result<ResultT>
auto invocation_value_from_result(std::optional<StoredT>&& result)
{
 return *std::move(result);
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
          [](transaction_handle& txn) {
            return ceph::libfdb::commit(txn);
          });
}

// Sadly, a RAII wrapper can't safely capture all of the failure conditions that can occur during an 
// FDB commit without throwing from its dtor; but, we generally work around that by wrapping as much 
// into function objects, simplifying the situation about as much as I can think of.
// Commit operation, possibly replay if FDB requests it:
template <supported_transaction_invocation FnT>
auto maybe_commit(transaction_handle txn, const commit_after_op commit_after, FnT&& fn)
 -> operation_result_t<FnT>
{
 if (commit_after_op::commit != commit_after) {
  return std::invoke(fn, txn);
 }

 return invoke_with_retry<invocation_failure_policy::no_retry>(
          txn, std::forward<FnT>(fn),
          [](transaction_handle& txn) {
            return ceph::libfdb::commit(txn);
	          });
}

template <supported_transaction_invocation FnT>
auto commit_in_new_transaction(database_handle dbh, FnT&& fn)
 -> operation_result_t<FnT>
{
 return maybe_commit(make_transaction(dbh),
                     commit_after_op::commit,
                     std::forward<FnT>(fn));
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
