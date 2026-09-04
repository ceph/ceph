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

#ifndef CEPH_FDB_SCAN_H
#define CEPH_FDB_SCAN_H

#include "conversion.h"
#include "transaction.h"

#include "common/container_concepts.h"

#include <span>
#include <string>
#include <vector>
#include <optional>
#include <string_view>
#include <initializer_list>

#include <ranges>
#include <iterator>
#include <generator>
#include <algorithm>

#include <cstddef>
#include <cstdint>
#include <utility>
#include <concepts>
#include <functional>
#include <type_traits>

namespace ceph::libfdb {

namespace concepts {

template <typename IteratorT>
concept string_pair_output_iterator =
 std::output_iterator<IteratorT, std::pair<std::string, std::string>>;

template <typename RangeT>
concept string_pair_output_range =
 not std::is_array_v<std::remove_reference_t<RangeT>> and
 std::ranges::range<RangeT> and
 ceph::concepts::can_append<RangeT, std::pair<std::string, std::string>>;

template <typename RangeT>
concept materializable_string_pair_output_range =
 string_pair_output_range<RangeT> and
 std::default_initializable<std::remove_cvref_t<RangeT>> and
 std::move_constructible<std::remove_cvref_t<RangeT>>;

} // namespace concepts

namespace detail {

// Owns an FDB range result while exposing the returned key/value array:
struct query_window final
{
 future_value result_owner;

 std::span<const FDBKeyValue> result_pairs;

 bool more_available = false;
};

// Owns an FDB split-point result while exposing the returned keys:
struct split_point_result final
{
 future_value result_owner;

 std::span<const FDBKey> result_keys;

 fdb_error_t error = 0;
};

inline query_window extract_result_pairs(future_value result_owner)
{
 fdb_bool_t more_available = false;
 int out_count = 0;
 const FDBKeyValue *out_kvs = nullptr;

 if (fdb_error_t r =
       fdb_future_get_keyvalue_array(result_owner.raw_ptr_or_throw(),
                                     &out_kvs,
                                     &out_count,
                                     &more_available);
     0 != r) {
  throw libfdb_exception(r);
 }

 return query_window {
  .result_owner = std::move(result_owner),
  .result_pairs = std::span<const FDBKeyValue>(
    out_kvs, checked_result_size(out_count)),
  .more_available = 0 != more_available
 };
}

inline split_point_result extract_split_points(future_value result_owner)
{
 const FDBKey *result_keys = nullptr;
 int result_count = 0;

 const auto error = fdb_future_get_key_array(
   result_owner.raw_ptr_or_throw(), &result_keys, &result_count);

 return split_point_result {
  .result_owner = std::move(result_owner),
  .result_keys = 0 == error
    ? std::span<const FDBKey>(result_keys, checked_result_size(result_count))
    : std::span<const FDBKey>(),
  .error = error
 };
}

/* FoundationDB range reads are stateless requests: callers must advance their
 * selectors and iteration number between requests. See
 * validate_and_update_parameters() in FoundationDB's fdb_c.cpp for the exact
 * selector interpretation: */
inline future_value get_range_future_from_transaction(
  transaction& txn,
  const select& selection,
  const int iteration)
{
 const auto& options = selection.options;
 const auto begin = as_fdb_bytes(selection.begin_key);
 const auto end = as_fdb_bytes(selection.end_key);

 const bool continuing_forward = !options.reverse_order && 1 < iteration;
 const bool continuing_reverse = options.reverse_order && 1 < iteration;

 const fdb_bool_t begin_or_eq = continuing_forward || !selection.begin_inclusive;
 const int begin_offset = 1;
 const fdb_bool_t end_or_eq = !continuing_reverse && selection.end_inclusive;
 const int end_offset = 1;
 const fdb_bool_t is_snapshot = false;

 // Hold your breath-- this call is a bit of a Swiss army knife!
 // It really helps to see the fdb_c reference, some of these are gnarly.
 return future_value(fdb_transaction_get_range(
   txn.raw_handle(),
   begin.data,
   begin.length,
   begin_or_eq,
   begin_offset,
   end.data,
   end.length,
   end_or_eq,
   end_offset,
   options.result_limit,
   options.target_bytes,
   options.streaming_mode,
   iteration,
   is_snapshot,
   options.reverse_order));
}

inline query_window read_query_window(transaction& txn,
                                      const select& key_range,
                                      const int iteration)
{
 return extract_result_pairs(await_future_of([&] {
  return get_range_future_from_transaction(txn, key_range, iteration);
 }));
}

inline std::optional<select> next_range_after(select key_range,
                                              const query_window& window)
{
 if (not window.more_available || window.result_pairs.empty()) {
  return std::nullopt;
 }

 const auto& last_key = window.result_pairs.back();
 const auto cursor = key_view(last_key);

 if (key_range.options.reverse_order) {
  key_range.end_key = cursor;
  key_range.end_inclusive = false;
  return key_range;
 }

 key_range.begin_key = cursor;
 key_range.begin_inclusive = false;

 return key_range;
}

/* Returned spans remain valid only while the coroutine retains the owning FDB
 * future. Consumers must copy their contents before advancing the generator: */
inline auto generate_FDB_pairs(transaction& txn, select key_range)
 -> std::generator<std::span<const FDBKeyValue>>
{
 int iteration = 1;

 for (auto more_available = true; more_available; ++iteration) {
  auto window = read_query_window(txn, key_range, iteration);
  auto next_range = next_range_after(key_range, window);

  more_available = next_range.has_value();

  co_yield window.result_pairs;

  if (next_range) {
   key_range = std::move(*next_range);
  }
 }
}

template <typename ValueT = std::string>
inline auto decode_pairs(std::span<const FDBKeyValue> pairs)
{
 return pairs | std::views::transform(to_decoded_kv_pair<ValueT>);
}

template <typename ValueT, typename AssocT>
inline AssocT collect_pairs(std::span<const FDBKeyValue> pairs)
{
 return ceph::util::collect_as<AssocT>(decode_pairs<ValueT>(pairs));
}

template <typename AssocT>
struct query_window_result final
{
 AssocT result_block;
 std::optional<select> next_range;
};

template <typename ValueT, typename AssocT>
inline auto materialize_query_window(transaction& txn,
                                     select key_range,
                                     const int iteration = 1)
 -> query_window_result<AssocT>
{
 auto window = read_query_window(txn, key_range, iteration);

 return {
  .result_block = collect_pairs<ValueT, AssocT>(window.result_pairs),
  .next_range = next_range_after(std::move(key_range), window)
 };
}

inline std::size_t for_each_decoded_kv_pair(transaction& txn,
                                            const select& key_range,
                                            auto&& fn)
{
 std::size_t nread = 0;

 for (const auto& kv : generate_FDB_pairs(txn, key_range)
                     | std::views::join) {
  std::invoke(fn, to_decoded_kv_pair<std::string>(kv));
  ++nread;
 }

 return nread;
}

template <typename OutIterT>
requires std::output_iterator<OutIterT,
                              std::pair<std::string, std::string>>
inline std::size_t get_value_range_from_transaction(
  transaction& txn,
  const select& key_range,
  OutIterT& out_iter)
{
 return for_each_decoded_kv_pair(
   txn, key_range,
   [&out_iter](auto&& kv) {
     *out_iter++ = std::forward<decltype(kv)>(kv);
   });
}

inline std::size_t get_value_range_from_transaction(
  transaction& txn,
  const select& key_range,
  concepts::string_pair_output_range auto& out)
{
 return for_each_decoded_kv_pair(
   txn, key_range,
   [&out](auto&& kv) {
     ceph::util::push_back(out, std::forward<decltype(kv)>(kv));
   });
}

inline std::vector<select> select_ranges_from_split_points(
  std::span<const FDBKey> keys,
  const select& parent)
{
 if (2 > keys.size()) {
  return {};
 }

 // Gather the flattened list into overlapping libfdb::select pairs:
 return ceph::util::collect_as<std::vector<select>>(
   std::views::iota(std::size_t {0}, keys.size() - 1)
   | std::views::transform([&parent, keys](const auto i) {
       const auto& first = keys[i];
       const auto& second = keys[1 + i];

       const auto first_key = key_view(first);
       const auto second_key = key_view(second);

       select split(first_key, second_key);

       split.options = parent.options;

       split.begin_inclusive = 0 == i ? parent.begin_inclusive : true;
       split.end_inclusive = 2 + i == keys.size()
        ? parent.end_inclusive
        : false;

       return split;
     }));
}

inline std::vector<select> plan_split_ranges(
  database_handle dbh,
  select selector,
  const std::int64_t remote_chunk_size)
{
 auto split_selector = as_half_open_select(selector);

 return retry_without_commit(
  make_transaction(std::move(dbh)),
  [split_selector = std::move(split_selector), remote_chunk_size](
    transaction_handle& txn) {
    const auto begin = as_fdb_bytes(split_selector.begin_key);
    const auto end = as_fdb_bytes(split_selector.end_key);

    auto result_owner = wait_until_ready(future_value(
     fdb_transaction_get_range_split_points(
      txn->raw_handle(),
      begin.data,
      begin.length,
      end.data,
      end.length,
      remote_chunk_size)));

    auto split_points = extract_split_points(std::move(result_owner));

    if (0 != split_points.error) {
     throw libfdb_exception(split_points.error);
    }

    return select_ranges_from_split_points(
      split_points.result_keys, split_selector);
  });
}

inline select select_from_initializer_list(
  std::initializer_list<std::string_view> keys)
{
 const auto first = std::begin(keys);

 if (1 == std::size(keys)) {
  return select(*first);
 }

 if (2 == std::size(keys)) {
  return select(*first, *std::next(first));
 }

 // You might except that std::invalid_argument should be thrown... and you would
 // be correct. However, we also don't want to make callers catch a zillion different
 // exceptions:
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
inline std::size_t get_value_selection_from_transaction(
  transaction& txn,
  const SelectionT& selection,
  OutT& out)
{
 std::size_t nread = 0;

 query::for_each_interval(selection,
   [&](const select& interval) {
     nread += get_value_range_from_transaction(txn, interval, out);
   });

 return nread;
}

template <typename OutT, query::expression SelectionT>
requires concepts::materializable_string_pair_output_range<OutT>
inline auto materialize_string_pair_selection(
  transaction& txn,
  const SelectionT& selection) -> materialized_string_pair_output<OutT>
{
 materialized_string_pair_output<OutT> result;
 result.nread = get_value_selection_from_transaction(txn, selection, result.values);

 return result;
}

} // namespace detail

// get() with a selector writes key/value pairs to its output and returns the
// number of pairs emitted:
inline std::size_t get(transaction_handle txn,
                       const query::expression auto& selection,
                       concepts::string_pair_output_iterator auto out_iter,
                       const commit_after_op commit_after)
{
 return detail::commit_noreplay(
   txn, commit_after,
   [&selection, out_iter](const transaction_handle& active_txn) mutable {
     return detail::get_value_selection_from_transaction(
       *active_txn, selection, out_iter);
   });
}

inline std::size_t get(transaction_handle txn,
                       const query::expression auto& selection,
                       concepts::string_pair_output_iterator auto out_iter)
{
 return get(txn, selection, out_iter, commit_after_op::no_commit);
}

inline std::size_t get(database_handle dbh,
                       const query::expression auto& selection,
                       concepts::string_pair_output_iterator auto out_iter)
{
 auto result = detail::in_transaction(
   std::move(dbh),
   [&selection](transaction_handle& txn) {
     using out_t = std::vector<std::pair<std::string, std::string>>;

     return detail::materialize_string_pair_selection<out_t>(
       *txn, selection);
   });

 std::ranges::move(result.values, out_iter);

 return result.nread;
}

inline std::size_t get(transaction_handle txn,
                       const query::expression auto& selection,
                       concepts::string_pair_output_range auto& out,
                       const commit_after_op commit_after)
{
 return detail::commit_noreplay(
   txn, commit_after,
   [&selection, &out](const transaction_handle& active_txn) {
     return detail::get_value_selection_from_transaction(
       *active_txn, selection, out);
   });
}

inline std::size_t get(transaction_handle txn,
                       const query::expression auto& selection,
                       concepts::string_pair_output_range auto& out)
{
 return get(txn, selection, out, commit_after_op::no_commit);
}

inline std::size_t get(database_handle dbh,
                       const query::expression auto& selection,
                       concepts::materializable_string_pair_output_range auto& out)
{
 using out_t = std::remove_cvref_t<decltype(out)>;

 auto result = detail::in_transaction(
   std::move(dbh),
   [&selection](transaction_handle& txn) {
     return detail::materialize_string_pair_selection<out_t>(
       *txn, selection);
   });

 detail::publish_string_pair_results(out, std::move(result.values));

 return result.nread;
}

inline std::size_t get(transaction_handle txn,
                       std::initializer_list<std::string_view> keys,
                       concepts::string_pair_output_range auto& out,
                       const commit_after_op commit_after)
{
 return get(txn, detail::select_from_initializer_list(keys), out,
            commit_after);
}

inline std::size_t get(transaction_handle txn,
                       std::initializer_list<std::string_view> keys,
                       concepts::string_pair_output_range auto& out)
{
 return get(txn, keys, out, commit_after_op::no_commit);
}

inline std::size_t get(database_handle dbh,
                       std::initializer_list<std::string_view> keys,
                       concepts::materializable_string_pair_output_range auto& out)
{
 return get(std::move(dbh), detail::select_from_initializer_list(keys), out);
}

namespace detail {

inline auto intervals(select selection)
{
 // Raw selectors still execute in the ordinary FDB keyspace:
 return std::views::single(
          query::intersection(std::move(selection), query::universal()))
      | std::views::filter([](const select& range) {
          return not query::is_empty(range);
        });
}

template <query::non_interval_expression QueryT>
inline auto intervals(const QueryT& query)
{
 return query::compile_intervals(query);
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
inline auto scan_selector(transaction_handle txn, select key_range)
 -> std::generator<std::pair<std::string, ValueT>>
{
 auto decoded_pairs = generate_FDB_pairs(*txn, std::move(key_range))
                    | std::views::join
                    | std::views::transform(to_decoded_kv_pair<ValueT>);

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

template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>>
auto blocks_selector(database_handle dbh, select selector)
 -> std::generator<AssocT>
{
 if (0 == selector.options.result_limit) {
  selector.options.result_limit = 4096;
 }

 /* Although this is tunable, early measurements show that it is not yet a
  * large factor. Real-world experience should guide further adjustment: */
 constexpr auto chunk_size = 4 * 1024 * 1024;

 auto split_ranges = plan_split_ranges(dbh, selector, chunk_size);
 auto txr = make_transactor(dbh);

 for (auto split_range : split_ranges) {
  for (int page = 1;; ++page) {
   auto read_result = txr(
    [](auto& txn, select range, const int iteration) {
      return materialize_query_window<ValueT, AssocT>(
        *txn, std::move(range), iteration);
    }, std::move(split_range), page);

   auto next_range = std::move(read_result.next_range);

   if (read_result.result_block.empty()) {
    break;
   }

   co_yield std::move(read_result.result_block);

   if (not next_range) {
    break;
   }

   split_range = std::move(*next_range);
  }
 }
}

} // namespace detail

/* blocks() uses split planning to tackle large sets; use scan(txn, ...) for
 * direct scans in a caller-owned transaction.
 *
 * What blocks() provides:
 * - avoids one large transaction getting too old
 * - gives the caller block-at-a-time processing
 * - bounds memory and transaction duration more readily than a monolithic scan
 *
 * blocks() was originally parallel, and could be again, but preliminary
 * benchmarking showed a significant performance penalty. The database must be
 * truly large before parallelism is expected to help. */
template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>,
          query::expression SelectionT>
auto blocks(database_handle dbh, SelectionT selection)
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
auto block_generator(database_handle dbh, SelectionT selection)
 -> std::generator<AssocT>
{
 return blocks<ValueT, AssocT>(std::move(dbh), std::move(selection));
}

// For ordinary range scans inside one explicit transaction, scan() is usually
// the right default:
template <typename ValueT = std::string,
          query::expression SelectionT>
inline auto scan(transaction_handle txn, SelectionT selection)
 -> std::generator<std::pair<std::string, ValueT>>
{
 for (auto& interval : detail::intervals(selection)) {
  co_yield std::ranges::elements_of(
    detail::scan_selector<ValueT>(txn, std::move(interval)));
 }
}

// Managed scans flatten the blocks() stream into key/value pairs.
template <typename ValueT = std::string,
          query::expression SelectionT>
inline auto scan(database_handle dbh, SelectionT selection)
 -> std::generator<std::pair<std::string, ValueT>>
{
 return detail::flatten_blocks<ValueT>(
   blocks<ValueT>(std::move(dbh), std::move(selection)));
}

// Legacy name retained for compatibility:
template <typename ValueT = std::string,
          query::expression SelectionT>
inline auto pair_generator(transaction_handle txn, SelectionT selection)
 -> std::generator<std::pair<std::string, ValueT>>
{
 return scan<ValueT>(txn, std::move(selection));
}

template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>,
          query::expression SelectionT>
inline AssocT collect(transaction_handle txn, SelectionT selection)
{
 return detail::collect_range<AssocT>(
   scan<ValueT>(txn, std::move(selection)));
}

template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>,
          query::expression SelectionT>
inline AssocT collect(database_handle dbh, SelectionT selection)
{
 return detail::collect_range<AssocT>(
   scan<ValueT>(std::move(dbh), std::move(selection)));
}

} // namespace ceph::libfdb

#endif
