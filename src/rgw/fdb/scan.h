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
#include <limits>
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
  const int iteration,
  const read_mode mode = read_mode::serializable)
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
 const fdb_bool_t is_snapshot = read_mode::snapshot == mode;

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
                                      const int iteration,
                                      const read_mode mode = read_mode::serializable)
{
 return extract_result_pairs(await_future_of([&] {
  return get_range_future_from_transaction(txn, key_range, iteration, mode);
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
inline auto generate_FDB_pairs(
  transaction& txn,
  select key_range,
  const read_mode mode = read_mode::serializable)
 -> std::generator<std::span<const FDBKeyValue>>
{
 int iteration = 1;

 for (auto more_available = true; more_available; ++iteration) {
  auto window = read_query_window(txn, key_range, iteration, mode);
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
                                     const int iteration = 1,
                                     const read_mode mode = read_mode::serializable)
 -> query_window_result<AssocT>
{
 auto window = read_query_window(txn, key_range, iteration, mode);

 return {
  .result_block = collect_pairs<ValueT, AssocT>(window.result_pairs),
  .next_range = next_range_after(std::move(key_range), window)
 };
}

inline std::size_t for_each_decoded_kv_pair(transaction& txn,
                                            const select& key_range,
                                            const read_mode mode,
                                            auto&& fn)
{
 std::size_t nread = 0;

 for (const auto& kv : generate_FDB_pairs(txn, key_range, mode)
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
  const read_mode mode,
  OutIterT& out_iter)
{
 return for_each_decoded_kv_pair(
   txn, key_range, mode,
   [&out_iter](auto&& kv) {
     *out_iter++ = std::forward<decltype(kv)>(kv);
   });
}

inline std::size_t get_value_range_from_transaction(
  transaction& txn,
  const select& key_range,
  const read_mode mode,
  concepts::string_pair_output_range auto& out)
{
 return for_each_decoded_kv_pair(
   txn, key_range, mode,
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
 auto ranges = ceph::util::collect_as<std::vector<select>>(
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

 if (parent.options.reverse_order) {
  std::ranges::reverse(ranges);
 }

 return ranges;
}

struct range_work_plan final
{
 std::vector<select> ranges;
};

inline range_work_plan plan_range_work(
  database_handle dbh,
  select selector,
  const std::int64_t remote_chunk_size)
{
 auto txn = make_transaction(dbh);
 auto split_selector = as_half_open_select(selector);
 const auto begin = as_byte_view(split_selector.begin_key);
 const auto end = as_byte_view(split_selector.end_key);

 for (;;) {
  auto split_points = extract_split_points(wait_until_ready(
    transaction_get_range_split_points(
      txn, begin, end, remote_chunk_size)));

  if (reset_for_replay_if_needed(txn, split_points.error)) {
   continue;
  }

  auto ranges = select_ranges_from_split_points(
    split_points.result_keys, split_selector);

  if (ranges.empty()) {
   ranges.push_back(std::move(selector));
  }

  return {.ranges = std::move(ranges)};
 }
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

 if (not std::empty(out) and out.front().options.reverse_order) {
  std::ranges::reverse(out);
 }

 return out;
}

template <query::expression SelectionT, typename OutT>
requires concepts::string_pair_output_iterator<OutT> ||
         concepts::string_pair_output_range<OutT>
inline std::size_t get_value_selection_from_transaction(
  transaction& txn,
  const SelectionT& selection,
  const read_mode mode,
  OutT& out)
{
 std::size_t nread = 0;

 for (const auto& interval : intervals(selection)) {
  nread += get_value_range_from_transaction(txn, interval, mode, out);
 }

 return nread;
}

template <typename OutT, query::expression SelectionT>
requires concepts::materializable_string_pair_output_range<OutT>
inline auto materialize_string_pair_selection(
  transaction& txn,
  const SelectionT& selection,
  const read_mode mode) -> materialized_string_pair_output<OutT>
{
 materialized_string_pair_output<OutT> result;
 result.nread = get_value_selection_from_transaction(
   txn, selection, mode, result.values);

 return result;
}

} // namespace detail

// get() with a selector writes key/value pairs to its output and returns the
// number of pairs emitted:
inline std::size_t get(ceph::libfdb::transaction_handle txn,
                       const query::expression auto& selection,
                       concepts::string_pair_output_iterator auto out_iter,
                       const read_mode mode,
                       const ceph::libfdb::commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [&selection, out_iter, mode](const transaction_handle& active_txn) mutable {
            return detail::get_value_selection_from_transaction(*active_txn, selection, mode, out_iter);
          });
}

inline std::size_t get(ceph::libfdb::transaction_handle txn,
                       const query::expression auto& selection,
                       concepts::string_pair_output_iterator auto out_iter,
                       const ceph::libfdb::commit_after_op commit_after)
{
 return get(txn, selection, out_iter, read_mode::serializable, commit_after);
}

inline std::size_t get(ceph::libfdb::transaction_handle txn,
                       const query::expression auto& selection,
                       concepts::string_pair_output_iterator auto out_iter,
                       const read_mode mode = read_mode::serializable)
{
 return get(txn, selection, out_iter, mode, commit_after_op::no_commit);
}

inline std::size_t get(ceph::libfdb::database_handle dbh,
                       const query::expression auto& selection,
                       concepts::string_pair_output_iterator auto out_iter,
                       const read_mode mode = read_mode::serializable)
{
 auto result = detail::in_read_transaction(dbh,
          [&selection, mode](transaction_handle& txn) {
            using out_t = std::vector<std::pair<std::string, std::string>>;
            return detail::materialize_string_pair_selection<out_t>(*txn, selection, mode);
          });

 std::ranges::move(result.values, out_iter);

 return result.nread;
}

inline std::size_t get(ceph::libfdb::transaction_handle txn,
                       const query::expression auto& selection,
                       concepts::string_pair_output_range auto& out,
                       const read_mode mode,
                       const ceph::libfdb::commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [&selection, &out, mode](const transaction_handle& active_txn) {
            return detail::get_value_selection_from_transaction(*active_txn, selection, mode, out);
          });
}

inline std::size_t get(ceph::libfdb::transaction_handle txn,
                       const query::expression auto& selection,
                       concepts::string_pair_output_range auto& out,
                       const ceph::libfdb::commit_after_op commit_after)
{
 return get(txn, selection, out, read_mode::serializable, commit_after);
}

inline std::size_t get(ceph::libfdb::transaction_handle txn,
                       const query::expression auto& selection,
                       concepts::string_pair_output_range auto& out,
                       const read_mode mode = read_mode::serializable)
{
 return get(txn, selection, out, mode, commit_after_op::no_commit);
}

inline std::size_t get(ceph::libfdb::database_handle dbh,
                       const query::expression auto& selection,
                       concepts::materializable_string_pair_output_range auto& out,
                       const read_mode mode = read_mode::serializable)
{
 using out_t = std::remove_cvref_t<decltype(out)>;

 auto result = detail::in_read_transaction(dbh,
          [&selection, mode](transaction_handle& txn) {
            return detail::materialize_string_pair_selection<out_t>(*txn, selection, mode);
          });

 detail::publish_string_pair_results(out, std::move(result.values));

 return result.nread;
}

inline std::size_t get(ceph::libfdb::transaction_handle txn,
                       std::initializer_list<std::string_view> keys,
                       concepts::string_pair_output_range auto& out,
                       const read_mode mode,
                       const ceph::libfdb::commit_after_op commit_after)
{
 return get(txn, detail::select_from_initializer_list(keys), out, mode, commit_after);
}

inline std::size_t get(ceph::libfdb::transaction_handle txn,
                       std::initializer_list<std::string_view> keys,
                       concepts::string_pair_output_range auto& out,
                       const ceph::libfdb::commit_after_op commit_after)
{
 return get(txn, keys, out, read_mode::serializable, commit_after);
}

inline std::size_t get(ceph::libfdb::transaction_handle txn,
                       std::initializer_list<std::string_view> keys,
                       concepts::string_pair_output_range auto& out,
                       const read_mode mode = read_mode::serializable)
{
 return get(txn, keys, out, mode, commit_after_op::no_commit);
}

inline std::size_t get(ceph::libfdb::database_handle dbh,
                       std::initializer_list<std::string_view> keys,
                       concepts::materializable_string_pair_output_range auto& out,
                       const read_mode mode = read_mode::serializable)
{
 return get(dbh, detail::select_from_initializer_list(keys), out, mode);
}

namespace detail {

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

template <query::expression SelectionT>
[[nodiscard]] inline std::int64_t approximate_range_size(transaction_handle txn,
                                                         SelectionT selection)
{
 std::int64_t out = 0;

 for (auto& interval : detail::intervals(std::move(selection))) {
  out += detail::extract_int64(
          detail::block_until_ready(
           detail::transaction_get_estimated_range_size(txn, interval)));
 }

 return out;
}

template <query::expression SelectionT>
[[nodiscard]] inline std::int64_t approximate_range_size(database_handle dbh,
                                                         SelectionT selection)
{
 return detail::in_read_transaction(dbh,
          [selection = std::move(selection)](transaction_handle& txn) {
            return approximate_range_size(txn, selection);
          });
}

// For ordinary range scans inside one explicit transaction, scan() is usually
// the right default:
template <typename ValueT = std::string,
          query::expression SelectionT>
inline auto scan(ceph::libfdb::transaction_handle txn,
                 SelectionT selection,
                 const read_mode mode = read_mode::serializable)
  -> std::generator<std::pair<std::string, ValueT>>
{
 for (auto& interval : detail::intervals(selection)) {
  auto decoded_pairs = detail::generate_FDB_pairs(*txn, std::move(interval), mode)
                     | std::views::join
                     | std::views::transform(detail::to_decoded_kv_pair<ValueT>);

  co_yield std::ranges::elements_of(decoded_pairs);
 }
}

// Compatibility name retained for existing callers:
template <typename ValueT = std::string,
          query::expression SelectionT>
inline auto pair_generator(ceph::libfdb::transaction_handle txn,
                           SelectionT selection,
                           const read_mode mode = read_mode::serializable)
  -> std::generator<std::pair<std::string, ValueT>>
{
 return scan<ValueT>(txn, std::move(selection), mode);
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

constexpr int range_limit_for(page p)
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

template <typename FnT, typename ValueT>
concept row_consumer =
 requires(FnT& fn, row_t<ValueT>&& row) {
  { std::invoke(fn, std::move(row)) } -> std::same_as<void>;
 };

template <typename PredT, typename ValueT>
concept row_predicate = std::predicate<PredT&, const row_t<ValueT>&>;

} // namespace detail

template <typename ValueT = std::string, typename FnT, query::expression SelectionT>
requires detail::row_consumer<FnT, ValueT>
inline void for_each(ceph::libfdb::transaction_handle txn,
                     SelectionT selection,
                     FnT&& fn,
                     const read_mode mode = read_mode::serializable)
{
 for (auto&& row : scan<ValueT>(std::move(txn), std::move(selection), mode)) {
  std::invoke(fn, std::move(row));
 }
}

// Database-handle functional helpers run inside the managed transaction loop.
// Keep callback side effects replay-safe; callback exceptions themselves escape
// without being classified as FoundationDB failures:
template <typename ValueT = std::string, typename FnT, query::expression SelectionT>
requires detail::row_consumer<FnT, ValueT>
inline void for_each(ceph::libfdb::database_handle dbh,
                     SelectionT selection,
                     FnT&& fn,
                     const read_mode mode = read_mode::serializable)
try
{
 detail::in_read_transaction(dbh,
  [selection = std::move(selection), fn = std::forward<FnT>(fn), mode](auto& txn) mutable {
   for_each<ValueT>(txn, selection,
                    [&fn](auto&& row) {
                     detail::invoke_user_callback(
                      fn, std::forward<decltype(row)>(row));
                    },
                    mode);
  });
}
catch (const detail::user_callback_failure& failure)
{
 std::rethrow_exception(failure.cause);
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
                          OutIterT out,
                          const read_mode mode = read_mode::serializable)
{
 for_each<ValueT>(std::move(txn), std::move(selection),
                  [&fn, &out](auto&& row) mutable {
                   *out++ = std::invoke(fn, std::move(row));
                  },
                  mode);

 return out;
}

template <typename ValueT = std::string, typename FnT, query::expression SelectionT>
requires detail::row_invocable<FnT, ValueT> &&
         concepts::storable_invocation_result<detail::row_transform_result_t<ValueT, FnT>>
[[nodiscard]] auto transform(ceph::libfdb::transaction_handle txn,
                             SelectionT selection,
                             FnT&& fn,
                             const read_mode mode = read_mode::serializable)
{
 using result_t =
  std::remove_cvref_t<detail::row_transform_result_t<ValueT, FnT>>;

 std::vector<result_t> out;
 transform<ValueT>(std::move(txn), std::move(selection),
                   std::forward<FnT>(fn), std::back_inserter(out), mode);

 return out;
}

template <typename ValueT = std::string, typename FnT, query::expression SelectionT>
requires detail::row_invocable<FnT, ValueT> &&
         concepts::storable_invocation_result<detail::row_transform_result_t<ValueT, FnT>>
[[nodiscard]] auto transform(ceph::libfdb::database_handle dbh,
                             SelectionT selection,
                             FnT&& fn,
                             const read_mode mode = read_mode::serializable)
try
{
 return detail::in_read_transaction(dbh,
  [selection = std::move(selection), fn = std::forward<FnT>(fn), mode](auto& txn) mutable {
   return transform<ValueT>(txn, selection,
                            [&fn](auto&& row) -> decltype(auto) {
                             return detail::invoke_user_callback(
                              fn, std::forward<decltype(row)>(row));
                            },
                            mode);
  });
}
catch (const detail::user_callback_failure& failure)
{
 std::rethrow_exception(failure.cause);
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
                          OutIterT out,
                          const read_mode mode = read_mode::serializable)
{
 auto transformed = transform<ValueT>(dbh, std::move(selection),
                                      std::forward<FnT>(fn), mode);

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
                        page p,
                        const read_mode mode = read_mode::serializable)
{
 using row_type = std::pair<std::string, ValueT>;

 if (0 == p.size) {
  return collect(scan<ValueT>(std::move(txn), std::move(selector), mode), p);
 }

 selector.options.result_limit = detail::range_limit_for(p);
 auto window = detail::read_query_window(*txn, selector, 1, mode);

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
                        page p,
                        const read_mode mode = read_mode::serializable)
{
 return detail::in_read_transaction(
  dbh, [selector = std::move(selector), p, mode](auto& txn) {
   return scan<ValueT>(txn, selector, p, mode);
  });
}

// blocks() is for truly large scans that benefit from split planning:
// it trades direct streaming for block-at-a-time processing, bounded
// transaction windows, and lower risk of one transaction getting too old.
// Prefer scan(txn, ...) for ordinary caller-owned transaction scans.
namespace detail {

template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>>
auto blocks_selector(ceph::libfdb::database_handle dbh,
                     ceph::libfdb::select selector,
                     const read_mode mode)
 -> std::generator<AssocT>
{
 if (0 == selector.options.result_limit) {
  selector.options.result_limit = 4096;
 }

 // Initial range-work target: measurements so far suggest low sensitivity here,
 // but large real workloads should drive future tuning.
 constexpr auto target_bytes = 4 * 1024 * 1024;

 auto plan = detail::plan_range_work(dbh, selector, target_bytes);

 auto read_blocks = [dbh, mode](this auto& self, ceph::libfdb::select range, const int iteration)
 -> std::generator<AssocT> {
  auto read_result = detail::in_read_transaction(
   dbh, [range = std::move(range), iteration, mode](transaction_handle& txn) {
    return detail::materialize_query_window<ValueT, AssocT>(
     *txn, range, iteration, mode);
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

 co_yield std::ranges::elements_of(plan.ranges
                                 | std::views::transform(expand_range)
                                 | std::views::join);
}

} // namespace detail

template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>,
          query::expression SelectionT>
auto blocks(ceph::libfdb::database_handle dbh,
            SelectionT selection,
            const read_mode mode = read_mode::serializable)
 -> std::generator<AssocT>
{
 for (auto& interval : detail::intervals(selection)) {
  co_yield std::ranges::elements_of(
   detail::blocks_selector<ValueT, AssocT>(dbh, std::move(interval), mode));
 }
}

// Compatibility name retained for existing callers:
template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>,
          query::expression SelectionT>
auto block_generator(ceph::libfdb::database_handle dbh,
                     SelectionT selection,
                     const read_mode mode = read_mode::serializable)
 -> std::generator<AssocT>
{
 return blocks<ValueT, AssocT>(dbh, std::move(selection), mode);
}

// Managed scans flatten the blocks() stream into key/value pairs.
template <typename ValueT = std::string,
          query::expression SelectionT>
inline auto scan(ceph::libfdb::database_handle dbh,
                 SelectionT selection,
                 const read_mode mode = read_mode::serializable)
  -> std::generator<std::pair<std::string, ValueT>>
{
 return detail::flatten_blocks<ValueT>(blocks<ValueT>(dbh, std::move(selection), mode));
}

template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>,
          query::expression SelectionT>
inline AssocT collect(ceph::libfdb::transaction_handle txn,
                      SelectionT selection,
                      const read_mode mode = read_mode::serializable)
{
 return ceph::util::collect_as<AssocT>(scan<ValueT>(txn, std::move(selection), mode));
}

template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>,
          query::expression SelectionT>
inline AssocT collect(ceph::libfdb::database_handle dbh,
                      SelectionT selection,
                      const read_mode mode = read_mode::serializable)
{
 return ceph::util::collect_as<AssocT>(scan<ValueT>(dbh, std::move(selection), mode));
}

} // namespace ceph::libfdb

#endif
