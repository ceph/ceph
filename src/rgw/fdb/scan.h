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

#include <boost/container/small_vector.hpp>

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

#include <memory>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <concepts>
#include <stdexcept>
#include <functional>
#include <type_traits>

namespace ceph::libfdb {

// Overload tag for callback-scoped access to FoundationDB's result bytes:
struct raw_t final {};
inline constexpr raw_t raw;

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

inline auto intervals(select selection)
{
 // Raw selectors still execute in the ordinary FDB keyspace:
 boost::container::small_vector<select, 1> out;
 auto interval = query::intersection(
   std::move(selection), query::universal());

 if (not query::is_empty(interval)) {
  out.push_back(std::move(interval));
 }

 return out;
}

template <query::non_interval_expression QueryT>
inline auto intervals(const QueryT& query)
{
 boost::container::small_vector<select, 1> out;

 query::for_each_interval(query, [&out](select interval) {
  out.push_back(std::move(interval));
 });

 if (not std::empty(out) and out.front().options.reverse_order) {
  std::ranges::reverse(out);
 }

 return out;
}

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
  .result_pairs = result_span(out_kvs, out_count),
  .more_available = 0 != more_available
 };
}

inline split_point_result extract_split_points(future_value result_owner)
{
 const FDBKey *result_keys = nullptr;
 int result_count = 0;

 if (const auto error = fdb_future_get_key_array(
       result_owner.raw_ptr_or_throw(), &result_keys, &result_count);
     0 != error) {
  throw libfdb_exception(error);
 }

 return split_point_result {
  .result_owner = std::move(result_owner),
  .result_keys = result_span(result_keys, result_count)
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
 const auto bytes_begin = as_fdb_bytes(selection.begin_key);
 const auto bytes_end = as_fdb_bytes(selection.end_key);

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
   bytes_begin.data,
   bytes_begin.length,
   begin_or_eq,
   begin_offset,
   bytes_end.data,
   bytes_end.length,
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
 return extract_result_pairs(block_until_ready(
   get_range_future_from_transaction(txn, key_range, iteration, mode)));
}

inline bool continue_range_after(
  select& key_range,
  const bool more_available,
  const std::span<const FDBKeyValue> result_pairs)
{
 if (not more_available or std::empty(result_pairs)) {
  return false;
 }

 const auto& last_key = result_pairs.back();
 const auto cursor = key_view(last_key);

 if (key_range.options.reverse_order) {
  key_range.end_key = cursor;
  key_range.end_inclusive = false;
  return true;
 }

 key_range.begin_key = cursor;
 key_range.begin_inclusive = false;

 return true;
}

inline auto key_bytes(const FDBKeyValue& pair) noexcept
{
 return std::span {pair.key, static_cast<std::size_t>(pair.key_length)};
}

inline auto value_bytes(const FDBKeyValue& pair) noexcept
{
 return std::span {pair.value, static_cast<std::size_t>(pair.value_length)};
}

class query_cursor final
{
 using selections_t = boost::container::small_vector<select, 1>;

 selections_t selections;
 std::size_t selection_index = 0;

 // FDB iterator streaming uses a one-based count, reset for each interval:
 int iteration = 1;

 select& current_selection() noexcept
 {
  return selections[selection_index];
 }

 void mark_exhausted() noexcept
 {
  selection_index = std::size(selections);
  iteration = 1;
 }

 public:
 template <query::expression SelectionT>
 explicit query_cursor(SelectionT selection)
  : selections(intervals(std::move(selection)))
 {}

 explicit query_cursor(selections_t selected_ranges)
  : selections(std::move(selected_ranges))
 {}

 query_cursor(query_cursor&& other) noexcept
  : selections(std::move(other.selections)),
    selection_index(other.selection_index),
    iteration(other.iteration)
 {
  other.mark_exhausted();
 }

 query_cursor(const query_cursor&) = delete;

 query_cursor& operator=(query_cursor&& other) noexcept
 {
  if (this == std::addressof(other)) {
   return *this;
  }

  selections = std::move(other.selections);
  selection_index = other.selection_index;
  iteration = other.iteration;
  other.mark_exhausted();

  return *this;
 }

 query_cursor& operator=(const query_cursor&) = delete;

 [[nodiscard]] explicit operator bool() const noexcept
 {
  return selection_index < std::size(selections);
 }

 [[nodiscard]] const select& current() const noexcept
 {
  return selections[selection_index];
 }

 [[nodiscard]] int fdb_iteration() const noexcept { return iteration; }

 void advance(const std::span<const FDBKeyValue> rows,
              const bool more_available)
 {
  if (continue_range_after(current_selection(), more_available, rows)) {
   ++iteration;
   return;
  }

  ++selection_index;
  iteration = 1;
 }

 std::optional<query_window> read_next(
   transaction& txn,
   const read_mode mode = read_mode::serializable)
 {
  if (not *this) {
   return std::nullopt;
  }

  auto window = read_query_window(txn, current(), iteration, mode);

  advance(window.result_pairs, window.more_available);

  return {std::move(window)};
 }
};

/* The transaction must outlive the returned FDB-owned window. */
inline std::optional<query_window> read_query_window_with_retry(
  transaction_handle& txn,
  query_cursor& cursor,
  const read_mode mode)
{
 return retry_without_commit(
   txn, [&cursor, mode](transaction_handle& active_txn) {
     return cursor.read_next(*active_txn, mode);
   });
}

template <query::expression SelectionT>
inline query_cursor make_managed_query_cursor(SelectionT selection)
{
 auto selected_ranges = intervals(std::move(selection));

 // Keep managed transactions substantial but bounded unless the caller chose
 // an explicit range-read limit:
 constexpr auto default_result_limit = 4096;

 for (auto& range : selected_ranges) {
  if (0 == range.options.result_limit) {
   range.options.result_limit = default_result_limit;
  }
 }

 return query_cursor {std::move(selected_ranges)};
}

/* Returned spans remain valid only while the coroutine retains the owning FDB
 * future. Consumers must copy their contents before advancing the generator: */
template <query::expression SelectionT>
inline auto generate_FDB_pairs(
  transaction& txn,
  SelectionT selection,
  const read_mode mode = read_mode::serializable)
 -> std::generator<std::span<const FDBKeyValue>>
{
 query_cursor cursor(std::move(selection));

 while (auto window = cursor.read_next(txn, mode)) {
  co_yield window->result_pairs;
 }
}

template <typename ValueT = std::string>
inline auto decode_pairs(std::span<const FDBKeyValue> pairs)
{
 return pairs | std::views::transform(to_decoded_kv_pair<ValueT>);
}

template <query::expression SelectionT>
inline std::size_t for_each_result_pair(transaction& txn,
                                        SelectionT selection,
                                        const read_mode mode,
                                        auto&& fn)
{
 query_cursor cursor(std::move(selection));
 std::size_t nread = 0;

 while (auto window = cursor.read_next(txn, mode)) {
  std::ranges::for_each(window->result_pairs, std::ref(fn));
  nread += std::size(window->result_pairs);
 }

 return nread;
}

template <query::expression SelectionT>
inline std::size_t for_each_decoded_kv_pair(transaction& txn,
                                            SelectionT selection,
                                            const read_mode mode,
                                            auto&& fn)
{
 return for_each_result_pair(
  txn, std::move(selection), mode, [&fn](const FDBKeyValue& pair) {
   std::invoke(fn, to_decoded_kv_pair<std::string>(pair));
  });
}

template <typename OutIterT, query::expression SelectionT>
requires std::output_iterator<OutIterT,
                              std::pair<std::string, std::string>>
inline std::size_t get_value_range_from_transaction(
  transaction& txn,
  SelectionT selection,
  const read_mode mode,
  OutIterT& out_iter)
{
 return for_each_decoded_kv_pair(
   txn, std::move(selection), mode,
   [&out_iter](auto&& kv) {
     *out_iter++ = std::forward<decltype(kv)>(kv);
   });
}

template <query::expression SelectionT>
inline std::size_t get_value_range_from_transaction(
  transaction& txn,
  SelectionT selection,
  const read_mode mode,
  concepts::string_pair_output_range auto& out)
{
 return for_each_decoded_kv_pair(
   txn, std::move(selection), mode,
   [&out](auto&& kv) {
     ceph::util::push_back(out, std::forward<decltype(kv)>(kv));
   });
}

inline std::vector<select> select_ranges_from_split_points(
  std::span<const FDBKey> keys,
  const select& parent)
{
 if (2 > std::size(keys)) {
  return {};
 }

 // Gather the flattened list into overlapping libfdb::select pairs:
 auto ranges = ceph::util::collect_as<std::vector<select>>(
   std::views::iota(std::size_t {0}, std::size(keys) - 1)
   | std::views::transform([&parent, keys](const auto i) {
       const auto& first = keys[i];
       const auto& second = keys[1 + i];

       const auto first_key = key_view(first);
       const auto second_key = key_view(second);

       select split(first_key, second_key);

       split.options = parent.options;

       split.begin_inclusive = 0 == i ? parent.begin_inclusive : true;
       split.end_inclusive = 2 + i == std::size(keys)
        ? parent.end_inclusive
        : false;

       return split;
     }));

 if (parent.options.reverse_order) {
  std::ranges::reverse(ranges);
 }

 return ranges;
}

inline std::vector<select> partition_interval(
  const transaction_handle& txn,
  select selector,
  const std::int64_t target_bytes)
{
 auto split_selector = as_half_open_select(selector);
 const auto bytes_begin = as_byte_view(split_selector.begin_key);
 const auto bytes_end = as_byte_view(split_selector.end_key);

 auto split_points = extract_split_points(block_until_ready(
   transaction_get_range_split_points(
     txn, bytes_begin, bytes_end, target_bytes)));
 auto ranges = select_ranges_from_split_points(
   split_points.result_keys, split_selector);

 if (std::empty(ranges)) {
  ranges.push_back(std::move(split_selector));
 }

 return ranges;
}

template <query::expression SelectionT>
inline std::vector<select> plan_partitions(
  const transaction_handle& txn,
  SelectionT selection,
  const std::int64_t target_bytes)
{
 std::vector<select> out;

 for (auto& interval : intervals(std::move(selection))) {
  auto planned = partition_interval(
    txn, std::move(interval), target_bytes);

  if (std::empty(out)) {
   out = std::move(planned);
   continue;
  }

  std::ranges::move(planned, std::back_inserter(out));
 }

 return out;
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

template <typename ContainerT>
inline void publish_string_pair_results(ContainerT& out, ContainerT&& tmp)
{
 if constexpr (ceph::concepts::has_empty<ContainerT> &&
               std::assignable_from<ContainerT&, ContainerT&&>) {
  if (std::empty(out)) {
   out = std::move(tmp);
   return;
  }
 }

 if constexpr (requires { out.merge(tmp); }) {
  out.merge(tmp);
  return;
 }

 ceph::util::append_range(out, std::views::as_rvalue(tmp));
}

template <typename OutT, query::expression SelectionT>
requires concepts::materializable_string_pair_output_range<OutT>
inline auto materialize_string_pair_selection(
  transaction& txn,
  const SelectionT& selection,
  const read_mode mode) -> materialized_string_pair_output<OutT>
{
 materialized_string_pair_output<OutT> result;
 result.nread = get_value_range_from_transaction(
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
            return detail::get_value_range_from_transaction(*active_txn, selection, mode, out_iter);
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
                       const transaction_options& options,
                       const query::expression auto& selection,
                       concepts::string_pair_output_iterator auto out_iter,
                       const read_mode mode = read_mode::serializable)
{
 auto result = detail::in_read_transaction(dbh, options,
          [&selection, mode](transaction_handle& txn) {
            using out_t = std::vector<std::pair<std::string, std::string>>;
            return detail::materialize_string_pair_selection<out_t>(*txn, selection, mode);
          });

 std::ranges::move(result.values, out_iter);

 return result.nread;
}

inline std::size_t get(ceph::libfdb::database_handle dbh,
                       const query::expression auto& selection,
                       concepts::string_pair_output_iterator auto out_iter,
                       const read_mode mode = read_mode::serializable)
{
 return get(std::move(dbh), transaction_options {}, selection, out_iter, mode);
}

inline std::size_t get(ceph::libfdb::transaction_handle txn,
                       const query::expression auto& selection,
                       concepts::string_pair_output_range auto& out,
                       const read_mode mode,
                       const ceph::libfdb::commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after,
          [&selection, &out, mode](const transaction_handle& active_txn) {
            return detail::get_value_range_from_transaction(*active_txn, selection, mode, out);
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
                       const transaction_options& options,
                       const query::expression auto& selection,
                       concepts::materializable_string_pair_output_range auto& out,
                       const read_mode mode = read_mode::serializable)
{
 using out_t = std::remove_cvref_t<decltype(out)>;

 auto result = detail::in_read_transaction(dbh, options,
          [&selection, mode](transaction_handle& txn) {
            return detail::materialize_string_pair_selection<out_t>(*txn, selection, mode);
          });

 detail::publish_string_pair_results(out, std::move(result.values));

 return result.nread;
}

inline std::size_t get(ceph::libfdb::database_handle dbh,
                       const query::expression auto& selection,
                       concepts::materializable_string_pair_output_range auto& out,
                       const read_mode mode = read_mode::serializable)
{
 return get(std::move(dbh), transaction_options {}, selection, out, mode);
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
                       const transaction_options& options,
                       std::initializer_list<std::string_view> keys,
                       concepts::materializable_string_pair_output_range auto& out,
                       const read_mode mode = read_mode::serializable)
{
 return get(std::move(dbh), options,
            detail::select_from_initializer_list(keys), out, mode);
}

inline std::size_t get(ceph::libfdb::database_handle dbh,
                       std::initializer_list<std::string_view> keys,
                       concepts::materializable_string_pair_output_range auto& out,
                       const read_mode mode = read_mode::serializable)
{
 return get(std::move(dbh), transaction_options {}, keys, out, mode);
}

// Sum FoundationDB's approximate byte estimates over a selection's canonical,
// nonoverlapping intervals. An empty selection is zero; disjoint estimates are
// submitted in bounded batches through one transaction before each batch is read:
template <query::expression SelectionT>
[[nodiscard]] inline std::int64_t approximate_range_size(transaction_handle txn,
                                                         const SelectionT& selection)
{
 // Four covers the common few-interval selection without allocating while
 // placing a modest bound on outstanding FoundationDB futures:
 constexpr std::size_t pipeline_width = 4;

 boost::container::small_vector<detail::future_value, pipeline_width> pending;
 std::int64_t out = 0;

 const auto finish_batch = [&pending, &out] {
  for (auto& result : pending) {
   out += detail::extract_int64(
           detail::block_until_ready(std::move(result)));
  }

  pending.clear();
 };

 query::for_each_interval(selection, [&](const select& interval) {
  pending.emplace_back(
    detail::transaction_get_estimated_range_size(txn, interval));

  if (pipeline_width == std::size(pending)) {
   finish_batch();
  }
 });

 finish_batch();

 return out;
}

template <query::expression SelectionT>
[[nodiscard]] inline std::int64_t approximate_range_size(database_handle dbh,
                                                         const transaction_options& options,
                                                         SelectionT selection)
{
 return detail::in_read_transaction(dbh, options,
          [selection = std::move(selection)](transaction_handle& txn) {
            return approximate_range_size(txn, selection);
          });
}

template <query::expression SelectionT>
[[nodiscard]] inline std::int64_t approximate_range_size(database_handle dbh,
                                                         SelectionT selection)
{
 return approximate_range_size(
  std::move(dbh), transaction_options {}, std::move(selection));
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
 auto decoded_pairs = detail::generate_FDB_pairs(
                        *txn, std::move(selection), mode)
                   | std::views::join
                   | std::views::transform(
                       detail::to_decoded_kv_pair<ValueT>);

 co_yield std::ranges::elements_of(decoded_pairs);
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

/* Apply a callback to each selected key/value pair without decoding or copying.
 * Both spans expire when the callback returns: */
template <query::expression SelectionT,
          concepts::raw_key_value_callback FnT>
inline std::size_t for_each(raw_t,
                            const transaction_handle& txn,
                            SelectionT selection,
                            FnT&& fn,
                            const read_mode mode = read_mode::serializable)
{
 return detail::for_each_result_pair(
   *txn, std::move(selection), mode, [&fn](const FDBKeyValue& pair) {
    std::invoke(fn, detail::key_bytes(pair), detail::value_bytes(pair));
   });
}

/* The database form retries each result window before exposing its borrowed
 * bytes. Callbacks are therefore never replayed, but a retry may move later
 * windows to a newer read version: */
template <query::expression SelectionT,
          concepts::raw_key_value_callback FnT>
inline std::size_t for_each(raw_t,
                            database_handle dbh,
                            const transaction_options& options,
                            SelectionT selection,
                            FnT&& fn,
                            const read_mode mode = read_mode::serializable)
{
 const detail::transaction_source source(std::move(dbh), options);
 auto cursor = detail::make_managed_query_cursor(std::move(selection));
 std::size_t nread = 0;

 while (cursor) {
  auto txn = source.make();
  auto window = detail::read_query_window_with_retry(txn, cursor, mode);

  if (not window) {
   break;
  }

  for (const auto& pair : window->result_pairs) {
   std::invoke(fn, detail::key_bytes(pair), detail::value_bytes(pair));
  }

  nread += std::size(window->result_pairs);
 }

 return nread;
}

template <query::expression SelectionT,
          concepts::raw_key_value_callback FnT>
inline std::size_t for_each(raw_t tag,
                            database_handle dbh,
                            SelectionT selection,
                            FnT&& fn,
                            const read_mode mode = read_mode::serializable)
{
 return for_each(tag, std::move(dbh), transaction_options {},
                 std::move(selection), std::forward<FnT>(fn), mode);
}

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
                     const transaction_options& options,
                     SelectionT selection,
                     FnT&& fn,
                     const read_mode mode = read_mode::serializable)
try
{
 detail::in_read_transaction(dbh, options,
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

template <typename ValueT = std::string, typename FnT, query::expression SelectionT>
requires detail::row_consumer<FnT, ValueT>
inline void for_each(ceph::libfdb::database_handle dbh,
                     SelectionT selection,
                     FnT&& fn,
                     const read_mode mode = read_mode::serializable)
{
 for_each<ValueT>(std::move(dbh), transaction_options {},
                  std::move(selection), std::forward<FnT>(fn), mode);
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
                             const transaction_options& options,
                             SelectionT selection,
                             FnT&& fn,
                             const read_mode mode = read_mode::serializable)
try
{
 return detail::in_read_transaction(dbh, options,
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

template <typename ValueT = std::string, typename FnT, query::expression SelectionT>
requires detail::row_invocable<FnT, ValueT> &&
         concepts::storable_invocation_result<detail::row_transform_result_t<ValueT, FnT>>
[[nodiscard]] auto transform(ceph::libfdb::database_handle dbh,
                             SelectionT selection,
                             FnT&& fn,
                             const read_mode mode = read_mode::serializable)
{
 return transform<ValueT>(std::move(dbh), transaction_options {},
                          std::move(selection), std::forward<FnT>(fn), mode);
}

template <typename ValueT = std::string,
          typename FnT,
          typename OutIterT,
          query::expression SelectionT>
requires detail::row_invocable<FnT, ValueT> &&
         concepts::storable_invocation_result<detail::row_transform_result_t<ValueT, FnT>> &&
         std::output_iterator<OutIterT, detail::row_transform_result_t<ValueT, FnT>>
inline OutIterT transform(ceph::libfdb::database_handle dbh,
                          const transaction_options& options,
                          SelectionT selection,
                          FnT&& fn,
                          OutIterT out,
                          const read_mode mode = read_mode::serializable)
{
 auto transformed = transform<ValueT>(std::move(dbh), options, std::move(selection),
                                      std::forward<FnT>(fn), mode);

 return std::ranges::move(transformed, std::move(out)).out;
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
 return transform<ValueT>(std::move(dbh), transaction_options {},
                          std::move(selection), std::forward<FnT>(fn),
                          std::move(out), mode);
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
                            const transaction_options& options,
                            SelectionT selection,
                            PredT&& pred)
{
 return detail::in_transaction(std::move(dbh), options,
  [selection = std::move(selection), pred = std::forward<PredT>(pred)](auto& txn) mutable {
   return erase_if<ValueT>(txn, selection, pred);
  });
}

template <typename ValueT = std::string, typename PredT, query::expression SelectionT>
requires detail::row_predicate<PredT, ValueT>
inline std::size_t erase_if(ceph::libfdb::database_handle dbh,
                            SelectionT selection,
                            PredT&& pred)
{
 return erase_if<ValueT>(std::move(dbh), transaction_options {},
                         std::move(selection), std::forward<PredT>(pred));
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
                        const transaction_options& options,
                        ceph::libfdb::select selector,
                        page p,
                        const read_mode mode = read_mode::serializable)
{
 return detail::in_read_transaction(
  dbh, options,
  [selector = std::move(selector), p, mode](auto& txn) {
   return scan<ValueT>(txn, selector, p, mode);
  });
}

template <typename ValueT = std::string>
[[nodiscard]] auto scan(ceph::libfdb::database_handle dbh,
                        ceph::libfdb::select selector,
                        page p,
                        const read_mode mode = read_mode::serializable)
{
 return scan<ValueT>(std::move(dbh), transaction_options {},
                     std::move(selector), p, mode);
}

namespace detail {

template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>>
AssocT materialize_managed_block(const transaction_source& source,
                                 query_cursor& cursor,
                                 const read_mode mode)
{
 auto txn = source.make();
 auto window = read_query_window_with_retry(txn, cursor, mode);

 if (not window) {
  return {};
 }

 return ceph::util::collect_as<AssocT>(
   decode_pairs<ValueT>(window->result_pairs));
}

template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>>
auto blocks_selector(transaction_source source,
                     query_cursor cursor,
                     const read_mode mode)
 -> std::generator<AssocT>
{
 while (cursor) {
  // Release the FDB-owned window before publishing its owning materialization:
  auto block = materialize_managed_block<ValueT, AssocT>(
    source, cursor, mode);

  if (std::empty(block)) {
   continue;
  }

  co_yield std::move(block);
 }
}

template <typename ValueT = std::string,
          query::expression SelectionT>
inline auto managed_scan_selector(transaction_source source,
                                  SelectionT selection,
                                  const read_mode mode)
 -> std::generator<std::pair<std::string, ValueT>>
{
 auto cursor = make_managed_query_cursor(std::move(selection));

 while (cursor) {
  auto txn = source.make();
  auto window = read_query_window_with_retry(txn, cursor, mode);

  if (not window) {
   co_return;
  }

  for (const auto& pair : window->result_pairs) {
   co_yield to_decoded_kv_pair<ValueT>(pair);
  }
 }
}

} // namespace detail

// Suggest approximately byte-sized, algebra-compatible ranges for callers
// that want to schedule independent work explicitly:
template <typename ResultT = std::vector<select>,
          query::expression SelectionT>
requires std::ranges::range<ResultT> and
         std::same_as<std::ranges::range_value_t<ResultT>, select> and
         requires(std::vector<select> source) {
          { std::ranges::to<ResultT>(std::move(source)) } ->
            std::same_as<ResultT>;
         }
[[nodiscard]] ResultT partitions(const transaction_handle& txn,
                                 SelectionT selection,
                                 const std::int64_t target_bytes)
{
 if (0 >= target_bytes) {
  throw std::invalid_argument("partition target byte size must be positive");
 }

 if (not txn or not *txn) {
  throw std::invalid_argument("invalid FoundationDB transaction");
 }

 return std::ranges::to<ResultT>(
   detail::plan_partitions(txn, std::move(selection), target_bytes));
}

template <typename ResultT = std::vector<select>,
          query::expression SelectionT>
requires std::ranges::range<ResultT> and
         std::same_as<std::ranges::range_value_t<ResultT>, select> and
         requires(std::vector<select> source) {
          { std::ranges::to<ResultT>(std::move(source)) } ->
            std::same_as<ResultT>;
         }
[[nodiscard]] ResultT partitions(database_handle dbh,
                                 const transaction_options& options,
                                 SelectionT selection,
                                 const std::int64_t target_bytes)
{
 if (0 >= target_bytes) {
  throw std::invalid_argument("partition target byte size must be positive");
 }

 auto planned = detail::in_read_transaction(
   dbh, options,
   [selection = std::move(selection), target_bytes](transaction_handle& txn) {
     return detail::plan_partitions(txn, selection, target_bytes);
   });

 return std::ranges::to<ResultT>(std::move(planned));
}

template <typename ResultT = std::vector<select>,
          query::expression SelectionT>
requires std::ranges::range<ResultT> and
         std::same_as<std::ranges::range_value_t<ResultT>, select> and
         requires(std::vector<select> source) {
          { std::ranges::to<ResultT>(std::move(source)) } ->
            std::same_as<ResultT>;
         }
[[nodiscard]] ResultT partitions(database_handle dbh,
                                 SelectionT selection,
                                 const std::int64_t target_bytes)
{
 return partitions<ResultT>(std::move(dbh), transaction_options {},
                            std::move(selection), target_bytes);
}

/* blocks() materializes bounded managed windows; use scan(txn, ...) for direct
 * scans in a caller-owned transaction. Use partitions() when independently
 * scheduling a large range earns the additional planning overhead. */
template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>,
          query::expression SelectionT>
auto blocks(ceph::libfdb::database_handle dbh,
            const transaction_options& options,
            SelectionT selection,
            const read_mode mode = read_mode::serializable)
 -> std::generator<AssocT>
{
 return detail::blocks_selector<ValueT, AssocT>(
  detail::transaction_source(std::move(dbh), options),
  detail::make_managed_query_cursor(std::move(selection)), mode);
}

template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>,
          query::expression SelectionT>
auto blocks(ceph::libfdb::database_handle dbh,
            SelectionT selection,
            const read_mode mode = read_mode::serializable)
 -> std::generator<AssocT>
{
 return blocks<ValueT, AssocT>(std::move(dbh), transaction_options {},
                               std::move(selection), mode);
}

// Compatibility name retained for existing callers:
template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>,
          query::expression SelectionT>
auto block_generator(ceph::libfdb::database_handle dbh,
                     const transaction_options& options,
                     SelectionT selection,
                     const read_mode mode = read_mode::serializable)
 -> std::generator<AssocT>
{
 return blocks<ValueT, AssocT>(
  std::move(dbh), options, std::move(selection), mode);
}

template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>,
          query::expression SelectionT>
auto block_generator(ceph::libfdb::database_handle dbh,
                     SelectionT selection,
                     const read_mode mode = read_mode::serializable)
 -> std::generator<AssocT>
{
 return blocks<ValueT, AssocT>(std::move(dbh), std::move(selection), mode);
}

// Managed scans decode directly from independently retried result windows.
template <typename ValueT = std::string,
          query::expression SelectionT>
inline auto scan(ceph::libfdb::database_handle dbh,
                 const transaction_options& options,
                 SelectionT selection,
                 const read_mode mode = read_mode::serializable)
  -> std::generator<std::pair<std::string, ValueT>>
{
 return detail::managed_scan_selector<ValueT>(
  detail::transaction_source(std::move(dbh), options),
  std::move(selection), mode);
}

template <typename ValueT = std::string,
          query::expression SelectionT>
inline auto scan(ceph::libfdb::database_handle dbh,
                 SelectionT selection,
                 const read_mode mode = read_mode::serializable)
  -> std::generator<std::pair<std::string, ValueT>>
{
 return scan<ValueT>(std::move(dbh), transaction_options {},
                     std::move(selection), mode);
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
                      const transaction_options& options,
                      SelectionT selection,
                      const read_mode mode = read_mode::serializable)
{
 return ceph::util::collect_as<AssocT>(
  scan<ValueT>(std::move(dbh), options, std::move(selection), mode));
}

template <typename ValueT = std::string,
          typename AssocT = std::vector<std::pair<std::string, ValueT>>,
          query::expression SelectionT>
inline AssocT collect(ceph::libfdb::database_handle dbh,
                      SelectionT selection,
                      const read_mode mode = read_mode::serializable)
{
 return collect<ValueT, AssocT>(std::move(dbh), transaction_options {},
                                std::move(selection), mode);
}

} // namespace ceph::libfdb

#endif
