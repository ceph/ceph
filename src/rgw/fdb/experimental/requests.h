// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_RGW_FDB_EXPERIMENTAL_REQUESTS_H
 #define CEPH_RGW_FDB_EXPERIMENTAL_REQUESTS_H

#include "results.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iterator>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/container/small_vector.hpp>

namespace ceph::libfdb::experimental::detail {

inline std::exception_ptr make_fdb_exception_ptr(const fdb_error_t error) noexcept
try
{
 throw libfdb_exception {std::error_code(error, ::ceph::libfdb::fdb_error_category())};
}
catch (...)
{
 return std::current_exception();
}

inline void validate_transaction(const transaction_handle& txn)
{
 if (not txn or not *txn) {
  throw libfdb_exception(std::errc::invalid_argument, "invalid FoundationDB transaction");
 }
}

// Database requests own their inputs and describe only future transitions.
// Each backend supplies initiation, cancellation, and terminal delivery. A null
// pending future means advance() can finish immediately without database I/O.
struct future_request
{
 using state_type = std::optional<::ceph::libfdb::detail::future_value>;
 static constexpr bool cancellable = true;

 static FDBFuture *pending_future(state_type& state)
 {
  return state ? state->raw_ptr_or_throw() : nullptr;
 }

 static void cancel(state_type& state) noexcept
 {
  if (state) {
   state->cancel();
  }
 }

 void cancel_before_start() noexcept {}

};

// A commit must observe registration failures itself to keep confirmed commit
// success separate from stamp failure. Other requests fail before extraction.
template <typename RequestT>
auto advance(RequestT& request, typename RequestT::state_type& state,
              const fdb_error_t wait_error)
{
 if constexpr (requires { request.advance(state, wait_error); }) {
  return request.advance(state, wait_error);
 }

 if constexpr (not requires { request.advance(state, wait_error); }) {
  if (0 != wait_error) {
   throw libfdb_exception(std::error_code(wait_error, ::ceph::libfdb::fdb_error_category()));
  }

  return request.advance(state);
 }
}

template <auto StartFuture>
struct int64_request final : future_request
{
 using value_type = std::int64_t;

 transaction_handle txn;

 public:
 void start(state_type& state)
 {
  validate_transaction(txn);
  state.emplace(StartFuture(txn->raw_handle()));
 }

 std::optional<value_type> advance(state_type& state)
 {
  return ::ceph::libfdb::detail::extract_int64(std::move(*state));
 }

};

using selection_intervals = boost::container::small_vector<select, 1>;

// Metadata requests share a bounded FIFO; only its front has an installed callback.
struct pipelined_future_request
{
 static constexpr bool cancellable = true;
 static constexpr std::size_t pipeline_width = 4;

 struct state_type final
 {
  std::array<std::optional<::ceph::libfdb::detail::future_value>, pipeline_width> pending;

  std::size_t current = 0;
  std::size_t count = 0;

  void push(::ceph::libfdb::detail::future_value value)
  {
   pending[(current + count) % pipeline_width].emplace(std::move(value));
   ++count;
  }

  auto pop()
  {
   auto value = std::move(*pending[current]);
   pending[current].reset();
   current = (1 + current) % pipeline_width;
   --count;
   return value;
  }

 };

 static FDBFuture *pending_future(state_type& state)
 {
  return state.count ? state.pending[state.current]->raw_ptr_or_throw() : nullptr;
 }

 static void cancel(state_type& state) noexcept
 {
  // The front's completion may run inline and destroy state. Cancel it last.
  for (std::size_t i = 1; i < state.count; ++i) {
   state.pending[(state.current + i) % pipeline_width]->cancel();
  }
  if (state.count) {
   state.pending[state.current]->cancel();
  }
 }

 void cancel_before_start() noexcept {}

};

struct range_size_request final : pipelined_future_request
{
 using value_type = std::int64_t;

 transaction_handle txn;

 selection_intervals selections;
 std::size_t next = 0;

 std::int64_t total = 0;

 public:
 void start(state_type& state)
 {
  validate_transaction(txn);
  fill(state);
 }

 void fill(state_type& state)
 {
  while (next < std::size(selections) and state.count < pipeline_width) {
   state.push(::ceph::libfdb::detail::transaction_get_estimated_range_size(
     txn, selections[next++]));
  }
 }

 std::optional<value_type> advance(state_type& state)
 {
  if (not state.count) {
   return total;
  }

  total += ::ceph::libfdb::detail::extract_int64(state.pop());
  fill(state);
  return state.count ? std::nullopt : std::optional {total};
 }

};

struct partition_request final : pipelined_future_request
{
 using value_type = std::vector<select>;

 transaction_handle txn;

 selection_intervals selections;
 std::int64_t target_bytes;
 std::size_t next = 0;

 value_type planned;

 public:
 void start(state_type& state)
 {
  if (0 >= target_bytes) {
   throw libfdb_exception(std::errc::invalid_argument, "partition target byte size must be positive");
  }

  validate_transaction(txn);
  fill(state);
 }

 void fill(state_type& state)
 {
  while (next < std::size(selections) and state.count < pipeline_width) {
   auto& selection = selections[next++];
   selection = ::ceph::libfdb::detail::as_half_open_select(std::move(selection));
   state.push(::ceph::libfdb::detail::transaction_get_range_split_points(
     txn, ::ceph::libfdb::detail::as_byte_view(selection.begin_key),
     ::ceph::libfdb::detail::as_byte_view(selection.end_key), target_bytes));
  }
 }

 std::optional<value_type> advance(state_type& state)
 {
  if (not state.count) {
   return std::move(planned);
  }

  auto& selection = selections[next - state.count];
  auto split_points = ::ceph::libfdb::detail::extract_split_points(state.pop());
  auto ranges = ::ceph::libfdb::detail::select_ranges_from_split_points(
    split_points.result_keys, selection);

  if (std::empty(ranges)) {
   ranges.push_back(std::move(selection));
  }

  // Transfer the first buffer intact; append only subsequent intervals:
  if (std::empty(planned)) {
   planned = std::move(ranges);
  } else {
   std::ranges::move(ranges, std::back_inserter(planned));
  }

  fill(state);
  if (state.count) {
   return std::nullopt;
  }

  return std::move(planned);
 }

};

struct point_request final : future_request
{
 using value_type = point_read_result;

 transaction_handle txn;

 std::string key;
 read_mode mode = read_mode::serializable;

 public:
 void start(state_type& state)
 {
  validate_transaction(txn);
  const auto bytes = ::ceph::libfdb::detail::as_byte_view(std::string_view {key});

  if (not std::in_range<int>(std::size(bytes))) {
   throw libfdb_exception(std::errc::value_too_large, "FoundationDB key is too large");
  }

  state.emplace(fdb_transaction_get(
    txn->raw_handle(), bytes.data(), static_cast<int>(std::size(bytes)),
    read_mode::snapshot == mode));
 }

 std::optional<value_type> advance(state_type& state)
 {
  fdb_bool_t present = false;
  const std::uint8_t *data = nullptr;
  int size = 0;

  if (const auto error = fdb_future_get_value(
        state->raw_handle(), &present, &data, &size); 0 != error) {
   throw libfdb_exception {std::error_code(error, ::ceph::libfdb::fdb_error_category())};
  }

  const auto bytes = ::ceph::libfdb::detail::result_bytes(data, size);
  return point_read_result {std::move(txn), std::move(*state),
                           present ? std::optional {bytes} : std::nullopt};
 }

};

// Public window reads own their cursor; managed traversal borrows retry progress.
template <typename CursorT>
struct scan_request final : future_request
{
 static constexpr bool owns_cursor = std::is_same_v<CursorT, scan_cursor>;
 using value_type = std::conditional_t<owns_cursor, scan_window, ::ceph::libfdb::detail::query_window>;

 transaction_handle txn;

 CursorT cursor;
 read_mode mode = read_mode::serializable;

 private:
 ::ceph::libfdb::detail::query_cursor& query_progress() noexcept
 {
  if constexpr (owns_cursor) {
   return cursor.progress;
  } else {
   return cursor;
  }
 }

 public:
 void start(state_type& state)
 {
  validate_transaction(txn);
  state.emplace(query_progress().start_read(*txn, mode));
 }

 std::optional<value_type> advance(state_type& state)
 {
  auto window = query_progress().finish_read(std::move(*state));
  if constexpr (owns_cursor) {
   return scan_window {std::move(txn), std::move(window), std::move(cursor)};
  } else {
   return std::move(window);
  }
 }

};

struct watch_request final : future_request
{
 using value_type = std::tuple<>;

 ::ceph::libfdb::detail::future_value future;

 public:
 void start(state_type& state) { state.emplace(std::move(future)); }
 void cancel_before_start() noexcept { future.cancel(); }

 std::optional<value_type> advance(state_type& state)
 {
  if (const auto error = fdb_future_get_error(state->raw_handle()); 0 != error) {
   throw libfdb_exception {std::error_code(error, ::ceph::libfdb::fdb_error_category())};
  }

  return value_type {};
 }

};

template <bool WithResult>
struct commit_request final
{
 using value_type = std::conditional_t<WithResult,
  std::expected<void, std::error_code>, std::tuple<>>;
 using state_type = std::optional<::ceph::libfdb::detail::commit_attempt>;
 static constexpr bool cancellable = false;

 transaction_handle txn;

 public:
 void start(state_type& state)
 {
  validate_transaction(txn);
  state.emplace(*txn);
 }

 static FDBFuture *pending_future(state_type& state) { return state->pending_future(); }
 static void cancel(state_type&) noexcept {}
 void cancel_before_start() noexcept {}

 static std::optional<value_type> advance(state_type& state, const fdb_error_t wait_error = 0)
 {
  const auto result = state->advance(wait_error);

  if (not result) {
   return std::nullopt;
  }

  if constexpr (WithResult) {
   return *result;
  }

  if constexpr (not WithResult) {
   if (not *result) {
    throw libfdb_exception(result->error());
   }

   return value_type {};
  }
 }

};

struct replay_request final : future_request
{
 using value_type = std::tuple<>;
 static constexpr bool cancellable = false;

 transaction_handle txn;

 std::error_code error;

 public:
 void start(state_type& state)
 {
  state.emplace(::ceph::libfdb::detail::reset_for_replay_if_needed(txn, error));
 }

 std::optional<value_type> advance(state_type& state)
 {
  if (const auto ec = fdb_future_get_error(state->raw_handle()); 0 != ec) {
   throw libfdb_exception {std::error_code(ec, ::ceph::libfdb::fdb_error_category())};
  }

  ::ceph::libfdb::detail::transaction_replay_ready(*txn);

  return value_type {};
 }

};

} // namespace ceph::libfdb::experimental::detail

#endif
