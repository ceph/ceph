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

#ifndef CEPH_RGW_FDB_EXPERIMENTAL_RESULTS_H
 #define CEPH_RGW_FDB_EXPERIMENTAL_RESULTS_H

#include "../scan.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <utility>

namespace ceph::libfdb::experimental {

namespace detail {

struct point_request;
template <typename CursorT>
struct scan_request;

} // namespace detail

// Owns the FoundationDB storage behind bytes(), which remain valid until this
// result is destroyed or moved from. A missing key is a successful empty result:
class point_read_result final
{
 transaction_handle txn;

 ::ceph::libfdb::detail::future_value future;
 std::optional<std::span<const std::uint8_t>> value;

 private:
 point_read_result(transaction_handle transaction,
                   ::ceph::libfdb::detail::future_value future_owner,
                   std::optional<std::span<const std::uint8_t>> bytes) noexcept
  : txn(std::move(transaction)),
    future(std::move(future_owner)),
    value(bytes)
 {}

 public:
 point_read_result(point_read_result&& other) noexcept
  : txn(std::move(other.txn)),
    future(std::move(other.future)),
    value(std::exchange(other.value, std::nullopt))
 {}

 point_read_result(const point_read_result&) = delete;

 point_read_result& operator=(point_read_result&& other) noexcept
 {
  if (this == std::addressof(other)) {
   return *this;
  }

  // The old future must be released before its transaction:
  future = std::move(other.future);
  txn = std::move(other.txn);
  value = std::exchange(other.value, std::nullopt);

  return *this;
 }

 point_read_result& operator=(const point_read_result&) = delete;

 public:
 [[nodiscard]] bool has_value() const noexcept { return value.has_value(); }
 explicit operator bool() const noexcept { return has_value(); }

 [[nodiscard]] std::optional<std::span<const std::uint8_t>> bytes() const& noexcept
 {
  return value;
 }

 std::optional<std::span<const std::uint8_t>> bytes() const&& = delete;

 private:
 friend struct detail::point_request;
};

struct scan_row final
{
 std::span<const std::uint8_t> key;
 std::span<const std::uint8_t> value;
};

/* Tracks progress through one compiled selection. The immutable query says
 * which keys to read; this move-only cursor records where its scan has reached. */
class scan_cursor final
{
 ::ceph::libfdb::detail::query_cursor progress;

 public:
 template <query::expression SelectionT>
 explicit scan_cursor(SelectionT selection)
  : progress(std::move(selection))
 {}

 scan_cursor(scan_cursor&&) noexcept = default;

 scan_cursor(const scan_cursor&) = delete;

 scan_cursor& operator=(scan_cursor&&) noexcept = default;

 scan_cursor& operator=(const scan_cursor&) = delete;

 public:
 [[nodiscard]] explicit operator bool() const noexcept
 {
  return static_cast<bool>(progress);
 }

 private:
 template <typename CursorT>
 friend struct detail::scan_request;
};

// Owns the ready FDB future behind rows(); its byte spans expire with it:
class scan_window final
{
 transaction_handle txn;

 std::optional<::ceph::libfdb::detail::query_window> window;

 scan_cursor cursor;

 private:
 scan_window(transaction_handle transaction, ::ceph::libfdb::detail::query_window rows,
             scan_cursor next_cursor) noexcept
  : txn(std::move(transaction)),
    window(std::move(rows)),
    cursor(std::move(next_cursor))
 {}

 public:
 scan_window(scan_window&& other) noexcept
  : txn(std::move(other.txn)),
    window(std::exchange(other.window, std::nullopt)),
    cursor(std::move(other.cursor))
 {}

 scan_window(const scan_window&) = delete;

 scan_window& operator=(scan_window&& other) noexcept
 {
  if (this == std::addressof(other)) {
   return *this;
  }

  // Release the old FDB result before its transaction:
  window = std::exchange(other.window, std::nullopt);
  txn = std::move(other.txn);
  cursor = std::move(other.cursor);

  return *this;
 }

 scan_window& operator=(const scan_window&) = delete;

 public:
 [[nodiscard]] auto rows() const& noexcept
 {
  const auto rows = window ? window->result_pairs : std::span<const FDBKeyValue> {};

  return rows |
         std::views::transform([](const FDBKeyValue& row) noexcept {
          return scan_row {
           .key = ::ceph::libfdb::detail::key_bytes(row),
           .value = ::ceph::libfdb::detail::value_bytes(row)
          };
         });
 }

 auto rows() const&& = delete;

 [[nodiscard]] scan_cursor next() && noexcept
 {
  auto next_cursor = std::move(cursor);

  window.reset();
  txn.reset();

  return next_cursor;
 }

 scan_cursor next() & = delete;

 private:
 template <typename CursorT>
 friend struct detail::scan_request;
};

} // namespace ceph::libfdb::experimental

#endif
