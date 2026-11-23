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

#ifndef CEPH_FDB_CONTENT_H
#define CEPH_FDB_CONTENT_H

#include "base.h"

#include <algorithm>
#include <array>
#include <compare>
#include <cstddef>
#include <iterator>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>

namespace ceph::libfdb::layer::content {

namespace detail {

constexpr char *copy_string_bytes(char *out, const std::string_view bytes) noexcept
{
 if (not bytes.empty()) {
  std::char_traits<char>::copy(out, bytes.data(), bytes.size());
 }

 return out + bytes.size();
}

constexpr char *write_string_segment(char *out, std::string_view bytes,
                                     const std::size_t first_nul) noexcept
{
 // FoundationDB's Tuple-layer byte-string type code makes the NUL-FF escape distinguishable from a
 // segment terminator followed by an FF byte:
 *out++ = '\x01';
 out = copy_string_bytes(out, bytes.substr(0, first_nul));

 if (std::string_view::npos != first_nul) {
  bytes.remove_prefix(first_nul);

  for (const char c : bytes) {
   *out++ = c;

   if ('\0' == c) {
    *out++ = '\xFF';
   }
  }
 }

 *out++ = '\0';

 return out;
}

// JFW: we should see how often this gets called in practice-- it may not "earn its keep", but we'll
// find out.
constexpr void require_valid_keyspace_root(const std::string_view segment)
{
 if (segment.empty()) {
  throw ::ceph::libfdb::libfdb_exception(std::errc::invalid_argument,
    "content key assembly requires a non-empty keyspace root");
 }
}

template <std::size_t N>
constexpr std::string_view segment_view(const char (&segment)[N])
{
 // Char array overloads model NUL-terminated string literals; raw byte arrays
 // should use std::string_view so that the intended size is explicit.
 if ('\0' != segment[N - 1]) {
  throw ::ceph::libfdb::libfdb_exception(std::errc::invalid_argument, "segment must be NUL-terminated");
 }

 return std::string_view(segment, N - 1);
}

constexpr std::string_view segment_view(const concepts::stringview_convertible auto& segment)
{
 return std::string_view(segment);
}

constexpr auto segment_views(const auto& ...segments)
{
 return std::array {segment_view(segments)...};
}

template <bool Escaped>
constexpr void write_string_segments(std::string& out, const std::string_view compiled_prefix,
                                      const auto& ...segments)
{
 const auto previous_size = out.size();
 const auto limit = out.max_size();

 auto size = previous_size;

 const auto add_size = [&](const std::size_t amount) {
  if (amount > limit - size) {
   throw std::length_error("content key exceeds maximum string size");
  }

  size += amount;
 };

 add_size(compiled_prefix.size());
 ((add_size(2), add_size(segments.size())), ...);

 if constexpr (Escaped) {
  (add_size(static_cast<std::size_t>(std::ranges::count(segments, '\0'))), ...);
 }

 // Preserve amortized growth when repeatedly extending an owned key:
 auto capacity = size;

 if (0 != previous_size and size > out.capacity()) {
  capacity = std::max(size, out.capacity() <= limit / 2 ? 2 * out.capacity() : limit);
 }

 out.resize_and_overwrite(capacity, [&](char *buffer, std::size_t) noexcept {
  auto *cursor = copy_string_bytes(buffer + previous_size, compiled_prefix);

  const auto emit = [&](const std::string_view segment) {
   auto first_nul = std::string_view::npos;

   if constexpr (Escaped) {
    first_nul = segment.find('\0');
   }

   cursor = write_string_segment(cursor, segment, first_nul);
  };

  (emit(segments), ...);

  return size;
 });
}

// Keep binary sizing and emission separate from the zero-free path:
[[gnu::noinline]]
constexpr void write_escaped_string_segments(std::string& out, const std::string_view compiled_prefix,
                                              const auto& ...segments)
{
 write_string_segments<true>(out, compiled_prefix, segments...);
}

template <std::size_t N>
constexpr void append_encoded_string_segments(std::string& out, const std::string_view compiled_prefix,
                                               const std::array<std::string_view, N>& segments)
{
 std::apply([&](const auto& ...views) {
  if (((std::string_view::npos == views.find('\0')) and ...)) [[likely]] {
   write_string_segments<false>(out, compiled_prefix, views...);

   return;
  }

  write_escaped_string_segments(out, compiled_prefix, views...);
 }, segments);
}

} // namespace detail

template <typename ...Segments>
concept key_segments = 0 < sizeof...(Segments) && (concepts::stringview_convertible<Segments> && ...);

class compiled_key final
{
 std::string encoded_bytes;

 public:
 compiled_key() = delete;

 private:
 template <std::size_t N>
 explicit constexpr compiled_key(const std::string_view compiled_prefix,
                                  const std::array<std::string_view, N>& segments)
 {
  detail::append_encoded_string_segments(encoded_bytes, compiled_prefix, segments);
 }

 public:
 constexpr std::size_t size() const noexcept
 {
  return std::size(encoded_bytes);
 }

 constexpr auto operator<=>(const compiled_key& rhs) const noexcept
 {
  return encoded_bytes <=> rhs.encoded_bytes;
 }

 constexpr bool operator==(const compiled_key& rhs) const noexcept = default;

 // Append child segments to an exclusively owned compiled key. This may invalidate views into its
 // bytes, so segment arguments must not alias them:
 template <typename ...Segments>
 requires key_segments<Segments...>
 constexpr compiled_key& append(const Segments& ...segments) &
 {
  detail::append_encoded_string_segments(encoded_bytes, {}, detail::segment_views(segments...));

  return *this;
 }

 template <typename Segment>
 requires concepts::stringview_convertible<Segment>
 constexpr compiled_key& operator/=(const Segment& segment) &
 {
  return append(segment);
 }

 template <typename Segment>
 requires concepts::stringview_convertible<Segment>
 friend constexpr compiled_key operator/(const compiled_key& lhs, const Segment& segment)
 {
  // Preserve an lvalue prefix while copying it directly into the final buffer:
  return compiled_key(lhs.encoded_bytes, detail::segment_views(segment));
 }

 template <typename Segment>
 requires concepts::stringview_convertible<Segment>
 friend constexpr compiled_key operator/(compiled_key&& lhs, const Segment& segment)
 {
  lhs /= segment;

  return lhs;
 }

 friend constexpr std::string_view libfdb_key_view(const compiled_key& key) noexcept
 {
  return key.encoded_bytes;
 }

 private:
 template <typename ...Segments>
 requires key_segments<Segments...>
 friend constexpr compiled_key key(const Segments& ...segments);

 template <typename ...Segments>
 requires key_segments<Segments...>
 friend constexpr compiled_key key(const compiled_key& prefix, const Segments& ...segments);

 template <typename ...Segments>
 requires key_segments<Segments...>
 friend constexpr compiled_key key(compiled_key&& prefix, const Segments& ...segments);

};

template <typename ...Segments>
requires key_segments<Segments...>
constexpr compiled_key key(const Segments& ...segments)
{
 // Only the root is constrained; later segments are user/domain data:
 const auto views = detail::segment_views(segments...);

 detail::require_valid_keyspace_root(views.front());

 return compiled_key(std::string_view {}, views);
}

template <typename ...Segments>
requires key_segments<Segments...>
constexpr compiled_key assemble(const Segments& ...segments)
{
 return key(segments...);
}

template <typename ...Segments>
requires key_segments<Segments...>
constexpr compiled_key key(const compiled_key& prefix, const Segments& ...segments)
{
 return compiled_key(prefix.encoded_bytes, detail::segment_views(segments...));
}

template <typename ...Segments>
requires key_segments<Segments...>
constexpr compiled_key key(compiled_key&& prefix, const Segments& ...segments)
{
 prefix.append(segments...);

 return prefix;
}

constexpr compiled_key keyspace(const std::string_view segment)
{
 return key(segment);
}

template <std::size_t N>
constexpr compiled_key keyspace(const char (&segment)[N])
{
 static_assert(1 < N, "content keyspace literal must not be empty");

 return key(segment);
}

inline select prefix(const compiled_key& key_prefix)
{
 return select(key_prefix);
}

} // namespace ceph::libfdb::layer::content

#endif
