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
#include <string>
#include <string_view>

namespace ceph::libfdb::layer::content {

namespace detail {

// Reuse the first-zero inspection for exact sizing and emission:
struct string_segment final
{
 std::string_view bytes;
 std::size_t first_nul;

 constexpr std::size_t encoded_size() const
 {
  if (first_nul == std::string_view::npos) {
   return 2 + bytes.size();
  }

  return 3 + bytes.size() +
   static_cast<std::size_t>(std::ranges::count(bytes.substr(first_nul + 1), '\0'));
 }
};

constexpr string_segment inspect_string_segment(const std::string_view bytes) noexcept
{
 return {bytes, bytes.find('\0')};
}

constexpr void append_encoded_string_segment(std::string& out, const string_segment segment)
{
 // FoundationDB's Tuple-layer byte-string type code makes the NUL-FF escape distinguishable from a
 // segment terminator followed by an FF byte:
 out.push_back('\x01');

 // User-readable key segments take the common bulk-append path:
 if (segment.first_nul == std::string_view::npos) {
  out.append(segment.bytes);
  out.push_back('\0');
  return;
 }

 out.append(segment.bytes.substr(0, segment.first_nul));

 for (const char c : segment.bytes.substr(segment.first_nul)) {
  out.push_back(c);

  if ('\0' == c) {
   out.push_back('\xFF');
  }
 }

 out.push_back('\0');
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

constexpr std::string_view first_segment_view(const auto& first, const auto&...)
{
 return segment_view(first);
}

constexpr auto inspect_string_segments(const auto& ...segments)
{
 return std::array {inspect_string_segment(segment_view(segments))...};
}

constexpr std::size_t encoded_string_segments_size(const auto& segments)
{
 std::size_t size = 0;

 for (const auto& segment : segments) {
  size += segment.encoded_size();
 }

 return size;
}

constexpr void append_encoded_string_segments(std::string& out, const auto& segments)
{
 for (const auto& segment : segments) {
  append_encoded_string_segment(out, segment);
 }
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
 template <typename ...Segments>
 requires key_segments<Segments...>
 explicit constexpr compiled_key(const std::string_view compiled_prefix, const Segments& ...segments)
 {
  const auto inspected = detail::inspect_string_segments(segments...);
  encoded_bytes.reserve(std::size(compiled_prefix) + detail::encoded_string_segments_size(inspected));

  encoded_bytes.append(compiled_prefix);

  detail::append_encoded_string_segments(encoded_bytes, inspected);
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

 // Append child segments to an exclusively owned compiled key. This may invalidate views into its bytes, so segment
 // arguments must not alias them:
 template <typename ...Segments>
 requires key_segments<Segments...>
 constexpr compiled_key& append(const Segments& ...segments) &
 {
  const auto inspected = detail::inspect_string_segments(segments...);
  encoded_bytes.reserve(std::size(encoded_bytes) + detail::encoded_string_segments_size(inspected));

  detail::append_encoded_string_segments(encoded_bytes, inspected);

  return *this;
 }

 template <typename Segment>
 requires concepts::stringview_convertible<Segment>
 constexpr compiled_key& operator/=(const Segment& segment) &
 {
  // Allow std::string retain spare capacity for a sequence of single-segment
  // extensions instead of forcing an exact-size allocation at every step:
  detail::append_encoded_string_segment(encoded_bytes,
    detail::inspect_string_segment(detail::segment_view(segment)));

  return *this;
 }

 template <typename Segment>
 requires concepts::stringview_convertible<Segment>
 friend constexpr compiled_key operator/(const compiled_key& lhs, const Segment& segment)
 {
  // Preserve an lvalue prefix while copying it directly into the final buffer:
  return compiled_key(lhs.encoded_bytes, segment);
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
 // Only the root is constrained; later segments are user/domain data.
 detail::require_valid_keyspace_root(detail::first_segment_view(segments...));

 return compiled_key(std::string_view {}, segments...);
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
 return compiled_key(prefix.encoded_bytes, segments...);
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
