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

#include <compare>
#include <cstddef>
#include <iterator>
#include <string>
#include <string_view>

namespace ceph::libfdb::layer::content {

namespace detail {

// Includes the Tuple byte-string type code and segment terminator. An embedded
// NUL may grow the string by one further escape byte during encoding:
constexpr std::size_t encoded_string_segment_reserve_size(
 const std::string_view segment) noexcept
{
 return 2 + std::size(segment);
}

constexpr void append_encoded_string_segment(std::string& out,
                                             const std::string_view segment)
{
 // FoundationDB's Tuple-layer byte-string type code makes the NUL-FF escape
 // distinguishable from a segment terminator followed by an FF byte:
 out.push_back('\x01');

 // User-readable key segments take the common bulk-append path.
 if (!segment.contains('\0')) {
  out.append(segment);
  out.push_back('\0');
  return;
 }

 for (const char c : segment) {
  out.push_back(c);

  if ('\0' == c) {
   out.push_back('\xFF');
  }
 }

 out.push_back('\0');
}

constexpr void require_valid_keyspace_root(const std::string_view segment)
{
 if (segment.empty()) {
  throw ::ceph::libfdb::libfdb_exception("content key assembly requires a non-empty keyspace root");
 }
}

template <std::size_t N>
constexpr std::string_view segment_view(const char (&segment)[N])
{
 // Char array overloads model NUL-terminated string literals; raw byte arrays
 // should use std::string_view so that the intended size is explicit.
 if ('\0' != segment[N - 1]) {
  throw ::ceph::libfdb::libfdb_exception("segment must be NUL-terminated");
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

constexpr std::size_t encoded_string_segments_reserve_size(
 const auto& ...segments)
{
 return (encoded_string_segment_reserve_size(segment_view(segments)) + ... + std::size_t {0});
}

constexpr void append_encoded_string_segments(std::string& out,
                                              const auto& ...segments)
{
 (append_encoded_string_segment(out, segment_view(segments)), ...);
}

} // namespace detail

template <typename ...Segments>
concept key_segments =
 0 < sizeof...(Segments) && (concepts::stringview_convertible<Segments> && ...);
struct compiled_key final
{
 std::string encoded_bytes;

 public:
 compiled_key() = delete;

 private:
 template <typename ...Segments>
 requires key_segments<Segments...>
 explicit constexpr compiled_key(const std::string_view compiled_prefix,
                                 const Segments& ...segments)
 {
  encoded_bytes.reserve(std::size(compiled_prefix) + detail::encoded_string_segments_reserve_size(segments...));

  encoded_bytes.append(compiled_prefix);

  detail::append_encoded_string_segments(encoded_bytes, segments...);
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

 // Append child segments to an exclusively owned compiled key. This may
 // invalidate views into its bytes, so segment arguments must not alias them:
 template <typename ...Segments>
 requires key_segments<Segments...>
 constexpr compiled_key& append(const Segments& ...segments) &
 {
  encoded_bytes.reserve(std::size(encoded_bytes) + detail::encoded_string_segments_reserve_size(segments...));

  detail::append_encoded_string_segments(encoded_bytes, segments...);

  return *this;
 }

 template <typename Segment>
 requires concepts::stringview_convertible<Segment>
 constexpr compiled_key& operator/=(const Segment& segment) &
 {
  // Allow std::string retain spare capacity for a sequence of single-segment
  // extensions instead of forcing an exact-size allocation at every step:
  detail::append_encoded_string_segment(encoded_bytes, detail::segment_view(segment));

  return *this;
 }

 template <typename Segment>
 requires concepts::stringview_convertible<Segment>
 friend constexpr compiled_key operator/(const compiled_key& lhs,
                                         const Segment& segment)
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
 friend constexpr compiled_key key(const compiled_key& prefix,
                                   const Segments& ...segments);

 template <typename ...Segments>
 requires key_segments<Segments...>
 friend constexpr compiled_key key(compiled_key&& prefix,
                                   const Segments& ...segments);
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
constexpr compiled_key key(const compiled_key& prefix,
                           const Segments& ...segments)
{
 return compiled_key(prefix.encoded_bytes, segments...);
}

template <typename ...Segments>
requires key_segments<Segments...>
constexpr compiled_key key(compiled_key&& prefix,
                           const Segments& ...segments)
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
