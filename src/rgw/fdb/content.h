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
#include <compare>
#include <cstddef>
#include <iterator>
#include <string>
#include <string_view>
#include <utility>

namespace ceph::libfdb::layer::content {

namespace detail {

// Includes the segment terminator and embedded-NUL escape bytes.
constexpr std::size_t encoded_string_segment_size(const std::string_view segment)
{
 const auto embedded_nuls =
  static_cast<std::size_t>(std::ranges::count(segment, '\0'));

 return std::size(segment) + embedded_nuls + 1;
}

constexpr void append_encoded_string_segment(std::string& out,
                                             const std::string_view segment)
{
 // User-readable key segments take the common bulk-append path.
 if (!segment.contains('\0')) {
  out.append(segment);
  out.push_back('\0');
  return;
 }

 for (const char c : segment) {
  out.push_back(c);

  if ('\0' == c) {
   out.push_back(static_cast<char>(0xFF));
  }
 }

 out.push_back('\0');
}

constexpr void require_valid_keyspace(const std::string_view segment)
{
 if (segment.empty()) {
  throw ::ceph::libfdb::libfdb_exception("content key assembly requires a non-empty keyspace segment");
 }

 if (static_cast<char>(0xFF) == segment.front()) {
  throw ::ceph::libfdb::libfdb_exception("invalid keyspace beginning with 0xFF");
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

constexpr void reserve_encoded_string_segments(std::string& out,
                                               const auto& ...segments)
{
 out.reserve(out.size() +
             (encoded_string_segment_size(segment_view(segments)) +
              ... + std::size_t{0}));
}

constexpr void append_encoded_string_segments(std::string& out,
                                              const auto& ...segments)
{
 (append_encoded_string_segment(out, segment_view(segments)), ...);
}

} // namespace detail

template <typename ...Segments>
concept key_segments =
 0 < sizeof...(Segments) &&
 (concepts::stringview_convertible<Segments> && ...);

class compiled_key;

template <typename ...Segments>
constexpr compiled_key assemble(const Segments&... segments);

class compiled_key final
{
 std::string bytes_;

 public:
 compiled_key() = delete;

 private:
 explicit constexpr compiled_key(std::string bytes)
  : bytes_(std::move(bytes))
 {}

 public:
 constexpr std::size_t size() const noexcept
 {
  return bytes_.size();
 }

 constexpr auto operator<=>(const compiled_key& rhs) const noexcept
 {
  return bytes_ <=> rhs.bytes_;
 }

 constexpr bool operator==(const compiled_key& rhs) const noexcept = default;

 template <typename Segment>
 requires concepts::stringview_convertible<Segment>
 friend constexpr compiled_key operator/(compiled_key lhs, const Segment& segment)
 {
  const auto segment_bytes = detail::segment_view(segment);

  detail::reserve_encoded_string_segments(lhs.bytes_, segment_bytes);
  detail::append_encoded_string_segment(lhs.bytes_, segment_bytes);

  return lhs;
 }

 friend constexpr std::string_view libfdb_key_view(const compiled_key& key) noexcept
 {
  return key.bytes_;
 }

 private:
 template <typename ...Segments>
 friend constexpr compiled_key assemble(const Segments&... segments);
};

template <typename ...Segments>
constexpr compiled_key assemble(const Segments&... segments)
{
 static_assert(key_segments<Segments...>,
               "content key assembly requires at least one string-view-convertible keyspace segment");

 if constexpr (key_segments<Segments...>) {
  detail::require_valid_keyspace(detail::first_segment_view(segments...));

  std::string out;
  detail::reserve_encoded_string_segments(out, segments...);
  detail::append_encoded_string_segments(out, segments...);

  return compiled_key(std::move(out));
 }

 return compiled_key(std::string {});
}

constexpr compiled_key keyspace(const std::string_view segment)
{
 return assemble(segment);
}

template <std::size_t N>
constexpr compiled_key keyspace(const char (&segment)[N])
{
 static_assert(1 < N, "content keyspace literal must not be empty");

 return assemble(segment);
}

template <typename ...Segments>
constexpr compiled_key key(const Segments&... segments)
{
 return assemble(segments...);
}

inline select prefix(const compiled_key& key_prefix)
{
 return select(key_prefix);
}

} // namespace ceph::libfdb::layer::content

#endif
