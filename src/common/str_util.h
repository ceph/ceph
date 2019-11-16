// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_STR_UTIL_H
#define CEPH_COMMON_STR_UTIL_H

#include <array>
#include <charconv>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>

namespace ceph {
// Because *somebody* built an entire operating system on the idea of
// NUL-terminated strings, and you don't want to have to allocate
// memory just to pass a filename to open(2).
//
// If the total length of source is less than N, copies the contents
// of source to dest, appending a NUL and returning true.
//
// Otherwise, returns false.
template<std::size_t N>
bool nul_terminated_copy(std::string_view source,
			 char* dest)
{
  if (source.size() < N) {
    auto i = std::copy(std::cbegin(source), std::cend(source),
		       dest);
    *i = '\0';
    return true;
  } else {
    return false;
  }
}


// Same thing with a C-style array
template<std::size_t N>
bool nul_terminated_copy(std::string_view source,
			 char (&dest)[N])
{
  if constexpr (N == 0) {
    return true;
  }
  return nul_terminated_copy<N>(source, &dest[0]);
}

// Same thing with a standard array
template<std::size_t N>
bool nul_terminated_copy(std::string_view source,
			 std::array<char, N>& dest)
{
  if constexpr (N == 0) {
    return true;
  }
  return nul_terminated_copy<N>(source, dest.data());
}

// Same as above, except with RVO instead of caller-provided-storage.
//
// If the total length of source is less than N, returns an optional
// array containing a NUL-terminated copy.
//
// Otherwise, returns an empty optional.
template<std::size_t N>
std::optional<std::array<char, N>>
nul_terminated_copy(std::string_view source)
{
  std::optional<std::array<char, N>> dest(std::in_place);
  auto b = nul_terminated_copy<N>(source, dest->data());
  if (!b)
    dest.reset();
  return dest;
}

// Control Flow results, to control breaking substr_do (and possibly
// other things down the road.)
enum class cf {
  go, // Continue execution
  stop // Stop here
};

// Default delimiters for substr_do and associates.
inline constexpr std::string_view default_delims = ";,= \t";
inline constexpr std::string_view space = " \f\n\r\t\v";

/// Split a string using the given delimiters, passing each piece as a
/// (non-null-terminated) std::string_view to the callback.
template<typename F>
void substr_do(
  std::string_view s, F&& f,
  std::enable_if_t<!std::is_same_v<std::invoke_result_t<F, std::string_view>,
                                   cf>, std::string_view> d = default_delims)
{
  auto pos = s.find_first_not_of(d);
  while (pos != s.npos) {
    s.remove_prefix(pos);
    auto end = s.find_first_of(d);
    f(s.substr(0, end));
    pos = s.find_first_not_of(d, end);
  }
}

// As above, but let the function specify whether to continue after
// every call. f, must return a member of cf at every
// execution. cf::stop will, unsurprisingly, stop.
template<typename F>
void substr_do(
  std::string_view s, F&& f,
  std::enable_if_t<std::is_same_v<std::invoke_result_t<F, std::string_view>,
                                  cf>, std::string_view> d = default_delims)
{
  auto pos = s.find_first_not_of(d);
  while (pos != s.npos) {
    s.remove_prefix(pos);
    auto end = s.find_first_of(d);
    cf b = f(s.substr(0, end));
    if (b == cf::stop)
      break;
    pos = s.find_first_not_of(d, end);
  }
}

// Insert each delimited substring into a container, specified by the
// output iterator. Will insert a std::string_view if possible, and a
// std::string otherwise.
template<typename Iterator>
Iterator substr_insert(std::string_view s,
		       Iterator i,
		       std::string_view d = default_delims)
{
  substr_do(s,
	    [&](auto&& s) {
	      if constexpr (std::is_assignable_v<decltype(*i),
			                         std::string_view>) {
	        *i = s;
	      } else {
	        *i = std::string(s);
	      }
	      ++i;
	    }, d);
  return i;
}

// Wrappers around std::from_chars.
//
// Why do we want this instead of strtol and friends? Because the
// string doesn't have to be NUL-terminated! (Also, for a lot of
// purposes, just putting a string_view in and getting an optional out
// is friendly.)
//
// Returns the found number on success. Returns an empty optional on
// failure OR on trailing characters.
template<typename T>
std::optional<T> parse(std::string_view s, int base = 10)
{
  T t;
  auto r = std::from_chars(s.data(), s.data() + s.size(), t, base);
  if ((r.ec != std::errc{}) || (r.ptr != s.data() + s.size())) {
    return std::nullopt;
  }
  return t;
}

// As above, but succeed on trailing characters and trim the supplied
// string_view to remove the parsed number. Set the supplied
// string_view to empty if it ends with the number.
template<typename T>
std::optional<T> consume(std::string_view& s, int base = 10)
{
  T t;
  auto r = std::from_chars(s.data(), s.data() + s.size(), t, base);
  if ((r.ec != std::errc{}))
    return std::nullopt;

  if (r.ptr == s.data() + s.size()) {
    s = std::string_view{};
  } else {
    s.remove_prefix(r.ptr - s.data());
  }
  return t;
}
}
#endif // CEPH_COMMON_STR_UTIL_H
