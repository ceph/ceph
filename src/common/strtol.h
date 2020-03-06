// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_STRTOL_H
#define CEPH_COMMON_STRTOL_H

#if __has_include(<charconv>)
#include <charconv>
#endif // __has_include(<charconv>)
#include <cinttypes>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>


namespace ceph {
#if __has_include(<charconv>)
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
auto parse(std::string_view s, int base = 10)
  -> std::enable_if_t<std::is_integral_v<T>, std::optional<T>>
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
auto consume(std::string_view& s, int base = 10)
  -> std::enable_if_t<std::is_integral_v<T>, std::optional<T>>
{
  T t;
  auto r = std::from_chars(s.data(), s.data() + s.size(), t, base);
  if (r.ec != std::errc{})
    return std::nullopt;

  if (r.ptr == s.data() + s.size()) {
    s = std::string_view{};
  } else {
    s.remove_prefix(r.ptr - s.data());
  }
  return t;
}
// Sadly GCC is missing the floating point versions.
#else // __has_include(<charconv>)
template<typename T>
auto parse(std::string_view sv, int base = 10)
  -> std::enable_if_t<std::is_integral_v<T>, std::optional<T>>
{
  std::string s(sv);
  char* end = nullptr;
  std::conditional_t<std::is_signed_v<T>, std::intmax_t, std::uintmax_t> v;
  errno = 0;

  if (s.size() > 0 && std::isspace(s[0]))
    return std::nullopt;

  if constexpr (std::is_signed_v<T>) {
    v = std::strtoimax(s.data(), &end, base);
  } else {
    if (s.size() > 0 && s[0] == '-')
      return std::nullopt;
    v = std::strtoumax(s.data(), &end, base);
  }
  if (errno != 0 ||
      end != s.data() + s.size() ||
      v > std::numeric_limits<T>::max() ||
      v < std::numeric_limits<T>::min())
    return std::nullopt;
  return static_cast<T>(v);
}

template<typename T>
auto consume(std::string_view& sv, int base = 10)
  -> std::enable_if_t<std::is_integral_v<T>, std::optional<T>>
{
  std::string s(sv);
  char* end = nullptr;
  std::conditional_t<std::is_signed_v<T>, std::intmax_t, std::uintmax_t> v;
  errno = 0;

  if (s.size() > 0 && std::isspace(s[0]))
    return std::nullopt;

  if constexpr (std::is_signed_v<T>) {
    v = std::strtoimax(s.data(), &end, base);
  } else {
    if (s.size() > 0 && s[0] == '-')
      return std::nullopt;
    v = std::strtoumax(s.data(), &end, base);
  }
  if (errno != 0 ||
      end == s.data() ||
      v > std::numeric_limits<T>::max() ||
      v < std::numeric_limits<T>::min())
    return std::nullopt;
  if (end == s.data() + s.size()) {
    sv = std::string_view{};
  } else {
    sv.remove_prefix(end - s.data());
  }
  return static_cast<T>(v);
}
#endif // __has_include(<charconv>)
} // namespace ceph

long long strict_strtoll(const char *str, int base, std::string *err);

int strict_strtol(const char *str, int base, std::string *err);

double strict_strtod(const char *str, std::string *err);

float strict_strtof(const char *str, std::string *err);

uint64_t strict_iecstrtoll(const char *str, std::string *err);

template<typename T>
T strict_iec_cast(const char *str, std::string *err);

uint64_t strict_sistrtoll(const char *str, std::string *err);

template<typename T>
T strict_si_cast(const char *str, std::string *err);

/* On enter buf points to the end of the buffer, e.g. where the least
 * significant digit of the input number will be printed. Returns pointer to
 * where the most significant digit were printed, including zero padding.
 * Does NOT add zero at the end of buffer, this is responsibility of the caller.
 */
template<typename T, const unsigned base = 10, const unsigned width = 1>
static inline
char* ritoa(T u, char *buf)
{
  static_assert(std::is_unsigned<T>::value, "signed types are not supported");
  static_assert(base <= 16, "extend character map below to support higher bases");
  unsigned digits = 0;
  while (u) {
    *--buf = "0123456789abcdef"[u % base];
    u /= base;
    digits++;
  }
  while (digits++ < width)
    *--buf = '0';
  return buf;
}

#endif
