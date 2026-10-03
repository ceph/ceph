// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */

#pragma once

#include <cstdint>
#include <string_view>
#include <utility>
#include <vector>

/*
 * Parser for the HTTP Range header field, RFC 9110 Section 14.1.1:
 *
 *   ranges-specifier = range-unit "=" range-set
 *   range-set        = 1#range-spec
 *   range-spec       = int-range / suffix-range / other-range
 *   int-range        = first-pos "-" [ last-pos ]
 *   suffix-range     = "-" suffix-length
 *
 * The parser is purely syntactic: it does not resolve positions against the
 * length of the selected representation, and it does not decide what to do
 * with a range-set that holds more than one range-spec. Both are the caller's
 * business.
 */

/* One range-spec. An int-range carries first and, when has_last is set, last.
 * A suffix-range carries only suffix_length, meaning the final suffix_length
 * bytes of the representation. */
struct rgw_byte_range {
  bool is_suffix = false;
  bool has_last = false;
  uint64_t first = 0;
  uint64_t last = 0;
  uint64_t suffix_length = 0;

  friend bool operator==(const rgw_byte_range& l, const rgw_byte_range& r) {
    if (l.is_suffix != r.is_suffix)
      return false;
    if (l.is_suffix)
      return l.suffix_length == r.suffix_length;
    if (l.first != r.first || l.has_last != r.has_last)
      return false;
    return !l.has_last || l.last == r.last;
  }
};

enum class rgw_range_parse_result {
  /* range-set held at least one range-spec and every one of them parsed */
  ok,
  /* the range-unit is not "bytes". RFC 9110 14.2 requires an origin server to
   * ignore a Range header field whose range unit it does not understand, so
   * this is not an error: the caller serves the whole representation. */
  not_bytes,
  /* syntactically invalid ranges-specifier. RFC 9110 14.2 lets a server ignore
   * or reject one; which of the two happens is the caller's policy. */
  invalid,
  /* more range-specs than the caller is willing to hold. Called out separately
   * from `invalid` because the request is well formed, and because a long
   * range-set is the shape a denial-of-service attempt takes (RFC 9110 17.15). */
  too_many,
};

namespace rgw_range_detail {

inline bool is_ows(char c) { return c == ' ' || c == '\t'; }

inline std::string_view trim_ows(std::string_view s) {
  while (!s.empty() && is_ows(s.front()))
    s.remove_prefix(1);
  while (!s.empty() && is_ows(s.back()))
    s.remove_suffix(1);
  return s;
}

inline bool equals_ignore_case(std::string_view a, std::string_view b) {
  if (a.size() != b.size())
    return false;
  for (size_t i = 0; i < a.size(); ++i) {
    char l = a[i], r = b[i];
    if (l >= 'A' && l <= 'Z') l = l - 'A' + 'a';
    if (r >= 'A' && r <= 'Z') r = r - 'A' + 'a';
    if (l != r)
      return false;
  }
  return true;
}

/*
 * 1*DIGIT with no sign, no whitespace and no trailing junk.
 *
 * first-pos, last-pos and suffix-length are 1*DIGIT with no upper bound, so a
 * value too large for uint64_t is well formed and may not be rejected. It is
 * saturated instead, which preserves its meaning in every position: RFC 9110
 * 14.1.2 reads an out-of-length last-pos as the remainder of the
 * representation and an over-long suffix-length as the whole representation,
 * and a first-pos past the end is unsatisfiable either way.
 */
inline bool parse_digits(std::string_view s, uint64_t* out, bool* saturated) {
  if (s.empty())
    return false;
  uint64_t v = 0;
  bool sat = false;
  for (char c : s) {
    if (c < '0' || c > '9')
      return false;
    unsigned d = c - '0';
    if (v > (UINT64_MAX - d) / 10)
      sat = true;
    else
      v = v * 10 + d;
  }
  *out = sat ? UINT64_MAX : v;
  *saturated = sat;
  return true;
}

/* Order two 1*DIGIT strings by value without converting them, so that
 * last-pos < first-pos can still be decided once either has saturated. */
inline bool digits_less(std::string_view a, std::string_view b) {
  while (a.size() > 1 && a.front() == '0')
    a.remove_prefix(1);
  while (b.size() > 1 && b.front() == '0')
    b.remove_prefix(1);
  if (a.size() != b.size())
    return a.size() < b.size();
  return a < b;
}

} // namespace rgw_range_detail

/*
 * Parse a Range header field value into `ranges`, which is cleared first.
 * `max_ranges` bounds the number of range-specs accepted; 0 means unbounded.
 *
 * Empty list elements are skipped, as RFC 9110 5.6.1 requires of a recipient
 * parsing a comma-separated list.
 */
inline rgw_range_parse_result rgw_parse_byte_ranges(
    std::string_view spec, size_t max_ranges,
    std::vector<rgw_byte_range>* ranges)
{
  using namespace rgw_range_detail;

  ranges->clear();

  const size_t eq = spec.find('=');
  if (eq == std::string_view::npos)
    return rgw_range_parse_result::not_bytes;
  if (!equals_ignore_case(trim_ows(spec.substr(0, eq)), "bytes"))
    return rgw_range_parse_result::not_bytes;

  std::string_view set = spec.substr(eq + 1);
  std::vector<rgw_byte_range> parsed;

  while (!set.empty()) {
    const size_t comma = set.find(',');
    std::string_view elem = trim_ows(set.substr(0, comma));
    set = (comma == std::string_view::npos) ? std::string_view{}
                                            : set.substr(comma + 1);
    if (elem.empty())
      continue; /* legacy empty list element */

    const size_t dash = elem.find('-');
    if (dash == std::string_view::npos)
      return rgw_range_parse_result::invalid;

    rgw_byte_range r;
    bool saturated = false;
    if (dash == 0) {
      /* suffix-range = "-" suffix-length */
      r.is_suffix = true;
      if (!parse_digits(elem.substr(1), &r.suffix_length, &saturated))
        return rgw_range_parse_result::invalid;
    } else {
      /* int-range = first-pos "-" [ last-pos ] */
      const std::string_view first = elem.substr(0, dash);
      if (!parse_digits(first, &r.first, &saturated))
        return rgw_range_parse_result::invalid;
      const std::string_view last = elem.substr(dash + 1);
      if (!last.empty()) {
        bool last_saturated = false;
        if (!parse_digits(last, &r.last, &last_saturated))
          return rgw_range_parse_result::invalid;
        r.has_last = true;
        /* "An int-range is invalid if the last-pos value is present and less
         * than the first-pos." RFC 9110 14.1.1. Once either position has
         * saturated the stored values no longer order them, so fall back to
         * comparing the digits. */
        const bool reversed = (saturated || last_saturated)
                                  ? digits_less(last, first)
                                  : r.last < r.first;
        if (reversed)
          return rgw_range_parse_result::invalid;
      }
    }

    if (max_ranges && parsed.size() == max_ranges)
      return rgw_range_parse_result::too_many;
    parsed.push_back(r);
  }

  /* range-set = 1#range-spec, so an empty set is invalid rather than ignorable */
  if (parsed.empty())
    return rgw_range_parse_result::invalid;

  *ranges = std::move(parsed);
  return rgw_range_parse_result::ok;
}
