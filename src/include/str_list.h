/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009-2010 Dreamhost
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_STRLIST_H
#define CEPH_STRLIST_H

#include <algorithm>
#include <concepts>
#include <functional>
#include <initializer_list>
#include <list>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "include/concepts.h"

namespace ceph {

// Split a string using the given delimiters, passing each piece back to
// a callback:
template <typename FnT>
requires std::invocable<FnT&, std::string_view>
void for_each_substr(std::string_view s, std::string_view delims, FnT&& fn)
{
  auto is_delim = [delims](char c) {
      return delims.contains(c);
    };

  for (auto first = std::ranges::find_if_not(s, is_delim);
       first != s.end();) {
    const auto last = std::ranges::find_if(first, s.end(), is_delim);

    std::invoke(fn, std::string_view { first, last });

    first = std::ranges::find_if_not(last, s.end(), is_delim);
  }
}

} // namespace ceph

namespace str_seq_details {

template <typename FnT>
concept string_view_transform =
  std::invocable<FnT&, std::string_view> &&
  std::same_as<std::remove_cvref_t<std::invoke_result_t<FnT&, std::string_view>>,
               std::string_view>;

inline std::string_view trim_ws(std::string_view str)
{
  constexpr auto whitespace = std::string_view { " \t\n\r\f\v" };

  const auto first = str.find_first_not_of(whitespace);

  if (first == str.npos)
    return {};

  const auto last = str.find_last_not_of(whitespace);

  return str.substr(first, last - first + 1);
}

template <typename FnT, typename AppendFnT>
requires string_view_transform<FnT>
void for_each_out_token(std::string_view str, std::string_view delims,
                        FnT& transform_token, AppendFnT&& append)
{
  ceph::for_each_substr(str, delims, [&](auto token) {
      auto out_token = std::string_view { std::invoke(transform_token, token) };

      if (out_token.empty())
        return;

      std::invoke(append, out_token);
    });
}

template <typename ContainerT, typename FnT>
requires string_view_transform<FnT>
void split_into(std::string_view str, std::string_view delims,
                ContainerT& out, FnT&& transform_token)
{
  ceph::util::clear(out);

  auto append = ceph::util::make_appender(out);

  if constexpr (std::same_as<std::remove_cvref_t<FnT>, std::identity>) {
    ceph::for_each_substr(str, delims, [&](auto token) {
        append.emplace(token);
      });
    return;
  }

  for_each_out_token(str, delims, transform_token, [&](std::string_view token) {
      append.emplace(token);
    });
}

} // namespace str_seq_details

/**
 * Split **str** into a sequence of strings, using any character in **delims**
 * as a delimiter. The sequence may be any suitable string container supported
 * by the Ceph Concepts helpers.
 *
 * @param [in] str String to split
 * @param [in] delims Characters used to split **str**
 * @return Sequence containing the split strings
**/
template <typename ContainerT>
ContainerT get_str_seq(std::string_view str, std::string_view delims)
{
  ContainerT out;
  str_seq_details::split_into(str, delims, out, std::identity {});
  return out;
}

// Output-parameter form for callers that need to provide the sequence.
template <typename ContainerT>
void get_str_seq(std::string_view str, std::string_view delims, ContainerT& out)
{
  str_seq_details::split_into(str, delims, out, std::identity {});
}

/**
 * Split **str** into a sequence of strings, using commas as delimiters.
 *
 * Leading and trailing whitespace is removed from each item. Empty items are
 * skipped.
 *
 * @param [in] str String to split
 * @return Sequence containing the split strings
**/
template <typename ContainerT>
ContainerT split_at_commas(std::string_view str)
{
  ContainerT out;
  str_seq_details::split_into(str, ",", out, str_seq_details::trim_ws);
  return out;
}

// Output-parameter form for callers that need to provide the sequence.
template <typename ContainerT>
void split_at_commas(std::string_view str, ContainerT& out)
{
  str_seq_details::split_into(str, ",", out, str_seq_details::trim_ws);
}

inline std::list<std::string> get_str_list(std::string_view str,
                                           std::string_view delims = ";,= \t")
{
  return get_str_seq<std::list<std::string>>(str, delims);
}

// Compatibility wrapper for the legacy list API.
inline void get_str_list(const std::string& str,
                         std::list<std::string>& str_list)
{
  get_str_seq(str, ";,= \t", str_list);
}

// Compatibility wrapper for the legacy list API.
inline void get_str_list(const std::string& str,
                         const char *delims,
                         std::list<std::string>& str_list)
{
  get_str_seq(str, delims, str_list);
}

inline std::vector<std::string> get_str_vec(std::string_view str,
                                            std::string_view delims = ";,= \t")
{
  return get_str_seq<std::vector<std::string>>(str, delims);
}

// Compatibility wrapper for the legacy vector API.
inline void get_str_vec(std::string_view str, std::vector<std::string>& str_vec)
{
  get_str_seq(str, ";,= \t", str_vec);
}

// Compatibility wrapper for the legacy vector API.
inline void get_str_vec(std::string_view str,
                        const char *delims,
                        std::vector<std::string>& str_vec)
{
  get_str_seq(str, delims, str_vec);
}

/**
 * Return a string containing the range **v** joined with **sep**.
 *
 * If **v** is empty, the function returns an empty string.
 *
 * @param [in] v Range to join as a string
 * @param [in] sep String used to join each element from **v**
 * @return Empty string if **v** is empty or concatenated string
**/
template <typename RangeT>
requires ceph::concepts::container_compatible_range<RangeT, std::string_view>
inline std::string str_join(const RangeT& v, std::string_view sep)
{
  std::string r;
  std::string_view next_sep;

  std::ranges::for_each(v, [&](const auto& s) {
      r += next_sep;
      r += std::string_view { s };
      next_sep = sep;
    });

  return r;
}

inline std::string str_join(std::initializer_list<std::string_view> v,
                            std::string_view sep)
{
  return str_join<std::initializer_list<std::string_view>>(v, sep);
}

#endif
