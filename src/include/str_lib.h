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

#ifndef CEPH_STR_LIB_H
#define CEPH_STR_LIB_H

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/container_concepts.h"

namespace ceph::str_lib_detail {

inline constexpr std::string_view default_delimiters = ";,= \t";

template <typename DelimsT>
concept string_delimiters =
  requires (std::string_view s, DelimsT delims) {
    { s.find_first_not_of(delims) } ->
      std::convertible_to<std::string_view::size_type>;
    { s.find_first_of(delims) } ->
      std::convertible_to<std::string_view::size_type>;
  };

template <typename DelimsT, typename FnT>
requires string_delimiters<DelimsT> && std::invocable<FnT&, std::string_view>
constexpr void for_each_substr(std::string_view s, const DelimsT& delims,
                               FnT&& fn)
{
  auto pos = s.find_first_not_of(delims);

  while (pos != s.npos) {
    s.remove_prefix(pos);
    const auto end = s.find_first_of(delims);
    std::invoke(fn, s.substr(0, end));

    if (end == s.npos) {
      return;
    }

    pos = s.find_first_not_of(delims, end);
  }
}

} // namespace ceph::str_lib_detail

namespace ceph {

// A forward iterator over the parts of a split string:
template <ceph::str_lib_detail::string_delimiters DelimsT = std::string_view>
class spliterator {
  std::string_view str; // full string
  DelimsT delims {}; // delimiters

  using size_type = std::string_view::size_type;
  size_type pos = 0; // start position of current part
  std::string_view part; // view of current part

  // Return the next part after the given position:
  constexpr std::string_view next(size_type end) {
    pos = str.find_first_not_of(delims, end);
    if (pos == str.npos) {
      return {};
    }

    const auto part_end = str.find_first_of(delims, pos);
    return str.substr(pos, part_end - pos);
  }
 public:
  // types required by std::iterator_traits
  using difference_type = int;
  using value_type = std::string_view;
  using pointer = const value_type *;
  using reference = const value_type&;
  using iterator_category = std::forward_iterator_tag;

  constexpr spliterator() = default;

  constexpr spliterator(std::string_view str, DelimsT delims)
    : str(str), delims(std::move(delims)), part(next(0))
  {}

  constexpr spliterator& operator++() {
    part = next(pos + std::size(part));
    return *this;
  }
  constexpr spliterator operator++(int) {
    spliterator tmp = *this;
    ++*this;
    return tmp;
  }

  constexpr reference operator*() const { return part; }
  constexpr pointer operator->() const { return &part; }

  friend constexpr bool operator==(const spliterator& lhs, const spliterator& rhs) {
    return lhs.part.data() == rhs.part.data()
        && std::size(lhs.part) == std::size(rhs.part);
  }
};

// Represents an immutable range of split string parts. Each supplied delimiter
// character separates parts, and adjacent delimiters do not produce empty
// parts. The default delimiters are semicolon, comma, equals, space, and tab.
// The returned string views refer to the input string, which must remain valid
// while they are used.
//
// Range-based for loop example:
//
//   for (std::string_view part : split(input)) {
//     ...
//   }
//
// Container initialization example:
//
//   auto parts = split(input);
//
//   std::vector<std::string> strings;
//   strings.assign(parts.begin(), parts.end());
//
template <ceph::str_lib_detail::string_delimiters DelimsT = std::string_view>
class split_view : public std::ranges::view_interface<split_view<DelimsT>> {
  std::string_view str; // full string
  DelimsT delims; // delimiters
 public:
  constexpr split_view(std::string_view str)
    requires std::same_as<DelimsT, std::string_view>
    : split_view(str, ceph::str_lib_detail::default_delimiters)
  {}

  constexpr split_view(std::string_view str, DelimsT delims)
    : str(str), delims(std::move(delims)) {}

  using iterator = spliterator<DelimsT>;
  using const_iterator = spliterator<DelimsT>;

  constexpr iterator begin() const { return {str, delims}; }
  constexpr const_iterator cbegin() const { return {str, delims}; }

  constexpr iterator end() const { return {}; }
  constexpr const_iterator cend() const { return {}; }
};

constexpr split_view<> split(
  std::string_view str,
  std::string_view delims = ceph::str_lib_detail::default_delimiters)
{
  return { str, delims };
}

constexpr split_view<char> split(std::string_view str, char delim)
{
  return { str, delim };
}

// Split a string using the given delimiters, passing each piece back to
// a callback.
template <typename DelimsT, typename FnT>
requires ceph::str_lib_detail::string_delimiters<DelimsT> &&
  std::invocable<FnT&, std::string_view>
constexpr void for_each_substr(std::string_view s, DelimsT delims, FnT&& fn)
{
  ceph::str_lib_detail::for_each_substr(s, delims, std::forward<FnT>(fn));
}

template <typename FnT>
requires std::invocable<FnT&, std::string_view>
constexpr void for_each_substr(std::string_view s, FnT&& fn)
{
  ceph::for_each_substr(s, ceph::str_lib_detail::default_delimiters,
                        std::forward<FnT>(fn));
}

} // namespace ceph

template <typename DelimsT>
inline constexpr bool
std::ranges::enable_borrowed_range<ceph::split_view<DelimsT>> = true;

namespace ceph::str_lib_detail {

template <typename RangeT>
concept sized_forward_string_view_range =
  std::ranges::forward_range<const RangeT&> &&
  std::ranges::sized_range<const RangeT&> &&
  ceph::concepts::container_compatible_range<const RangeT&, std::string_view>;

template <typename RangeT>
requires sized_forward_string_view_range<RangeT>
constexpr std::size_t join_size(const RangeT& v, std::string_view sep)
{
  const auto count = std::ranges::size(v);

  if (0 == count) {
    return 0;
  }

  std::size_t size = std::size(sep) * (count - 1);

  for (const auto& s : v) {
    size += std::size(std::string_view { s });
  }

  return size;
}

} // namespace ceph::str_lib_detail

namespace ceph {

/**
 * Split **str** into an owning vector of strings, using any character in
 * **delims** as a delimiter.
 *
 * @param [in] str String to split
 * @param [in] delims Characters used to split **str**
 * @return Vector containing the split strings
**/
constexpr std::vector<std::string> split_strings(
  std::string_view str,
  std::string_view delims = ceph::str_lib_detail::default_delimiters)
{
  const auto parts = ceph::split(str, delims);
  return { parts.begin(), parts.end() };
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
requires ceph::concepts::container_compatible_range<const RangeT&, std::string_view>
constexpr std::string str_join(const RangeT& v, std::string_view sep)
{
  if constexpr (ceph::str_lib_detail::sized_forward_string_view_range<RangeT>) {
    std::string r;
    const auto output_size = ceph::str_lib_detail::join_size(v, sep);
    r.resize_and_overwrite(
      output_size, [&v, sep, output_size](char* output, std::size_t) {
        auto cursor = output;
        std::string_view next_sep;

        for (const auto& s : v) {
          cursor = std::ranges::copy(next_sep, cursor).out;
          cursor = std::ranges::copy(std::string_view { s }, cursor).out;
          next_sep = sep;
        }

        return output_size;
      });

    return r;
  } else {
    std::string r;
    std::string_view next_sep;

    for (const auto& s : v) {
      r += next_sep;
      r += std::string_view { s };
      next_sep = sep;
    }

    return r;
  }
}

constexpr std::string str_join(std::initializer_list<std::string_view> v,
                               std::string_view sep)
{
  return ceph::str_join<std::initializer_list<std::string_view>>(v, sep);
}

} // namespace ceph

#endif
