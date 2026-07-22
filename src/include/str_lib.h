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

#include <concepts>
#include <cstddef>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <ranges>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/container_concepts.h"

namespace ceph::str_lib_detail {

inline constexpr char split_str_default_delims[] = ";,= \t";
inline constexpr char comma_delims[] = ",";

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
constexpr void for_each_substr(std::string_view s, DelimsT delims, FnT&& fn)
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
class spliterator {
  std::string_view str; // full string
  std::string_view delims; // delimiters

  using size_type = std::string_view::size_type;
  size_type pos = 0; // start position of current part
  std::string_view part; // view of current part

  // return the next part after the given position
  constexpr std::string_view next(size_type end) {
    pos = str.find_first_not_of(delims, end);
    if (pos == str.npos) {
      return {};
    }

    const auto part_end = str.find_first_of(delims, pos);
    return str.substr(pos, part_end - pos);
  }
 public:
  static constexpr std::string_view default_delims = ";,= \t\n";

  // types required by std::iterator_traits
  using difference_type = int;
  using value_type = std::string_view;
  using pointer = const value_type *;
  using reference = const value_type&;
  using iterator_category = std::forward_iterator_tag;

  spliterator() = default;

  constexpr spliterator(std::string_view str, std::string_view delims)
    : str(str), delims(delims), pos(0), part(next(0))
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
  friend constexpr bool operator!=(const spliterator& lhs, const spliterator& rhs) {
    return !(lhs == rhs);
  }
};

// Represents an immutable range of split string parts.
//
// Ranged-for loop example:
//
//   for (std::string_view s : split(input)) {
//     ...
//
// Container initialization example:
//
//   auto parts = split(input);
//
//   std::vector<std::string> strings;
//   strings.assign(parts.begin(), parts.end());
//
class split {
  std::string_view str; // full string
  std::string_view delims; // delimiters
 public:
  constexpr split(std::string_view str,
                  std::string_view delims = spliterator::default_delims)
    : str(str), delims(delims) {}

  using iterator = spliterator;
  using const_iterator = spliterator;

  constexpr iterator begin() const { return {str, delims}; }
  constexpr const_iterator cbegin() const { return {str, delims}; }

  constexpr iterator end() const { return {}; }
  constexpr const_iterator cend() const { return {}; }
};

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
  ceph::for_each_substr(s, ceph::str_lib_detail::split_str_default_delims,
                        std::forward<FnT>(fn));
}

} // namespace ceph

namespace ceph::str_lib_detail {

template <typename FnT>
concept string_view_transform =
  std::invocable<FnT&, std::string_view> &&
  std::same_as<std::remove_cvref_t<std::invoke_result_t<FnT&, std::string_view>>,
               std::string_view>;

template <typename RangeT>
concept sized_forward_string_view_range =
  std::ranges::forward_range<const RangeT&> &&
  std::ranges::sized_range<const RangeT&> &&
  ceph::concepts::container_compatible_range<const RangeT&, std::string_view>;

template <typename ContainerT>
concept split_output_container =
  requires (ContainerT& out, std::string_view token) {
    ceph::util::clear(out);
    ceph::util::make_appender(out).emplace(token);
  };

template <template <typename...> typename ContainerTemplate>
concept split_output_container_template =
  split_output_container<ContainerTemplate<std::string>>;

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

constexpr std::string_view trim_ws(std::string_view str)
{
  constexpr auto whitespace = std::string_view { " \t\n\r\f\v" };

  const auto first = str.find_first_not_of(whitespace);

  if (first == str.npos) {
    return {};
  }

  const auto last = str.find_last_not_of(whitespace);

  return str.substr(first, last - first + 1);
}

template <typename ContainerT, typename FnT>
requires split_output_container<ContainerT> && string_view_transform<FnT>
constexpr void split_into(std::string_view str, std::string_view delims,
                          ContainerT& out, FnT&& transform_token)
{
  ceph::util::clear(out);

  auto append = ceph::util::make_appender(out);

  ceph::str_lib_detail::for_each_substr(str, delims, [&](std::string_view token) {
    if constexpr (!std::same_as<std::remove_cvref_t<FnT>, std::identity>) {
      token = std::invoke(transform_token, token);

      if (std::empty(token)) {
        return;
      }
    }

    if constexpr (std::ranges::bidirectional_range<ContainerT> &&
                  !std::ranges::random_access_range<ContainerT> &&
                  requires (ContainerT& c) {
                    c.emplace_back(std::begin(token), std::end(token));
                  }) {
      out.emplace_back(std::begin(token), std::end(token));
      return;
    }

    append.emplace(token);
  });
}

} // namespace ceph::str_lib_detail

namespace ceph {

/**
 * Split **str** into a sequence of strings, using any character in **delims**
 * as a delimiter. The sequence may be any suitable string container supported
 * by the Ceph Concepts helpers.
 *
 * @param [in] str String to split
 * @param [in] delims Characters used to split **str**
 * @return Sequence containing the split strings
**/
template <typename ContainerT = std::vector<std::string>>
constexpr ContainerT split_str(std::string_view str,
                               std::string_view delims =
                                 ceph::str_lib_detail::split_str_default_delims)
{
  ContainerT out;
  ceph::str_lib_detail::split_into(str, delims, out, std::identity {});
  return out;
}

template <template <typename...> typename ContainerTemplate>
requires ceph::str_lib_detail::split_output_container_template<ContainerTemplate>
constexpr ContainerTemplate<std::string> split_str(
  std::string_view str,
  std::string_view delims = ceph::str_lib_detail::split_str_default_delims)
{
  return ceph::split_str<ContainerTemplate<std::string>>(str, delims);
}

// Output-parameter form for callers that need to provide the sequence.
template <typename ContainerT>
requires ceph::str_lib_detail::split_output_container<ContainerT>
constexpr void split_str(std::string_view str, std::string_view delims,
                         ContainerT& out)
{
  ceph::str_lib_detail::split_into(str, delims, out, std::identity {});
}

// Output-parameter form using the default string-list delimiters.
template <typename ContainerT>
requires ceph::str_lib_detail::split_output_container<ContainerT>
constexpr void split_str(std::string_view str, ContainerT& out)
{
  ceph::split_str(str, ceph::str_lib_detail::split_str_default_delims, out);
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
template <typename ContainerT = std::vector<std::string>>
constexpr ContainerT comma_split(std::string_view str)
{
  ContainerT out;
  ceph::str_lib_detail::split_into(str, ceph::str_lib_detail::comma_delims, out,
                                   ceph::str_lib_detail::trim_ws);
  return out;
}

template <template <typename...> typename ContainerTemplate>
requires ceph::str_lib_detail::split_output_container_template<ContainerTemplate>
constexpr ContainerTemplate<std::string> comma_split(std::string_view str)
{
  return ceph::comma_split<ContainerTemplate<std::string>>(str);
}

// Output-parameter form for callers that need to provide the sequence.
template <typename ContainerT>
requires ceph::str_lib_detail::split_output_container<ContainerT>
constexpr void comma_split(std::string_view str, ContainerT& out)
{
  ceph::str_lib_detail::split_into(str, ceph::str_lib_detail::comma_delims, out,
                                   ceph::str_lib_detail::trim_ws);
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
std::string str_join(const RangeT& v, std::string_view sep)
{
  std::string r;

  if constexpr (ceph::str_lib_detail::sized_forward_string_view_range<RangeT>) {
    r.reserve(ceph::str_lib_detail::join_size(v, sep));
  }

  std::string_view next_sep;

  for (const auto& s : v) {
    r += next_sep;
    r += std::string_view { s };
    next_sep = sep;
  }

  return r;
}

inline std::string str_join(std::initializer_list<std::string_view> v,
                            std::string_view sep)
{
  return ceph::str_join<std::initializer_list<std::string_view>>(v, sep);
}

} // namespace ceph

#endif
