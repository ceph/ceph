// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cctype>
#include <string>
#include <string_view>

/// \file common/string_view_ci.h
///
/// \brief A case insensitive string_view
///
/// We want to compare strings that aren't null terminated, and we
/// want that comparison to be case insensitive. Based on an example
/// from CPPReference and musl.


namespace ceph {
/// \brief Case-insensitive character traits
///
/// These are specified only for `char` as Ceph does not use wide
/// characters anywhere.
///
/// These may be supplied as the second template argument to
/// std::string_view or std::string.
struct char_traits_ci : public std::char_traits<char> {
  using char_traits<char>::char_type;
  using char_traits<char>::int_type;
  using char_traits<char>::pos_type;
  static bool eq(char_type lhs, char_type rhs) {
    return std::tolower(lhs) == std::tolower(rhs);
  }

  static bool lt(char_type lhs, char_type rhs) {
    return std::tolower(lhs) < std::tolower(rhs);
  }

  static bool compare(const char_type* lhs, const char_type* rhs,
		      std::size_t n) {
    if (n == 0) return 0;
    for (; *lhs && *rhs && n &&
	   (*lhs == *rhs || std::tolower(*lhs) == std::tolower(*rhs));
	 lhs++, rhs++, n--);
    return std::tolower(*lhs) - std::tolower(*rhs);
  }
};

/// \brief Cast between character traits for a `string_view`
///
/// For example, to compare two normal string_views insensitively, use:
/// `traits_cast<string_view_ci>(lhs) == traits_cast<string_view_ci>(rhs)`
template <class ToView, class CharT, class SrcTraits>
auto traits_cast(const std::basic_string_view<CharT, SrcTraits> src) noexcept
  -> std::enable_if_t<
      std::is_same_v<ToView, std::basic_string_view<
			       CharT, typename ToView::traits_type>>,
      ToView>
{
  return {src.data(), src.size()};
}

/// \brief Cast between character traits for a `string`
///
/// \warning This is a *copying* operation. The semantics of ownership
///          don't let us move-construct the result.
///
/// To insert an existing string in a collection of case-insensitive
/// strings, we could, for example:
///
/// `ci_set.insert(trait_cast<string_ci>(sensitive_string);`
template <class ToStr, class CharT, class SrcTraits>
constexpr auto
traits_cast(const std::basic_string<CharT, SrcTraits>& src) noexcept
  -> std::enable_if_t<
      std::is_same_v<ToStr,
                     std::basic_string<CharT, typename ToStr::traits_type>>,
      ToStr>
{
  return {src.data(), src.size()};
}

/// Type of a case-insensitive `char` string view
using string_view_ci = std::basic_string_view<char, char_traits_ci>;
/// Type of a case-insensitive `char` string
using string_ci = std::basic_string<char, char_traits_ci>;

namespace literals {
/// Literal case insensitive string views. e.g. for constants to compare
/// against.
///
/// \example `inline constexpr auto http_content_length = "Content-Length"_sv_ci;
constexpr string_view_ci operator ""_sv_ci(const char* str,
					   std::size_t len) noexcept
{
  return {str, len};
}

/// Literal case insensitive strings. e.g. for constants to compare against.
///
/// \example `inline constexpr auto http_content_length = "Content-Length"_s_ci;
constexpr string_ci operator ""_s_ci(const char* str, std::size_t len) noexcept
{
  return {str, len};
}

}
} // namespace ceph
