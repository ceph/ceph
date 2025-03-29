// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <fmt/base.h>
#include <optional>
#include <type_traits>

/**
 * \file default fmtlib formatters for specifically-tagged types
 */
#include <fmt/compile.h>
#include <fmt/format.h>

/**
 * an implementation note:
 * not including fmt/ranges.h here because it grabs every structure that
 * has a begin()/end() method pair. This is a problem because we have
 * such classes in Crimson.
 */

#if FMT_VERSION < 110000
// TODO: drop me once fmt v11 is required
namespace fmt {
  template <typename T, typename Char = char>
  concept formattable = is_formattable<std::remove_reference_t<T>, Char>::value>;
}
#endif

/**
 * Tagging classes that provide support for default fmtlib formatting,
 * by having either
 * std::string fmt_print() const
 * *or*
 * std::string alt_fmt_print(bool short_format) const
 * as public member functions.
 * *or*
 * auto fmt_print_ctx(auto &ctx) -> decltype(ctx.out());
 */
template<class T>
concept has_fmt_print = requires(T t) {
  { t.fmt_print() } -> std::same_as<std::string>;
};
template<class T>
concept has_alt_fmt_print = requires(T t) {
  { t.alt_fmt_print(bool{}) } -> std::same_as<std::string>;
};
#if FMT_VERSION >= 110000
template<class T>
concept has_fmt_print_ctx = requires(
  T t, fmt::buffered_context<char> &ctx) {
  { t.fmt_print_ctx(ctx) } -> std::same_as<decltype(ctx.out())>;
};
#else
template<class T>
concept has_fmt_print_ctx = requires(
  T t, fmt::buffer_context<char> &ctx) {
  { t.fmt_print_ctx(ctx) } -> std::same_as<decltype(ctx.out())>;
};
#endif

namespace fmt {

template <has_fmt_print T>
struct formatter<T> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const T& k, FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "{}", k.fmt_print());
  }
};

template <has_alt_fmt_print T>
struct formatter<T> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    auto it = ctx.begin();
    if (it != ctx.end() && *it == 's') {
      verbose = false;
      ++it;
    }
    return it;
  }
  template <typename FormatContext>
  auto format(const T& k, FormatContext& ctx) const {
    if (verbose) {
      return fmt::format_to(ctx.out(), "{}", k.alt_fmt_print(true));
    }
    return fmt::format_to(ctx.out(), "{}", k.alt_fmt_print(false));
  }
  bool verbose{true};
};

template <has_fmt_print_ctx T>
struct formatter<T> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const T& k, FormatContext& ctx) const {
    return k.fmt_print_ctx(ctx);
  }
};

template <typename T>
struct formatter<std::optional<T>> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const std::optional<T> &v, FormatContext& ctx) const {
    if (v.has_value()) {
      return fmt::format_to(ctx.out(), "{}", *v);
    }
    return fmt::format_to(ctx.out(), "<null>");
  }
};

}  // namespace fmt
