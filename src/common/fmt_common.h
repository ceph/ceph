// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

/**
 * \file default fmtlib formatters for specifically-tagged types
 */
#include <fmt/format.h>

/**
 * an implementation note:
 * not including fmt/ranges.h here because it grabs every structure that
 * has a begin()/end() method pair. This is a problem because we have
 * such classes in Crimson.
 */
/**
 * Tagging classes that provide support for default fmtlib formatting,
 * by having either
 * std::string fmt_print() const
 * *or*
 * std::string alt_fmt_print(bool short_format) const
 * as public member functions.
 */
template<class T>
concept has_fmt_print = requires(T t) {
  { t.fmt_print() } -> std::same_as<std::string>;
};
template<class T>
concept has_alt_fmt_print = requires(T t) {
  { t.alt_fmt_print(bool{}) } -> std::same_as<std::string>;
};

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
}  // namespace fmt
