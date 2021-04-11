// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once
/**
 * \file fmtlib formatters for some types.h classes
 */

#include <fmt/format.h>

#include <string_view>

#include "include/types.h"

template <class A, class B, class Comp, class Alloc>
struct fmt::formatter<std::map<A, B, Comp, Alloc>> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const std::map<A, B, Comp, Alloc>& m, FormatContext& ctx)
  {
    std::string_view sep = "{";
    for (const auto& [k, v] : m) {
      fmt::format_to(ctx.out(), "{}{}={}", sep, k, v);
      sep = ",";
    }
    return fmt::format_to(ctx.out(), "}}");
  }
};

template <class A>
struct fmt::formatter<std::list<A>> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const std::list<A>& l, FormatContext& ctx)
  {
    std::string_view sep = "";
    for (const auto& e : l) {
      fmt::format_to(ctx.out(), "{}{}", sep, e);
      sep = ",";
    }
    return ctx.out();
  }
};

template <class A>
struct fmt::formatter<std::set<A>> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const std::set<A>& l, FormatContext& ctx)
  {
    std::string_view sep = "";
    for (const auto& e : l) {
      fmt::format_to(ctx.out(), "{}{}", sep, e);
      sep = ",";
    }
    return ctx.out();
  }
};
