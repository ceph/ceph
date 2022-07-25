// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once
/**
 * \file fmtlib formatters for some types.h classes
 */

#include <fmt/format.h>

#include <string_view>

#include "include/types.h"

template <class Key, class T>
struct fmt::formatter<std::pair<const Key, T>> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const std::pair<const Key, T>& p, FormatContext& ctx)
  {
    return fmt::format_to(ctx.out(), "{}={}", p.first, p.second);
  }
};

template <class A, class B, class Comp, class Alloc>
struct fmt::formatter<std::map<A, B, Comp, Alloc>> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const std::map<A, B, Comp, Alloc>& m, FormatContext& ctx)
  {
    return fmt::format_to(ctx.out(), "{{{}}}", fmt::join(m, ","));
  }
};

template <class... Args>
struct fmt::formatter<std::list<Args...>> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const std::list<Args...>& l, FormatContext& ctx)
  {
    return fmt::format_to(ctx.out(), "{}", fmt::join(l, ","));
  }
};

template <class... Args>
struct fmt::formatter<std::vector<Args...>> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const std::vector<Args...>& v, FormatContext& ctx)
  {
    return fmt::format_to(ctx.out(), "[{}]", fmt::join(v, ","));
  }
};

template <class... Args>
struct fmt::formatter<std::set<Args...>> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const std::set<Args...>& s, FormatContext& ctx)
  {
    return fmt::format_to(ctx.out(), "{}", fmt::join(s, ","));
  }
};
