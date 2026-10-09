// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

/**
 * \file fmtlib formatters for some msg_types.h classes
 */

#include <fmt/format.h>
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

#include "common/fmt_common.h"
#include "msg/msg_types.h"

template <typename FormatContext>
auto entity_name_t::fmt_print_ctx(FormatContext& ctx) const {
  if (is_new() || _num < 0) {
    return fmt::format_to(ctx.out(), "{}.?", type_str());
  } else {
    return fmt::format_to(ctx.out(), "{}.{}",type_str(), _num);
  }
}

template <>
struct fmt::formatter<entity_name_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const entity_name_t& addr, FormatContext& ctx) const
  {
    if (addr.is_new() || addr.num() < 0) {
      return fmt::format_to(ctx.out(), "{}.?", addr.type_str());
    }
    return fmt::format_to(ctx.out(), "{}.{}", addr.type_str(), addr.num());
  }
};

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<entity_addrvec_t> : fmt::ostream_formatter {};
#endif
