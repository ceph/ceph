// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

/**
 * \file fmtlib formatters for some msg_types.h classes
 */

#include <fmt/format.h>

#include "msg/msg_types.h"

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
