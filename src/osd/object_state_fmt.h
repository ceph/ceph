// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
#pragma once
/**
 * \file fmtlib formatters for some types.h classes
 */

#include "osd/object_state.h"
#include "osd/osd_types_fmt.h"
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

template <>
struct fmt::formatter<ObjectState> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const ObjectState& os, FormatContext& ctx) const
  {
    return fmt::format_to(ctx.out(), "exists {} oi {}", os.exists, os.oi);
  }
};
