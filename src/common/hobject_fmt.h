// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#pragma once

#include "hobject.h"

#include <fmt/compile.h>
#include <fmt/format.h>

#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

namespace fmt {
template <>
struct formatter<hobject_t> {

  template <typename FormatContext>
  static inline auto
  append_sanitized(FormatContext& ctx, const std::string& in, int sep = 0)
  {
    for (const auto i : in) {
      if (i == '%' || i == ':' || i == '/' || i < 32 || i >= 127) {
	fmt::format_to(
	    ctx.out(), FMT_COMPILE("%{:02x}"), static_cast<unsigned char>(i));
      } else {
	fmt::format_to(ctx.out(), FMT_COMPILE("{:c}"), i);
      }
    }
    if (sep) {
      fmt::format_to(
	  ctx.out(), FMT_COMPILE("{:c}"), sep);
    }
    return ctx.out();
  }

  constexpr auto parse(format_parse_context& ctx) const { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const hobject_t& ho, FormatContext& ctx) const
  {
    if (ho == hobject_t{}) {
      return fmt::format_to(ctx.out(), "MIN");
    }

    if (ho.is_max()) {
      return fmt::format_to(ctx.out(), "MAX");
    }

    fmt::format_to(
	ctx.out(), FMT_COMPILE("{}:{:08x}:"), static_cast<uint64_t>(ho.pool),
	ho.get_bitwise_key_u32());
    append_sanitized(ctx, ho.nspace, ':');
    append_sanitized(ctx, ho.get_key(), ':');
    append_sanitized(ctx, ho.oid.name);
    return fmt::format_to(ctx.out(), FMT_COMPILE(":{}"), ho.snap);
  }
};
}  // namespace fmt

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<ghobject_t> : fmt::ostream_formatter {};
#endif
