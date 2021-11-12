// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once
/**
 * \file fmtlib formatter for utime_t
 */
#include <fmt/format.h>
#include <fmt/chrono.h>

#include <string_view>

#include "include/utime.h"

template <>
struct fmt::formatter<utime_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const utime_t& utime, FormatContext& ctx)
  {
    if (utime.sec() < ((time_t)(60 * 60 * 24 * 365 * 10))) {
      // raw seconds.  this looks like a relative time.
      return fmt::format_to(ctx.out(), "{}.{:06}", (long)utime.sec(),
                            utime.usec());
    }

    // this looks like an absolute time.
    // conform to http://en.wikipedia.org/wiki/ISO_8601
    auto asgmt = fmt::gmtime(utime.sec());
    return fmt::format_to(ctx.out(), "{:%FT%T}.{:06}{:%z}", asgmt, utime.usec(), asgmt);
  }
};
