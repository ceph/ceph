// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once
/**
 * \file fmtlib formatter for utime_t
 */
#include <fmt/chrono.h>
#include <fmt/format.h>

#include "include/utime.h"

template <>
struct fmt::formatter<utime_t> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx)
  {
    auto it = ctx.begin();
    if (it != ctx.end() && *it == 's') {
      short_format = true;
      ++it;
    }
    return it;
  }

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
    // (unless short_format is set)
    auto aslocal = fmt::localtime(utime.sec());
    if (short_format) {
      return fmt::format_to(ctx.out(), "{:%FT%T}.{:03}", aslocal,
			    utime.usec() / 1000);
    }
    return fmt::format_to(ctx.out(), "{:%FT%T}.{:06}{:%z}", aslocal,
			  utime.usec(), aslocal);
  }

  bool short_format{false};
};
