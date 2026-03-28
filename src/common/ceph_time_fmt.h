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
 * Foundation.	See file COPYING.
 *
 */

#pragma once

#include "ceph_time.h"

#include <fmt/chrono.h>

// concept helpers for the formatters:

template <typename TimeP>
concept SteadyTimepoint = TimeP::clock::is_steady;

template <typename TimeP>
concept UnsteadyTimepoint = ! TimeP::clock::is_steady;

namespace fmt {
template <UnsteadyTimepoint T>
struct formatter<T> {
  constexpr auto parse(fmt::format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const T& t, FormatContext& ctx) const
  {
    struct tm bdt;
    time_t tt = T::clock::to_time_t(t);
    localtime_r(&tt, &bdt);
    char tz[32] = {0};
    strftime(tz, sizeof(tz), "%z", &bdt);

    return fmt::format_to(
	ctx.out(), "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}:{:06}{}",
	(bdt.tm_year + 1900), (bdt.tm_mon + 1), bdt.tm_mday, bdt.tm_hour,
	bdt.tm_min, bdt.tm_sec,
	duration_cast<std::chrono::microseconds>(
	    t.time_since_epoch() % std::chrono::seconds(1))
	    .count(),
	tz);
  }
};

template <SteadyTimepoint T>
struct formatter<T> {
  constexpr auto parse(fmt::format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const T& t, FormatContext& ctx) const
  {
    return fmt::format_to(
	ctx.out(), "{}s",
	std::chrono::duration<double>(t.time_since_epoch()).count());
  }
};
}  // namespace fmt
